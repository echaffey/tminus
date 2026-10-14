"""JobSpec: a description of a job to submit, independent of any Backend."""
from __future__ import annotations

import dataclasses
import enum
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from .errors import InvalidSpecError

# Tag keys and values are single words so they survive the trip through Slurm's --comment.
_TAG_WORD = re.compile(r"^[A-Za-z0-9_.:/@+-]+$")
_COMMENT_PREFIX = "tminus:"
# An sbatch --array value: comma-separated N, N-M, or N-M:STEP items, optionally ending in %LIMIT.
_ARRAY_ITEM = r"\d+(?:-\d+(?::[1-9]\d*)?)?"
_ARRAY = re.compile(rf"^{_ARRAY_ITEM}(?:,{_ARRAY_ITEM})*(?:%[1-9]\d*)?$")


def expand_array(text: str) -> List[str]:
    """Every task id in an array expression such as ``0-9:2,15%4`` (brackets and limit ignored).

    Raises ValueError for anything that is not an array expression.
    """
    body = text.strip().strip("[]").split("%", 1)[0]
    task_ids: List[str] = []
    for item in body.split(","):
        first, sep, rest = item.partition("-")
        if not sep:
            task_ids.append(str(int(first)))
            continue
        last, _, step = rest.partition(":")
        start, stop, stride = int(first), int(last), int(step or 1)
        if stop < start or stride < 1:
            raise ValueError(f"bad array range {item!r}")
        task_ids.extend(str(n) for n in range(start, stop + 1, stride))
    return task_ids


def tags_to_comment(tags: Dict[str, str]) -> str:
    """Encode tags as a Slurm job comment, e.g. ``tminus:exp=exp42,run=1``."""
    return _COMMENT_PREFIX + ",".join(f"{key}={value}" for key, value in sorted(tags.items()))


def tags_from_comment(comment: str) -> Dict[str, str]:
    """Decode tags from a job comment; any comment tminus did not write yields no tags."""
    if not comment.startswith(_COMMENT_PREFIX):
        return {}
    tags = {}
    for pair in comment[len(_COMMENT_PREFIX):].split(","):
        key, sep, value = pair.partition("=")
        if sep and _TAG_WORD.match(key) and _TAG_WORD.match(value):
            tags[key] = value
    return tags


class DependencyType(enum.Enum):
    """When a dependent job may start, relative to the jobs it depends on."""

    AFTER = "after"            # once every dependency has started (or been cancelled)
    AFTEROK = "afterok"        # once every dependency has completed successfully
    AFTERANY = "afterany"      # once every dependency has finished, however it ended
    AFTERNOTOK = "afternotok"  # once every dependency has finished unsuccessfully


@dataclass(frozen=True)
class JobSpec:
    """What to run and what resources to ask Slurm for.

    Exactly one of ``command`` (a shell command, submitted with ``--wrap``) or
    ``script`` (the path of a batch script) must be given. Every other field is
    optional and is left to the cluster's defaults when unset. ``extra_args``
    passes additional ``sbatch`` arguments through untouched. ``tags`` are
    key/value labels used to find and group jobs later; keys and values are
    single words (letters, digits, and ``_ . : / @ + -``). ``depends_on``
    lists jobs (Job handles or job ids) that must reach the condition in
    ``dependency_type`` (default: afterok) before this one may start.
    ``array`` makes this an array job (sbatch ``--array``, e.g. ``0-9%4``).
    ``scratch_paths`` are files or directories the job owns (per-job scratch
    space, for example) that ``tminus.cleanup.purge`` may delete along with
    its logs; they accept the same ``%j``/``%x``/``%u`` patterns as ``output``.
    """

    command: Optional[str] = None
    script: Optional[str] = None
    name: Optional[str] = None
    partition: Optional[str] = None
    account: Optional[str] = None
    qos: Optional[str] = None
    nodes: Optional[int] = None
    ntasks: Optional[int] = None
    cpus: Optional[int] = None
    mem: Optional[str] = None
    time: Optional[str] = None
    workdir: Optional[str] = None
    output: Optional[str] = None
    error: Optional[str] = None
    extra_args: List[str] = field(default_factory=list)
    tags: Dict[str, str] = field(default_factory=dict)
    depends_on: List[Any] = field(default_factory=list)
    dependency_type: DependencyType = DependencyType.AFTEROK
    scratch_paths: List[str] = field(default_factory=list)
    array: Optional[str] = None

    def __post_init__(self) -> None:
        # Normalise Job handles to ids and strings to DependencyType (the dataclass is frozen).
        ids = [str(getattr(dependency, "job_id", dependency)) for dependency in self.depends_on]
        object.__setattr__(self, "depends_on", ids)
        if not isinstance(self.dependency_type, DependencyType):
            try:
                object.__setattr__(self, "dependency_type", DependencyType(self.dependency_type))
            except ValueError:
                known = ", ".join(kind.value for kind in DependencyType)
                raise InvalidSpecError(
                    f"unknown dependency type {self.dependency_type!r} (expected one of: {known})"
                ) from None
        if (self.command is None) == (self.script is None):
            raise InvalidSpecError("a JobSpec needs exactly one of command or script")
        if self.array is not None:
            try:
                valid = bool(_ARRAY.match(self.array)) and bool(expand_array(self.array))
            except ValueError:
                valid = False
            if not valid:
                raise InvalidSpecError(
                    f"invalid array {self.array!r}: expected items like 0-9, 1,3,5, or 0-15:4, "
                    "optionally ending in %LIMIT"
                )
        for key, value in self.tags.items():
            if not (_TAG_WORD.match(key) and _TAG_WORD.match(value)):
                raise InvalidSpecError(
                    f"tag {key}={value} is not allowed: keys and values must be single words "
                    "of letters, digits, and _ . : / @ + -"
                )

    def task_ids(self) -> List[str]:
        """The array task ids this spec asks for, in order (empty if it is not an array job)."""
        return expand_array(self.array) if self.array is not None else []

    def to_dict(self) -> Dict[str, Any]:
        """A JSON-ready copy of every field."""
        data = dataclasses.asdict(self)
        data["dependency_type"] = self.dependency_type.value
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "JobSpec":
        """Rebuild a JobSpec from ``to_dict`` output, ignoring fields this version does not know."""
        known = {f.name for f in dataclasses.fields(cls)}
        return cls(**{key: value for key, value in data.items() if key in known})
