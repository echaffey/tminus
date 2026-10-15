"""JobSpec: a description of a job to submit, independent of any Backend."""
from __future__ import annotations

import dataclasses
import enum
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Union

from .errors import InvalidSpecError

# Tags are single words so they survive the trip through Slurm's --comment.
_TAG_WORD = re.compile(r"^[A-Za-z0-9_.:/@+=-]+$")
TAG_RULE = "a single word of letters, digits, and _ . : / @ + - ="
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


# A tag, a list of tags, or none: what every ``tags=`` argument accepts.
Tags = Union[None, str, Iterable[str]]


def normalize_tags(tags: Tags) -> List[str]:
    """``tags`` as a list of names in their original order, without repeats.

    A single string is one tag. Raises InvalidSpecError for a name that is not
    a single word, and for a dict (tags are names, not key/value pairs).
    """
    if tags is None:
        return []
    if isinstance(tags, str):
        tags = [tags]
    elif isinstance(tags, dict):
        raise InvalidSpecError(f"tags are plain names, not key/value pairs: give a list such as {list(tags)!r}")
    names: List[str] = []
    for tag in tags:
        if not (isinstance(tag, str) and _TAG_WORD.match(tag)):
            raise InvalidSpecError(f"tag {tag!r} is not allowed: a tag must be {TAG_RULE}")
        if tag not in names:
            names.append(tag)
    return names


def is_tag(text: str) -> bool:
    """True if ``text`` can be used as a tag."""
    return bool(_TAG_WORD.match(text))


def tags_to_comment(tags: List[str]) -> str:
    """Encode tags as a Slurm job comment, e.g. ``tminus:sweep3,train``."""
    return _COMMENT_PREFIX + ",".join(tags)


def tags_from_comment(comment: str) -> List[str]:
    """Decode tags from a job comment; any comment tminus did not write yields no tags.

    Comments written by tminus 0.1.0 (``tminus:exp=exp42``) decode to names such as ``exp=exp42``.
    """
    if not comment.startswith(_COMMENT_PREFIX):
        return []
    return normalize_tags(name for name in comment[len(_COMMENT_PREFIX):].split(",") if _TAG_WORD.match(name))


def legacy_tags(tags: Any) -> Any:
    """Tags as tminus 0.1.0 saved them (a key/value dict) turned into ``key=value`` names."""
    if isinstance(tags, dict):
        return [f"{key}={value}" for key, value in sorted(tags.items())]
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
    names used to find and group jobs later (a list, or one string for a
    single tag); each is a single word of letters, digits, and
    ``_ . : / @ + - =``. ``depends_on``
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
    tags: List[str] = field(default_factory=list)
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
        object.__setattr__(self, "tags", normalize_tags(self.tags))

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
        fields = {key: value for key, value in data.items() if key in known}
        if "tags" in fields:
            fields["tags"] = legacy_tags(fields["tags"])
        return cls(**fields)
