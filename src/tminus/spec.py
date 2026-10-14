"""JobSpec: a description of a job to submit, independent of any Backend."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from .errors import InvalidSpecError


@dataclass(frozen=True)
class JobSpec:
    """What to run and what resources to ask Slurm for.

    Exactly one of ``command`` (a shell command, submitted with ``--wrap``) or
    ``script`` (the path of a batch script) must be given. Every other field is
    optional and is left to the cluster's defaults when unset. ``extra_args``
    passes additional ``sbatch`` arguments through untouched.
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

    def __post_init__(self) -> None:
        if (self.command is None) == (self.script is None):
            raise InvalidSpecError("a JobSpec needs exactly one of command or script")
