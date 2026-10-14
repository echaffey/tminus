"""JobState: where a job is in its life, independent of how Slurm spells it."""
from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


class JobState(enum.Enum):
    """A job's state. Values are Slurm's own state names."""

    PENDING = "PENDING"
    CONFIGURING = "CONFIGURING"
    RUNNING = "RUNNING"
    COMPLETING = "COMPLETING"
    SUSPENDED = "SUSPENDED"
    STOPPED = "STOPPED"
    REQUEUED = "REQUEUED"
    RESIZING = "RESIZING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    TIMEOUT = "TIMEOUT"
    OUT_OF_MEMORY = "OUT_OF_MEMORY"
    NODE_FAIL = "NODE_FAIL"
    BOOT_FAIL = "BOOT_FAIL"
    DEADLINE = "DEADLINE"
    PREEMPTED = "PREEMPTED"
    REVOKED = "REVOKED"
    SPECIAL_EXIT = "SPECIAL_EXIT"
    UNKNOWN = "UNKNOWN"

    @property
    def is_terminal(self) -> bool:
        """True once the job has finished and will not change state on its own."""
        return self in _TERMINAL

    @property
    def is_success(self) -> bool:
        """True only for a job that ran to completion with exit status 0."""
        return self is JobState.COMPLETED

    @classmethod
    def from_slurm(cls, raw: Any) -> "JobState":
        """Map a Slurm state to a JobState.

        Accepts the plain string older Slurm prints (``"RUNNING"``, or
        ``"CANCELLED by 1234"`` from sacct) and the list newer Slurm prints,
        where the base state is mixed with flags (``["PENDING", "REQUEUED"]``).
        Anything unrecognised maps to UNKNOWN rather than raising, so a new
        Slurm release cannot crash a monitoring loop.
        """
        if isinstance(raw, str):
            names = [raw]
        elif isinstance(raw, (list, tuple)):
            names = [name for name in raw if isinstance(name, str)]
        else:
            return cls.UNKNOWN
        states = [_BY_NAME.get(name.split(" ", 1)[0].upper(), cls.UNKNOWN) for name in names]
        for state in states:
            if state in _BASE:
                return state
        for state in states:
            if state is not cls.UNKNOWN:
                return state
        return cls.UNKNOWN


_BY_NAME = {state.value: state for state in JobState}

# Slurm's base states. In the list form every other entry is a flag on one of these.
_BASE = frozenset({
    JobState.PENDING, JobState.RUNNING, JobState.SUSPENDED, JobState.COMPLETED,
    JobState.CANCELLED, JobState.FAILED, JobState.TIMEOUT, JobState.NODE_FAIL,
    JobState.PREEMPTED, JobState.BOOT_FAIL, JobState.DEADLINE, JobState.OUT_OF_MEMORY,
})

_TERMINAL = frozenset({
    JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED, JobState.TIMEOUT,
    JobState.OUT_OF_MEMORY, JobState.NODE_FAIL, JobState.BOOT_FAIL, JobState.DEADLINE,
    JobState.PREEMPTED, JobState.REVOKED, JobState.SPECIAL_EXIT,
})


@dataclass(frozen=True)
class JobStatus:
    """A snapshot of one job as the scheduler reported it.

    ``reason`` explains why a job is pending or how it ended (None when Slurm
    gives none). ``exit_code`` is the batch script's exit status, known only
    once the job is terminal. ``tags`` are those tminus wrote into the job's
    Slurm comment at submission (empty for jobs tminus did not tag).
    """

    job_id: str
    state: JobState
    reason: Optional[str] = None
    exit_code: Optional[int] = None
    tags: Dict[str, str] = field(default_factory=dict)

    @property
    def dependency_never_satisfied(self) -> bool:
        """True for a pending job whose dependencies can no longer be met, so it will never start."""
        return not self.state.is_terminal and self.reason == "DependencyNeverSatisfied"
