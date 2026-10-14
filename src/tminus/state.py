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
    Slurm comment at submission (empty for jobs tminus did not tag). For an
    array job, ``tasks`` maps each task id (``"1234_7"``) to its own status and
    the other fields summarise them (see ``for_array``).
    """

    job_id: str
    state: JobState
    reason: Optional[str] = None
    exit_code: Optional[int] = None
    tags: Dict[str, str] = field(default_factory=dict)
    tasks: Dict[str, "JobStatus"] = field(default_factory=dict)

    @property
    def dependency_never_satisfied(self) -> bool:
        """True for a pending job whose dependencies can no longer be met, so it will never start."""
        return not self.state.is_terminal and self.reason == "DependencyNeverSatisfied"

    def task_counts(self) -> Dict[JobState, int]:
        """How many array tasks are in each state (empty for a job that is not an array)."""
        counts: Dict[JobState, int] = {}
        for task in self.tasks.values():
            counts[task.state] = counts.get(task.state, 0) + 1
        return counts

    @classmethod
    def for_array(cls, job_id: str, tasks: Dict[str, "JobStatus"]) -> "JobStatus":
        """Summarise an array job's tasks as one status.

        While any task is unfinished the array is RUNNING if any task runs,
        else SUSPENDED if any is suspended, else PENDING (with the first
        pending task's reason). Once every task has finished it is COMPLETED
        if they all completed; otherwise it takes the state its unsuccessful
        tasks share (TIMEOUT, CANCELLED, ...), or FAILED if they differ. The
        exit code is the highest any finished task reported.
        """
        ordered = dict(sorted(tasks.items(), key=lambda item: _task_number(item[0])))
        statuses = list(ordered.values())
        tags = statuses[0].tags if statuses else {}
        if not statuses:
            return cls(job_id, JobState.UNKNOWN, tags=tags, tasks=ordered)
        unfinished = [s for s in statuses if not s.state.is_terminal]
        if unfinished:
            if any(s.state in _RUNNING_LIKE for s in unfinished):
                return cls(job_id, JobState.RUNNING, tags=tags, tasks=ordered)
            if any(s.state in (JobState.SUSPENDED, JobState.STOPPED) for s in unfinished):
                return cls(job_id, JobState.SUSPENDED, tags=tags, tasks=ordered)
            reason = next((s.reason for s in unfinished if s.reason), None)
            return cls(job_id, JobState.PENDING, reason=reason, tags=tags, tasks=ordered)
        exit_codes = [s.exit_code for s in statuses if s.exit_code is not None]
        exit_code = max(exit_codes) if exit_codes else None
        failures = {s.state for s in statuses if not s.state.is_success}
        if not failures:
            state = JobState.COMPLETED
        elif len(failures) == 1:
            state = failures.pop()
        else:
            state = JobState.FAILED
        return cls(job_id, state, exit_code=exit_code, tags=tags, tasks=ordered)


_RUNNING_LIKE = frozenset({JobState.RUNNING, JobState.COMPLETING, JobState.CONFIGURING, JobState.RESIZING})


def _task_number(task_id: str) -> int:
    tail = task_id.rsplit("_", 1)[-1]
    return int(tail) if tail.isdigit() else -1
