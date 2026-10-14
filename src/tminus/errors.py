"""Exceptions raised by tminus. Every one a caller might handle subclasses TminusError."""
from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Optional

if TYPE_CHECKING:
    from .state import JobStatus


class TminusError(Exception):
    """Base class for all tminus errors."""


class InvalidSpecError(TminusError):
    """A JobSpec is incomplete or contradictory, detected before anything reaches Slurm."""


class SubmitError(TminusError):
    """A submission did not produce a usable job id."""


class SlurmOutputError(TminusError):
    """A Slurm command succeeded but printed output tminus cannot read."""


class JobNotFound(TminusError):
    """The scheduler has no record of a job, in its queue or its accounting."""

    def __init__(self, job_id: str) -> None:
        self.job_id = job_id
        super().__init__(f"no record of job {job_id} in squeue or sacct")


class SlurmCommandError(TminusError):
    """A Slurm command failed to run or exited with an error.

    ``command`` is the full command line, ``returncode`` its exit status (None if
    it never ran or timed out), and ``stderr`` whatever it wrote to stderr.
    """

    def __init__(
        self,
        command: List[str],
        returncode: Optional[int],
        stderr: str,
        message: Optional[str] = None,
    ) -> None:
        self.command = list(command)
        self.returncode = returncode
        self.stderr = stderr
        if message is None:
            detail = stderr.strip() or "no output on stderr"
            message = f"{command[0]} exited with status {returncode}: {detail}"
        super().__init__(message)


class CommandTimeoutError(SlurmCommandError):
    """A Slurm command was still running when its timeout expired."""


class WaitTimeout(TminusError):
    """Jobs were still unfinished when a wait's timeout expired.

    ``statuses`` holds the latest status seen for every job waited on. The jobs
    themselves are untouched and keep running.
    """

    def __init__(self, statuses: Dict[str, JobStatus], timeout: Optional[float]) -> None:
        self.statuses = statuses
        unfinished = sorted(job_id for job_id, status in statuses.items() if not status.state.is_terminal)
        super().__init__(f"{len(unfinished)} job(s) still unfinished after {timeout} seconds: {', '.join(unfinished)}")


class InvalidUpdateError(TminusError):
    """A job update named a field that cannot be changed after submission."""


class UnsatisfiableDependency(TminusError):
    """Waited-on jobs can never start because their dependencies can no longer be met.

    Slurm leaves such jobs pending indefinitely; they are not cancelled.
    """

    def __init__(self, job_ids: List[str]) -> None:
        self.job_ids = list(job_ids)
        super().__init__(
            f"job(s) {', '.join(job_ids)} can never start: their dependencies can no longer be "
            "satisfied (cancel them, or fix the dependency and release them)"
        )
