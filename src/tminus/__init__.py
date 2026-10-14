"""tminus: schedule, monitor, modify, clean up after, and report on Slurm jobs."""
from __future__ import annotations

from typing import Dict, Optional, Sequence

from .backend import get_backend
from .backend.base import Backend
from .errors import (
    CommandTimeoutError,
    InvalidSpecError,
    JobNotFound,
    SlurmCommandError,
    SlurmOutputError,
    SubmitError,
    TminusError,
)
from .job import Job
from .spec import JobSpec
from .state import JobState, JobStatus

__version__ = "0.1.0"

__all__ = [
    "Backend",
    "CommandTimeoutError",
    "InvalidSpecError",
    "Job",
    "JobSpec",
    "JobNotFound",
    "JobState",
    "JobStatus",
    "SlurmCommandError",
    "SlurmOutputError",
    "SubmitError",
    "TminusError",
    "get_backend",
    "status",
    "submit",
]


def submit(spec: JobSpec, backend: Optional[Backend] = None) -> Job:
    """Submit ``spec`` and return a handle on the new job.

    Without an explicit ``backend``, uses the one ``TMINUS_BACKEND`` selects
    (real Slurm by default).
    """
    if backend is None:
        backend = get_backend()
    return Job(job_id=backend.submit(spec), spec=spec, backend=backend)


def status(job_ids: Sequence[str], backend: Optional[Backend] = None) -> Dict[str, JobStatus]:
    """Current status of each job in ``job_ids``, fetched in as few queries as possible.

    Jobs the scheduler has no record of are left out of the result.
    """
    if backend is None:
        backend = get_backend()
    return backend.status(list(job_ids))
