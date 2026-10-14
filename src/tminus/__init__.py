"""tminus: schedule, monitor, modify, clean up after, and report on Slurm jobs."""
from __future__ import annotations

from typing import Optional

from .backend import get_backend
from .backend.base import Backend
from .errors import (
    CommandTimeoutError,
    InvalidSpecError,
    SlurmCommandError,
    SubmitError,
    TminusError,
)
from .job import Job
from .spec import JobSpec

__version__ = "0.1.0"

__all__ = [
    "Backend",
    "CommandTimeoutError",
    "InvalidSpecError",
    "Job",
    "JobSpec",
    "SlurmCommandError",
    "SubmitError",
    "TminusError",
    "get_backend",
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
