"""tminus: schedule, monitor, modify, clean up after, and report on Slurm jobs."""
from __future__ import annotations

from typing import Dict, List, Optional, Sequence

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
    WaitTimeout,
)
from .job import Job
from . import aio
from .monitor import PollPolicy, wait_all
from .spec import JobSpec
from .state import JobState, JobStatus
from .store import Store

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
    "PollPolicy",
    "SlurmCommandError",
    "SlurmOutputError",
    "Store",
    "SubmitError",
    "TminusError",
    "WaitTimeout",
    "aio",
    "get_backend",
    "jobs",
    "status",
    "submit",
    "wait_all",
]


def submit(spec: JobSpec, backend: Optional[Backend] = None, store: Optional[Store] = None) -> Job:
    """Submit ``spec``, record it in the Store, and return a handle on the new job.

    Without an explicit ``backend``, uses the one ``TMINUS_BACKEND`` selects
    (real Slurm by default); without an explicit ``store``, the default one
    (``TMINUS_DB``).
    """
    if backend is None:
        backend = get_backend()
    if store is None:
        store = Store()
    job_id = backend.submit(spec)
    store.record_submission(backend.name, job_id, spec)
    return Job(job_id=job_id, spec=spec, backend=backend)


def jobs(
    tags: Optional[Dict[str, str]] = None,
    backend: Optional[Backend] = None,
    store: Optional[Store] = None,
) -> List[Job]:
    """Every tracked job on ``backend`` that carries all of ``tags``, oldest first."""
    if backend is None:
        backend = get_backend()
    if store is None:
        store = Store()
    return [Job(job_id=job_id, spec=spec, backend=backend) for job_id, spec in store.find(backend.name, tags)]


def status(job_ids: Sequence[str], backend: Optional[Backend] = None) -> Dict[str, JobStatus]:
    """Current status of each job in ``job_ids``, fetched in as few queries as possible.

    Jobs the scheduler has no record of are left out of the result.
    """
    if backend is None:
        backend = get_backend()
    return backend.status(list(job_ids))
