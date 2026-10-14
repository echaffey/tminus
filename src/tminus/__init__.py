"""tminus: schedule, monitor, modify, clean up after, and report on Slurm jobs."""
from __future__ import annotations

import os
from typing import Dict, List, Optional, Sequence

from .backend import get_backend
from .backend.base import Backend
from .errors import (
    CommandTimeoutError,
    InvalidAgeError,
    InvalidSpecError,
    InvalidUpdateError,
    JobNotFound,
    SlurmCommandError,
    SlurmOutputError,
    SubmitError,
    TminusError,
    UnsatisfiableDependency,
    WaitTimeout,
)
from . import cleanup
from .cleanup import Policy, prune
from .job import Job
from . import aio
from .monitor import PollPolicy, wait_all
from .report import JobUsage, Report
from .spec import DependencyType, JobSpec
from .state import JobState, JobStatus
from .store import Store

__version__ = "0.1.0"

__all__ = [
    "Backend",
    "CommandTimeoutError",
    "DependencyType",
    "InvalidAgeError",
    "InvalidSpecError",
    "InvalidUpdateError",
    "Job",
    "JobSpec",
    "JobNotFound",
    "JobState",
    "JobStatus",
    "JobUsage",
    "Policy",
    "PollPolicy",
    "Report",
    "SlurmCommandError",
    "SlurmOutputError",
    "Store",
    "SubmitError",
    "TminusError",
    "UnsatisfiableDependency",
    "WaitTimeout",
    "aio",
    "cleanup",
    "get_backend",
    "jobs",
    "prune",
    "report",
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
    submit_dir = os.getcwd()
    job_id = backend.submit(spec)
    store.record_submission(backend.name, job_id, spec, submit_dir)
    return Job(job_id=job_id, spec=spec, backend=backend, submit_dir=submit_dir)


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
    return [
        Job(job_id=tracked.job_id, spec=tracked.spec, backend=backend, submit_dir=tracked.submit_dir)
        for tracked in store.find(backend.name, tags)
    ]


def status(job_ids: Sequence[str], backend: Optional[Backend] = None) -> Dict[str, JobStatus]:
    """Current status of each job in ``job_ids``, fetched in as few queries as possible.

    Jobs the scheduler has no record of are left out of the result.
    """
    if backend is None:
        backend = get_backend()
    return backend.status(list(job_ids))


def report(
    tags: Optional[Dict[str, str]] = None,
    jobs: Optional[Sequence[Job]] = None,
    backend: Optional[Backend] = None,
    store: Optional[Store] = None,
) -> Report:
    """A Report on ``jobs``, or else on every tracked job carrying all of ``tags``.

    Figures come from one accounting query. Array jobs are reported task by task.
    """
    if backend is None:
        backend = get_backend()
    if store is None:
        store = Store()
    if jobs is None:
        job_ids = [tracked.job_id for tracked in store.find(backend.name, tags)]
    else:
        job_ids = [job.job_id for job in jobs]
    return Report(backend.usage(job_ids))
