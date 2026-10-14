"""Job: a handle on one submitted job."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from .errors import JobNotFound
from .monitor import wait_all
from .spec import JobSpec
from .state import JobStatus

if TYPE_CHECKING:
    from .backend.base import Backend


@dataclass
class Job:
    """A job that has been submitted, identified by its Slurm job id.

    Job ids are strings because array tasks and heterogeneous jobs use ids
    such as ``"1234_7"`` and ``"1234+0"``. ``spec`` is None for a job tminus
    did not submit itself. ``status`` holds the last JobStatus seen by
    ``refresh()``, or None before the first one.
    """

    job_id: str
    spec: Optional[JobSpec]
    backend: Backend
    status: Optional[JobStatus] = field(default=None, compare=False)

    def refresh(self) -> JobStatus:
        """Ask the scheduler for this job's current status, remember it, and return it.

        Raises JobNotFound if the scheduler has no record of the job.
        """
        found = self.backend.status([self.job_id])
        if self.job_id not in found:
            raise JobNotFound(self.job_id)
        self.status = found[self.job_id]
        return self.status

    def wait(self, timeout: Optional[float] = None, **options: Any) -> JobStatus:
        """Block until this job is terminal and return its final status.

        Takes the same options as ``tminus.wait_all``; raises WaitTimeout if
        ``timeout`` seconds pass first.
        """
        return wait_all([self], timeout=timeout, **options)[self.job_id]
