"""Job: a handle on one submitted job."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Optional

from .backend.base import UPDATABLE_FIELDS, Backend
from .errors import InvalidUpdateError, JobNotFound
from .monitor import wait_all
from .spec import JobSpec
from .state import JobStatus
from .store import Store, default_store

@dataclass
class Job:
    """A job that has been submitted, identified by its Slurm job id.

    Job ids are strings because array tasks and heterogeneous jobs use ids
    such as ``"1234_7"`` and ``"1234+0"``. ``spec`` is None for a job tminus
    did not submit itself. ``status`` holds the last JobStatus seen by
    ``refresh()``, or None before the first one. ``submit_dir`` is the
    directory it was submitted from, when known; Slurm resolves relative log
    paths against it.
    """

    job_id: str
    spec: Optional[JobSpec]
    backend: Backend
    status: Optional[JobStatus] = field(default=None, compare=False)
    submit_dir: Optional[str] = field(default=None, compare=False)

    @property
    def tasks(self) -> List["Job"]:
        """A handle per task of an array job, in task order (empty for other jobs).

        Taken from the spec when tminus submitted the job, otherwise from the scheduler.
        """
        if self.spec is not None:
            task_ids = [f"{self.job_id}_{task}" for task in self.spec.task_ids()]
        else:
            task_ids = list((self.status or self.refresh()).tasks)
        return [Job(task_id, self.spec, self.backend, submit_dir=self.submit_dir) for task_id in task_ids]

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

    def cancel(self, store: Optional[Store] = None) -> bool:
        """Cancel the job if it is still active. Returns False, doing nothing, if it already finished."""
        if self.refresh().state.is_terminal:
            return False
        self.backend.cancel([self.job_id])
        self._record("cancel", store)
        return True

    def hold(self, store: Optional[Store] = None) -> None:
        """Keep a pending job from starting until ``release()``."""
        self.backend.hold([self.job_id])
        self._record("hold", store)

    def release(self, store: Optional[Store] = None) -> None:
        """Let a held job start again."""
        self.backend.release([self.job_id])
        self._record("release", store)

    def requeue(self, store: Optional[Store] = None) -> None:
        """Put the job back in the queue to run again."""
        self.backend.requeue([self.job_id])
        self._record("requeue", store)

    def update(self, store: Optional[Store] = None, **changes: Any) -> None:
        """Change fields of the job: any of time_limit, partition, name, qos, account, nodes.

        Raises InvalidUpdateError, before anything reaches the scheduler, for any other field.
        """
        unsupported = sorted(set(changes) - set(UPDATABLE_FIELDS))
        if unsupported:
            raise InvalidUpdateError(
                f"cannot update {', '.join(unsupported)}; updatable fields are {', '.join(UPDATABLE_FIELDS)}"
            )
        if not changes:
            return
        self.backend.update(self.job_id, changes)
        detail = ",".join(f"{name}={changes[name]}" for name in UPDATABLE_FIELDS if name in changes)
        self._record("update", store, detail)

    def _record(self, action: str, store: Optional[Store], detail: Optional[str] = None) -> None:
        if store is None:
            store = default_store()
        if store is not None:
            store.record_event(self.backend.name, self.job_id, action, detail)
