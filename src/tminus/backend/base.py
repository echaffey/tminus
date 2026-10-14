"""The Backend interface every scheduler implementation provides."""
from __future__ import annotations

import abc
from typing import Any, Dict, Sequence

from ..spec import JobSpec
from ..state import JobStatus


# JobSpec-style names of the fields that can be changed after submission.
UPDATABLE_FIELDS = ("time_limit", "partition", "name", "qos", "account", "nodes")


class Backend(abc.ABC):
    """Submits and controls jobs on behalf of the rest of tminus.

    ``name`` identifies the scheduler a job id belongs to, so the Store never
    confuses ids from different backends.
    """

    name = "abstract"

    @abc.abstractmethod
    def submit(self, spec: JobSpec) -> str:
        """Submit ``spec`` and return the new job's id."""

    @abc.abstractmethod
    def status(self, job_ids: Sequence[str]) -> Dict[str, JobStatus]:
        """Current status of each job in ``job_ids``, in as few queries as possible.

        Jobs the scheduler has no record of are left out of the result.
        """

    @abc.abstractmethod
    def cancel(self, job_ids: Sequence[str]) -> None:
        """Cancel every job in ``job_ids``. Jobs that have already finished are not an error."""

    @abc.abstractmethod
    def hold(self, job_ids: Sequence[str]) -> None:
        """Stop pending jobs from starting until released."""

    @abc.abstractmethod
    def release(self, job_ids: Sequence[str]) -> None:
        """Let held jobs start again."""

    @abc.abstractmethod
    def requeue(self, job_ids: Sequence[str]) -> None:
        """Put jobs back in the queue to run again."""

    @abc.abstractmethod
    def update(self, job_id: str, changes: Dict[str, Any]) -> None:
        """Change fields of a submitted job. ``changes`` uses names from UPDATABLE_FIELDS."""
