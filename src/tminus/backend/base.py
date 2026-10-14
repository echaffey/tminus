"""The Backend interface every scheduler implementation provides."""
from __future__ import annotations

import abc
from typing import Dict, Sequence

from ..spec import JobSpec
from ..state import JobStatus


class Backend(abc.ABC):
    """Submits and controls jobs on behalf of the rest of tminus."""

    @abc.abstractmethod
    def submit(self, spec: JobSpec) -> str:
        """Submit ``spec`` and return the new job's id."""

    @abc.abstractmethod
    def status(self, job_ids: Sequence[str]) -> Dict[str, JobStatus]:
        """Current status of each job in ``job_ids``, in as few queries as possible.

        Jobs the scheduler has no record of are left out of the result.
        """
