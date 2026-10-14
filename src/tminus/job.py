"""Job: a handle on one submitted job."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .spec import JobSpec

if TYPE_CHECKING:
    from .backend.base import Backend


@dataclass
class Job:
    """A job that has been submitted, identified by its Slurm job id.

    Job ids are strings because array tasks and heterogeneous jobs use ids
    such as ``"1234_7"`` and ``"1234+0"``.
    """

    job_id: str
    spec: JobSpec
    backend: Backend
