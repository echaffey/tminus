"""FakeBackend: an in-memory scheduler for tests and for development without Slurm."""
from __future__ import annotations

from typing import Dict

from ..spec import JobSpec
from .base import Backend


class FakeBackend(Backend):
    """Accepts every submission and hands out increasing job ids.

    ``submitted`` maps each job id to the JobSpec it was submitted with.
    """

    def __init__(self, first_job_id: int = 1000) -> None:
        self._next_id = first_job_id
        self.submitted: Dict[str, JobSpec] = {}

    def submit(self, spec: JobSpec) -> str:
        job_id = str(self._next_id)
        self._next_id += 1
        self.submitted[job_id] = spec
        return job_id
