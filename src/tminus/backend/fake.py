"""FakeBackend: an in-memory scheduler for tests and for development without Slurm."""
from __future__ import annotations

from typing import Dict, Optional, Sequence

from ..spec import JobSpec
from ..state import JobState, JobStatus
from .base import Backend


class FakeBackend(Backend):
    """Accepts every submission and hands out increasing job ids.

    New jobs start PENDING and stay there until a test moves them with
    ``set_state``. ``submitted`` maps each job id to the JobSpec it was
    submitted with.
    """

    def __init__(self, first_job_id: int = 1000) -> None:
        self._next_id = first_job_id
        self.submitted: Dict[str, JobSpec] = {}
        self._statuses: Dict[str, JobStatus] = {}

    def submit(self, spec: JobSpec) -> str:
        job_id = str(self._next_id)
        self._next_id += 1
        self.submitted[job_id] = spec
        self._statuses[job_id] = JobStatus(job_id, JobState.PENDING)
        return job_id

    def status(self, job_ids: Sequence[str]) -> Dict[str, JobStatus]:
        return {job_id: self._statuses[job_id] for job_id in job_ids if job_id in self._statuses}

    def set_state(
        self,
        job_id: str,
        state: JobState,
        reason: Optional[str] = None,
        exit_code: Optional[int] = None,
    ) -> None:
        """Move a submitted job to ``state``, as the scheduler would."""
        if job_id not in self._statuses:
            raise KeyError(f"FakeBackend never submitted job {job_id}")
        self._statuses[job_id] = JobStatus(job_id, state, reason, exit_code)
