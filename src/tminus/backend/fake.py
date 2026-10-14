"""FakeBackend: an in-memory scheduler for tests and for development without Slurm."""
from __future__ import annotations

import json
import os
import tempfile
from typing import Any, Dict, Optional, Sequence

from ..spec import JobSpec
from ..state import JobState, JobStatus
from .base import Backend


class FakeBackend(Backend):
    """Accepts every submission and hands out increasing job ids.

    New jobs start PENDING and stay there until moved with ``set_state``.
    ``submitted`` maps each job id to the JobSpec it was submitted with (in
    this process only).

    By default everything lives in memory. With ``state_path``, the scheduler's
    job ids and states are kept in that JSON file instead, so separate
    processes (such as successive ``tminus`` commands) share one fake scheduler.
    """

    name = "fake"

    def __init__(self, first_job_id: int = 1000, state_path: Optional[str] = None) -> None:
        self._state_path = state_path
        self._next_id = first_job_id
        self._statuses: Dict[str, JobStatus] = {}
        self.submitted: Dict[str, JobSpec] = {}

    def submit(self, spec: JobSpec) -> str:
        self._load()
        job_id = str(self._next_id)
        self._next_id += 1
        self.submitted[job_id] = spec
        self._statuses[job_id] = JobStatus(job_id, JobState.PENDING, tags=dict(spec.tags))
        self._save()
        return job_id

    def status(self, job_ids: Sequence[str]) -> Dict[str, JobStatus]:
        self._load()
        return {job_id: self._statuses[job_id] for job_id in job_ids if job_id in self._statuses}

    def set_state(
        self,
        job_id: str,
        state: JobState,
        reason: Optional[str] = None,
        exit_code: Optional[int] = None,
    ) -> None:
        """Move a submitted job to ``state``, as the scheduler would."""
        self._load()
        if job_id not in self._statuses:
            raise KeyError(f"FakeBackend never submitted job {job_id}")
        tags = self._statuses[job_id].tags
        self._statuses[job_id] = JobStatus(job_id, state, reason, exit_code, tags)
        self._save()

    def _load(self) -> None:
        if self._state_path is None or not os.path.exists(self._state_path):
            return
        with open(self._state_path, encoding="utf-8") as handle:
            data = json.load(handle)
        self._next_id = data["next_id"]
        self._statuses = {job_id: _status_from_json(job_id, raw) for job_id, raw in data["jobs"].items()}

    def _save(self) -> None:
        if self._state_path is None:
            return
        data = {
            "next_id": self._next_id,
            "jobs": {job_id: _status_to_json(status) for job_id, status in self._statuses.items()},
        }
        directory = os.path.dirname(os.path.abspath(self._state_path))
        os.makedirs(directory, exist_ok=True)
        # Write then rename, so a concurrent reader never sees a half-written file.
        handle, temp_path = tempfile.mkstemp(dir=directory, suffix=".tmp")
        with os.fdopen(handle, "w", encoding="utf-8") as out:
            json.dump(data, out, indent=1, sort_keys=True)
        os.replace(temp_path, self._state_path)


def _status_to_json(status: JobStatus) -> Dict[str, Any]:
    return {"state": status.state.value, "reason": status.reason,
            "exit_code": status.exit_code, "tags": status.tags}


def _status_from_json(job_id: str, raw: Dict[str, Any]) -> JobStatus:
    return JobStatus(job_id, JobState(raw["state"]), raw.get("reason"),
                     raw.get("exit_code"), raw.get("tags") or {})
