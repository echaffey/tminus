"""FakeBackend: an in-memory scheduler for tests and for development without Slurm."""
from __future__ import annotations

import json
import os
import tempfile
from typing import Any, Dict, List, Optional, Sequence, Tuple

from ..errors import JobNotFound
from ..spec import DependencyType, JobSpec, legacy_tags
from ..report import JobUsage
from ..state import JobState, JobStatus
from .base import Backend


class FakeBackend(Backend):
    """Accepts every submission and hands out increasing job ids.

    New jobs start PENDING and stay there until moved with ``set_state``. A job
    with dependencies is held with reason ``Dependency`` until they are met,
    or marked ``DependencyNeverSatisfied`` once they cannot be, as Slurm does.
    An array job gets one task per array index (``"1000_0"``, ``"1000_1"``,
    ...); its own status summarises them, and acting on the array acts on
    every task.
    ``submitted`` maps each job id to the JobSpec it was submitted with, and
    ``updates`` to the changes applied with ``update`` (both in this process only).

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
        self.updates: Dict[str, Dict[str, Any]] = {}
        # job id -> (dependency type, ids it depends on)
        self._dependencies: Dict[str, Tuple[str, List[str]]] = {}
        # array job id -> its task ids
        self._arrays: Dict[str, List[str]] = {}
        # job id -> JobUsage fields set with set_usage (in this process only)
        self._usage: Dict[str, Dict[str, Any]] = {}

    def submit(self, spec: JobSpec) -> str:
        self._load()
        job_id = str(self._next_id)
        self._next_id += 1
        self.submitted[job_id] = spec
        ids = [f"{job_id}_{task}" for task in spec.task_ids()] or [job_id]
        if spec.array is not None:
            self._arrays[job_id] = ids
        for one_id in ids:
            self._statuses[one_id] = JobStatus(one_id, JobState.PENDING, tags=list(spec.tags))
            if spec.depends_on:
                self._dependencies[one_id] = (spec.dependency_type.value, list(spec.depends_on))
                self._statuses[one_id] = JobStatus(one_id, JobState.PENDING, "Dependency", tags=list(spec.tags))
        self._check_dependencies()
        self._save()
        return job_id

    def status(self, job_ids: Sequence[str]) -> Dict[str, JobStatus]:
        self._load()
        found = {job_id: self._current(job_id) for job_id in job_ids}
        return {job_id: status for job_id, status in found.items() if status is not None}

    def _current(self, job_id: str) -> Optional[JobStatus]:
        if job_id in self._arrays:
            return JobStatus.for_array(job_id, {t: self._statuses[t] for t in self._arrays[job_id]})
        return self._statuses.get(job_id)

    def set_state(
        self,
        job_id: str,
        state: JobState,
        reason: Optional[str] = None,
        exit_code: Optional[int] = None,
    ) -> None:
        """Move a submitted job (or every task of an array) to ``state``, as the scheduler would."""
        self._load()
        for one_id in self._expand([job_id]):
            tags = self._statuses[one_id].tags
            self._statuses[one_id] = JobStatus(one_id, state, reason, exit_code, tags)
        self._check_dependencies()
        self._save()

    def cancel(self, job_ids: Sequence[str]) -> None:
        for job_id in self._expand(job_ids):
            if not self._statuses[job_id].state.is_terminal:
                self.set_state(job_id, JobState.CANCELLED)

    def hold(self, job_ids: Sequence[str]) -> None:
        for job_id in self._expand(job_ids):
            self.set_state(job_id, JobState.PENDING, reason="JobHeldUser")

    def release(self, job_ids: Sequence[str]) -> None:
        for job_id in self._expand(job_ids):
            self.set_state(job_id, JobState.PENDING)

    def requeue(self, job_ids: Sequence[str]) -> None:
        for job_id in self._expand(job_ids):
            self.set_state(job_id, JobState.PENDING)

    def update(self, job_id: str, changes: Dict[str, Any]) -> None:
        self._expand([job_id])
        self.updates.setdefault(job_id, {}).update(changes)

    def usage(self, job_ids: Sequence[str]) -> List[JobUsage]:
        """Each job's state from the fake scheduler, plus any figures given to ``set_usage``."""
        usages = []
        for one_id in self._expand(job_ids):
            status = self._statuses[one_id]
            usages.append(JobUsage(one_id, status.state, status.exit_code, **self._usage.get(one_id, {})))
        return usages

    def set_usage(self, job_id: str, **figures: Any) -> None:
        """Set accounting figures (JobUsage fields such as elapsed or peak_memory) for a job."""
        self._expand([job_id])
        self._usage.setdefault(job_id, {}).update(figures)

    def _check_dependencies(self) -> None:
        """Release dependent jobs whose dependencies are met; flag those that never can be."""
        for job_id, (kind, depends_on) in self._dependencies.items():
            current = self._statuses[job_id]
            if current.state is not JobState.PENDING or current.reason != "Dependency":
                continue
            verdict = _dependency_verdict(DependencyType(kind), [self._current(d) for d in depends_on])
            if verdict is not None:
                reason = None if verdict else "DependencyNeverSatisfied"
                self._statuses[job_id] = JobStatus(job_id, JobState.PENDING, reason, tags=current.tags)

    def _expand(self, job_ids: Sequence[str]) -> List[str]:
        """The individual job and task ids behind ``job_ids``; raises JobNotFound for unknown ones."""
        self._load()
        expanded: List[str] = []
        for job_id in job_ids:
            if job_id in self._arrays:
                expanded.extend(self._arrays[job_id])
            elif job_id in self._statuses:
                expanded.append(job_id)
            else:
                raise JobNotFound(job_id)
        return expanded

    def _load(self) -> None:
        if self._state_path is None or not os.path.exists(self._state_path):
            return
        with open(self._state_path, encoding="utf-8") as handle:
            data = json.load(handle)
        self._next_id = data["next_id"]
        self._dependencies = {job_id: (kind, ids) for job_id, (kind, ids) in data.get("dependencies", {}).items()}
        self._arrays = data.get("arrays", {})
        self._statuses = {job_id: _status_from_json(job_id, raw) for job_id, raw in data["jobs"].items()}

    def _save(self) -> None:
        if self._state_path is None:
            return
        data = {
            "next_id": self._next_id,
            "jobs": {job_id: _status_to_json(status) for job_id, status in self._statuses.items()},
            "dependencies": self._dependencies,
            "arrays": self._arrays,
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
                     raw.get("exit_code"), legacy_tags(raw.get("tags")) or [])


def _dependency_verdict(kind: DependencyType, dependencies: List[Optional[JobStatus]]) -> Optional[bool]:
    """True once ``dependencies`` satisfy ``kind``, False once they never can, None while undecided."""
    if any(status is None for status in dependencies):
        return False  # depending on a job that does not exist
    states = [status.state for status in dependencies]
    finished = all(state.is_terminal for state in states)
    if kind is DependencyType.AFTER:
        return True if all(state is not JobState.PENDING for state in states) else None
    if kind is DependencyType.AFTERANY:
        return True if finished else None
    if kind is DependencyType.AFTEROK:
        if any(state.is_terminal and not state.is_success for state in states):
            return False
        return True if finished else None
    # AFTERNOTOK
    if any(state.is_success for state in states):
        return False
    return True if finished else None
