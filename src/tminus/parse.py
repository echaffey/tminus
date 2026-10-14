"""Reading Slurm's JSON output.

The JSON schema changes between Slurm releases (21.08 prints states as strings
and numbers as ints; 23.x and later print states as lists and numbers as
``{"set", "infinite", "number"}`` objects), so every field is read through a
small accessor that understands each shape. Nothing outside this module
indexes a raw Slurm JSON document.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from .errors import SlurmOutputError
from .state import JobState, JobStatus


def parse_squeue(text: str) -> List[JobStatus]:
    """Job statuses from ``squeue --json`` output."""
    statuses = []
    for job in _jobs(text, "squeue"):
        state = JobState.from_slurm(job.get("job_state"))
        statuses.append(JobStatus(
            job_id=_job_id(job),
            state=state,
            reason=_reason(job.get("state_reason")),
            exit_code=_return_code(job.get("exit_code")) if state.is_terminal else None,
        ))
    return statuses


def parse_sacct(text: str) -> List[JobStatus]:
    """Job statuses from ``sacct --json`` output.

    A requeued job can appear more than once; the last record is the current one.
    """
    statuses: Dict[str, JobStatus] = {}
    for job in _jobs(text, "sacct"):
        state_info = job.get("state") or {}
        state = JobState.from_slurm(state_info.get("current"))
        job_id = _job_id(job)
        statuses[job_id] = JobStatus(
            job_id=job_id,
            state=state,
            reason=_reason(state_info.get("reason")),
            exit_code=_return_code(job.get("exit_code")) if state.is_terminal else None,
        )
    return list(statuses.values())


def _jobs(text: str, program: str) -> List[Dict[str, Any]]:
    try:
        document = json.loads(text)
    except ValueError as exc:
        raise SlurmOutputError(f"{program} printed output that is not JSON: {exc}") from None
    jobs = document.get("jobs") if isinstance(document, dict) else None
    if not isinstance(jobs, list):
        raise SlurmOutputError(f"{program} JSON output has no 'jobs' list")
    return [job for job in jobs if isinstance(job, dict)]


def _number(value: Any) -> Optional[int]:
    """An integer field: a plain int (older Slurm) or {"set", "infinite", "number"} (newer)."""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, dict):
        if not value.get("set", False) or value.get("infinite", False):
            return None
        return _number(value.get("number"))
    return None


def _job_id(job: Dict[str, Any]) -> str:
    return str(_number(job.get("job_id")))


def _reason(value: Any) -> Optional[str]:
    """Slurm spells "no reason" as the string "None"."""
    if not isinstance(value, str) or value in ("", "None"):
        return None
    return value


def _return_code(exit_code: Any) -> Optional[int]:
    """The exit status from an exit_code field: a plain int or {"return_code": ...}."""
    if isinstance(exit_code, dict):
        return _number(exit_code.get("return_code"))
    return _number(exit_code)
