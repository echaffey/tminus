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
from .spec import expand_array, tags_from_comment
from .report import JobUsage
from .state import JobState, JobStatus


def parse_squeue(text: str) -> List[JobStatus]:
    """Job statuses from ``squeue --json`` output.

    Array tasks are reported individually as ``<array id>_<task id>``,
    including pending tasks squeue collapses into one record.
    """
    statuses = []
    for job in _jobs(text, "squeue"):
        state = JobState.from_slurm(job.get("job_state"))
        ids = _ids(job, job.get("array_job_id"), job.get("array_task_id"), job.get("array_task_string"))
        for job_id in ids:
            statuses.append(JobStatus(
                job_id=job_id,
                state=state,
                reason=_reason(job.get("state_reason")),
                exit_code=_return_code(job.get("exit_code")) if state.is_terminal else None,
                tags=tags_from_comment(_comment(job.get("comment"))),
            ))
    return statuses


def parse_sacct(text: str) -> List[JobStatus]:
    """Job statuses from ``sacct --json`` output.

    A requeued job can appear more than once; the last record is the current
    one. Array tasks are reported individually, as for ``parse_squeue``.
    """
    statuses: Dict[str, JobStatus] = {}
    for job in _jobs(text, "sacct"):
        state_info = job.get("state") or {}
        state = JobState.from_slurm(state_info.get("current"))
        array = job.get("array") if isinstance(job.get("array"), dict) else {}
        for job_id in _ids(job, array.get("job_id"), array.get("task_id"), array.get("task")):
            statuses[job_id] = JobStatus(
                job_id=job_id,
                state=state,
                reason=_reason(state_info.get("reason")),
                exit_code=_return_code(job.get("exit_code")) if state.is_terminal else None,
                tags=tags_from_comment(_comment(job.get("comment"))),
            )
    return list(statuses.values())


def parse_sacct_usage(text: str) -> List[JobUsage]:
    """Accounting figures from ``sacct --json`` output, one per job or array task.

    Memory requested is the allocated memory when sacct reports it, else the
    request: memory per node times nodes, or memory per CPU times CPUs.
    """
    usages: Dict[str, JobUsage] = {}
    for job in _jobs(text, "sacct"):
        state_info = job.get("state") or {}
        state = JobState.from_slurm(state_info.get("current"))
        array = job.get("array") if isinstance(job.get("array"), dict) else {}
        time_info = job.get("time") if isinstance(job.get("time"), dict) else {}
        tres = job.get("tres") if isinstance(job.get("tres"), dict) else {}
        allocated = _tres_counts(tres.get("allocated"))
        required = job.get("required") if isinstance(job.get("required"), dict) else {}
        cpus = allocated.get("cpu") or _number(required.get("CPUs"))
        nodes = allocated.get("node") or 1
        limit_minutes = _number(time_info.get("limit"))
        for job_id in _ids(job, array.get("job_id"), array.get("task_id"), array.get("task")):
            usages[job_id] = JobUsage(
                job_id=job_id,
                state=state,
                exit_code=_return_code(job.get("exit_code")) if state.is_terminal else None,
                elapsed=_number(time_info.get("elapsed")),
                time_limit=None if limit_minutes is None else limit_minutes * 60,
                cpu_seconds=_seconds(time_info.get("total")),
                cpus=cpus,
                nodes=nodes,
                peak_memory=_peak_memory(job.get("steps")),
                requested_memory=_requested_memory(allocated, required, cpus, nodes),
            )
    return list(usages.values())


def _seconds(value: Any) -> Optional[float]:
    """A duration: {"seconds", "microseconds"} or a plain number of seconds."""
    if isinstance(value, dict):
        seconds = _number(value.get("seconds"))
        if seconds is None:
            return None
        return seconds + (_number(value.get("microseconds")) or 0) / 1e6
    number = _number(value)
    return None if number is None else float(number)


def _tres_counts(entries: Any) -> Dict[str, int]:
    """A TRES list ([{"type": "cpu", "count": 4}, ...]) as {type: count}, first entry of each type."""
    counts: Dict[str, int] = {}
    for entry in entries if isinstance(entries, list) else []:
        if isinstance(entry, dict) and isinstance(entry.get("type"), str):
            count = _number(entry.get("count"))
            if count is not None:
                counts.setdefault(entry["type"], count)
    return counts


def _peak_memory(steps: Any) -> Optional[int]:
    """The largest per-task peak memory (MaxRSS, bytes) over all steps, if any was sampled."""
    peaks = []
    for step in steps if isinstance(steps, list) else []:
        tres = step.get("tres") if isinstance(step, dict) else None
        requested = tres.get("requested") if isinstance(tres, dict) else None
        maxima = requested.get("max") if isinstance(requested, dict) else None
        memory = _tres_counts(maxima).get("mem")
        if memory is not None:
            peaks.append(memory)
    return max(peaks) if peaks else None


def _requested_memory(allocated: Dict[str, int], required: Dict[str, Any], cpus: Optional[int],
                      nodes: int) -> Optional[int]:
    """Total memory for the job in bytes; sacct reports megabytes."""
    megabytes = allocated.get("mem")
    if megabytes is None:
        per_node = _number(required.get("memory_per_node"))
        per_cpu = _number(required.get("memory_per_cpu"))
        if per_node:
            megabytes = per_node * nodes
        elif per_cpu and cpus:
            megabytes = per_cpu * cpus
        else:
            # Older Slurm prints one "memory" figure; treat it as per node.
            plain = _number(required.get("memory"))
            megabytes = plain * nodes if plain else None
    return None if megabytes is None else megabytes * 1024 * 1024


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


def _ids(job: Dict[str, Any], array_job_id: Any, task_id: Any, task_string: Any) -> List[str]:
    """The ids one record describes: its own id, one array task, or a collapsed range of tasks."""
    master = _number(array_job_id)
    if not master:
        return [_job_id(job)]
    task = _number(task_id)
    if task is not None:
        return [f"{master}_{task}"]
    if isinstance(task_string, str) and task_string.strip():
        try:
            return [f"{master}_{t}" for t in expand_array(task_string)]
        except ValueError:
            pass  # an unreadable range: fall back to the array as a whole
    return [str(master)]


def _reason(value: Any) -> Optional[str]:
    """Slurm spells "no reason" as the string "None"."""
    if not isinstance(value, str) or value in ("", "None"):
        return None
    return value


def _comment(value: Any) -> str:
    """The user's job comment: a string in squeue, {"job": ...} alongside admin comments in sacct."""
    if isinstance(value, dict):
        value = value.get("job")
    return value if isinstance(value, str) else ""


def _return_code(exit_code: Any) -> Optional[int]:
    """The exit status from an exit_code field: a plain int or {"return_code": ...}."""
    if isinstance(exit_code, dict):
        return _number(exit_code.get("return_code"))
    return _number(exit_code)
