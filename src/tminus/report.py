"""Reports: how well finished jobs used the resources they asked for."""
from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence

from .state import JobState

_COLUMNS = (
    "job_id", "state", "exit_code", "elapsed", "time_limit", "cpu_seconds", "cpus", "nodes",
    "peak_memory", "requested_memory", "cpu_efficiency", "memory_efficiency", "time_used",
)


@dataclass(frozen=True)
class JobUsage:
    """Accounting figures for one job (or one array task).

    Times are in seconds and memory in bytes; any figure the scheduler did not
    report is None. ``requested_memory`` is the job's total across all nodes.
    The efficiency properties are None until the job has finished, and
    whenever a figure they need is missing, so unknowns never read as zero.
    """

    job_id: str
    state: JobState
    exit_code: Optional[int] = None
    elapsed: Optional[float] = None
    time_limit: Optional[float] = None
    cpu_seconds: Optional[float] = None
    cpus: Optional[int] = None
    nodes: Optional[int] = None
    peak_memory: Optional[int] = None
    requested_memory: Optional[int] = None

    @property
    def cpu_efficiency(self) -> Optional[float]:
        """CPU time used divided by CPU time allocated (elapsed time times allocated CPUs)."""
        if not self.state.is_terminal or self.cpu_seconds is None or not self.elapsed or not self.cpus:
            return None
        return self.cpu_seconds / (self.elapsed * self.cpus)

    @property
    def memory_efficiency(self) -> Optional[float]:
        """Peak memory of the largest task divided by the memory requested per node.

        Like Slurm's seff, this is an estimate: it assumes the busiest task
        shows how full a node got.
        """
        if not self.state.is_terminal or self.peak_memory is None or not self.requested_memory:
            return None
        return self.peak_memory / (self.requested_memory / (self.nodes or 1))

    @property
    def time_used(self) -> Optional[float]:
        """Elapsed time divided by the time limit."""
        if not self.state.is_terminal or self.elapsed is None or not self.time_limit:
            return None
        return self.elapsed / self.time_limit

    def as_dict(self) -> Dict[str, Any]:
        """Every column, with the state as its name and None for anything unavailable."""
        row = {name: getattr(self, name) for name in _COLUMNS}
        row["state"] = self.state.value
        return row


class Report:
    """Usage of a set of jobs, with a summary across them."""

    def __init__(self, jobs: Sequence[JobUsage]) -> None:
        self.jobs = list(jobs)

    def state_counts(self) -> Dict[JobState, int]:
        """How many jobs ended (or are) in each state."""
        counts: Dict[JobState, int] = {}
        for usage in self.jobs:
            counts[usage.state] = counts.get(usage.state, 0) + 1
        return counts

    def mean_cpu_efficiency(self) -> Optional[float]:
        """Average CPU efficiency over the jobs that have one."""
        return _mean(lambda u: u.cpu_efficiency, self.jobs)

    def mean_memory_efficiency(self) -> Optional[float]:
        """Average memory efficiency over the jobs that have one."""
        return _mean(lambda u: u.memory_efficiency, self.jobs)

    def mean_time_used(self) -> Optional[float]:
        """Average fraction of the time limit used, over the jobs that have one."""
        return _mean(lambda u: u.time_used, self.jobs)

    def summary(self) -> Dict[str, Any]:
        return {
            "jobs": len(self.jobs),
            "state_counts": {state.value: n for state, n in self.state_counts().items()},
            "mean_cpu_efficiency": self.mean_cpu_efficiency(),
            "mean_memory_efficiency": self.mean_memory_efficiency(),
            "mean_time_used": self.mean_time_used(),
        }

    def to_json(self) -> str:
        """The report as JSON: ``{"jobs": [...], "summary": {...}}``, with null for unavailable figures."""
        return json.dumps({"jobs": [u.as_dict() for u in self.jobs], "summary": self.summary()}, indent=2)

    def to_csv(self) -> str:
        """One CSV row per job, with empty cells for unavailable figures."""
        out = io.StringIO()
        writer = csv.DictWriter(out, fieldnames=list(_COLUMNS), lineterminator="\n")
        writer.writeheader()
        for usage in self.jobs:
            writer.writerow({k: ("" if v is None else v) for k, v in usage.as_dict().items()})
        return out.getvalue()

    def to_table(self) -> str:
        """A human-readable table, with ``-`` for unavailable figures, followed by the summary."""
        rows = [["JOBID", "STATE", "EXIT", "ELAPSED", "TIME_USED", "CPU_EFF", "MEM_EFF", "PEAK_MEM", "REQ_MEM"]]
        for u in self.jobs:
            rows.append([
                u.job_id, u.state.value, _text(u.exit_code), _duration(u.elapsed), _percent(u.time_used),
                _percent(u.cpu_efficiency), _percent(u.memory_efficiency), _bytes(u.peak_memory),
                _bytes(u.requested_memory),
            ])
        widths = [max(len(row[col]) for row in rows) for col in range(len(rows[0]))]
        lines = ["  ".join(cell.ljust(width) for cell, width in zip(row, widths)).rstrip() for row in rows]
        counts = ", ".join(f"{state.value}={n}" for state, n in self.state_counts().items()) or "none"
        lines.append("")
        lines.append(f"{len(self.jobs)} job(s): {counts}")
        lines.append(
            f"mean CPU efficiency {_percent(self.mean_cpu_efficiency())}, "
            f"mean memory efficiency {_percent(self.mean_memory_efficiency())}, "
            f"mean time limit used {_percent(self.mean_time_used())}"
        )
        return "\n".join(lines) + "\n"


def _mean(figure: Callable[[JobUsage], Optional[float]], jobs: List[JobUsage]) -> Optional[float]:
    values = [figure(u) for u in jobs]
    known = [v for v in values if v is not None]
    return sum(known) / len(known) if known else None


def _text(value: Any) -> str:
    return "-" if value is None else str(value)


def _percent(value: Optional[float]) -> str:
    return "-" if value is None else f"{value * 100:.0f}%"


def _duration(seconds: Optional[float]) -> str:
    if seconds is None:
        return "-"
    whole = int(seconds)
    return f"{whole // 3600}:{whole % 3600 // 60:02d}:{whole % 60:02d}"


def _bytes(count: Optional[int]) -> str:
    if count is None:
        return "-"
    for unit, size in (("T", 1024 ** 4), ("G", 1024 ** 3), ("M", 1024 ** 2), ("K", 1024)):
        if count >= size:
            return f"{count / size:.1f}{unit}"
    return f"{count}B"
