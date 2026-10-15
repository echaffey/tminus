"""Check tminus's Slurm parsing against output captured on a real cluster.

Run from the repo root on the cluster, pointing at the capture directory:

    python tools/check_capture.py ~/tminus-capture

The directory holds the files from the capture steps (version.txt, ids.txt,
squeue_single.json, squeue_array.json, sacct_single.json, sacct_array.json,
sacct_failed_timeout_cancelled.json). Every line ends in OK or MISMATCH; only
the version line, the shapes line, and MISMATCH lines need reporting back.
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))

from tminus import JobState, JobStatus  # noqa: E402
from tminus.parse import parse_sacct, parse_sacct_usage, parse_squeue  # noqa: E402


def load(directory, name):
    path = os.path.join(directory, name)
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as handle:
        return handle.read()


def shape(value):
    """A short description of a JSON value's shape, e.g. list[str] or dict(set,number)."""
    if isinstance(value, list):
        inner = sorted({type(v).__name__ for v in value}) or ["empty"]
        return "list[" + ",".join(inner) + "]"
    if isinstance(value, dict):
        return "dict(" + ",".join(sorted(value)[:4]) + ")"
    return type(value).__name__


def check(label, ok, got):
    print(f"{label}: {'OK' if ok else 'MISMATCH, got ' + got}")


def statuses_for(job_id, statuses):
    """The status of ``job_id``, combining array tasks the way the Slurm backend does."""
    tasks = {s.job_id: s for s in statuses if s.job_id.startswith(job_id + "_")}
    if tasks:
        return JobStatus.for_array(job_id, tasks)
    found = [s for s in statuses if s.job_id == job_id]
    return found[-1] if found else None


def describe(status):
    if status is None:
        return "not found"
    text = f"{status.state.value} exit={status.exit_code} reason={status.reason} tags={status.tags}"
    if status.tasks:
        text += " tasks=" + ",".join(f"{k.value}={n}" for k, n in status.task_counts().items())
    return text


def main(directory):
    directory = os.path.expanduser(directory)
    version = (load(directory, "version.txt") or "version.txt missing").strip()
    ids = (load(directory, "ids.txt") or "").split()
    if len(ids) != 5:
        print("ids.txt should hold five job ids: single array failed timeout cancelled")
        return 1
    single, array, failed, timeout, cancelled = ids
    print(f"version: {version}")

    shapes = []
    for name, keys in (("squeue_single.json", ("job_state", "exit_code", "array_job_id", "comment")),
                       ("sacct_single.json", ("state", "exit_code", "time", "tres", "required", "comment"))):
        text = load(directory, name)
        if text is None:
            print(f"{name}: MISMATCH, got missing file")
            continue
        jobs = json.loads(text).get("jobs") or [{}]
        first = jobs[0]
        shapes.append(" ".join(f"{name.split('_')[0]}.{k}={shape(first.get(k, 'MISSING'))}" for k in keys))
        if name.startswith("sacct"):
            time_info = first.get("time") or {}
            shapes.append(f"sacct.time.limit={shape(time_info.get('limit', 'MISSING'))} "
                          f"sacct.time.total={shape(time_info.get('total', 'MISSING'))} "
                          f"steps={len(first.get('steps') or [])}")
    print("shapes: " + " | ".join(shapes))

    def parsed(name, parser):
        text = load(directory, name)
        return [] if text is None else parser(text)

    queued = statuses_for(single, parsed("squeue_single.json", parse_squeue))
    check("squeue single (pending or running, tag exp=capture)",
          queued is not None and queued.state in (JobState.PENDING, JobState.RUNNING)
          and queued.tags == ["exp=capture"], describe(queued))

    queued_array = statuses_for(array, parsed("squeue_array.json", parse_squeue))
    check("squeue array (some of tasks 0-3 listed)",
          queued_array is not None and set(queued_array.tasks) <= {f"{array}_{i}" for i in range(4)}
          and bool(queued_array.tasks), describe(queued_array))

    done = statuses_for(single, parsed("sacct_single.json", parse_sacct))
    check("sacct single (COMPLETED, exit 0)",
          done is not None and done.state is JobState.COMPLETED and done.exit_code == 0, describe(done))

    finished_array = statuses_for(array, parsed("sacct_array.json", parse_sacct))
    check("sacct array (4 tasks, all COMPLETED)",
          finished_array is not None and finished_array.task_counts() == {JobState.COMPLETED: 4},
          describe(finished_array))

    others = parsed("sacct_failed_timeout_cancelled.json", parse_sacct)
    for label, job_id, state, exit_code in (("failed", failed, JobState.FAILED, 3),
                                            ("timeout", timeout, JobState.TIMEOUT, None),
                                            ("cancelled", cancelled, JobState.CANCELLED, None)):
        status = statuses_for(job_id, others)
        check(f"sacct {label} ({state.value}" + (f", exit {exit_code})" if exit_code is not None else ")"),
              status is not None and status.state is state
              and (exit_code is None or status.exit_code == exit_code), describe(status))

    usage = [u for u in parsed("sacct_single.json", parse_sacct_usage) if u.job_id == single]
    if not usage:
        print("usage single: MISMATCH, got not found")
    else:
        u = usage[0]
        summary = (f"elapsed={u.elapsed} limit={u.time_limit} cpu_s={u.cpu_seconds} cpus={u.cpus} "
                   f"nodes={u.nodes} peak={u.peak_memory} requested={u.requested_memory}")
        check("usage single (elapsed 80-200 s, limit 180 s, cpus and requested memory known)",
              u.elapsed is not None and 80 <= u.elapsed <= 200 and u.time_limit == 180
              and bool(u.cpus) and bool(u.requested_memory), summary)
        check("usage single peak memory sampled", u.peak_memory is not None, summary)
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__)
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
