"""Cleaning up after jobs: cancelling by tag, deleting their files, pruning old records."""
from __future__ import annotations

import enum
import getpass
import os
import re
import shutil
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple, Union

from .backend import get_backend
from .backend.base import Backend
from .errors import InvalidAgeError, TminusError
from .job import Job
from .spec import Tags
from .state import JobState
from .store import Store, default_store, require_store

_AGE = re.compile(r"^(\d+(?:\.\d+)?)([smhdw])$")
_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


def parse_age(text: str) -> float:
    """Seconds in an age such as ``45s``, ``90m``, ``12h``, ``30d``, or ``2w``."""
    match = _AGE.match(text.strip())
    if match is None:
        raise InvalidAgeError(f"invalid age {text!r}: expected a number and a unit (s, m, h, d, w), e.g. 30d")
    return float(match.group(1)) * _UNIT_SECONDS[match.group(2)]


def prune(
    older_than: Union[str, float],
    tags: Tags = None,
    backend: Optional[Backend] = None,
    store: Optional[Store] = None,
    dry_run: bool = False,
) -> List[str]:
    """Remove Store records of finished jobs submitted more than ``older_than`` ago.

    ``older_than`` is an age string (``30d``) or seconds. Only jobs carrying all
    of ``tags`` are considered. A job counts as finished if the Store recorded
    a terminal state for it, or if the scheduler (asked in one batched query)
    reports it terminal or no longer knows it at all. Active jobs are never
    removed. Each removed job's tags, history, and events go with it. Returns
    the ids removed, or that would be with ``dry_run``. Needs the job record
    (``store`` or ``TMINUS_DB``).
    """
    store = require_store(store, "pruning")
    if backend is None:
        backend = get_backend()
    age = parse_age(older_than) if isinstance(older_than, str) else float(older_than)
    candidates = store.submitted_before(backend.name, store.now() - age, tags)
    unsure = [job_id for job_id, last_state in candidates if last_state is None or not last_state.is_terminal]
    current = backend.status(unsure) if unsure else {}
    finished = [
        job_id for job_id, last_state in candidates
        if job_id not in unsure or job_id not in current or current[job_id].state.is_terminal
    ]
    if finished and not dry_run:
        store.delete(backend.name, finished)
    return finished


class Policy(enum.Enum):
    """Which finished jobs ``purge`` deletes files for."""

    ALWAYS = "always"          # every finished job
    ON_SUCCESS = "on_success"  # only jobs that completed; failures keep their files for debugging
    NEVER = "never"            # none


@dataclass
class PurgeResult:
    """What ``purge`` did (or, in a dry run, would do) for one finished job.

    ``deleted`` lists existing paths removed; ``skipped`` paths whose filename
    patterns tminus cannot expand (such as ``%N``); ``refused`` paths that are
    never deleted (the filesystem root, the home directory, or a parent of it).
    """

    job_id: str
    state: Optional[JobState]
    deleted: List[str] = field(default_factory=list)
    skipped: List[str] = field(default_factory=list)
    refused: List[str] = field(default_factory=list)


def cancel(
    tags: Tags = None,
    backend: Optional[Backend] = None,
    store: Optional[Store] = None,
    dry_run: bool = False,
    jobs: Optional[Sequence[Job]] = None,
) -> List[str]:
    """Cancel every still-active job in ``jobs``, or every tracked job carrying all of
    ``tags``, in one scheduler call.

    Returns the ids cancelled, or that would be with ``dry_run``. Selecting by
    tags needs the job record (``store`` or ``TMINUS_DB``).
    """
    backend, store, tracked = _selected(tags, jobs, backend, store)
    statuses = backend.status([job.job_id for job in tracked]) if tracked else {}
    active = [job.job_id for job in tracked if job.job_id in statuses and not statuses[job.job_id].state.is_terminal]
    if active and not dry_run:
        backend.cancel(active)
        if store is not None:
            for job_id in active:
                store.record_event(backend.name, job_id, "cancel")
    return active


def purge(
    tags: Tags = None,
    policy: Union[Policy, str] = Policy.ON_SUCCESS,
    backend: Optional[Backend] = None,
    store: Optional[Store] = None,
    dry_run: bool = False,
    jobs: Optional[Sequence[Job]] = None,
) -> List[PurgeResult]:
    """Delete the log files and ``scratch_paths`` of finished jobs: those in ``jobs``, or
    every tracked job carrying all of ``tags`` (which needs the job record).

    ``policy`` picks which finished jobs qualify. Active jobs are never touched,
    and only paths derived from what the Store recorded for each job are
    deleted: its ``output``/``error`` files (Slurm's ``slurm-%j.out`` when no
    output was given) and its ``scratch_paths``, never its working directory.
    Relative paths resolve against the job's ``workdir`` or, failing that, the
    directory it was submitted from. A job the scheduler no longer knows counts
    as finished in an unknown state, so only ``Policy.ALWAYS`` purges it.
    """
    policy = Policy(policy)
    backend, store, tracked = _selected(tags, jobs, backend, store)
    if policy is Policy.NEVER or not tracked:
        return []
    statuses = backend.status([job.job_id for job in tracked])
    results = []
    for job in tracked:
        state = statuses[job.job_id].state if job.job_id in statuses else None
        if state is not None and not state.is_terminal:
            continue
        if policy is Policy.ON_SUCCESS and not (state is not None and state.is_success):
            continue
        result = PurgeResult(job.job_id, state)
        for path, expanded in _job_paths(job):
            if not expanded:
                result.skipped.append(path)
            elif _protected(path):
                result.refused.append(path)
            elif os.path.lexists(path):
                result.deleted.append(path)
                if not dry_run:
                    _remove(path)
        if not dry_run and store is not None:
            store.record_event(backend.name, job.job_id, "purge", " ".join(result.deleted) or None)
        results.append(result)
    return results


def _selected(
    tags: Tags,
    jobs: Optional[Sequence[Job]],
    backend: Optional[Backend],
    store: Optional[Store],
) -> Tuple[Backend, Optional[Store], List[Job]]:
    """The jobs to act on: ``jobs`` as given, or the tracked jobs carrying ``tags``."""
    if jobs is not None:
        if backend is None:
            backend = jobs[0].backend if jobs else get_backend()
        if store is None:
            store = default_store()
        return backend, store, list(jobs)
    if not tags:
        raise TminusError("cleanup needs jobs or at least one tag to select them; there is no 'everything' default")
    store = require_store(store, "selecting jobs by tag")
    if backend is None:
        backend = get_backend()
    tracked = [
        Job(job_id=t.job_id, spec=t.spec, backend=backend, submit_dir=t.submit_dir)
        for t in store.find(backend.name, tags)
    ]
    return backend, store, tracked


_PATTERN = re.compile(r"%(.)")


def _job_paths(job: Job) -> List[Tuple[str, bool]]:
    """``(absolute path, fully expanded?)`` for every file or directory the job owns.

    An array job's paths are expanded once per task, with ``%A`` as the array
    id and ``%a`` as the task id. ``%j`` is then the task's own job id, which
    tminus does not know, so such paths are reported as unexpandable.
    """
    spec = job.spec
    if spec is None:
        return []
    default_name = "wrap" if spec.command is not None else os.path.basename(spec.script or "")
    common = {"x": spec.name or default_name, "u": getpass.getuser(), "%": "%"}
    if spec.array is None:
        expansions = [dict(common, j=job.job_id, A=job.job_id)]
    else:
        expansions = [dict(common, A=job.job_id, a=task) for task in spec.task_ids()]
    paths: List[Tuple[str, bool]] = []
    for values in expansions:
        for path in _expand_paths(job, spec, values):
            if path not in paths:
                paths.append(path)
    return paths


def _expand_paths(job: Job, spec, values: Dict[str, str]) -> List[Tuple[str, bool]]:
    default_output = "slurm-%A_%a.out" if spec.array is not None else "slurm-%j.out"
    patterns = [spec.output or default_output]
    if spec.error:
        patterns.append(spec.error)
    patterns.extend(spec.scratch_paths)
    base = spec.workdir or job.submit_dir
    paths = []
    for pattern in patterns:
        if not os.path.isabs(pattern):
            if base is None:
                paths.append((pattern, False))
                continue
            pattern = os.path.join(base, pattern)
        expandable = all(code in values for code in _PATTERN.findall(pattern))
        if expandable:
            paths.append((os.path.normpath(_PATTERN.sub(lambda match: values[match.group(1)], pattern)), True))
        else:
            paths.append((os.path.normpath(pattern), False))
    return paths


def _protected(path: str) -> bool:
    """True for the filesystem root, the user's home directory, and any directory above it."""
    real = os.path.realpath(path)
    if os.path.dirname(real) == real:
        return True
    home = os.path.realpath(os.path.expanduser("~"))
    return real == home or home.startswith(real.rstrip(os.sep) + os.sep)


def _remove(path: str) -> None:
    if os.path.isdir(path) and not os.path.islink(path):
        shutil.rmtree(path)
    else:
        os.remove(path)
