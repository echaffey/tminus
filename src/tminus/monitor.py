"""Waiting for jobs: batched polling with backoff.

``poll_once`` and ``next_delay`` hold the polling rules; ``wait_all`` here and
the coroutines in ``tminus.aio`` are two drivers around them.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Dict, Iterable, Iterator, List, Optional, Sequence

from .errors import JobNotFound, UnsatisfiableDependency, WaitTimeout
from .state import JobStatus
from .store import Store, default_store

if TYPE_CHECKING:
    from .job import Job


@dataclass(frozen=True)
class PollPolicy:
    """How often to ask the scheduler while waiting.

    The first wait is ``initial`` seconds, each later one ``factor`` times
    longer, never more than ``maximum``. The defaults keep a long wait to about
    one query a minute, so many waiting scripts do not overload slurmctld.
    """

    initial: float = 5.0
    factor: float = 1.5
    maximum: float = 60.0

    def intervals(self) -> Iterator[float]:
        """The successive sleep intervals, forever."""
        interval = self.initial
        while True:
            yield min(interval, self.maximum)
            interval *= self.factor


def wait_all(
    jobs: Sequence[Job],
    timeout: Optional[float] = None,
    policy: Optional[PollPolicy] = None,
    store: Optional[Store] = None,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> Dict[str, JobStatus]:
    """Block until every job in ``jobs`` is terminal; return each job's final status.

    Each poll asks every backend about all of its outstanding jobs in one
    query. Each job's ``status`` is updated as polls come in, and, when the job
    record is on, every state change seen is appended to the job's history. If ``timeout``
    seconds pass first, raises WaitTimeout carrying the latest statuses; the
    jobs themselves are left running. Raises JobNotFound if the scheduler has
    no record of a job. ``sleep`` and ``clock`` exist so tests and simulations
    can run without real time passing.
    """
    if policy is None:
        policy = PollPolicy()
    if store is None:
        store = default_store()
    deadline = None if timeout is None else clock() + timeout
    intervals = policy.intervals()
    latest: Dict[str, JobStatus] = {}
    outstanding = list({job.job_id: job for job in jobs}.values())
    while True:
        latest.update(poll_once(outstanding, store))
        outstanding = [job for job in outstanding if not latest[job.job_id].state.is_terminal]
        if not outstanding:
            return {job.job_id: latest[job.job_id] for job in jobs}
        stuck = never_starting(latest[job.job_id] for job in outstanding)
        if stuck:
            raise UnsatisfiableDependency(stuck)
        delay = next_delay(intervals, [deadline] if deadline is not None else [], clock)
        if delay is None:
            raise WaitTimeout(dict(latest), timeout)
        sleep(delay)


def poll_once(jobs: Sequence[Job], store: Optional[Store]) -> Dict[str, JobStatus]:
    """Query the current status of ``jobs``, one query per backend.

    Updates each job's ``status``, records state changes in ``store`` if given, and
    raises JobNotFound if a backend has no record of one of them.
    """
    latest: Dict[str, JobStatus] = {}
    for group in _by_backend(jobs):
        backend = group[0].backend
        found = backend.status([job.job_id for job in group])
        for job in group:
            if job.job_id not in found:
                raise JobNotFound(job.job_id)
            job.status = latest[job.job_id] = found[job.job_id]
        if store is not None:
            store.record_statuses(backend.name, [found[job.job_id] for job in group])
    return latest


def never_starting(statuses: Iterable[JobStatus]) -> List[str]:
    """Ids of jobs that will never start because a dependency can no longer be met."""
    return [status.job_id for status in statuses if status.dependency_never_satisfied]


def next_delay(
    intervals: Iterator[float], deadlines: Iterable[float], clock: Callable[[], float]
) -> Optional[float]:
    """How long to sleep before the next poll: the next backoff interval,
    shortened so the earliest deadline is not overshot. None once a deadline
    has already passed.
    """
    delay = next(intervals)
    now = clock()
    for deadline in deadlines:
        remaining = deadline - now
        if remaining <= 0:
            return None
        delay = min(delay, remaining)
    return delay


def _by_backend(jobs: Sequence[Job]) -> List[List[Job]]:
    """Group jobs by the Backend object they belong to, keeping their order."""
    groups: Dict[int, List[Job]] = {}
    for job in jobs:
        groups.setdefault(id(job.backend), []).append(job)
    return list(groups.values())
