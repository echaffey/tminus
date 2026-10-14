"""Waiting for jobs from asyncio code without blocking the event loop.

Every concurrent wait in one event loop (with the same Store, PollPolicy,
sleep, and clock) is served by one shared poller, so many coroutines waiting
on many jobs still cost one batched scheduler query per poll. The query itself
is the same synchronous ``poll_once`` the blocking ``tminus.wait_all`` uses,
run in a worker thread so the event loop stays responsive.
"""
from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any, AsyncIterator, Awaitable, Callable, Dict, List, Optional, Sequence, Tuple

from .errors import JobNotFound, WaitTimeout
from .monitor import PollPolicy, next_delay, poll_once
from .state import JobStatus
from .store import Store

if TYPE_CHECKING:
    from .job import Job

__all__ = ["as_completed", "wait", "wait_all"]

AsyncSleep = Callable[[float], Awaitable[Any]]


async def wait_all(
    jobs: Sequence[Job],
    timeout: Optional[float] = None,
    policy: Optional[PollPolicy] = None,
    store: Optional[Store] = None,
    sleep: AsyncSleep = asyncio.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> Dict[str, JobStatus]:
    """Await every job in ``jobs`` reaching a terminal state; return each final status.

    Behaves like the blocking ``tminus.wait_all``: raises WaitTimeout (jobs left
    running) or JobNotFound. Cancelling the awaiting task stops this wait
    without touching the Slurm jobs.
    """
    if policy is None:
        policy = PollPolicy()
    if store is None:
        store = Store()
    unique = list({job.job_id: job for job in jobs}.values())
    waiter = _Waiter(unique, None if timeout is None else clock() + timeout, timeout)
    _coordinator(store, policy, sleep, clock).add(waiter)
    statuses = await waiter.future
    return {job.job_id: statuses[job.job_id] for job in jobs}


async def wait(job: Job, timeout: Optional[float] = None, **options: Any) -> JobStatus:
    """Await one job reaching a terminal state; return its final status."""
    return (await wait_all([job], timeout=timeout, **options))[job.job_id]


async def as_completed(jobs: Sequence[Job], timeout: Optional[float] = None, **options: Any) -> AsyncIterator[Job]:
    """Yield each job as it reaches a terminal state, in the order they finish.

    Each yielded job's ``status`` holds its final status.
    """
    unique = list({job.job_id: job for job in jobs}.values())

    async def finished(job: Job) -> Job:
        await wait(job, timeout=timeout, **options)
        return job

    tasks = [asyncio.ensure_future(finished(job)) for job in unique]
    try:
        for next_finished in asyncio.as_completed(tasks):
            yield await next_finished
    finally:
        for task in tasks:
            task.cancel()


class _Waiter:
    """One wait_all call: its jobs, its deadline, and the future its caller awaits."""

    def __init__(self, jobs: List[Job], deadline: Optional[float], timeout: Optional[float]) -> None:
        self.jobs = jobs
        self.deadline = deadline
        self.timeout = timeout
        self.latest: Dict[str, JobStatus] = {}
        self.future: asyncio.Future = asyncio.get_event_loop().create_future()


_CoordinatorKey = Tuple[int, str, PollPolicy, AsyncSleep, Callable[[], float]]
_COORDINATORS: Dict[_CoordinatorKey, "_Coordinator"] = {}


def _coordinator(store: Store, policy: PollPolicy, sleep: AsyncSleep, clock: Callable[[], float]) -> "_Coordinator":
    key = (id(asyncio.get_event_loop()), store.path, policy, sleep, clock)
    if key not in _COORDINATORS:
        _COORDINATORS[key] = _Coordinator(key, store, policy, sleep, clock)
    return _COORDINATORS[key]


class _Coordinator:
    """Polls on behalf of every waiter registered with it, until none are left."""

    def __init__(self, key, store: Store, policy: PollPolicy, sleep: AsyncSleep, clock: Callable[[], float]) -> None:
        self._key = key
        self._store = store
        self._policy = policy
        self._sleep = sleep
        self._clock = clock
        self._waiters: List[_Waiter] = []
        self._wake = asyncio.Event()
        self._task: Optional[asyncio.Future] = None

    def add(self, waiter: _Waiter) -> None:
        self._waiters.append(waiter)
        self._wake.set()
        if self._task is None:
            self._task = asyncio.ensure_future(self._run())

    async def _run(self) -> None:
        loop = asyncio.get_event_loop()
        intervals = self._policy.intervals()
        try:
            while True:
                # Waiters whose caller was cancelled simply drop out.
                self._waiters = [w for w in self._waiters if not w.future.done()]
                if not self._waiters:
                    return
                jobs = list({job.job_id: job for w in self._waiters for job in w.jobs}.values())
                self._wake.clear()
                try:
                    latest = await loop.run_in_executor(None, poll_once, jobs, self._store)
                except JobNotFound as exc:
                    # Only the waits that include the unknown job fail; poll again for the rest.
                    self._fail([w for w in self._waiters if any(j.job_id == exc.job_id for j in w.jobs)], exc)
                    continue
                except Exception as exc:  # a scheduler failure affects every wait sharing this poll
                    self._fail(self._waiters, exc)
                    return
                self._settle(latest)
                if not self._waiters:
                    return
                deadlines = [w.deadline for w in self._waiters if w.deadline is not None]
                delay = next_delay(intervals, deadlines, self._clock)
                if delay is not None and await self._sleep_unless_woken(delay):
                    # A new wait arrived: poll for it now and restart the backoff.
                    intervals = self._policy.intervals()
        finally:
            _COORDINATORS.pop(self._key, None)

    def _settle(self, latest: Dict[str, JobStatus]) -> None:
        """Resolve every waiter whose jobs are all terminal, and time out any past its deadline."""
        now = self._clock()
        remaining = []
        for waiter in self._waiters:
            if waiter.future.done():
                continue
            waiter.latest.update({job.job_id: latest[job.job_id] for job in waiter.jobs})
            if all(status.state.is_terminal for status in waiter.latest.values()):
                waiter.future.set_result(dict(waiter.latest))
            elif waiter.deadline is not None and waiter.deadline <= now:
                waiter.future.set_exception(WaitTimeout(dict(waiter.latest), waiter.timeout))
            else:
                remaining.append(waiter)
        self._waiters = remaining

    def _fail(self, waiters: List[_Waiter], exc: Exception) -> None:
        for waiter in waiters:
            if not waiter.future.done():
                waiter.future.set_exception(exc)
        self._waiters = [w for w in self._waiters if w not in waiters]

    async def _sleep_unless_woken(self, delay: float) -> bool:
        """Sleep for ``delay``; return True if a new waiter cut the sleep short."""
        sleeper = asyncio.ensure_future(self._sleep(delay))
        waker = asyncio.ensure_future(self._wake.wait())
        done, pending = await asyncio.wait({sleeper, waker}, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        return sleeper not in done
