from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
import unittest

import tminus
from tminus import JobState, PollPolicy, aio
from tminus.store import Store

from tests.helpers import CountingFake, Timeline


class AsyncWaitTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="tminus-test-")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.backend = CountingFake()
        self.timeline = Timeline(self.backend)
        self.store = Store(os.path.join(self.tmp, "jobs.db"), clock=self.timeline.clock)
        self.options = dict(policy=PollPolicy(initial=5, factor=2, maximum=30), store=self.store,
                            sleep=self.timeline.async_sleep, clock=self.timeline.clock)

    def submit(self):
        return tminus.submit(tminus.JobSpec(command="true"), backend=self.backend, store=self.store)

    def test_wait_returns_the_final_status_once_the_job_finishes(self):
        job = self.submit()
        self.timeline.at(1, job, JobState.RUNNING).at(2, job, JobState.COMPLETED, exit_code=0)

        status = asyncio.run(aio.wait(job, **self.options))

        self.assertIs(status.state, JobState.COMPLETED)
        self.assertIs(job.status, status)
        self.assertEqual(len(self.timeline.sleeps), 2)

    def test_wait_all_returns_every_final_status(self):
        first, second = self.submit(), self.submit()
        self.timeline.at(1, first, JobState.COMPLETED).at(2, second, JobState.FAILED, exit_code=1)

        statuses = asyncio.run(aio.wait_all([first, second], **self.options))

        self.assertIs(statuses[first.job_id].state, JobState.COMPLETED)
        self.assertIs(statuses[second.job_id].state, JobState.FAILED)

    def test_as_completed_yields_jobs_in_the_order_they_finish(self):
        a, b, c = self.submit(), self.submit(), self.submit()
        self.timeline.at(1, c, JobState.COMPLETED).at(2, a, JobState.FAILED).at(3, b, JobState.COMPLETED)

        async def finish_order():
            return [job.job_id async for job in aio.as_completed([a, b, c], **self.options)]

        self.assertEqual(asyncio.run(finish_order()), [c.job_id, a.job_id, b.job_id])

    def test_concurrent_waits_share_one_query_per_poll(self):
        a, b = self.submit(), self.submit()
        self.timeline.at(1, a, JobState.COMPLETED).at(2, b, JobState.COMPLETED)

        async def both():
            return await asyncio.gather(aio.wait(a, **self.options), aio.wait(b, **self.options))

        asyncio.run(both())

        self.assertEqual(self.backend.status_calls, [
            [a.job_id, b.job_id],
            [a.job_id, b.job_id],
            [b.job_id],
        ])

    def test_a_timeout_raises_with_the_latest_statuses(self):
        job = self.submit()
        self.timeline.at(1, job, JobState.RUNNING)

        with self.assertRaises(tminus.WaitTimeout) as caught:
            asyncio.run(aio.wait_all([job], timeout=12, **self.options))

        self.assertIs(caught.exception.statuses[job.job_id].state, JobState.RUNNING)
        self.assertLessEqual(self.timeline.now, 12)

    def test_cancelling_the_waiting_task_stops_polling_and_leaves_the_job_alone(self):
        job = self.submit()

        async def cancel_during_third_sleep():
            waiting = asyncio.ensure_future(aio.wait(job, **self.options))
            self.timeline.call_at(3, waiting.cancel)
            with self.assertRaises(asyncio.CancelledError):
                await waiting
            for _ in range(20):  # give a still-running poller every chance to poll again
                await asyncio.sleep(0.01)

        asyncio.run(cancel_during_third_sleep())

        self.assertEqual(len(self.timeline.sleeps), 3)
        self.assertIs(job.refresh().state, JobState.PENDING)

    def test_a_scheduler_error_reaches_every_waiter(self):
        stranger = tminus.Job(job_id="424242", spec=None, backend=self.backend)

        with self.assertRaises(tminus.JobNotFound):
            asyncio.run(aio.wait(stranger, **self.options))


if __name__ == "__main__":
    unittest.main()
