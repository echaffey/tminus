from __future__ import annotations

import os
import shutil
import tempfile
import unittest

import tminus
from tminus import JobState, PollPolicy
from tminus.backend.fake import FakeBackend
from tminus.store import Store


class CountingFake(FakeBackend):
    """A FakeBackend that counts how many status queries it receives."""

    def __init__(self):
        super().__init__()
        self.status_calls = []

    def status(self, job_ids):
        self.status_calls.append(list(job_ids))
        return super().status(job_ids)


class Timeline:
    """A fake clock whose sleep() moves jobs through scripted states.

    ``at(n, job, state)`` makes ``job`` reach ``state`` during the n-th sleep,
    so the poll after that sleep sees it.
    """

    def __init__(self, backend):
        self.backend = backend
        self.now = 0.0
        self.sleeps = []
        self._events = {}

    def at(self, sleep_number, job, state, exit_code=None):
        self._events.setdefault(sleep_number, []).append((job.job_id, state, exit_code))
        return self

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds
        for job_id, state, exit_code in self._events.get(len(self.sleeps), []):
            self.backend.set_state(job_id, state, exit_code=exit_code)


class WaitTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="tminus-test-")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.backend = CountingFake()
        self.timeline = Timeline(self.backend)
        # The Store stamps history with wall-clock time; share the fake clock so stamps are predictable.
        self.store = Store(os.path.join(self.tmp, "jobs.db"), clock=self.timeline.clock)
        self.policy = PollPolicy(initial=5, factor=2, maximum=30)

    def submit(self):
        return tminus.submit(tminus.JobSpec(command="true"), backend=self.backend, store=self.store)

    def wait_all(self, jobs, timeout=None):
        return tminus.wait_all(jobs, timeout=timeout, policy=self.policy, store=self.store,
                               sleep=self.timeline.sleep, clock=self.timeline.clock)

    def test_wait_returns_the_final_status_once_the_job_finishes(self):
        job = self.submit()
        self.timeline.at(1, job, JobState.RUNNING).at(2, job, JobState.COMPLETED, exit_code=0)

        status = job.wait(policy=self.policy, store=self.store,
                          sleep=self.timeline.sleep, clock=self.timeline.clock)

        self.assertIs(status.state, JobState.COMPLETED)
        self.assertEqual(status.exit_code, 0)
        self.assertEqual(len(self.timeline.sleeps), 2)
        self.assertIs(job.status, status)

    def test_wait_returns_immediately_for_a_job_that_already_finished(self):
        job = self.submit()
        self.backend.set_state(job.job_id, JobState.FAILED, exit_code=1)

        statuses = self.wait_all([job])

        self.assertIs(statuses[job.job_id].state, JobState.FAILED)
        self.assertEqual(self.timeline.sleeps, [])

    def test_wait_all_makes_one_query_per_poll_for_every_outstanding_job(self):
        first, second, third = self.submit(), self.submit(), self.submit()
        self.timeline.at(1, first, JobState.COMPLETED)
        self.timeline.at(2, second, JobState.COMPLETED).at(2, third, JobState.FAILED)

        statuses = self.wait_all([first, second, third])

        self.assertEqual(self.backend.status_calls, [
            [first.job_id, second.job_id, third.job_id],
            [first.job_id, second.job_id, third.job_id],
            [second.job_id, third.job_id],
        ])
        self.assertIs(statuses[third.job_id].state, JobState.FAILED)

    def test_poll_intervals_grow_and_then_stay_at_the_cap(self):
        job = self.submit()
        self.timeline.at(6, job, JobState.COMPLETED)

        self.wait_all([job])

        self.assertEqual(self.timeline.sleeps, [5, 10, 20, 30, 30, 30])

    def test_a_timeout_raises_with_the_latest_statuses_and_leaves_jobs_alone(self):
        job = self.submit()
        self.timeline.at(1, job, JobState.RUNNING)

        with self.assertRaises(tminus.WaitTimeout) as caught:
            self.wait_all([job], timeout=12)

        self.assertIs(caught.exception.statuses[job.job_id].state, JobState.RUNNING)
        self.assertLessEqual(self.timeline.now, 12)
        self.assertIs(job.refresh().state, JobState.RUNNING)

    def test_state_transitions_seen_while_waiting_are_recorded_once_each(self):
        job = self.submit()
        self.timeline.at(2, job, JobState.RUNNING).at(4, job, JobState.COMPLETED, exit_code=0)

        self.wait_all([job])

        history = self.store.history(self.backend.name, job.job_id)
        self.assertEqual([(h.state, h.seen_at) for h in history], [
            (JobState.PENDING, 0.0),
            (JobState.RUNNING, 15.0),
            (JobState.COMPLETED, 65.0),
        ])

    def test_a_job_the_scheduler_has_never_heard_of_raises(self):
        stranger = tminus.Job(job_id="424242", spec=None, backend=self.backend)

        with self.assertRaises(tminus.JobNotFound):
            self.wait_all([stranger])


if __name__ == "__main__":
    unittest.main()
