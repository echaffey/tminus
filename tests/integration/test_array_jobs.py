from __future__ import annotations

import os
import shutil
import tempfile
import unittest

import tminus
from tminus import JobState, PollPolicy
from tminus.backend.fake import FakeBackend
from tminus.store import Store

from tests.helpers import Timeline


class ArrayJobTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="tminus-test-")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.store = Store(os.path.join(self.tmp, "jobs.db"))
        self.backend = FakeBackend()
        self.job = tminus.submit(tminus.JobSpec(command="python sweep.py", array="0-3%2"),
                                 backend=self.backend, store=self.store)

    def test_an_array_job_has_one_task_handle_per_task(self):
        self.assertEqual([task.job_id for task in self.job.tasks],
                         [f"{self.job.job_id}_{i}" for i in range(4)])

    def test_each_task_has_its_own_state(self):
        self.backend.set_state(f"{self.job.job_id}_0", JobState.COMPLETED, exit_code=0)
        self.backend.set_state(f"{self.job.job_id}_1", JobState.RUNNING)

        states = [task.refresh().state for task in self.job.tasks]

        self.assertEqual(states, [JobState.COMPLETED, JobState.RUNNING, JobState.PENDING, JobState.PENDING])
        self.assertIs(self.job.refresh().state, JobState.RUNNING)

    def test_wait_returns_once_every_task_is_finished(self):
        timeline = Timeline(self.backend)
        tasks = self.job.tasks
        timeline.at(1, tasks[0], JobState.COMPLETED).at(1, tasks[1], JobState.COMPLETED)
        timeline.at(2, tasks[2], JobState.COMPLETED).at(3, tasks[3], JobState.FAILED, exit_code=1)

        status = self.job.wait(policy=PollPolicy(initial=1), store=self.store,
                               sleep=timeline.sleep, clock=timeline.clock)

        self.assertEqual(len(timeline.sleeps), 3)
        self.assertIs(status.state, JobState.FAILED)
        self.assertEqual(status.task_counts(), {JobState.COMPLETED: 3, JobState.FAILED: 1})

    def test_cancelling_the_array_cancels_every_unfinished_task(self):
        self.backend.set_state(f"{self.job.job_id}_0", JobState.COMPLETED)

        self.job.cancel(store=self.store)

        self.assertEqual([task.refresh().state for task in self.job.tasks],
                         [JobState.COMPLETED, JobState.CANCELLED, JobState.CANCELLED, JobState.CANCELLED])

    def test_a_shared_fake_scheduler_remembers_arrays_across_processes(self):
        path = os.path.join(self.tmp, "fake.json")
        array = tminus.submit(tminus.JobSpec(command="true", array="1-2"),
                              backend=FakeBackend(state_path=path), store=self.store)
        FakeBackend(state_path=path).set_state(f"{array.job_id}_1", JobState.COMPLETED)

        status = FakeBackend(state_path=path).status([array.job_id])[array.job_id]

        self.assertEqual(status.task_counts(), {JobState.COMPLETED: 1, JobState.PENDING: 1})


if __name__ == "__main__":
    unittest.main()
