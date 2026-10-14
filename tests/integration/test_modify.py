from __future__ import annotations

import os
import shutil
import tempfile
import unittest

import tminus
from tminus import JobState
from tminus.backend.fake import FakeBackend
from tminus.store import Store


class ModifyTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="tminus-test-")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.store = Store(os.path.join(self.tmp, "jobs.db"))
        self.backend = FakeBackend()
        self.job = tminus.submit(tminus.JobSpec(command="sleep 600"), backend=self.backend, store=self.store)

    def test_cancelling_an_active_job_cancels_it(self):
        acted = self.job.cancel(store=self.store)

        self.assertTrue(acted)
        self.assertIs(self.job.refresh().state, JobState.CANCELLED)

    def test_cancelling_a_finished_job_does_nothing(self):
        self.backend.set_state(self.job.job_id, JobState.COMPLETED, exit_code=0)

        acted = self.job.cancel(store=self.store)

        self.assertFalse(acted)
        self.assertIs(self.job.refresh().state, JobState.COMPLETED)

    def test_hold_and_release(self):
        self.job.hold(store=self.store)
        held = self.job.refresh()
        self.job.release(store=self.store)
        released = self.job.refresh()

        self.assertIs(held.state, JobState.PENDING)
        self.assertEqual(held.reason, "JobHeldUser")
        self.assertIs(released.state, JobState.PENDING)
        self.assertIsNone(released.reason)

    def test_requeue_puts_a_job_back_in_the_queue(self):
        self.backend.set_state(self.job.job_id, JobState.RUNNING)

        self.job.requeue(store=self.store)

        self.assertIs(self.job.refresh().state, JobState.PENDING)

    def test_update_changes_the_allowed_fields(self):
        self.job.update(time_limit="04:00:00", partition="long", store=self.store)

        self.assertEqual(self.backend.updates[self.job.job_id], {"time_limit": "04:00:00", "partition": "long"})

    def test_updating_an_unsupported_field_raises_before_anything_is_sent(self):
        with self.assertRaises(tminus.InvalidUpdateError):
            self.job.update(time_limit="04:00:00", colour="blue", store=self.store)

        self.assertNotIn(self.job.job_id, self.backend.updates)

    def test_every_modification_is_recorded_in_the_store(self):
        self.job.hold(store=self.store)
        self.job.update(time_limit="04:00:00", store=self.store)
        self.job.release(store=self.store)
        self.job.cancel(store=self.store)

        events = self.store.events(self.backend.name, self.job.job_id)

        self.assertEqual([(e.action, e.detail) for e in events], [
            ("hold", None),
            ("update", "time_limit=04:00:00"),
            ("release", None),
            ("cancel", None),
        ])


if __name__ == "__main__":
    unittest.main()
