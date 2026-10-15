from __future__ import annotations

import os
import shutil
import tempfile
import unittest

import tminus
from tminus import JobState
from tminus.backend.fake import FakeBackend
from tminus.cleanup import parse_age
from tminus.store import Store

DAY = 86400.0


class PruneTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="tminus-test-")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.now = 0.0
        self.store = Store(os.path.join(self.tmp, "jobs.db"), clock=lambda: self.now)
        self.backend = FakeBackend()

    def submit_at(self, day, state=None, tags=None):
        self.now = day * DAY
        job = tminus.submit(tminus.JobSpec(command="true", tags=tags or []), backend=self.backend, store=self.store)
        if state is not None:
            self.backend.set_state(job.job_id, state)
        return job

    def tracked_ids(self, backend=None):
        return [job.job_id for job in tminus.jobs(backend=backend or self.backend, store=self.store)]

    def prune(self, older_than, **options):
        self.now = 40 * DAY
        return tminus.prune(older_than, backend=self.backend, store=self.store, **options)

    def test_finished_jobs_submitted_before_the_cutoff_are_removed(self):
        old_done = self.submit_at(1, JobState.COMPLETED)
        old_failed = self.submit_at(2, JobState.FAILED)
        recent_done = self.submit_at(35, JobState.COMPLETED)

        removed = self.prune("30d")

        self.assertEqual(removed, [old_done.job_id, old_failed.job_id])
        self.assertEqual(self.tracked_ids(), [recent_done.job_id])

    def test_active_jobs_are_never_removed_whatever_their_age(self):
        old_running = self.submit_at(1, JobState.RUNNING)
        old_pending = self.submit_at(2)

        removed = self.prune("30d")

        self.assertEqual(removed, [])
        self.assertEqual(self.tracked_ids(), [old_running.job_id, old_pending.job_id])

    def test_jobs_the_scheduler_has_forgotten_count_as_finished(self):
        forgotten = self.submit_at(1)
        self.now = 40 * DAY

        removed = tminus.prune("30d", backend=_ForgetfulFake(), store=self.store)

        self.assertEqual(removed, [forgotten.job_id])

    def test_a_tag_limits_pruning_to_matching_jobs(self):
        exp = self.submit_at(1, JobState.COMPLETED, tags=["exp42"])
        other = self.submit_at(1, JobState.COMPLETED, tags=["other"])

        removed = self.prune("30d", tags=["exp42"])

        self.assertEqual(removed, [exp.job_id])
        self.assertEqual(self.tracked_ids(), [other.job_id])

    def test_a_dry_run_reports_without_removing_anything(self):
        old_done = self.submit_at(1, JobState.COMPLETED)

        removed = self.prune("30d", dry_run=True)

        self.assertEqual(removed, [old_done.job_id])
        self.assertEqual(self.tracked_ids(), [old_done.job_id])

    def test_history_and_events_go_with_the_job(self):
        job = self.submit_at(1)
        job.hold(store=self.store)
        self.backend.set_state(job.job_id, JobState.COMPLETED)
        job.wait(store=self.store, sleep=lambda s: None)

        self.prune("30d")

        self.assertEqual(self.store.history(self.backend.name, job.job_id), [])
        self.assertEqual(self.store.events(self.backend.name, job.job_id), [])


class _ForgetfulFake(FakeBackend):
    """A scheduler whose records have been purged: it knows no jobs at all."""

    def status(self, job_ids):
        return {}


class ParseAgeTest(unittest.TestCase):
    def test_ages_with_units(self):
        for text, seconds in (("45s", 45), ("90m", 5400), ("12h", 43200), ("30d", 2592000),
                              ("2w", 1209600), ("1.5d", 129600)):
            with self.subTest(text=text):
                self.assertEqual(parse_age(text), seconds)

    def test_invalid_ages_are_rejected(self):
        for text in ("", "30", "abc", "10y", "-3d", "d"):
            with self.subTest(text=text):
                with self.assertRaises(tminus.TminusError):
                    parse_age(text)


if __name__ == "__main__":
    unittest.main()
