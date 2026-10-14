from __future__ import annotations

import unittest

import tminus
from tminus import JobState
from tminus.backend.fake import FakeBackend


class JobStatusTest(unittest.TestCase):
    def setUp(self):
        self.backend = FakeBackend()
        self.job = tminus.submit(tminus.JobSpec(command="sleep 60"), backend=self.backend)

    def test_a_new_job_is_pending(self):
        status = self.job.refresh()

        self.assertIs(status.state, JobState.PENDING)

    def test_refresh_reports_the_latest_state_and_remembers_it(self):
        self.backend.set_state(self.job.job_id, JobState.FAILED, reason="NonZeroExitCode", exit_code=2)

        status = self.job.refresh()

        self.assertIs(status.state, JobState.FAILED)
        self.assertEqual(status.exit_code, 2)
        self.assertEqual(status.reason, "NonZeroExitCode")
        self.assertIs(self.job.status, status)

    def test_a_job_the_scheduler_has_no_record_of_raises_job_not_found(self):
        stranger = tminus.Job(job_id="424242", spec=None, backend=self.backend)

        with self.assertRaises(tminus.JobNotFound):
            stranger.refresh()

    def test_status_reports_many_jobs_at_once_and_skips_unknown_ones(self):
        other = tminus.submit(tminus.JobSpec(command="true"), backend=self.backend)
        self.backend.set_state(other.job_id, JobState.RUNNING)

        statuses = tminus.status([self.job.job_id, other.job_id, "424242"], backend=self.backend)

        self.assertEqual(set(statuses), {self.job.job_id, other.job_id})
        self.assertIs(statuses[other.job_id].state, JobState.RUNNING)


if __name__ == "__main__":
    unittest.main()
