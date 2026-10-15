from __future__ import annotations

import os
import shutil
import tempfile
import unittest

import tminus
from tminus import JobState
from tminus.backend.fake import FakeBackend
from tminus.store import Store

GIB = 1024 ** 3


class ReportApiTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="tminus-test-")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.store = Store(os.path.join(self.tmp, "jobs.db"))
        self.backend = FakeBackend()

    def submit(self, **fields):
        fields.setdefault("command", "true")
        return tminus.submit(tminus.JobSpec(**fields), backend=self.backend, store=self.store)

    def test_a_report_by_tag_covers_exactly_the_tagged_jobs(self):
        done = self.submit(tags=["exp42"])
        self.backend.set_state(done.job_id, JobState.COMPLETED, exit_code=0)
        self.backend.set_usage(done.job_id, elapsed=100, time_limit=200, cpu_seconds=80, cpus=1,
                               peak_memory=GIB, requested_memory=2 * GIB)
        running = self.submit(tags=["exp42"])
        self.backend.set_state(running.job_id, JobState.RUNNING)
        self.submit(tags=["other"])

        report = tminus.report(tags=["exp42"], backend=self.backend, store=self.store)

        self.assertEqual([u.job_id for u in report.jobs], [done.job_id, running.job_id])
        self.assertAlmostEqual(report.jobs[0].cpu_efficiency, 0.8)
        self.assertAlmostEqual(report.jobs[0].memory_efficiency, 0.5)
        self.assertIsNone(report.jobs[1].cpu_efficiency)
        self.assertEqual(report.state_counts(), {JobState.COMPLETED: 1, JobState.RUNNING: 1})

    def test_a_report_can_be_built_for_given_jobs(self):
        job = self.submit()

        report = tminus.report(jobs=[job], backend=self.backend, store=self.store)

        self.assertEqual([u.job_id for u in report.jobs], [job.job_id])
        self.assertIs(report.jobs[0].state, JobState.PENDING)

    def test_an_array_is_reported_per_task(self):
        array = self.submit(array="0-1")

        report = tminus.report(jobs=[array], backend=self.backend, store=self.store)

        self.assertEqual([u.job_id for u in report.jobs], [f"{array.job_id}_0", f"{array.job_id}_1"])


if __name__ == "__main__":
    unittest.main()
