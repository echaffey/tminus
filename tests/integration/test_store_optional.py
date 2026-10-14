from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from unittest import mock

import tminus
from tminus import JobState, Policy
from tminus.backend import BACKENDS
from tminus.backend.fake import FakeBackend

from tests.integration.test_cli import run_cli


class WithoutAStoreTest(unittest.TestCase):
    """With TMINUS_DB unset and no store passed, tminus keeps no record and writes no files."""

    def setUp(self):
        self.home = os.path.realpath(tempfile.mkdtemp(prefix="tminus-home-"))
        self.addCleanup(shutil.rmtree, self.home, ignore_errors=True)
        env = {k: v for k, v in os.environ.items() if k != "TMINUS_DB"}
        env.update(HOME=self.home, USERPROFILE=self.home, XDG_DATA_HOME=os.path.join(self.home, "data"))
        patcher = mock.patch.dict(os.environ, env, clear=True)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.backend = FakeBackend()

    def submit(self, **fields):
        fields.setdefault("command", "true")
        return tminus.submit(tminus.JobSpec(**fields), backend=self.backend)

    def files_written(self):
        return [os.path.join(d, f) for d, _, files in os.walk(self.home) for f in files]

    def test_submitting_waiting_and_modifying_write_nothing(self):
        job = self.submit(tags={"exp": "x"})
        job.hold()
        job.update(time_limit="01:00:00")
        job.release()
        self.backend.set_state(job.job_id, JobState.COMPLETED, exit_code=0)

        status = job.wait(sleep=lambda seconds: None)

        self.assertIs(status.state, JobState.COMPLETED)
        self.assertEqual(self.files_written(), [])

    def test_a_script_can_report_on_and_clean_up_the_jobs_it_holds(self):
        workdir = os.path.join(self.home, "work")
        os.makedirs(workdir)
        done = self.submit(workdir=workdir, output="%j.out")
        running = self.submit()
        self.backend.set_state(done.job_id, JobState.COMPLETED, exit_code=0)
        self.backend.set_state(running.job_id, JobState.RUNNING)
        log = os.path.join(workdir, f"{done.job_id}.out")
        open(log, "w").close()

        report = tminus.report(jobs=[done, running], backend=self.backend)
        cancelled = tminus.cleanup.cancel(jobs=[done, running], backend=self.backend)
        purged = tminus.cleanup.purge(jobs=[done, running], policy=Policy.ON_SUCCESS, backend=self.backend)

        self.assertEqual([u.job_id for u in report.jobs], [done.job_id, running.job_id])
        self.assertEqual(cancelled, [running.job_id])
        self.assertEqual([r.deleted for r in purged], [[log]])
        self.assertFalse(os.path.exists(log))

    def test_looking_jobs_up_by_tag_explains_how_to_turn_the_record_on(self):
        for lookup in (lambda: tminus.jobs(tags={"exp": "x"}, backend=self.backend),
                       lambda: tminus.report(tags={"exp": "x"}, backend=self.backend),
                       lambda: tminus.cleanup.cancel({"exp": "x"}, backend=self.backend),
                       lambda: tminus.prune("30d", backend=self.backend)):
            with self.assertRaises(tminus.StoreNotEnabled) as caught:
                lookup()
            self.assertIn("TMINUS_DB", str(caught.exception))

    def test_cleanup_needs_tags_or_jobs(self):
        with self.assertRaises(tminus.TminusError):
            tminus.cleanup.purge(policy=Policy.ALWAYS, backend=self.backend)

    def test_the_cli_works_by_job_id_and_explains_tag_lookups(self):
        with mock.patch.dict(BACKENDS, {"fake": lambda: self.backend}), \
                mock.patch.dict(os.environ, {"TMINUS_BACKEND": "fake"}):
            submit_status, out, _ = run_cli("submit", "--tag", "exp=x", "--wrap", "true",
                                            env={"TMINUS_BACKEND": "fake"})
            status_status, status_out, _ = run_cli("status", out.strip(), env={"TMINUS_BACKEND": "fake"})
            tag_status, _, tag_err = run_cli("status", "--tag", "exp=x", env={"TMINUS_BACKEND": "fake"})

        self.assertEqual(submit_status, 0)
        self.assertEqual(status_status, 0)
        self.assertIn("PENDING", status_out)
        self.assertEqual(tag_status, 1)
        self.assertIn("TMINUS_DB", tag_err)
        self.assertEqual(self.files_written(), [])


class WithAStoreTest(unittest.TestCase):
    def test_setting_tminus_db_turns_the_record_on(self):
        directory = tempfile.mkdtemp(prefix="tminus-test-")
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        db = os.path.join(directory, "jobs.db")
        backend = FakeBackend()
        with mock.patch.dict(os.environ, {"TMINUS_DB": db}):
            job = tminus.submit(tminus.JobSpec(command="true", tags={"exp": "x"}), backend=backend)

            found = tminus.jobs(tags={"exp": "x"}, backend=backend)

        self.assertTrue(os.path.exists(db))
        self.assertEqual([j.job_id for j in found], [job.job_id])


if __name__ == "__main__":
    unittest.main()
