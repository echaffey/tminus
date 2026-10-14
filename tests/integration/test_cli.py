from __future__ import annotations

import contextlib
import io
import os
import shutil
import tempfile
import unittest
from unittest import mock

import tminus
from tminus import JobState
from tminus.backend import BACKENDS
from tminus.backend.fake import FakeBackend
from tminus.cli import main


def run_cli(*argv, env=None):
    """Run the CLI in-process; return (exit status, stdout, stderr)."""
    out, err = io.StringIO(), io.StringIO()
    with mock.patch.dict(os.environ, env or {"TMINUS_BACKEND": "fake"}), \
            contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        try:
            status = main(list(argv))
        except SystemExit as exc:
            status = exc.code
    return status, out.getvalue(), err.getvalue()


class SubmitCommandTest(unittest.TestCase):
    def test_submitting_a_wrapped_command_prints_the_job_id(self):
        status, out, _ = run_cli("submit", "--cpus", "4", "--mem", "8G", "--wrap", "echo hi")

        self.assertEqual(status, 0)
        self.assertRegex(out.strip(), r"^\d+$")

    def test_submitting_a_script_prints_the_job_id(self):
        status, out, _ = run_cli("submit", "--partition", "debug", "job.sh")

        self.assertEqual(status, 0)
        self.assertRegex(out.strip(), r"^\d+$")

    def test_submit_without_a_command_or_script_is_a_usage_error(self):
        status, _, err = run_cli("submit", "--cpus", "4")

        self.assertEqual(status, 2)
        self.assertIn("usage", err)

    def test_an_unknown_backend_is_reported_without_a_traceback(self):
        status, _, err = run_cli("submit", "--wrap", "true", env={"TMINUS_BACKEND": "nope"})

        self.assertEqual(status, 1)
        self.assertIn("nope", err)
        self.assertNotIn("Traceback", err)


class FakeBackendAcrossProcessesTest(unittest.TestCase):
    """The fake backend must behave like one scheduler across separate CLI runs."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="tminus-test-")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.env = {"TMINUS_BACKEND": "fake", "TMINUS_DB": os.path.join(self.tmp, "jobs.db")}

    def test_separate_runs_get_distinct_ids_and_see_each_others_jobs(self):
        _, first, _ = run_cli("submit", "--tag", "exp=exp42", "--wrap", "true", env=self.env)
        _, second, _ = run_cli("submit", "--tag", "exp=exp42", "--wrap", "true", env=self.env)

        status, out, err = run_cli("status", "--tag", "exp=exp42", env=self.env)

        self.assertNotEqual(first.strip(), second.strip())
        self.assertEqual(status, 0, err)
        rows = [line.split()[:2] for line in out.strip().splitlines()[1:]]
        self.assertEqual(rows, [[first.strip(), "PENDING"], [second.strip(), "PENDING"]])


class StatusCommandTest(unittest.TestCase):
    def setUp(self):
        # Every CLI run builds its backend afresh, so hand it this prepared one.
        self.backend = FakeBackend()
        patcher = mock.patch.dict(BACKENDS, {"fake": lambda: self.backend})
        patcher.start()
        self.addCleanup(patcher.stop)
        spec = tminus.JobSpec(command="true")
        self.pending = tminus.submit(spec, backend=self.backend).job_id
        self.failed = tminus.submit(spec, backend=self.backend).job_id
        self.backend.set_state(self.failed, JobState.FAILED, reason="NonZeroExitCode", exit_code=3)

    def test_prints_one_row_per_job_with_state_reason_and_exit_code(self):
        status, out, _ = run_cli("status", self.pending, self.failed)

        self.assertEqual(status, 0)
        rows = [line.split() for line in out.strip().splitlines()]
        self.assertEqual(rows[0], ["JOBID", "STATE", "EXIT", "REASON"])
        self.assertEqual(rows[1], [self.pending, "PENDING", "-", "-"])
        self.assertEqual(rows[2], [self.failed, "FAILED", "3", "NonZeroExitCode"])

    def test_status_by_tag_shows_every_tracked_job_with_that_tag(self):
        _, out, _ = run_cli("submit", "--tag", "exp=exp42", "--wrap", "true")
        tagged = out.strip()
        run_cli("submit", "--tag", "exp=other", "--wrap", "true")

        status, out, _ = run_cli("status", "--tag", "exp=exp42")

        self.assertEqual(status, 0)
        job_ids = [line.split()[0] for line in out.strip().splitlines()[1:]]
        self.assertEqual(job_ids, [tagged])

    def test_a_tag_that_matches_no_jobs_is_reported_plainly(self):
        for command in ("status", "wait"):
            with self.subTest(command=command):
                status, _, err = run_cli(command, "--tag", "exp=nothing-has-this")

                self.assertEqual(status, 1)
                self.assertIn("no tracked jobs", err)
                self.assertNotIn("Traceback", err)

    def test_a_tag_without_a_value_is_a_usage_error(self):
        status, _, err = run_cli("status", "--tag", "exp42")

        self.assertEqual(status, 2)
        self.assertIn("KEY=VALUE", err)

    def test_wait_exits_zero_when_every_job_completed(self):
        self.backend.set_state(self.pending, JobState.COMPLETED, exit_code=0)

        status, out, _ = run_cli("wait", self.pending)

        self.assertEqual(status, 0)
        self.assertIn("COMPLETED", out)

    def test_wait_exits_one_when_any_job_did_not_complete(self):
        self.backend.set_state(self.pending, JobState.COMPLETED, exit_code=0)

        status, out, _ = run_cli("wait", self.pending, self.failed)

        self.assertEqual(status, 1)
        self.assertIn("FAILED", out)

    def test_wait_exits_124_when_the_timeout_expires(self):
        status, out, err = run_cli("wait", "--timeout", "0", self.pending)

        self.assertEqual(status, 124)
        self.assertIn("PENDING", out)
        self.assertIn("unfinished", err)

    def test_unknown_jobs_are_reported_and_make_the_exit_status_nonzero(self):
        status, out, err = run_cli("status", self.pending, "424242")

        self.assertEqual(status, 1)
        self.assertIn(self.pending, out)
        self.assertIn("424242", err)


if __name__ == "__main__":
    unittest.main()
