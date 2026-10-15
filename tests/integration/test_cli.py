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
        _, first, _ = run_cli("submit", "--tag", "exp42", "--wrap", "true", env=self.env)
        _, second, _ = run_cli("submit", "--tag", "exp42", "--wrap", "true", env=self.env)

        status, out, err = run_cli("status", "--tag", "exp42", env=self.env)

        self.assertNotEqual(first.strip(), second.strip())
        self.assertEqual(status, 0, err)
        rows = [line.split()[:2] for line in out.strip().splitlines()[1:]]
        self.assertEqual(rows, [[first.strip(), "PENDING"], [second.strip(), "PENDING"]])


class CleanupCommandTest(unittest.TestCase):
    def setUp(self):
        self.tmp = os.path.realpath(tempfile.mkdtemp(prefix="tminus-test-"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.env = {"TMINUS_BACKEND": "fake", "TMINUS_DB": os.path.join(self.tmp, "jobs.db")}
        self.backend = FakeBackend()
        patcher = mock.patch.dict(BACKENDS, {"fake": lambda: self.backend})
        patcher.start()
        self.addCleanup(patcher.stop)
        store = tminus.Store(self.env["TMINUS_DB"])
        spec = tminus.JobSpec(command="true", tags=["exp42"], workdir=self.tmp, output="%j.out")
        self.running = tminus.submit(spec, backend=self.backend, store=store).job_id
        self.done = tminus.submit(spec, backend=self.backend, store=store).job_id
        self.backend.set_state(self.running, JobState.RUNNING)
        self.backend.set_state(self.done, JobState.COMPLETED)
        self.log = os.path.join(self.tmp, f"{self.done}.out")
        open(self.log, "w").close()

    def test_cancel_by_tag_previews_and_needs_yes(self):
        _, preview, _ = run_cli("cancel", "--tag", "exp42", env=self.env)
        before = self.backend.status([self.running])[self.running].state
        status, out, _ = run_cli("cancel", "--tag", "exp42", "--yes", env=self.env)

        self.assertIn(f"would cancel {self.running}", preview)
        self.assertIs(before, JobState.RUNNING)
        self.assertEqual(status, 0)
        self.assertIn(f"cancelled {self.running}", out)
        self.assertIs(self.backend.status([self.running])[self.running].state, JobState.CANCELLED)

    def test_clean_previews_and_needs_yes(self):
        _, preview, _ = run_cli("clean", "--tag", "exp42", "--policy", "on_success", env=self.env)
        existed = os.path.exists(self.log)
        status, out, _ = run_cli("clean", "--tag", "exp42", "--policy", "on_success", "--yes", env=self.env)

        self.assertIn(f"would delete {self.log}", preview)
        self.assertTrue(existed)
        self.assertEqual(status, 0)
        self.assertIn(f"deleted {self.log}", out)
        self.assertFalse(os.path.exists(self.log))

    def test_clean_requires_a_tag(self):
        status, _, err = run_cli("clean", "--policy", "always", env=self.env)

        self.assertEqual(status, 2)
        self.assertIn("--tag", err)


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
        _, out, _ = run_cli("submit", "--tag", "exp42", "--wrap", "true")
        tagged = out.strip()
        run_cli("submit", "--tag", "other", "--wrap", "true")

        status, out, _ = run_cli("status", "--tag", "exp42")

        self.assertEqual(status, 0)
        job_ids = [line.split()[0] for line in out.strip().splitlines()[1:]]
        self.assertEqual(job_ids, [tagged])

    def test_a_tag_that_matches_no_jobs_is_reported_plainly(self):
        for command in ("status", "wait"):
            with self.subTest(command=command):
                status, _, err = run_cli(command, "--tag", "nothing-has-this")

                self.assertEqual(status, 1)
                self.assertIn("no tracked jobs", err)
                self.assertNotIn("Traceback", err)

    def test_a_tag_that_is_not_a_single_word_is_a_usage_error(self):
        status, _, err = run_cli("submit", "--tag", "two words", "--wrap", "true")

        self.assertEqual(status, 2)
        self.assertIn("single word", err)

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

    def test_submit_can_depend_on_earlier_jobs(self):
        _, out, _ = run_cli("submit", "--depends-on", self.pending, "--dependency-type", "afterany",
                            "--wrap", "true")

        spec = self.backend.submitted[out.strip()]
        self.assertEqual(spec.depends_on, [self.pending])
        self.assertIs(spec.dependency_type, tminus.DependencyType.AFTERANY)
        self.assertEqual(self.backend.status([out.strip()])[out.strip()].reason, "Dependency")

    def test_submit_can_create_an_array_job(self):
        _, out, _ = run_cli("submit", "--array", "0-2%2", "--wrap", "true")

        self.assertEqual(self.backend.submitted[out.strip()].array, "0-2%2")
        self.assertEqual(len(self.backend.status([out.strip()])[out.strip()].tasks), 3)

    def test_an_unknown_dependency_type_is_a_usage_error(self):
        status, _, _ = run_cli("submit", "--depends-on", "1", "--dependency-type", "whenever", "--wrap", "true")

        self.assertEqual(status, 2)

    def test_cancel_cancels_active_jobs_and_skips_finished_ones(self):
        status, out, _ = run_cli("cancel", self.pending, self.failed)

        self.assertEqual(status, 0)
        self.assertIs(self.backend.status([self.pending])[self.pending].state, JobState.CANCELLED)
        self.assertIn(f"cancelled {self.pending}", out)
        self.assertIn(f"{self.failed} already finished", out)

    def test_update_changes_the_requested_fields(self):
        status, _, _ = run_cli("update", self.pending, "--time-limit", "04:00:00", "--partition", "long")

        self.assertEqual(status, 0)
        self.assertEqual(self.backend.updates[self.pending], {"time_limit": "04:00:00", "partition": "long"})

    def test_update_without_any_change_is_a_usage_error(self):
        status, _, err = run_cli("update", self.pending)

        self.assertEqual(status, 1)
        self.assertIn("nothing to update", err)

    def test_hold_and_release(self):
        run_cli("hold", self.pending)
        held = self.backend.status([self.pending])[self.pending]
        run_cli("release", self.pending)
        released = self.backend.status([self.pending])[self.pending]

        self.assertEqual(held.reason, "JobHeldUser")
        self.assertIsNone(released.reason)

    def test_prune_previews_by_default_and_acts_with_yes(self):
        store = tminus.Store(os.environ["TMINUS_DB"])
        store.delete("fake", [self.pending, self.failed])
        old = tminus.submit(tminus.JobSpec(command="true"), backend=self.backend,
                            store=tminus.Store(os.environ["TMINUS_DB"], clock=lambda: 0.0)).job_id  # in 1970
        self.backend.set_state(old, JobState.COMPLETED)

        preview_status, preview, _ = run_cli("prune", "--older-than", "30d")
        still_there = [job.job_id for job in tminus.jobs(backend=self.backend)]
        status, out, _ = run_cli("prune", "--older-than", "30d", "--yes")

        self.assertEqual(preview_status, 0)
        self.assertIn(f"would remove 1 record(s): {old}", preview)
        self.assertIn(old, still_there)
        self.assertEqual(status, 0)
        self.assertIn(f"removed 1 record(s): {old}", out)
        self.assertNotIn(old, [job.job_id for job in tminus.jobs(backend=self.backend)])

    def test_prune_with_an_invalid_age_is_a_usage_error(self):
        status, _, err = run_cli("prune", "--older-than", "soon")

        self.assertEqual(status, 2)
        self.assertIn("invalid age", err)

    def test_status_of_an_array_shows_how_many_tasks_are_in_each_state(self):
        array = tminus.submit(tminus.JobSpec(command="true", array="0-2"), backend=self.backend).job_id
        self.backend.set_state(f"{array}_0", JobState.COMPLETED)

        status, out, _ = run_cli("status", array)

        self.assertEqual(status, 0)
        self.assertIn(f"{array} tasks: COMPLETED=1, PENDING=2", out)

    def test_report_prints_a_table_or_csv_or_json(self):
        for fmt, marker in (("table", "CPU_EFF"), ("csv", "cpu_efficiency,"), ("json", '"summary"')):
            with self.subTest(format=fmt):
                status, out, _ = run_cli("report", self.pending, self.failed, "--format", fmt)

                self.assertEqual(status, 0)
                self.assertIn(marker, out)
                self.assertIn(self.failed, out)

    def test_unknown_jobs_are_reported_and_make_the_exit_status_nonzero(self):
        status, out, err = run_cli("status", self.pending, "424242")

        self.assertEqual(status, 1)
        self.assertIn(self.pending, out)
        self.assertIn("424242", err)


if __name__ == "__main__":
    unittest.main()
