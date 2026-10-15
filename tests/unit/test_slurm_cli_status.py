from __future__ import annotations

import unittest

from tminus import JobState, SlurmCommandError, SlurmOutputError
from tminus.backend.slurm_cli import SlurmCliBackend

from tests.helpers import FIXTURE_VERSIONS, ScriptedRunner, fixture

INVALID_JOB_ID = (1, "", "slurm_load_jobs error: Invalid job id specified\n")


class SlurmCliStatusTest(unittest.TestCase):
    """Runs every test against each recorded Slurm version's output."""

    def runner(self, version, **overrides):
        replies = {"squeue": fixture(version, "squeue.json"), "sacct": fixture(version, "sacct.json")}
        replies.update(overrides)
        return ScriptedRunner(**replies)

    def test_queued_jobs_are_read_from_squeue(self):
        for version in FIXTURE_VERSIONS:
            with self.subTest(version=version):
                runner = self.runner(version)

                statuses = SlurmCliBackend(runner=runner).status(["1001", "1002"])

                self.assertEqual(runner.calls, [["squeue", "--json", "--jobs=1001,1002"]])
                self.assertIs(statuses["1001"].state, JobState.RUNNING)
                self.assertIsNone(statuses["1001"].reason)
                self.assertIsNone(statuses["1001"].exit_code)
                self.assertIs(statuses["1002"].state, JobState.PENDING)
                self.assertEqual(statuses["1002"].reason, "Priority")

    def test_jobs_that_left_the_queue_are_read_from_sacct(self):
        for version in FIXTURE_VERSIONS:
            with self.subTest(version=version):
                runner = self.runner(version)

                statuses = SlurmCliBackend(runner=runner).status(["1001", "1003", "1004", "1005"])

                self.assertEqual(runner.calls[1], ["sacct", "--json", "--jobs=1003,1004,1005"])
                self.assertIs(statuses["1001"].state, JobState.RUNNING)
                self.assertIs(statuses["1003"].state, JobState.COMPLETED)
                self.assertEqual(statuses["1003"].exit_code, 0)
                self.assertIs(statuses["1004"].state, JobState.FAILED)
                self.assertEqual(statuses["1004"].exit_code, 3)
                self.assertEqual(statuses["1004"].reason, "NonZeroExitCode")
                self.assertIs(statuses["1005"].state, JobState.TIMEOUT)

    def test_tags_are_read_back_from_the_job_comment(self):
        for version in FIXTURE_VERSIONS:
            with self.subTest(version=version):
                runner = self.runner(version)

                statuses = SlurmCliBackend(runner=runner).status(["1001", "1002", "1003", "1004"])

                self.assertEqual(statuses["1001"].tags, ["exp42", "run1"])
                self.assertEqual(statuses["1002"].tags, [])  # an ordinary, non-tminus comment
                self.assertEqual(statuses["1003"].tags, ["exp=exp42"])  # written by tminus 0.1.0
                self.assertEqual(statuses["1004"].tags, [])

    def test_sacct_is_not_called_when_squeue_knows_every_job(self):
        runner = self.runner(FIXTURE_VERSIONS[0])

        SlurmCliBackend(runner=runner).status(["1002"])

        self.assertEqual(runner.programs_called(), ["squeue"])

    def test_jobs_neither_tool_knows_are_left_out(self):
        runner = self.runner(FIXTURE_VERSIONS[0])

        statuses = SlurmCliBackend(runner=runner).status(["1001", "9999"])

        self.assertEqual(set(statuses), {"1001"})

    def test_an_invalid_job_id_from_squeue_falls_back_to_sacct(self):
        # squeue errors out when a requested job has been purged from slurmctld's memory.
        runner = self.runner(FIXTURE_VERSIONS[0], squeue=INVALID_JOB_ID)

        statuses = SlurmCliBackend(runner=runner).status(["1003"])

        self.assertIs(statuses["1003"].state, JobState.COMPLETED)

    def test_other_squeue_failures_are_raised(self):
        runner = self.runner(FIXTURE_VERSIONS[0], squeue=(1, "", "slurm_load_jobs error: Unable to contact slurm controller\n"))

        with self.assertRaises(SlurmCommandError):
            SlurmCliBackend(runner=runner).status(["1001"])

    def test_output_that_is_not_slurm_json_raises_an_output_error(self):
        for bad in ("JOBID PARTITION NAME\n1001 gpu train\n", '{"meta": {}}'):
            with self.subTest(output=bad):
                runner = self.runner(FIXTURE_VERSIONS[0], squeue=bad)

                with self.assertRaises(SlurmOutputError):
                    SlurmCliBackend(runner=runner).status(["1001"])

    def test_an_empty_request_runs_nothing(self):
        runner = self.runner(FIXTURE_VERSIONS[0])

        self.assertEqual(SlurmCliBackend(runner=runner).status([]), {})
        self.assertEqual(runner.calls, [])


if __name__ == "__main__":
    unittest.main()
