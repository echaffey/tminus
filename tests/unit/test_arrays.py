from __future__ import annotations

import unittest

import tminus
from tminus import JobState, JobStatus
from tminus.backend.slurm_cli import SlurmCliBackend, sbatch_args

from tests.helpers import FIXTURE_VERSIONS, ScriptedRunner, fixture


class ArraySpecTest(unittest.TestCase):
    def test_valid_array_specs_are_passed_to_sbatch(self):
        for array in ("0-9", "0-9%4", "1,3,5", "0-15:4", "1-3,7,10-20:5%2"):
            with self.subTest(array=array):
                self.assertIn(f"--array={array}", sbatch_args(tminus.JobSpec(command="true", array=array)))

    def test_invalid_array_specs_are_rejected(self):
        for array in ("", "a-b", "9-0", "0-9%", "%4", "1,,2", "0-9:0"):
            with self.subTest(array=array):
                with self.assertRaises(tminus.InvalidSpecError):
                    tminus.JobSpec(command="true", array=array)

    def test_task_ids_lists_every_task_in_order(self):
        spec = tminus.JobSpec(command="true", array="1-3,7,10-20:5%2")

        self.assertEqual(spec.task_ids(), ["1", "2", "3", "7", "10", "15", "20"])


def task_statuses(*states):
    return {f"9_{i}": JobStatus(f"9_{i}", state) for i, state in enumerate(states)}


class ArrayStateRulesTest(unittest.TestCase):
    def combined(self, *states):
        return JobStatus.for_array("9", task_statuses(*states)).state

    def test_an_array_is_running_while_any_task_runs(self):
        self.assertIs(self.combined(JobState.COMPLETED, JobState.RUNNING, JobState.PENDING), JobState.RUNNING)

    def test_an_array_is_pending_while_unfinished_tasks_are_all_waiting(self):
        self.assertIs(self.combined(JobState.COMPLETED, JobState.PENDING), JobState.PENDING)

    def test_an_array_completes_only_when_every_task_completed(self):
        self.assertIs(self.combined(JobState.COMPLETED, JobState.COMPLETED), JobState.COMPLETED)

    def test_a_finished_array_takes_the_state_its_unsuccessful_tasks_share(self):
        self.assertIs(self.combined(JobState.COMPLETED, JobState.TIMEOUT, JobState.TIMEOUT), JobState.TIMEOUT)
        self.assertIs(self.combined(JobState.CANCELLED, JobState.COMPLETED), JobState.CANCELLED)

    def test_a_finished_array_with_mixed_failures_is_failed(self):
        self.assertIs(self.combined(JobState.COMPLETED, JobState.TIMEOUT, JobState.OUT_OF_MEMORY), JobState.FAILED)

    def test_counts_by_state(self):
        status = JobStatus.for_array("9", task_statuses(JobState.COMPLETED, JobState.FAILED, JobState.COMPLETED))

        self.assertEqual(status.task_counts(), {JobState.COMPLETED: 2, JobState.FAILED: 1})


class SlurmArrayStatusTest(unittest.TestCase):
    def runner(self, version):
        return ScriptedRunner(squeue=fixture(version, "squeue.json"), sacct=fixture(version, "sacct.json"))

    def test_a_partly_finished_array_merges_squeue_and_sacct(self):
        for version in FIXTURE_VERSIONS:
            with self.subTest(version=version):
                runner = self.runner(version)

                status = SlurmCliBackend(runner=runner).status(["2000"])["2000"]

                self.assertEqual(runner.calls, [["squeue", "--json", "--jobs=2000"],
                                                ["sacct", "--json", "--jobs=2000"]])
                self.assertIs(status.state, JobState.RUNNING)
                self.assertEqual({task_id: s.state for task_id, s in status.tasks.items()}, {
                    "2000_0": JobState.COMPLETED,
                    "2000_1": JobState.FAILED,
                    "2000_2": JobState.RUNNING,
                    "2000_3": JobState.PENDING,
                })
                self.assertEqual(status.tasks["2000_1"].exit_code, 2)
                self.assertEqual(status.tasks["2000_3"].reason, "JobArrayTaskLimit")
                self.assertEqual(status.tags, ["arrays"])

    def test_a_finished_array_is_read_from_sacct(self):
        for version in FIXTURE_VERSIONS:
            with self.subTest(version=version):
                status = SlurmCliBackend(runner=self.runner(version)).status(["2010"])["2010"]

                self.assertIs(status.state, JobState.COMPLETED)
                self.assertEqual(sorted(status.tasks), ["2010_0", "2010_1"])

    def test_a_single_task_can_be_asked_for_by_its_id(self):
        for version in FIXTURE_VERSIONS:
            with self.subTest(version=version):
                statuses = SlurmCliBackend(runner=self.runner(version)).status(["2000_1", "2000_2"])

                self.assertIs(statuses["2000_1"].state, JobState.FAILED)
                self.assertIs(statuses["2000_2"].state, JobState.RUNNING)


if __name__ == "__main__":
    unittest.main()
