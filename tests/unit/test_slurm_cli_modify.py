from __future__ import annotations

import unittest

from tminus import SlurmCommandError
from tminus.backend.slurm_cli import SlurmCliBackend

from tests.helpers import ScriptedRunner


class SlurmCliModifyTest(unittest.TestCase):
    def setUp(self):
        self.runner = ScriptedRunner(scancel="", scontrol="")
        self.backend = SlurmCliBackend(runner=self.runner)

    def test_cancel_passes_every_job_to_one_scancel(self):
        self.backend.cancel(["1001", "1002"])

        self.assertEqual(self.runner.calls, [["scancel", "1001", "1002"]])

    def test_hold_release_and_requeue_use_scontrol_with_a_job_list(self):
        self.backend.hold(["1001", "1002"])
        self.backend.release(["1001"])
        self.backend.requeue(["1002"])

        self.assertEqual(self.runner.calls, [
            ["scontrol", "hold", "1001,1002"],
            ["scontrol", "release", "1001"],
            ["scontrol", "requeue", "1002"],
        ])

    def test_update_sends_each_change_as_a_scontrol_field(self):
        self.backend.update("1001", {"time_limit": "02:00:00", "partition": "long", "name": "retry",
                                     "qos": "high", "account": "lab", "nodes": 2})

        self.assertEqual(self.runner.calls, [[
            "scontrol", "update", "JobId=1001", "TimeLimit=02:00:00", "Partition=long",
            "JobName=retry", "QOS=high", "Account=lab", "NumNodes=2",
        ]])

    def test_cancelling_a_job_that_finished_in_the_meantime_is_not_an_error(self):
        stderr = "scancel: error: Kill job error on job id 1001: Job/step already completing or completed\n"
        backend = SlurmCliBackend(runner=ScriptedRunner(scancel=(1, "", stderr)))

        backend.cancel(["1001"])

    def test_other_scontrol_failures_are_raised(self):
        stderr = "Job is no longer pending execution for job 1001\n"
        backend = SlurmCliBackend(runner=ScriptedRunner(scontrol=(1, "", stderr)))

        with self.assertRaises(SlurmCommandError):
            backend.update("1001", {"partition": "long"})


if __name__ == "__main__":
    unittest.main()
