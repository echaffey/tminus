from __future__ import annotations

import unittest

from tminus import JobSpec
from tminus.backend.runner import CommandResult
from tminus.backend.slurm_cli import SlurmCliBackend
from tminus.errors import SlurmCommandError, SubmitError


class RecordingRunner:
    """Stands in for the subprocess boundary: records commands, replays a canned result."""

    def __init__(self, returncode=0, stdout="", stderr=""):
        self.calls = []
        self._returncode = returncode
        self._stdout = stdout
        self._stderr = stderr

    def run(self, args, timeout=None):
        self.calls.append(list(args))
        return CommandResult(list(args), self._returncode, self._stdout, self._stderr)


class SbatchSubmitTest(unittest.TestCase):
    def test_a_command_is_wrapped_and_its_job_id_returned(self):
        runner = RecordingRunner(stdout="4242\n")

        job_id = SlurmCliBackend(runner=runner).submit(JobSpec(command="python train.py"))

        self.assertEqual(job_id, "4242")
        self.assertEqual(runner.calls, [["sbatch", "--parsable", "--wrap=python train.py"]])

    def test_every_spec_field_becomes_an_sbatch_option_before_the_script(self):
        runner = RecordingRunner(stdout="7\n")
        spec = JobSpec(
            script="/home/u/run.sh",
            name="train",
            partition="gpu",
            account="lab",
            qos="high",
            nodes=2,
            ntasks=8,
            cpus=4,
            mem="16G",
            time="02:00:00",
            workdir="/scratch/u/exp",
            output="logs/%j.out",
            error="logs/%j.err",
            extra_args=["--exclusive", "--gres=gpu:2"],
        )

        SlurmCliBackend(runner=runner).submit(spec)

        self.assertEqual(
            runner.calls,
            [[
                "sbatch",
                "--parsable",
                "--job-name=train",
                "--partition=gpu",
                "--account=lab",
                "--qos=high",
                "--nodes=2",
                "--ntasks=8",
                "--cpus-per-task=4",
                "--mem=16G",
                "--time=02:00:00",
                "--chdir=/scratch/u/exp",
                "--output=logs/%j.out",
                "--error=logs/%j.err",
                "--exclusive",
                "--gres=gpu:2",
                "/home/u/run.sh",
            ]],
        )

    def test_tags_are_copied_into_the_job_comment(self):
        runner = RecordingRunner(stdout="7\n")

        SlurmCliBackend(runner=runner).submit(JobSpec(command="true", tags={"run": "1", "exp": "exp42"}))

        self.assertIn("--comment=tminus:exp=exp42,run=1", runner.calls[0])

    def test_the_cluster_name_is_dropped_from_parsable_output(self):
        runner = RecordingRunner(stdout="4242;cluster-a\n")

        job_id = SlurmCliBackend(runner=runner).submit(JobSpec(command="true"))

        self.assertEqual(job_id, "4242")

    def test_a_rejected_submission_raises_with_the_command_exit_code_and_stderr(self):
        stderr = "sbatch: error: invalid partition specified: nope\n"
        runner = RecordingRunner(returncode=1, stderr=stderr)

        with self.assertRaises(SlurmCommandError) as caught:
            SlurmCliBackend(runner=runner).submit(JobSpec(command="true", partition="nope"))

        self.assertEqual(caught.exception.command[0], "sbatch")
        self.assertEqual(caught.exception.returncode, 1)
        self.assertEqual(caught.exception.stderr, stderr)
        self.assertIn("invalid partition", str(caught.exception))

    def test_output_that_is_not_a_job_id_raises_a_submit_error(self):
        runner = RecordingRunner(stdout="Submitted batch job 12\n")

        with self.assertRaises(SubmitError):
            SlurmCliBackend(runner=runner).submit(JobSpec(command="true"))


if __name__ == "__main__":
    unittest.main()
