from __future__ import annotations

import unittest

import tminus
from tminus.backend.fake import FakeBackend


class SubmitTest(unittest.TestCase):
    def test_submitting_a_command_returns_a_job_with_an_id(self):
        job = tminus.submit(tminus.JobSpec(command="echo hi"), backend=FakeBackend())

        self.assertIsInstance(job, tminus.Job)
        self.assertTrue(job.job_id)

    def test_each_submission_gets_its_own_id(self):
        backend = FakeBackend()
        spec = tminus.JobSpec(command="echo hi")

        first = tminus.submit(spec, backend=backend)
        second = tminus.submit(spec, backend=backend)

        self.assertNotEqual(first.job_id, second.job_id)

    def test_a_spec_needs_exactly_one_of_command_or_script(self):
        with self.assertRaises(tminus.InvalidSpecError):
            tminus.JobSpec()
        with self.assertRaises(tminus.InvalidSpecError):
            tminus.JobSpec(command="echo hi", script="job.sh")


if __name__ == "__main__":
    unittest.main()
