from __future__ import annotations

import asyncio
import os
import shutil
import tempfile
import unittest

import tminus
from tminus import DependencyType, JobState, PollPolicy, aio
from tminus.backend.fake import FakeBackend
from tminus.backend.slurm_cli import sbatch_args
from tminus.store import Store


class DependencySpecTest(unittest.TestCase):
    def test_dependencies_become_one_sbatch_dependency_option(self):
        spec = tminus.JobSpec(command="true", depends_on=["1001", "1002"])

        self.assertIn("--dependency=afterok:1001:1002", sbatch_args(spec))

    def test_each_dependency_type_is_passed_through(self):
        for kind, expected in ((DependencyType.AFTER, "after"), (DependencyType.AFTERANY, "afterany"),
                               (DependencyType.AFTERNOTOK, "afternotok"), ("afterok", "afterok")):
            with self.subTest(kind=kind):
                spec = tminus.JobSpec(command="true", depends_on=["7"], dependency_type=kind)

                self.assertIn(f"--dependency={expected}:7", sbatch_args(spec))

    def test_job_handles_are_accepted_as_dependencies(self):
        backend = FakeBackend()
        first = tminus.submit(tminus.JobSpec(command="true"), backend=backend)

        spec = tminus.JobSpec(command="true", depends_on=[first])

        self.assertEqual(spec.depends_on, [first.job_id])

    def test_an_unknown_dependency_type_is_rejected(self):
        with self.assertRaises(tminus.InvalidSpecError):
            tminus.JobSpec(command="true", depends_on=["7"], dependency_type="whenever")

    def test_no_dependency_option_without_dependencies(self):
        self.assertFalse(any(a.startswith("--dependency") for a in sbatch_args(tminus.JobSpec(command="true"))))


class DependencyBehaviourTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="tminus-test-")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.store = Store(os.path.join(self.tmp, "jobs.db"))
        self.backend = FakeBackend()
        self.first = self.submit()

    def submit(self, **fields):
        return tminus.submit(tminus.JobSpec(command="true", **fields), backend=self.backend, store=self.store)

    def test_a_dependent_job_waits_until_its_dependency_completes(self):
        second = self.submit(depends_on=[self.first])

        waiting = second.refresh()
        self.backend.set_state(self.first.job_id, JobState.COMPLETED, exit_code=0)
        eligible = second.refresh()

        self.assertEqual((waiting.state, waiting.reason), (JobState.PENDING, "Dependency"))
        self.assertEqual((eligible.state, eligible.reason), (JobState.PENDING, None))

    def test_afterany_is_satisfied_by_any_finish(self):
        second = self.submit(depends_on=[self.first], dependency_type=DependencyType.AFTERANY)

        self.backend.set_state(self.first.job_id, JobState.FAILED, exit_code=1)

        self.assertIsNone(second.refresh().reason)

    def test_an_afterok_dependency_on_a_failed_job_is_reported_as_never_satisfiable(self):
        second = self.submit(depends_on=[self.first])

        self.backend.set_state(self.first.job_id, JobState.FAILED, exit_code=1)
        status = second.refresh()

        self.assertEqual(status.reason, "DependencyNeverSatisfied")
        self.assertTrue(status.dependency_never_satisfied)

    def test_waiting_on_a_job_that_can_never_start_raises_instead_of_hanging(self):
        second = self.submit(depends_on=[self.first])
        self.backend.set_state(self.first.job_id, JobState.FAILED, exit_code=1)
        options = dict(policy=PollPolicy(initial=1), store=self.store)

        with self.assertRaises(tminus.UnsatisfiableDependency) as caught:
            second.wait(sleep=lambda seconds: None, **options)
        with self.assertRaises(tminus.UnsatisfiableDependency):
            asyncio.run(aio.wait(second, **options))

        self.assertEqual(caught.exception.job_ids, [second.job_id])

    def test_dependencies_survive_the_trip_through_the_store(self):
        second = self.submit(depends_on=[self.first], dependency_type=DependencyType.AFTERANY)

        [found] = [j for j in tminus.jobs(backend=self.backend, store=self.store) if j.job_id == second.job_id]

        self.assertEqual(found.spec.depends_on, [self.first.job_id])
        self.assertIs(found.spec.dependency_type, DependencyType.AFTERANY)


if __name__ == "__main__":
    unittest.main()
