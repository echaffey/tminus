from __future__ import annotations

import os
import shutil
import tempfile
import unittest
from unittest import mock

import tminus
from tminus import JobState, Policy
from tminus.store import Store

from tests.helpers import CountingFake


class CleanupTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = os.path.realpath(tempfile.mkdtemp(prefix="tminus-test-"))
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.store = Store(os.path.join(self.tmp, "jobs.db"))
        self.backend = CountingFake()
        self.workdir = os.path.join(self.tmp, "work")
        os.makedirs(os.path.join(self.workdir, "logs"))

    def submit(self, state=None, tags=None, **fields):
        fields.setdefault("command", "true")
        job = tminus.submit(tminus.JobSpec(tags=tags or {"exp": "exp42"}, **fields),
                            backend=self.backend, store=self.store)
        if state is not None:
            self.backend.set_state(job.job_id, state)
        return job

    def state_of(self, job):
        return self.backend.status([job.job_id])[job.job_id].state


class CancelByTagTest(CleanupTestCase):
    def test_every_active_job_with_the_tag_is_cancelled_in_one_call(self):
        pending, running = self.submit(), self.submit(JobState.RUNNING)
        done = self.submit(JobState.COMPLETED)
        other = self.submit(JobState.RUNNING, tags={"exp": "other"})

        cancelled = tminus.cleanup.cancel({"exp": "exp42"}, backend=self.backend, store=self.store)

        self.assertEqual(cancelled, [pending.job_id, running.job_id])
        self.assertIs(self.state_of(pending), JobState.CANCELLED)
        self.assertIs(self.state_of(running), JobState.CANCELLED)
        self.assertIs(self.state_of(done), JobState.COMPLETED)
        self.assertIs(self.state_of(other), JobState.RUNNING)

    def test_a_dry_run_cancels_nothing(self):
        pending = self.submit()

        cancelled = tminus.cleanup.cancel({"exp": "exp42"}, backend=self.backend, store=self.store, dry_run=True)

        self.assertEqual(cancelled, [pending.job_id])
        self.assertIs(self.state_of(pending), JobState.PENDING)


class PurgeTest(CleanupTestCase):
    def submit_with_files(self, state):
        job = self.submit(state, workdir=self.workdir, output="logs/%j.out", error="logs/%x-%j.err",
                          name="train", scratch_paths=["scratch/%j"])
        paths = [os.path.join(self.workdir, "logs", f"{job.job_id}.out"),
                 os.path.join(self.workdir, "logs", f"train-{job.job_id}.err"),
                 os.path.join(self.workdir, "scratch", job.job_id)]
        for path in paths[:2]:
            open(path, "w").close()
        os.makedirs(os.path.join(paths[2], "nested"))
        return job, paths

    def purge(self, policy, **options):
        return tminus.cleanup.purge({"exp": "exp42"}, policy, backend=self.backend, store=self.store, **options)

    def test_each_policy_deletes_exactly_the_expected_jobs_files(self):
        expectations = {
            Policy.ALWAYS: {JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED},
            Policy.ON_SUCCESS: {JobState.COMPLETED},
            Policy.NEVER: set(),
        }
        for policy, purged_states in expectations.items():
            with self.subTest(policy=policy):
                self.backend = CountingFake()  # numbers jobs from 1000 again, so give it fresh files
                self.store = Store(os.path.join(self.tmp, f"{policy.value}.db"))
                self.workdir = os.path.join(self.tmp, f"work-{policy.value}")
                os.makedirs(os.path.join(self.workdir, "logs"))
                made = {state: self.submit_with_files(state) for state in
                        (JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED)}

                self.purge(policy)

                for state, (_, paths) in made.items():
                    for path in paths:
                        self.assertEqual(os.path.exists(path), state not in purged_states, (state, path))

    def test_files_of_active_jobs_are_never_deleted(self):
        _, paths = self.submit_with_files(JobState.RUNNING)

        self.purge(Policy.ALWAYS)

        self.assertTrue(all(os.path.exists(path) for path in paths))

    def test_only_paths_recorded_for_the_job_are_deleted(self):
        _, paths = self.submit_with_files(JobState.COMPLETED)
        unrelated = os.path.join(self.workdir, "logs", "notes.txt")
        open(unrelated, "w").close()

        self.purge(Policy.ALWAYS)

        self.assertTrue(os.path.exists(unrelated))
        self.assertTrue(os.path.isdir(os.path.join(self.workdir, "logs")))
        self.assertTrue(os.path.isdir(self.workdir))

    def test_without_an_output_path_slurms_default_log_in_the_submit_directory_is_deleted(self):
        previous = os.getcwd()
        os.chdir(self.tmp)
        self.addCleanup(os.chdir, previous)
        job = self.submit(JobState.COMPLETED)
        default_log = os.path.join(self.tmp, f"slurm-{job.job_id}.out")
        open(default_log, "w").close()

        self.purge(Policy.ALWAYS)

        self.assertFalse(os.path.exists(default_log))

    def test_paths_with_patterns_tminus_cannot_expand_are_left_and_reported(self):
        job = self.submit(JobState.COMPLETED, workdir=self.workdir, output="logs/%N-%j.out")

        results = self.purge(Policy.ALWAYS)

        self.assertEqual(results[0].job_id, job.job_id)
        self.assertEqual(results[0].skipped, [os.path.join(self.workdir, "logs", "%N-%j.out")])

    def test_the_home_directory_and_filesystem_root_are_never_deleted(self):
        home = os.path.join(self.tmp, "home")
        os.makedirs(home)
        with mock.patch.dict(os.environ, {"HOME": home, "USERPROFILE": home}):
            self.submit(JobState.COMPLETED, workdir=self.workdir, output="logs/x.out",
                        scratch_paths=[home, os.path.dirname(home), os.path.abspath(os.sep)])

            results = self.purge(Policy.ALWAYS)

        self.assertTrue(os.path.isdir(home))
        self.assertEqual(len(results[0].refused), 3)

    def test_a_dry_run_lists_paths_without_deleting_them(self):
        _, paths = self.submit_with_files(JobState.COMPLETED)

        results = self.purge(Policy.ALWAYS, dry_run=True)

        self.assertEqual(sorted(results[0].deleted), sorted(paths))
        self.assertTrue(all(os.path.exists(path) for path in paths))

    def test_purging_is_recorded_against_the_job(self):
        job, _ = self.submit_with_files(JobState.COMPLETED)

        self.purge(Policy.ALWAYS)

        self.assertEqual([event.action for event in self.store.events(self.backend.name, job.job_id)], ["purge"])


if __name__ == "__main__":
    unittest.main()
