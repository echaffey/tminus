from __future__ import annotations

import unittest

from tminus import JobState


class JobStateFromSlurmTest(unittest.TestCase):
    def test_plain_state_names_map_to_job_states(self):
        self.assertIs(JobState.from_slurm("PENDING"), JobState.PENDING)
        self.assertIs(JobState.from_slurm("RUNNING"), JobState.RUNNING)
        self.assertIs(JobState.from_slurm("COMPLETED"), JobState.COMPLETED)
        self.assertIs(JobState.from_slurm("OUT_OF_MEMORY"), JobState.OUT_OF_MEMORY)
        self.assertIs(JobState.from_slurm("NODE_FAIL"), JobState.NODE_FAIL)

    def test_sacct_cancelled_by_suffix_is_ignored(self):
        self.assertIs(JobState.from_slurm("CANCELLED by 51234"), JobState.CANCELLED)

    def test_newer_slurm_list_form_uses_the_base_state_not_the_flags(self):
        self.assertIs(JobState.from_slurm(["PENDING", "REQUEUED"]), JobState.PENDING)
        self.assertIs(JobState.from_slurm(["COMPLETING", "RUNNING"]), JobState.RUNNING)

    def test_unrecognised_states_map_to_unknown_instead_of_raising(self):
        self.assertIs(JobState.from_slurm("SOMETHING_NEW"), JobState.UNKNOWN)
        self.assertIs(JobState.from_slurm([]), JobState.UNKNOWN)
        self.assertIs(JobState.from_slurm(None), JobState.UNKNOWN)

    def test_finished_states_are_terminal_and_live_ones_are_not(self):
        for state in (JobState.COMPLETED, JobState.FAILED, JobState.CANCELLED,
                      JobState.TIMEOUT, JobState.OUT_OF_MEMORY, JobState.NODE_FAIL):
            with self.subTest(state=state):
                self.assertTrue(state.is_terminal)
        for state in (JobState.PENDING, JobState.RUNNING, JobState.SUSPENDED, JobState.UNKNOWN):
            with self.subTest(state=state):
                self.assertFalse(state.is_terminal)

    def test_only_completed_counts_as_success(self):
        self.assertTrue(JobState.COMPLETED.is_success)
        self.assertFalse(JobState.FAILED.is_success)
        self.assertFalse(JobState.RUNNING.is_success)


if __name__ == "__main__":
    unittest.main()
