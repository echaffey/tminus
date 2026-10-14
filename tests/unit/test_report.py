from __future__ import annotations

import csv
import io
import json
import unittest

from tminus import JobState, JobUsage, Report
from tminus.backend.slurm_cli import SlurmCliBackend

from tests.helpers import FIXTURE_VERSIONS, ScriptedRunner, fixture

GIB = 1024 ** 3


class SlurmUsageTest(unittest.TestCase):
    def usage(self, version, job_ids):
        runner = ScriptedRunner(sacct=fixture(version, "sacct.json"))
        return {u.job_id: u for u in SlurmCliBackend(runner=runner).usage(job_ids)}, runner

    def test_efficiencies_of_finished_jobs_match_hand_calculations(self):
        # Worked by hand from the fixture numbers (see tests/fixtures/README.md).
        expected = {
            # job: (cpu efficiency, memory efficiency, fraction of time limit used)
            "1003": (10800 / (3600 * 8), (8 * GIB) / (32 * GIB / 2), 3600 / 7200),  # two nodes
            "1004": (300 / (600 * 2), (1 * GIB) / (4 * GIB), 600 / 3600),           # memory per CPU
            "1005": (0.0, None, 1.0),                                                # no memory sample
        }
        for version in FIXTURE_VERSIONS:
            usage, runner = self.usage(version, list(expected))
            self.assertEqual(runner.calls, [["sacct", "--json", "--jobs=1003,1004,1005"]])
            for job_id, (cpu, memory, time_used) in expected.items():
                with self.subTest(version=version, job=job_id):
                    self.assertAlmostEqual(usage[job_id].cpu_efficiency, cpu)
                    if memory is None:
                        self.assertIsNone(usage[job_id].memory_efficiency)
                    else:
                        self.assertAlmostEqual(usage[job_id].memory_efficiency, memory)
                    self.assertAlmostEqual(usage[job_id].time_used, time_used)

    def test_an_array_is_reported_task_by_task(self):
        for version in FIXTURE_VERSIONS:
            with self.subTest(version=version):
                usage, _ = self.usage(version, ["2010"])

                self.assertEqual(sorted(usage), ["2010_0", "2010_1"])
                self.assertAlmostEqual(usage["2010_0"].cpu_efficiency, 0.5)
                self.assertAlmostEqual(usage["2010_0"].memory_efficiency, 0.5)

    def test_unfinished_jobs_have_no_efficiency_figures(self):
        for version in FIXTURE_VERSIONS:
            with self.subTest(version=version):
                usage, _ = self.usage(version, ["2000"])

                for job_id in ("2000_2", "2000_3"):
                    self.assertIsNone(usage[job_id].cpu_efficiency)
                    self.assertIsNone(usage[job_id].memory_efficiency)
                    self.assertIsNone(usage[job_id].time_used)
                self.assertIs(usage["2000_1"].state, JobState.FAILED)
                self.assertEqual(usage["2000_1"].exit_code, 2)


def usage(job_id, state, cpu_seconds=None, elapsed=None, cpus=None, peak=None, requested=None, nodes=1,
          limit=None, exit_code=None):
    return JobUsage(job_id=job_id, state=state, exit_code=exit_code, elapsed=elapsed, time_limit=limit,
                    cpu_seconds=cpu_seconds, cpus=cpus, nodes=nodes, peak_memory=peak,
                    requested_memory=requested)


class ReportTest(unittest.TestCase):
    def setUp(self):
        self.report = Report([
            usage("1", JobState.COMPLETED, cpu_seconds=50, elapsed=100, cpus=1, peak=GIB, requested=4 * GIB,
                  limit=200, exit_code=0),
            usage("2", JobState.COMPLETED, cpu_seconds=90, elapsed=100, cpus=1, peak=3 * GIB,
                  requested=4 * GIB, limit=100, exit_code=0),
            usage("3", JobState.FAILED, exit_code=1),
            usage("4", JobState.RUNNING, cpu_seconds=10, elapsed=10, cpus=1),
        ])

    def test_the_summary_counts_states_and_averages_only_known_figures(self):
        self.assertEqual(self.report.state_counts(), {JobState.COMPLETED: 2, JobState.FAILED: 1, JobState.RUNNING: 1})
        self.assertAlmostEqual(self.report.mean_cpu_efficiency(), (0.5 + 0.9) / 2)
        self.assertAlmostEqual(self.report.mean_memory_efficiency(), (0.25 + 0.75) / 2)
        self.assertAlmostEqual(self.report.mean_time_used(), (0.5 + 1.0) / 2)

    def test_an_empty_report_has_no_averages(self):
        empty = Report([])

        self.assertEqual(empty.state_counts(), {})
        self.assertIsNone(empty.mean_cpu_efficiency())

    def test_csv_and_json_carry_the_same_data_and_mark_missing_figures_as_unavailable(self):
        rows = list(csv.DictReader(io.StringIO(self.report.to_csv())))
        document = json.loads(self.report.to_json())

        self.assertEqual([row["job_id"] for row in rows], ["1", "2", "3", "4"])
        self.assertEqual([job["job_id"] for job in document["jobs"]], ["1", "2", "3", "4"])
        for row, job in zip(rows, document["jobs"]):
            for key, value in job.items():
                with self.subTest(job=job["job_id"], field=key):
                    self.assertEqual(row[key], "" if value is None else str(value))
        self.assertIsNone(document["jobs"][2]["cpu_efficiency"])
        self.assertEqual(document["summary"]["state_counts"], {"COMPLETED": 2, "FAILED": 1, "RUNNING": 1})

    def test_the_table_shows_unavailable_figures_as_dashes_not_zeros(self):
        table = self.report.to_table()
        failed_row = next(line for line in table.splitlines() if line.startswith("3 "))

        self.assertIn("FAILED", failed_row)
        self.assertNotIn("0%", failed_row)
        self.assertIn("-", failed_row)
        self.assertIn("50%", table)


if __name__ == "__main__":
    unittest.main()
