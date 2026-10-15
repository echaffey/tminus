"""End-to-end tests against a real Slurm cluster. Skipped unless TMINUS_LIVE=1.

Run on a login node from the repo root:

    TMINUS_LIVE=1 python -m unittest tests.cluster.test_live -v

Settings (all optional):

    TMINUS_LIVE_PARTITION, TMINUS_LIVE_ACCOUNT, TMINUS_LIVE_QOS
        passed to every job when set
    TMINUS_LIVE_MEM
        memory per job (default: 256M; set it empty to leave --mem off)
    TMINUS_LIVE_SBATCH_ARGS
        extra sbatch arguments for every job, e.g. "--reservation=test"
    TMINUS_LIVE_DIR
        a directory visible from the compute nodes (default: ~/tminus-live)
    TMINUS_LIVE_TIMEOUT
        seconds to wait for any one job, queue time included (default: 900)

Every job is tagged with a per-run tag and cancelled at the end, and the
per-run working directory is removed, even when a test fails. The jobs are
tiny (a few seconds to two minutes, one CPU), but how long the suite takes
depends on your queue.
"""
from __future__ import annotations

import asyncio
import os
import shlex
import shutil
import time
import unittest
import uuid

import tminus
from tminus import JobState, PollPolicy, Policy, aio
from tminus.backend.slurm_cli import SlurmCliBackend
from tminus.store import Store

LIVE = os.environ.get("TMINUS_LIVE") == "1"
TIMEOUT = float(os.environ.get("TMINUS_LIVE_TIMEOUT", "900"))
POLICY = PollPolicy(initial=3, factor=1.5, maximum=15)


@unittest.skipUnless(LIVE, "set TMINUS_LIVE=1 on a Slurm login node to run the live tests")
class LiveClusterTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.run_id = uuid.uuid4().hex[:8]
        base = os.path.expanduser(os.environ.get("TMINUS_LIVE_DIR", "~/tminus-live"))
        cls.workdir = os.path.join(base, cls.run_id)
        os.makedirs(cls.workdir)
        cls.store = Store(os.path.join(cls.workdir, "jobs.db"))
        cls.backend = SlurmCliBackend()
        cls.tags = {"tminus-live": cls.run_id}

    @classmethod
    def tearDownClass(cls):
        try:
            tminus.cleanup.cancel(cls.tags, backend=cls.backend, store=cls.store)
        finally:
            shutil.rmtree(cls.workdir, ignore_errors=True)

    # ---- helpers -------------------------------------------------------

    def spec(self, command, **fields):
        fields.setdefault("time", "00:03:00")
        fields.setdefault("cpus", 1)
        memory = os.environ.get("TMINUS_LIVE_MEM", "256M")
        if memory:
            fields.setdefault("mem", memory)
        fields.setdefault("workdir", self.workdir)
        fields.setdefault("output", "%x-%j.out")
        fields.setdefault("name", "tminus-live")
        for name, variable in (("partition", "TMINUS_LIVE_PARTITION"), ("account", "TMINUS_LIVE_ACCOUNT"),
                               ("qos", "TMINUS_LIVE_QOS")):
            if os.environ.get(variable):
                fields.setdefault(name, os.environ[variable])
        extra = shlex.split(os.environ.get("TMINUS_LIVE_SBATCH_ARGS", ""))
        fields["extra_args"] = extra + list(fields.get("extra_args", []))
        fields["tags"] = dict(self.tags, **fields.get("tags", {}))
        return tminus.JobSpec(command=command, **fields)

    def submit(self, command, **fields):
        return tminus.submit(self.spec(command, **fields), backend=self.backend, store=self.store)

    def wait(self, *jobs):
        return tminus.wait_all(list(jobs), timeout=TIMEOUT, policy=POLICY, store=self.store)

    # ---- tests ---------------------------------------------------------

    def test_a_job_runs_to_completion_and_is_reported(self):
        job = self.submit("sleep 20")

        status = self.wait(job)[job.job_id]
        usage = tminus.report(jobs=[job], backend=self.backend).jobs[0]
        history = [entry.state for entry in self.store.history(self.backend.name, job.job_id)]

        self.assertIs(status.state, JobState.COMPLETED)
        self.assertEqual(status.exit_code, 0)
        self.assertEqual(status.tags, self.tags)
        self.assertIs(history[-1], JobState.COMPLETED)
        self.assertGreaterEqual(usage.elapsed, 15)
        self.assertEqual(usage.time_limit, 180)
        self.assertIsNotNone(usage.cpu_efficiency)
        self.assertIsNotNone(usage.requested_memory)

    def test_failures_and_time_limits_are_reported_as_such(self):
        failing = self.submit("exit 3")
        overrunning = self.submit("sleep 300", time="00:01:00")

        statuses = self.wait(failing, overrunning)

        self.assertIs(statuses[failing.job_id].state, JobState.FAILED)
        self.assertEqual(statuses[failing.job_id].exit_code, 3)
        self.assertIs(statuses[overrunning.job_id].state, JobState.TIMEOUT)

    def test_an_array_job_runs_every_task(self):
        job = self.submit("sleep 5", array="0-2")

        status = self.wait(job)[job.job_id]

        self.assertEqual([task.job_id for task in job.tasks], [f"{job.job_id}_{i}" for i in range(3)])
        self.assertIs(status.state, JobState.COMPLETED)
        self.assertEqual(status.task_counts(), {JobState.COMPLETED: 3})

    def test_a_dependency_that_can_never_be_met_is_surfaced(self):
        first = self.submit("exit 1")
        second = self.submit("true", depends_on=[first])

        self.wait(first)
        try:
            final = self.wait(second)[second.job_id]
        except tminus.UnsatisfiableDependency:
            final = None  # left pending: tminus reported it instead of hanging
        finally:
            second.cancel(store=self.store)

        if final is not None:
            # Sites with kill_invalid_depend cancel such jobs instead of leaving them pending.
            self.assertIs(final.state, JobState.CANCELLED)

    def test_a_held_job_can_be_updated_released_and_cancelled(self):
        job = self.submit("sleep 120", extra_args=["--hold"])

        held = job.refresh()
        job.update(time_limit="00:02:00", store=self.store)
        job.release(store=self.store)
        cancelled = job.cancel(store=self.store)
        final = self.wait(job)[job.job_id]
        events = [event.action for event in self.store.events(self.backend.name, job.job_id)]

        self.assertIs(held.state, JobState.PENDING)
        self.assertIn("Held", held.reason or "")
        self.assertTrue(cancelled)
        self.assertIs(final.state, JobState.CANCELLED)
        self.assertEqual(events, ["update", "release", "cancel"])

    def test_cleanup_by_tag_cancels_active_jobs_and_purges_successful_logs(self):
        tags = {"step": "cleanup"}
        quick = self.submit("echo hello", tags=tags)
        sleeper = self.submit("sleep 600", tags=tags)
        group = dict(self.tags, **tags)
        self.wait(quick)

        cancelled = tminus.cleanup.cancel(group, backend=self.backend, store=self.store)
        self.wait(sleeper)
        results = tminus.cleanup.purge(group, Policy.ON_SUCCESS, backend=self.backend, store=self.store)

        self.assertEqual(cancelled, [sleeper.job_id])
        log = os.path.join(self.workdir, f"tminus-live-{quick.job_id}.out")
        self.assertEqual([r.job_id for r in results], [quick.job_id])
        self.assertEqual(results[0].deleted, [log])
        self.assertFalse(os.path.exists(log))
        self.assertTrue(os.path.isdir(self.workdir))

    def test_async_waits_share_polls_and_finish(self):
        jobs = [self.submit("sleep 10"), self.submit("sleep 15")]

        async def finish_order():
            return [job.job_id async for job in aio.as_completed(jobs, timeout=TIMEOUT, policy=POLICY,
                                                                   store=self.store)]

        order = asyncio.run(finish_order())

        self.assertEqual(sorted(order), sorted(job.job_id for job in jobs))
        self.assertTrue(all(job.status.state is JobState.COMPLETED for job in jobs))

    def test_prune_forgets_finished_jobs_but_keeps_active_ones(self):
        tags = {"step": "prune"}
        done = self.submit("true", tags=tags)
        active = self.submit("sleep 600", tags=tags)
        self.wait(done)
        time.sleep(1)

        removed = tminus.prune(0, tags=dict(self.tags, **tags), backend=self.backend, store=self.store)
        remaining = [job.job_id for job in tminus.jobs(tags=dict(self.tags, **tags), backend=self.backend,
                                                       store=self.store)]
        active.cancel(store=self.store)

        self.assertEqual(removed, [done.job_id])
        self.assertEqual(remaining, [active.job_id])


if __name__ == "__main__":
    unittest.main()
