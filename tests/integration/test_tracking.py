from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import unittest

import tminus
from tminus.backend.fake import FakeBackend
from tminus.store import Store

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TrackingTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="tminus-test-")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.store = Store(os.path.join(self.tmp, "jobs.db"))
        self.backend = FakeBackend()

    def submit(self, **spec_fields):
        spec_fields.setdefault("command", "true")
        return tminus.submit(tminus.JobSpec(**spec_fields), backend=self.backend, store=self.store)

    def test_a_submitted_job_can_be_found_again_with_its_spec(self):
        job = self.submit(command="python train.py", cpus=4, workdir="/scratch/exp",
                          output="out.log", tags={"exp": "exp42"})

        found = tminus.jobs(backend=self.backend, store=self.store)

        self.assertEqual([j.job_id for j in found], [job.job_id])
        self.assertEqual(found[0].spec, job.spec)

    def test_jobs_can_be_selected_by_tag(self):
        first = self.submit(tags={"exp": "exp42", "run": "1"})
        second = self.submit(tags={"exp": "exp42", "run": "2"})
        self.submit(tags={"exp": "other"})
        self.submit()

        by_exp = tminus.jobs(tags={"exp": "exp42"}, backend=self.backend, store=self.store)
        by_both = tminus.jobs(tags={"exp": "exp42", "run": "2"}, backend=self.backend, store=self.store)

        self.assertEqual([j.job_id for j in by_exp], [first.job_id, second.job_id])
        self.assertEqual([j.job_id for j in by_both], [second.job_id])

    def test_jobs_from_another_backend_are_not_mixed_in(self):
        self.submit(tags={"exp": "exp42"})

        found = tminus.jobs(tags={"exp": "exp42"}, backend=FakeBackend(), store=self.store)
        self.assertEqual(len(found), 1)
        other_backend = tminus.get_backend("slurm")
        self.assertEqual(tminus.jobs(tags={"exp": "exp42"}, backend=other_backend, store=self.store), [])

    def test_a_second_process_sees_jobs_recorded_by_the_first(self):
        db = os.path.join(self.tmp, "shared.db")
        env = dict(os.environ, TMINUS_DB=db, TMINUS_BACKEND="fake", PYTHONPATH=os.path.join(ROOT, "src"))
        subprocess.run(
            [sys.executable, "-m", "tminus", "submit", "--tag", "exp=exp42", "--wrap", "true"],
            env=env, check=True, capture_output=True,
        )

        found = tminus.jobs(tags={"exp": "exp42"}, backend=FakeBackend(), store=Store(db))

        self.assertEqual(len(found), 1)
        self.assertEqual(found[0].spec.tags, {"exp": "exp42"})

    def test_tminus_db_chooses_the_default_database(self):
        db = os.path.join(self.tmp, "from-env.db")
        os.environ["TMINUS_DB"], saved = db, os.environ["TMINUS_DB"]
        self.addCleanup(os.environ.__setitem__, "TMINUS_DB", saved)

        tminus.submit(tminus.JobSpec(command="true"), backend=self.backend)

        self.assertTrue(os.path.exists(db))
        self.assertEqual(len(tminus.jobs(backend=self.backend)), 1)

    def test_a_reused_job_id_replaces_the_old_record(self):
        # Slurm reuses ids after its counter wraps; the earlier job is long finished by then.
        self.submit(tags={"exp": "old"})
        self.backend = FakeBackend()  # starts numbering from the same first id again
        new = self.submit(tags={"exp": "new"})

        found = tminus.jobs(backend=self.backend, store=self.store)

        self.assertEqual([(j.job_id, j.spec.tags) for j in found], [(new.job_id, {"exp": "new"})])
        self.assertEqual(tminus.jobs(tags={"exp": "old"}, backend=self.backend, store=self.store), [])

    def test_tags_must_be_simple_words(self):
        for bad in ({"exp": "has space"}, {"": "x"}, {"k": "a,b"}, {"k=v": "x"}):
            with self.subTest(tags=bad):
                with self.assertRaises(tminus.InvalidSpecError):
                    tminus.JobSpec(command="true", tags=bad)


if __name__ == "__main__":
    unittest.main()
