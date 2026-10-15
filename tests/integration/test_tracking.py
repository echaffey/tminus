from __future__ import annotations

import json
import os
import shutil
import sqlite3
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
                          output="out.log", tags=["exp42"])

        found = tminus.jobs(backend=self.backend, store=self.store)

        self.assertEqual([j.job_id for j in found], [job.job_id])
        self.assertEqual(found[0].spec, job.spec)

    def test_jobs_can_be_selected_by_tag(self):
        first = self.submit(tags=["exp42", "run1"])
        second = self.submit(tags=["exp42", "run2"])
        self.submit(tags=["other"])
        self.submit()

        by_exp = tminus.jobs(tags=["exp42"], backend=self.backend, store=self.store)
        by_both = tminus.jobs(tags=["exp42", "run2"], backend=self.backend, store=self.store)

        self.assertEqual([j.job_id for j in by_exp], [first.job_id, second.job_id])
        self.assertEqual([j.job_id for j in by_both], [second.job_id])

    def test_jobs_from_another_backend_are_not_mixed_in(self):
        self.submit(tags=["exp42"])

        found = tminus.jobs(tags=["exp42"], backend=FakeBackend(), store=self.store)
        self.assertEqual(len(found), 1)
        other_backend = tminus.get_backend("slurm")
        self.assertEqual(tminus.jobs(tags=["exp42"], backend=other_backend, store=self.store), [])

    def test_a_second_process_sees_jobs_recorded_by_the_first(self):
        db = os.path.join(self.tmp, "shared.db")
        env = dict(os.environ, TMINUS_DB=db, TMINUS_BACKEND="fake", PYTHONPATH=os.path.join(ROOT, "src"))
        subprocess.run(
            [sys.executable, "-m", "tminus", "submit", "--tag", "exp42", "--wrap", "true"],
            env=env, check=True, capture_output=True,
        )

        found = tminus.jobs(tags=["exp42"], backend=FakeBackend(), store=Store(db))

        self.assertEqual(len(found), 1)
        self.assertEqual(found[0].spec.tags, ["exp42"])

    def test_tminus_db_chooses_the_default_database(self):
        db = os.path.join(self.tmp, "from-env.db")
        os.environ["TMINUS_DB"], saved = db, os.environ["TMINUS_DB"]
        self.addCleanup(os.environ.__setitem__, "TMINUS_DB", saved)

        tminus.submit(tminus.JobSpec(command="true"), backend=self.backend)

        self.assertTrue(os.path.exists(db))
        self.assertEqual(len(tminus.jobs(backend=self.backend)), 1)

    def test_a_reused_job_id_replaces_the_old_record(self):
        # Slurm reuses ids after its counter wraps; the earlier job is long finished by then.
        self.submit(tags=["old"])
        self.backend = FakeBackend()  # starts numbering from the same first id again
        new = self.submit(tags=["new"])

        found = tminus.jobs(backend=self.backend, store=self.store)

        self.assertEqual([(j.job_id, j.spec.tags) for j in found], [(new.job_id, ["new"])])
        self.assertEqual(tminus.jobs(tags=["old"], backend=self.backend, store=self.store), [])

    def test_tags_must_be_simple_words(self):
        for bad in (["has space"], [""], ["a,b"], {"exp": "x"}, [42]):
            with self.subTest(tags=bad):
                with self.assertRaises(tminus.InvalidSpecError):
                    tminus.JobSpec(command="true", tags=bad)

    def test_tags_are_plain_names_kept_in_order_without_repeats(self):
        spec = tminus.JobSpec(command="true", tags=["sweep3", "train", "sweep3", "exp=42"])
        single = tminus.JobSpec(command="true", tags="sweep3")

        self.assertEqual(spec.tags, ["sweep3", "train", "exp=42"])
        self.assertEqual(single.tags, ["sweep3"])

    def test_a_single_tag_can_be_given_as_a_string_when_selecting(self):
        tagged = self.submit(tags=["sweep3", "train"])
        self.submit(tags=["sweep4"])

        found = tminus.jobs(tags="sweep3", backend=self.backend, store=self.store)

        self.assertEqual([j.job_id for j in found], [tagged.job_id])

    def test_records_from_before_plain_tags_are_read_as_key_equals_value_names(self):
        db = os.path.join(self.tmp, "old.db")
        connection = sqlite3.connect(db)
        connection.executescript(OLD_SCHEMA)
        connection.execute(
            "INSERT INTO jobs (id, backend, job_id, spec, submitted_at, submit_dir) VALUES (1, 'fake', '7', ?, 0, '/w')",
            (json.dumps({"command": "true", "tags": {"exp": "exp42"}}),),
        )
        connection.execute("INSERT INTO tags (job, key, value) VALUES (1, 'exp', 'exp42')")
        connection.commit()
        connection.close()

        found = tminus.jobs(tags=["exp=exp42"], backend=self.backend, store=Store(db))

        self.assertEqual([j.job_id for j in found], ["7"])
        self.assertEqual(found[0].spec.tags, ["exp=exp42"])


# The job record as tminus 0.1.0 created it, with key/value tags (schema version 4).
OLD_SCHEMA = """
CREATE TABLE jobs (id INTEGER PRIMARY KEY, backend TEXT NOT NULL, job_id TEXT NOT NULL, spec TEXT NOT NULL,
                   submitted_at REAL NOT NULL, submit_dir TEXT, UNIQUE (backend, job_id));
CREATE TABLE tags (job INTEGER NOT NULL REFERENCES jobs (id) ON DELETE CASCADE, key TEXT NOT NULL,
                   value TEXT NOT NULL, PRIMARY KEY (job, key));
CREATE INDEX tags_by_key_value ON tags (key, value);
CREATE TABLE history (job INTEGER NOT NULL REFERENCES jobs (id) ON DELETE CASCADE, seen_at REAL NOT NULL,
                      state TEXT NOT NULL, reason TEXT, exit_code INTEGER);
CREATE INDEX history_by_job ON history (job, seen_at);
CREATE TABLE events (job INTEGER NOT NULL REFERENCES jobs (id) ON DELETE CASCADE, at REAL NOT NULL,
                     action TEXT NOT NULL, detail TEXT);
CREATE INDEX events_by_job ON events (job, at);
PRAGMA user_version = 4;
"""


if __name__ == "__main__":
    unittest.main()
