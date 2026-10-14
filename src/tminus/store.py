"""Store: the local SQLite record of every job tminus has submitted.

The database lives at ``TMINUS_DB`` or, by default, under
``$XDG_DATA_HOME/tminus`` (``~/.local/share/tminus``). Home directories on
HPC systems are often NFS, so the Store keeps SQLite's default rollback
journal (WAL mode does not work on network filesystems) and holds each
connection only for one short transaction. If locking on NFS still causes
trouble, point ``TMINUS_DB`` at node-local disk.
"""
from __future__ import annotations

import contextlib
import json
import os
import sqlite3
import time
from typing import Callable, Dict, Iterator, List, Optional, Tuple

from .spec import JobSpec

# Each entry upgrades the schema by one version; PRAGMA user_version records how many ran.
_MIGRATIONS = [
    """
    CREATE TABLE jobs (
        id           INTEGER PRIMARY KEY,
        backend      TEXT NOT NULL,
        job_id       TEXT NOT NULL,
        spec         TEXT NOT NULL,
        submitted_at REAL NOT NULL,
        UNIQUE (backend, job_id)
    );
    CREATE TABLE tags (
        job      INTEGER NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
        key      TEXT NOT NULL,
        value    TEXT NOT NULL,
        PRIMARY KEY (job, key)
    );
    CREATE INDEX tags_by_key_value ON tags (key, value);
    """,
]


def default_path() -> str:
    """Where the Store lives unless told otherwise."""
    if os.environ.get("TMINUS_DB"):
        return os.environ["TMINUS_DB"]
    data_home = os.environ.get("XDG_DATA_HOME") or os.path.join(os.path.expanduser("~"), ".local", "share")
    return os.path.join(data_home, "tminus", "jobs.db")


class Store:
    """Records submitted jobs and their tags, shared by every process using the same file."""

    def __init__(self, path: Optional[str] = None, clock: Callable[[], float] = time.time) -> None:
        self.path = path if path is not None else default_path()
        self._clock = clock
        self._migrated = False

    def record_submission(self, backend: str, job_id: str, spec: JobSpec) -> None:
        """Remember that ``backend`` accepted ``spec`` as ``job_id``.

        Slurm reuses job ids once its counter wraps, so a new submission with a
        known id replaces the old (long finished) record and its tags.
        """
        with self._transaction() as db:
            db.execute("DELETE FROM jobs WHERE backend = ? AND job_id = ?", (backend, job_id))
            cursor = db.execute(
                "INSERT INTO jobs (backend, job_id, spec, submitted_at) VALUES (?, ?, ?, ?)",
                (backend, job_id, json.dumps(spec.to_dict(), sort_keys=True), self._clock()),
            )
            db.executemany(
                "INSERT INTO tags (job, key, value) VALUES (?, ?, ?)",
                [(cursor.lastrowid, key, value) for key, value in sorted(spec.tags.items())],
            )

    def find(self, backend: str, tags: Optional[Dict[str, str]] = None) -> List[Tuple[str, JobSpec]]:
        """``(job_id, spec)`` for every job on ``backend`` carrying all of ``tags``, oldest first."""
        sql = "SELECT job_id, spec FROM jobs WHERE backend = ?"
        params: list = [backend]
        for key, value in sorted((tags or {}).items()):
            sql += " AND id IN (SELECT job FROM tags WHERE key = ? AND value = ?)"
            params += [key, value]
        sql += " ORDER BY id"
        with self._transaction() as db:
            rows = db.execute(sql, params).fetchall()
        return [(job_id, JobSpec.from_dict(json.loads(spec))) for job_id, spec in rows]

    @contextlib.contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        directory = os.path.dirname(os.path.abspath(self.path))
        os.makedirs(directory, exist_ok=True)
        with contextlib.closing(sqlite3.connect(self.path, timeout=30)) as db:
            db.execute("PRAGMA foreign_keys = ON")
            with db:
                if not self._migrated:
                    _migrate(db)
                    self._migrated = True
                yield db


def _migrate(db: sqlite3.Connection) -> None:
    version = db.execute("PRAGMA user_version").fetchone()[0]
    for number, script in enumerate(_MIGRATIONS[version:], start=version + 1):
        for statement in script.split(";"):
            if statement.strip():
                db.execute(statement)
        db.execute(f"PRAGMA user_version = {number}")
