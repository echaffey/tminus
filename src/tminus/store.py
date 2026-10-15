"""Store: an optional local SQLite record of the jobs tminus submits.

The record is off unless ``TMINUS_DB`` names a database file (or a Store is
passed in explicitly). With it on, jobs can be found again by tag from later
runs, and tminus keeps each job's state history and the changes it made.
Without it, tminus works purely from job ids and the Job objects a script
holds, and writes nothing to disk. Home directories on
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
from dataclasses import dataclass
from typing import Callable, Iterator, List, Optional, Sequence, Tuple

from .errors import StoreNotEnabled
from .spec import JobSpec, Tags, normalize_tags
from .state import JobState, JobStatus

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
    """
    CREATE TABLE history (
        job       INTEGER NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
        seen_at   REAL NOT NULL,
        state     TEXT NOT NULL,
        reason    TEXT,
        exit_code INTEGER
    );
    CREATE INDEX history_by_job ON history (job, seen_at);
    """,
    """
    CREATE TABLE events (
        job    INTEGER NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
        at     REAL NOT NULL,
        action TEXT NOT NULL,
        detail TEXT
    );
    CREATE INDEX events_by_job ON events (job, at);
    """,
    """
    ALTER TABLE jobs ADD COLUMN submit_dir TEXT;
    """,
    # Tags became plain names; key/value tags from tminus 0.1.0 become "key=value" names.
    """
    CREATE TABLE tag_names (
        job  INTEGER NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
        name TEXT NOT NULL,
        PRIMARY KEY (job, name)
    );
    INSERT INTO tag_names (job, name) SELECT job, key || '=' || value FROM tags;
    DROP TABLE tags;
    ALTER TABLE tag_names RENAME TO tags;
    CREATE INDEX tags_by_name ON tags (name);
    """,
]


@dataclass(frozen=True)
class TrackedJob:
    """A job as the Store remembers it."""

    job_id: str
    spec: JobSpec
    submit_dir: Optional[str]


@dataclass(frozen=True)
class Event:
    """Something tminus did to a tracked job after submitting it (cancel, update, ...)."""

    at: float
    action: str
    detail: Optional[str]


@dataclass(frozen=True)
class HistoryEntry:
    """One state change of a tracked job, stamped with when tminus first saw it."""

    seen_at: float
    state: JobState
    reason: Optional[str]
    exit_code: Optional[int]


def data_dir() -> str:
    """tminus's per-user data directory: ``$XDG_DATA_HOME/tminus`` (``~/.local/share/tminus``)."""
    data_home = os.environ.get("XDG_DATA_HOME") or os.path.join(os.path.expanduser("~"), ".local", "share")
    return os.path.join(data_home, "tminus")


def default_store() -> Optional["Store"]:
    """The Store named by ``TMINUS_DB``, or None when the record is turned off."""
    path = os.environ.get("TMINUS_DB")
    return Store(path) if path else None


def require_store(store: Optional["Store"], what: str) -> "Store":
    """``store``, else the default one; raises StoreNotEnabled if neither exists."""
    if store is None:
        store = default_store()
    if store is None:
        raise StoreNotEnabled(what)
    return store


class Store:
    """Records submitted jobs and their tags, shared by every process using the same file."""

    def __init__(self, path: str, clock: Callable[[], float] = time.time) -> None:
        self.path = path
        self._clock = clock
        self._migrated = False

    def record_submission(
        self, backend: str, job_id: str, spec: JobSpec, submit_dir: Optional[str] = None
    ) -> None:
        """Remember that ``backend`` accepted ``spec`` as ``job_id``, submitted from ``submit_dir``.

        Slurm reuses job ids once its counter wraps, so a new submission with a
        known id replaces the old (long finished) record and its tags.
        """
        with self._transaction() as db:
            db.execute("DELETE FROM jobs WHERE backend = ? AND job_id = ?", (backend, job_id))
            cursor = db.execute(
                "INSERT INTO jobs (backend, job_id, spec, submitted_at, submit_dir) VALUES (?, ?, ?, ?, ?)",
                (backend, job_id, json.dumps(spec.to_dict(), sort_keys=True), self._clock(), submit_dir),
            )
            db.executemany(
                "INSERT INTO tags (job, name) VALUES (?, ?)",
                [(cursor.lastrowid, name) for name in spec.tags],
            )

    def find(self, backend: str, tags: Tags = None) -> List[TrackedJob]:
        """Every job on ``backend`` carrying all of ``tags``, oldest first."""
        sql = "SELECT job_id, spec, submit_dir FROM jobs WHERE backend = ?"
        params: list = [backend]
        for name in normalize_tags(tags):
            sql += " AND id IN (SELECT job FROM tags WHERE name = ?)"
            params.append(name)
        sql += " ORDER BY id"
        with self._transaction() as db:
            rows = db.execute(sql, params).fetchall()
        return [TrackedJob(job_id, JobSpec.from_dict(json.loads(spec)), submit_dir)
                for job_id, spec, submit_dir in rows]

    def record_statuses(self, backend: str, statuses: Sequence[JobStatus]) -> None:
        """Append each status to its job's history if its state changed since the last one.

        Statuses of jobs the Store is not tracking are ignored.
        """
        now = self._clock()
        with self._transaction() as db:
            for status in statuses:
                row = db.execute(
                    "SELECT id FROM jobs WHERE backend = ? AND job_id = ?", (backend, status.job_id)
                ).fetchone()
                if row is None:
                    continue
                last = db.execute(
                    "SELECT state FROM history WHERE job = ? ORDER BY seen_at DESC, rowid DESC LIMIT 1", row
                ).fetchone()
                if last is not None and last[0] == status.state.value:
                    continue
                db.execute(
                    "INSERT INTO history (job, seen_at, state, reason, exit_code) VALUES (?, ?, ?, ?, ?)",
                    (row[0], now, status.state.value, status.reason, status.exit_code),
                )

    def history(self, backend: str, job_id: str) -> List[HistoryEntry]:
        """Every recorded state change of one job, oldest first."""
        with self._transaction() as db:
            rows = db.execute(
                "SELECT h.seen_at, h.state, h.reason, h.exit_code FROM history h"
                " JOIN jobs j ON j.id = h.job WHERE j.backend = ? AND j.job_id = ?"
                " ORDER BY h.seen_at, h.rowid",
                (backend, job_id),
            ).fetchall()
        return [HistoryEntry(seen_at, JobState(state), reason, exit_code)
                for seen_at, state, reason, exit_code in rows]

    def now(self) -> float:
        """The current time by this Store's clock (wall-clock seconds by default)."""
        return self._clock()

    def submitted_before(
        self, backend: str, cutoff: float, tags: Tags = None
    ) -> List[Tuple[str, Optional[JobState]]]:
        """``(job_id, last recorded state or None)`` for jobs on ``backend`` submitted before
        ``cutoff`` and carrying all of ``tags``, oldest first."""
        sql = (
            "SELECT j.job_id, (SELECT h.state FROM history h WHERE h.job = j.id"
            " ORDER BY h.seen_at DESC, h.rowid DESC LIMIT 1)"
            " FROM jobs j WHERE j.backend = ? AND j.submitted_at < ?"
        )
        params: list = [backend, cutoff]
        for name in normalize_tags(tags):
            sql += " AND j.id IN (SELECT job FROM tags WHERE name = ?)"
            params.append(name)
        sql += " ORDER BY j.id"
        with self._transaction() as db:
            rows = db.execute(sql, params).fetchall()
        return [(job_id, None if state is None else JobState(state)) for job_id, state in rows]

    def delete(self, backend: str, job_ids: Sequence[str]) -> None:
        """Forget jobs entirely, with their tags, history, and events."""
        with self._transaction() as db:
            db.executemany(
                "DELETE FROM jobs WHERE backend = ? AND job_id = ?", [(backend, job_id) for job_id in job_ids]
            )

    def record_event(self, backend: str, job_id: str, action: str, detail: Optional[str] = None) -> None:
        """Note that tminus did ``action`` to a job. Jobs the Store is not tracking are ignored."""
        with self._transaction() as db:
            db.execute(
                "INSERT INTO events (job, at, action, detail)"
                " SELECT id, ?, ?, ? FROM jobs WHERE backend = ? AND job_id = ?",
                (self._clock(), action, detail, backend, job_id),
            )

    def events(self, backend: str, job_id: str) -> List[Event]:
        """Everything tminus did to one job, oldest first."""
        with self._transaction() as db:
            rows = db.execute(
                "SELECT e.at, e.action, e.detail FROM events e JOIN jobs j ON j.id = e.job"
                " WHERE j.backend = ? AND j.job_id = ? ORDER BY e.at, e.rowid",
                (backend, job_id),
            ).fetchall()
        return [Event(at, action, detail) for at, action, detail in rows]

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
