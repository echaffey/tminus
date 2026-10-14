"""Cleaning up after jobs: pruning old records from the Store."""
from __future__ import annotations

import re
from typing import Dict, List, Optional, Union

from .backend import get_backend
from .backend.base import Backend
from .errors import InvalidAgeError
from .store import Store

_AGE = re.compile(r"^(\d+(?:\.\d+)?)([smhdw])$")
_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


def parse_age(text: str) -> float:
    """Seconds in an age such as ``45s``, ``90m``, ``12h``, ``30d``, or ``2w``."""
    match = _AGE.match(text.strip())
    if match is None:
        raise InvalidAgeError(f"invalid age {text!r}: expected a number and a unit (s, m, h, d, w), e.g. 30d")
    return float(match.group(1)) * _UNIT_SECONDS[match.group(2)]


def prune(
    older_than: Union[str, float],
    tags: Optional[Dict[str, str]] = None,
    backend: Optional[Backend] = None,
    store: Optional[Store] = None,
    dry_run: bool = False,
) -> List[str]:
    """Remove Store records of finished jobs submitted more than ``older_than`` ago.

    ``older_than`` is an age string (``30d``) or seconds. Only jobs carrying all
    of ``tags`` are considered. A job counts as finished if the Store recorded
    a terminal state for it, or if the scheduler (asked in one batched query)
    reports it terminal or no longer knows it at all. Active jobs are never
    removed. Each removed job's tags, history, and events go with it. Returns
    the ids removed, or that would be with ``dry_run``.
    """
    if backend is None:
        backend = get_backend()
    if store is None:
        store = Store()
    age = parse_age(older_than) if isinstance(older_than, str) else float(older_than)
    candidates = store.submitted_before(backend.name, store.now() - age, tags)
    unsure = [job_id for job_id, last_state in candidates if last_state is None or not last_state.is_terminal]
    current = backend.status(unsure) if unsure else {}
    finished = [
        job_id for job_id, last_state in candidates
        if job_id not in unsure or job_id not in current or current[job_id].state.is_terminal
    ]
    if finished and not dry_run:
        store.delete(backend.name, finished)
    return finished
