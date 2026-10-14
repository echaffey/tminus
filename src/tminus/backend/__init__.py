"""Backends: the only part of tminus that talks to a scheduler."""
from __future__ import annotations

import os
from typing import Callable, Dict, Optional

from ..errors import TminusError
from ..store import data_dir
from .base import Backend
from .fake import FakeBackend
from .slurm_cli import SlurmCliBackend


def fake_state_path() -> str:
    """Where the shared fake scheduler keeps its state: ``TMINUS_FAKE_STATE``, else beside the
    Store if one is configured, else in tminus's data directory."""
    if os.environ.get("TMINUS_FAKE_STATE"):
        return os.environ["TMINUS_FAKE_STATE"]
    directory = os.path.dirname(os.path.abspath(os.environ["TMINUS_DB"])) if os.environ.get("TMINUS_DB") else data_dir()
    return os.path.join(directory, "fake-scheduler.json")


BACKENDS: Dict[str, Callable[[], Backend]] = {
    "slurm": SlurmCliBackend,
    "fake": lambda: FakeBackend(state_path=fake_state_path()),
}


def get_backend(name: Optional[str] = None) -> Backend:
    """Create the Backend called ``name``, or the one ``TMINUS_BACKEND`` names (default ``slurm``)."""
    if name is None:
        name = os.environ.get("TMINUS_BACKEND", "slurm")
    try:
        factory = BACKENDS[name]
    except KeyError:
        known = ", ".join(sorted(BACKENDS))
        raise TminusError(f"unknown backend {name!r} (expected one of: {known})") from None
    return factory()
