"""Backends: the only part of tminus that talks to a scheduler."""
from __future__ import annotations

import os
from typing import Optional

from ..errors import TminusError
from .base import Backend
from .fake import FakeBackend
from .slurm_cli import SlurmCliBackend

BACKENDS = {
    "slurm": SlurmCliBackend,
    "fake": FakeBackend,
}


def get_backend(name: Optional[str] = None) -> Backend:
    """Create the Backend called ``name``, or the one ``TMINUS_BACKEND`` names (default ``slurm``)."""
    if name is None:
        name = os.environ.get("TMINUS_BACKEND", "slurm")
    try:
        backend_class = BACKENDS[name]
    except KeyError:
        known = ", ".join(sorted(BACKENDS))
        raise TminusError(f"unknown backend {name!r} (expected one of: {known})") from None
    return backend_class()
