"""The Backend interface every scheduler implementation provides."""
from __future__ import annotations

import abc

from ..spec import JobSpec


class Backend(abc.ABC):
    """Submits and controls jobs on behalf of the rest of tminus."""

    @abc.abstractmethod
    def submit(self, spec: JobSpec) -> str:
        """Submit ``spec`` and return the new job's id."""
