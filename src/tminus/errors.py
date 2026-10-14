"""Exceptions raised by tminus. Every one a caller might handle subclasses TminusError."""
from __future__ import annotations

from typing import List, Optional


class TminusError(Exception):
    """Base class for all tminus errors."""


class InvalidSpecError(TminusError):
    """A JobSpec is incomplete or contradictory, detected before anything reaches Slurm."""


class SubmitError(TminusError):
    """A submission did not produce a usable job id."""


class SlurmCommandError(TminusError):
    """A Slurm command failed to run or exited with an error.

    ``command`` is the full command line, ``returncode`` its exit status (None if
    it never ran or timed out), and ``stderr`` whatever it wrote to stderr.
    """

    def __init__(
        self,
        command: List[str],
        returncode: Optional[int],
        stderr: str,
        message: Optional[str] = None,
    ) -> None:
        self.command = list(command)
        self.returncode = returncode
        self.stderr = stderr
        if message is None:
            detail = stderr.strip() or "no output on stderr"
            message = f"{command[0]} exited with status {returncode}: {detail}"
        super().__init__(message)


class CommandTimeoutError(SlurmCommandError):
    """A Slurm command was still running when its timeout expired."""
