"""The subprocess boundary: the one place tminus runs external commands."""
from __future__ import annotations

import abc
import subprocess
from dataclasses import dataclass
from typing import List, Optional, Sequence

from ..errors import CommandTimeoutError, SlurmCommandError


@dataclass(frozen=True)
class CommandResult:
    """The outcome of running one command."""

    args: List[str]
    returncode: int
    stdout: str
    stderr: str


class Runner(abc.ABC):
    """Runs a command and reports how it went. Tests substitute their own."""

    @abc.abstractmethod
    def run(self, args: Sequence[str], timeout: Optional[float] = None) -> CommandResult:
        """Run ``args`` and return its result, whatever its exit status.

        Raises SlurmCommandError if the command cannot be started at all, and
        CommandTimeoutError if it is still running after ``timeout`` seconds.
        """


class SubprocessRunner(Runner):
    """Runs commands as local subprocesses, capturing their output as text."""

    def run(self, args: Sequence[str], timeout: Optional[float] = None) -> CommandResult:
        args = list(args)
        try:
            completed = subprocess.run(
                args,
                capture_output=True,
                text=True,
                errors="replace",
                timeout=timeout,
                check=False,
            )
        except FileNotFoundError:
            raise SlurmCommandError(
                args, None, "", message=f"{args[0]} not found: is Slurm installed and on PATH?"
            ) from None
        except subprocess.TimeoutExpired as exc:
            stderr = exc.stderr if isinstance(exc.stderr, str) else ""
            raise CommandTimeoutError(
                args, None, stderr, message=f"{args[0]} did not finish within {timeout} seconds"
            ) from None
        return CommandResult(args, completed.returncode, completed.stdout, completed.stderr)
