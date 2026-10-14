"""Shared test doubles and fixture loading."""
from __future__ import annotations

import os

from tminus.backend.runner import CommandResult, Runner

FIXTURES = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures")

# Every directory of recorded Slurm output, e.g. "slurm-21.08".
FIXTURE_VERSIONS = sorted(
    name for name in os.listdir(FIXTURES) if os.path.isdir(os.path.join(FIXTURES, name))
)


def fixture(version, name):
    """The text of a recorded Slurm output file."""
    with open(os.path.join(FIXTURES, version, name), encoding="utf-8") as handle:
        return handle.read()


class ScriptedRunner(Runner):
    """Stands in for the subprocess boundary.

    Replies to each command according to its program name (``squeue``,
    ``sacct``, ...) and records every command it was asked to run.
    """

    def __init__(self, **replies):
        # program name -> (returncode, stdout, stderr), or just stdout for success
        self._replies = replies
        self.calls = []

    def run(self, args, timeout=None):
        args = list(args)
        self.calls.append(args)
        reply = self._replies.get(args[0], (127, "", f"{args[0]}: command not scripted"))
        if isinstance(reply, str):
            reply = (0, reply, "")
        returncode, stdout, stderr = reply
        return CommandResult(args, returncode, stdout, stderr)

    def programs_called(self):
        return [call[0] for call in self.calls]
