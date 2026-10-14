"""Shared test doubles and fixture loading."""
from __future__ import annotations

import asyncio
import os

from tminus.backend.fake import FakeBackend
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


class CountingFake(FakeBackend):
    """A FakeBackend that counts how many status queries it receives."""

    def __init__(self):
        super().__init__()
        self.status_calls = []

    def status(self, job_ids):
        self.status_calls.append(list(job_ids))
        return super().status(job_ids)


class Timeline:
    """A fake clock whose sleep() moves jobs through scripted states.

    ``at(n, job, state)`` makes ``job`` reach ``state`` during the n-th sleep,
    so the poll after that sleep sees it.
    """

    def __init__(self, backend):
        self.backend = backend
        self.now = 0.0
        self.sleeps = []
        self._events = {}

    def at(self, sleep_number, job, state, exit_code=None):
        return self.call_at(sleep_number, lambda: self.backend.set_state(job.job_id, state, exit_code=exit_code))

    def call_at(self, sleep_number, action):
        """Run ``action()`` during the n-th sleep."""
        self._events.setdefault(sleep_number, []).append(action)
        return self

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds
        for action in self._events.get(len(self.sleeps), []):
            action()

    async def async_sleep(self, seconds):
        """The asyncio flavour of sleep(): no real time passes, but other tasks get to run."""
        self.sleep(seconds)
        await asyncio.sleep(0)
