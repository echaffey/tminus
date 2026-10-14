"""SlurmCliBackend: drives a real Slurm cluster through its command-line tools."""
from __future__ import annotations

from typing import List, Optional

from ..errors import SlurmCommandError, SubmitError
from ..spec import JobSpec
from .base import Backend
from .runner import Runner, SubprocessRunner

# JobSpec field -> sbatch long option, in the order they appear on the command line.
_SBATCH_OPTIONS = (
    ("name", "--job-name"),
    ("partition", "--partition"),
    ("account", "--account"),
    ("qos", "--qos"),
    ("nodes", "--nodes"),
    ("ntasks", "--ntasks"),
    ("cpus", "--cpus-per-task"),
    ("mem", "--mem"),
    ("time", "--time"),
    ("workdir", "--chdir"),
    ("output", "--output"),
    ("error", "--error"),
)


def sbatch_args(spec: JobSpec) -> List[str]:
    """The ``sbatch`` command line that submits ``spec``."""
    args = ["sbatch", "--parsable"]
    for field_name, option in _SBATCH_OPTIONS:
        value = getattr(spec, field_name)
        if value is not None:
            args.append(f"{option}={value}")
    args.extend(spec.extra_args)
    if spec.command is not None:
        args.append(f"--wrap={spec.command}")
    else:
        args.append(spec.script)
    return args


class SlurmCliBackend(Backend):
    """Runs ``sbatch`` and friends through a command Runner (local subprocesses by default)."""

    def __init__(self, runner: Optional[Runner] = None) -> None:
        self._runner = runner if runner is not None else SubprocessRunner()

    def submit(self, spec: JobSpec) -> str:
        result = self._runner.run(sbatch_args(spec))
        if result.returncode != 0:
            raise SlurmCommandError(result.args, result.returncode, result.stderr)
        # --parsable prints "<job_id>" or, on multi-cluster setups, "<job_id>;<cluster>".
        job_id = result.stdout.strip().split(";", 1)[0]
        if not job_id.isdigit():
            raise SubmitError(f"sbatch did not print a job id: {result.stdout.strip()!r}")
        return job_id
