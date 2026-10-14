"""SlurmCliBackend: drives a real Slurm cluster through its command-line tools."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from ..errors import SlurmCommandError, SubmitError
from ..parse import parse_sacct, parse_squeue
from ..spec import JobSpec, tags_to_comment
from ..state import JobStatus
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


# UPDATABLE_FIELDS name -> scontrol update field, in command-line order.
_SCONTROL_FIELDS = (
    ("time_limit", "TimeLimit"),
    ("partition", "Partition"),
    ("name", "JobName"),
    ("qos", "QOS"),
    ("account", "Account"),
    ("nodes", "NumNodes"),
)


def sbatch_args(spec: JobSpec) -> List[str]:
    """The ``sbatch`` command line that submits ``spec``."""
    args = ["sbatch", "--parsable"]
    for field_name, option in _SBATCH_OPTIONS:
        value = getattr(spec, field_name)
        if value is not None:
            args.append(f"{option}={value}")
    if spec.array is not None:
        args.append(f"--array={spec.array}")
    if spec.depends_on:
        args.append(f"--dependency={spec.dependency_type.value}:" + ":".join(spec.depends_on))
    if spec.tags:
        args.append("--comment=" + tags_to_comment(spec.tags))
    args.extend(spec.extra_args)
    if spec.command is not None:
        args.append(f"--wrap={spec.command}")
    else:
        args.append(spec.script)
    return args


class SlurmCliBackend(Backend):
    """Runs ``sbatch`` and friends through a command Runner (local subprocesses by default)."""

    name = "slurm"

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

    def status(self, job_ids: Sequence[str]) -> Dict[str, JobStatus]:
        """Ask squeue about every job, then sacct about any squeue no longer knows.

        An array job is summarised from all of its tasks. Finished tasks drop
        out of squeue, so for an array squeue still lists, sacct is asked too
        and squeue's view wins for tasks both report.
        """
        wanted = list(dict.fromkeys(job_ids))
        if not wanted:
            return {}
        queued = self._query("squeue", wanted, parse_squeue)
        direct = {s.job_id: s for s in queued if s.job_id in wanted}
        arrays_in_queue = [job_id for job_id in wanted if _tasks_of(job_id, queued)]
        ask_sacct = [job_id for job_id in wanted if job_id not in direct and job_id not in arrays_in_queue]
        accounted = self._query("sacct", ask_sacct + arrays_in_queue, parse_sacct) if (
            ask_sacct or arrays_in_queue) else []
        result: Dict[str, JobStatus] = {}
        for job_id in wanted:
            tasks = {**_tasks_of(job_id, accounted), **_tasks_of(job_id, queued)}
            if tasks:
                result[job_id] = JobStatus.for_array(job_id, tasks)
            elif job_id in direct:
                result[job_id] = direct[job_id]
            else:
                found = [s for s in accounted if s.job_id == job_id]
                if found:
                    result[job_id] = found[-1]
        return result

    def _query(self, program, job_ids, parse) -> List[JobStatus]:
        result = self._runner.run([program, "--json", "--jobs=" + ",".join(job_ids)])
        if result.returncode != 0:
            # squeue rejects ids slurmctld has already forgotten; sacct still has them.
            if program == "squeue" and "Invalid job id" in result.stderr:
                return {}
            raise SlurmCommandError(result.args, result.returncode, result.stderr)
        # Older Slurm ignores --jobs when printing JSON, so filter here as well: keep the
        # jobs asked for, plus the tasks of any array asked for.
        wanted = set(job_ids)
        return [s for s in parse(result.stdout) if s.job_id in wanted or s.job_id.split("_", 1)[0] in wanted]

    def cancel(self, job_ids: Sequence[str]) -> None:
        result = self._runner.run(["scancel"] + list(job_ids))
        # A job can finish between deciding to cancel it and scancel running; that is fine.
        if result.returncode != 0 and "already completing or completed" not in result.stderr:
            raise SlurmCommandError(result.args, result.returncode, result.stderr)

    def hold(self, job_ids: Sequence[str]) -> None:
        self._scontrol(["hold", ",".join(job_ids)])

    def release(self, job_ids: Sequence[str]) -> None:
        self._scontrol(["release", ",".join(job_ids)])

    def requeue(self, job_ids: Sequence[str]) -> None:
        self._scontrol(["requeue", ",".join(job_ids)])

    def update(self, job_id: str, changes: Dict[str, Any]) -> None:
        fields = [f"{slurm_name}={changes[name]}" for name, slurm_name in _SCONTROL_FIELDS if name in changes]
        self._scontrol(["update", f"JobId={job_id}"] + fields)

    def _scontrol(self, args: List[str]) -> None:
        result = self._runner.run(["scontrol"] + args)
        if result.returncode != 0:
            raise SlurmCommandError(result.args, result.returncode, result.stderr)


def _tasks_of(array_id: str, statuses: List[JobStatus]) -> Dict[str, JobStatus]:
    """The task statuses belonging to ``array_id`` (none if it is a task id itself)."""
    if "_" in array_id:
        return {}
    prefix = array_id + "_"
    return {s.job_id: s for s in statuses if s.job_id.startswith(prefix)}
