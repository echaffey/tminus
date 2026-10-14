"""The ``tminus`` command line: a thin layer over the public API."""
from __future__ import annotations

import argparse
import sys
from typing import List, Optional, Tuple

from . import DependencyType, InvalidAgeError, Job, JobSpec, JobStatus, TminusError, WaitTimeout, __version__, get_backend, jobs, prune, submit, wait_all
from .cleanup import parse_age


def _tag(text: str) -> Tuple[str, str]:
    key, sep, value = text.partition("=")
    if not sep or not key or not value:
        raise argparse.ArgumentTypeError(f"expected KEY=VALUE, got {text!r}")
    return key, value


def _age(text: str) -> float:
    try:
        return parse_age(text)
    except InvalidAgeError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from None


def _add_tag_option(parser: argparse.ArgumentParser, help_text: str) -> None:
    parser.add_argument(
        "--tag", dest="tags", type=_tag, action="append", default=[], metavar="KEY=VALUE", help=help_text
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tminus",
        description="Schedule, monitor, modify, clean up after, and report on Slurm jobs.",
    )
    parser.add_argument("--version", action="version", version=f"tminus {__version__}")
    commands = parser.add_subparsers(dest="command", metavar="COMMAND")
    commands.required = True

    submit_cmd = commands.add_parser("submit", help="submit a job and print its id")
    what = submit_cmd.add_mutually_exclusive_group(required=True)
    what.add_argument("script", nargs="?", help="batch script to submit")
    what.add_argument("--wrap", metavar="COMMAND", help="shell command to run instead of a script")
    submit_cmd.add_argument("--name", help="job name")
    submit_cmd.add_argument("--partition")
    submit_cmd.add_argument("--account")
    submit_cmd.add_argument("--qos")
    submit_cmd.add_argument("--nodes", type=int)
    submit_cmd.add_argument("--ntasks", type=int)
    submit_cmd.add_argument("--cpus", type=int, help="CPUs per task")
    submit_cmd.add_argument("--mem", help="memory per node, e.g. 16G")
    submit_cmd.add_argument("--time", help="time limit, e.g. 02:00:00")
    submit_cmd.add_argument("--workdir", help="working directory for the job")
    submit_cmd.add_argument("--output", help="stdout path (sbatch filename patterns allowed)")
    submit_cmd.add_argument("--error", help="stderr path (sbatch filename patterns allowed)")
    submit_cmd.add_argument(
        "--sbatch-arg",
        dest="extra_args",
        action="append",
        default=[],
        metavar="ARG",
        help="extra sbatch argument, passed through as-is (repeatable), e.g. --sbatch-arg=--exclusive",
    )
    _add_tag_option(submit_cmd, "label the job (repeatable), e.g. --tag exp=exp42")
    submit_cmd.add_argument(
        "--depends-on", dest="depends_on", action="append", default=[], metavar="JOBID",
        help="only start after this job (repeatable); see --dependency-type",
    )
    submit_cmd.add_argument(
        "--dependency-type", dest="dependency_type", default="afterok",
        choices=[kind.value for kind in DependencyType],
        help="when dependent jobs may start (default: afterok, once all dependencies succeed)",
    )
    submit_cmd.set_defaults(handler=_submit)

    status_cmd = commands.add_parser("status", help="show the current state of jobs")
    status_cmd.add_argument("job_ids", nargs="*", metavar="JOBID")
    _add_tag_option(status_cmd, "include every tracked job with this tag (repeatable; all must match)")
    status_cmd.set_defaults(handler=_status)

    wait_cmd = commands.add_parser(
        "wait",
        help="block until jobs finish",
        description="Block until every job is finished, then show their final states. Exits 0 if "
        "every job completed, 1 if any did not, and 124 if the timeout expired first.",
    )
    wait_cmd.add_argument("job_ids", nargs="*", metavar="JOBID")
    _add_tag_option(wait_cmd, "include every tracked job with this tag (repeatable; all must match)")
    wait_cmd.add_argument("--timeout", type=float, metavar="SECONDS", help="give up after this long")
    wait_cmd.set_defaults(handler=_wait)

    cancel_cmd = commands.add_parser("cancel", help="cancel jobs (finished jobs are skipped)")
    cancel_cmd.add_argument("job_ids", nargs="+", metavar="JOBID")
    cancel_cmd.set_defaults(handler=_cancel)

    update_cmd = commands.add_parser("update", help="change a submitted job's time limit, partition, ...")
    update_cmd.add_argument("job_id", metavar="JOBID")
    update_cmd.add_argument("--time-limit", dest="time_limit", help="new time limit, e.g. 04:00:00")
    update_cmd.add_argument("--partition")
    update_cmd.add_argument("--name", help="new job name")
    update_cmd.add_argument("--qos")
    update_cmd.add_argument("--account")
    update_cmd.add_argument("--nodes", type=int)
    update_cmd.set_defaults(handler=_update)

    for name, help_text in (
        ("hold", "keep pending jobs from starting"),
        ("release", "let held jobs start"),
        ("requeue", "put jobs back in the queue to run again"),
    ):
        control_cmd = commands.add_parser(name, help=help_text)
        control_cmd.add_argument("job_ids", nargs="+", metavar="JOBID")
        control_cmd.set_defaults(handler=_control, action=name)

    prune_cmd = commands.add_parser(
        "prune",
        help="forget old finished jobs (dry run unless --yes)",
        description="Remove tracking records of finished jobs submitted more than --older-than ago. "
        "Active jobs are never removed. Lists what would be removed unless --yes is given.",
    )
    prune_cmd.add_argument("--older-than", dest="older_than", type=_age, required=True, metavar="AGE",
                           help="e.g. 30d, 12h, 2w")
    _add_tag_option(prune_cmd, "only consider jobs with this tag (repeatable; all must match)")
    prune_cmd.add_argument("--yes", action="store_true", help="actually remove the records")
    prune_cmd.set_defaults(handler=_prune)
    return parser


def _submit(args: argparse.Namespace) -> int:
    spec = JobSpec(
        command=args.wrap,
        script=args.script,
        name=args.name,
        partition=args.partition,
        account=args.account,
        qos=args.qos,
        nodes=args.nodes,
        ntasks=args.ntasks,
        cpus=args.cpus,
        mem=args.mem,
        time=args.time,
        workdir=args.workdir,
        output=args.output,
        error=args.error,
        extra_args=args.extra_args,
        tags=dict(args.tags),
        depends_on=args.depends_on,
        dependency_type=args.dependency_type,
    )
    print(submit(spec).job_id)
    return 0


def _status(args: argparse.Namespace) -> int:
    selected = _selected_jobs(args)
    statuses = selected[0].backend.status([job.job_id for job in selected])
    _print_statuses([statuses[job.job_id] for job in selected if job.job_id in statuses])
    missing = [job.job_id for job in selected if job.job_id not in statuses]
    for job_id in missing:
        print(f"tminus: error: no record of job {job_id}", file=sys.stderr)
    return 1 if missing else 0


def _wait(args: argparse.Namespace) -> int:
    selected = _selected_jobs(args)
    try:
        statuses = wait_all(selected, timeout=args.timeout)
    except WaitTimeout as exc:
        _print_statuses([exc.statuses[job.job_id] for job in selected if job.job_id in exc.statuses])
        print(f"tminus: error: {exc}", file=sys.stderr)
        return 124
    _print_statuses([statuses[job.job_id] for job in selected])
    return 0 if all(found.state.is_success for found in statuses.values()) else 1


def _cancel(args: argparse.Namespace) -> int:
    backend = get_backend()
    for job_id in args.job_ids:
        if Job(job_id, None, backend).cancel():
            print(f"cancelled {job_id}")
        else:
            print(f"{job_id} already finished")
    return 0


def _update(args: argparse.Namespace) -> int:
    changes = {
        name: getattr(args, name)
        for name in ("time_limit", "partition", "name", "qos", "account", "nodes")
        if getattr(args, name) is not None
    }
    if not changes:
        raise TminusError("nothing to update: give at least one of --time-limit, --partition, "
                          "--name, --qos, --account, --nodes")
    Job(args.job_id, None, get_backend()).update(**changes)
    return 0


def _control(args: argparse.Namespace) -> int:
    backend = get_backend()
    for job_id in args.job_ids:
        getattr(Job(job_id, None, backend), args.action)()
    return 0


def _prune(args: argparse.Namespace) -> int:
    removed = prune(args.older_than, tags=dict(args.tags) or None, dry_run=not args.yes)
    verb = "removed" if args.yes else "would remove"
    print(f"{verb} {len(removed)} record(s)" + (": " + " ".join(removed) if removed else ""))
    if removed and not args.yes:
        print("run again with --yes to remove them")
    return 0


def _selected_jobs(args: argparse.Namespace) -> List[Job]:
    """The jobs named by JOBID arguments followed by those matching every --tag, without repeats."""
    backend = get_backend()
    selected = {job_id: Job(job_id, None, backend) for job_id in args.job_ids}
    if args.tags:
        for job in jobs(tags=dict(args.tags), backend=backend):
            selected.setdefault(job.job_id, job)
    elif not selected:
        raise TminusError("give at least one JOBID or --tag")
    if not selected:
        wanted = " ".join(f"{key}={value}" for key, value in args.tags)
        raise TminusError(f"no tracked jobs have every tag in: {wanted}")
    return list(selected.values())


def _print_statuses(statuses: List[JobStatus]) -> None:
    rows = [["JOBID", "STATE", "EXIT", "REASON"]]
    for found in statuses:
        exit_code = "-" if found.exit_code is None else str(found.exit_code)
        rows.append([found.job_id, found.state.value, exit_code, found.reason or "-"])
    _print_table(rows)


def _print_table(rows: List[List[str]]) -> None:
    widths = [max(len(row[col]) for row in rows) for col in range(len(rows[0]))]
    for row in rows:
        print("  ".join(cell.ljust(width) for cell, width in zip(row, widths)).rstrip())


def main(argv: Optional[List[str]] = None) -> int:
    """Run the CLI with ``argv`` (default: ``sys.argv[1:]``) and return its exit status."""
    args = _build_parser().parse_args(argv)
    try:
        return args.handler(args)
    except TminusError as exc:
        print(f"tminus: error: {exc}", file=sys.stderr)
        return 1
