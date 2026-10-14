"""The ``tminus`` command line: a thin layer over the public API."""
from __future__ import annotations

import argparse
import sys
from typing import List, Optional, Tuple

from . import JobSpec, TminusError, __version__, jobs, status, submit


def _tag(text: str) -> Tuple[str, str]:
    key, sep, value = text.partition("=")
    if not sep or not key or not value:
        raise argparse.ArgumentTypeError(f"expected KEY=VALUE, got {text!r}")
    return key, value


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
    submit_cmd.set_defaults(handler=_submit)

    status_cmd = commands.add_parser("status", help="show the current state of jobs")
    status_cmd.add_argument("job_ids", nargs="*", metavar="JOBID")
    _add_tag_option(status_cmd, "include every tracked job with this tag (repeatable; all must match)")
    status_cmd.set_defaults(handler=_status)
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
    )
    print(submit(spec).job_id)
    return 0


def _status(args: argparse.Namespace) -> int:
    job_ids = list(args.job_ids)
    if args.tags:
        job_ids += [job.job_id for job in jobs(tags=dict(args.tags)) if job.job_id not in job_ids]
    elif not job_ids:
        raise TminusError("give at least one JOBID or --tag")
    statuses = status(job_ids)
    rows = [["JOBID", "STATE", "EXIT", "REASON"]]
    for job_id in job_ids:
        found = statuses.get(job_id)
        if found is not None:
            exit_code = "-" if found.exit_code is None else str(found.exit_code)
            rows.append([job_id, found.state.value, exit_code, found.reason or "-"])
    _print_table(rows)
    missing = [job_id for job_id in job_ids if job_id not in statuses]
    for job_id in missing:
        print(f"tminus: error: no record of job {job_id}", file=sys.stderr)
    return 1 if missing else 0


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
