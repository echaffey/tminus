"""The ``tminus`` command line: a thin layer over the public API."""
from __future__ import annotations

import argparse
import sys
from typing import List, Optional

from . import JobSpec, TminusError, __version__, submit


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
    submit_cmd.set_defaults(handler=_submit)
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
    )
    print(submit(spec).job_id)
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    """Run the CLI with ``argv`` (default: ``sys.argv[1:]``) and return its exit status."""
    args = _build_parser().parse_args(argv)
    try:
        return args.handler(args)
    except TminusError as exc:
        print(f"tminus: error: {exc}", file=sys.stderr)
        return 1
