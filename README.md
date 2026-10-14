# tminus

A Python library and command line for working with Slurm jobs from scripts on an HPC login node: submit them, wait for them, change them, clean up after them, and see how well they used what they asked for.

By default tminus keeps nothing on disk: you work with job ids, or the `Job` objects your script holds. If you also want to come back later, from a new shell or another script, and say "wait for everything tagged `exp=sweep3`" or "delete the logs of the ones that succeeded", turn on its job record by setting `TMINUS_DB` (see [Finding jobs again later](#finding-jobs-again-later)).

> **Status: early.** The test suite runs against recorded Slurm output and a built-in fake scheduler, but tminus has not yet been run against a live cluster. Expect rough edges, especially in parsing output from your site's Slurm version. Bug reports with the failing `squeue --json` or `sacct --json` output are very welcome.

## Requirements

- Python 3.7 or newer
- Slurm 21.08 or newer (tminus reads `squeue --json` and `sacct --json`)
- Nothing else: tminus uses only the Python standard library, so it works on clusters where `pip` cannot reach PyPI.

## Install

Run it straight from a checkout, with nothing installed:

```bash
git clone https://github.com/echaffey/tminus.git
export PATH="$PWD/tminus/bin:$PATH"     # gives you the `tminus` command
export PYTHONPATH="$PWD/tminus/src"     # lets scripts `import tminus`
```

Or install it without any network access:

```bash
cd tminus
pip install --user --no-index --no-build-isolation .
```

## From the command line

```bash
# Submit, tagging the job so you can find it later
tminus submit --tag exp=sweep3 --partition gpu --cpus 4 --mem 16G --time 02:00:00 \
    --output logs/%j.out --wrap "python train.py"

# An array job, and a job that only runs if the first one succeeds
tminus submit --tag exp=sweep3 --array 0-9%4 job.sh
tminus submit --tag exp=sweep3 --depends-on 123456 --wrap "python summarize.py"

tminus status 123456 123457                 # one line per job; arrays show task counts
tminus wait 123456 --timeout 3600           # exits 0 if all completed, 1 if any failed, 124 on timeout
tminus report 123456                        # CPU, memory, and time-limit efficiency (also --format csv|json)
tminus update 123456 --time-limit 04:00:00  # also: hold, release, requeue, cancel
```

With the job record on (`TMINUS_DB` set before submitting), commands can select jobs by tag instead of id:

```bash
tminus status --tag exp=sweep3
tminus wait --tag exp=sweep3 --timeout 3600
tminus report --tag exp=sweep3
tminus cancel --tag exp=sweep3 --yes        # without --yes it only lists what it would do
tminus clean --tag exp=sweep3 --policy on_success --yes   # delete logs of jobs that succeeded
tminus prune --older-than 30d --yes         # forget old finished jobs
```

`tminus <command> --help` describes every option.

## From Python

```python
import tminus

spec = tminus.JobSpec(
    command="python train.py",
    cpus=4, mem="16G", time="02:00:00", partition="gpu",
    output="logs/%j.out",
    tags={"exp": "sweep3"},
)
jobs = [tminus.submit(spec) for _ in range(3)]

status = jobs[0].wait(timeout=3600)      # blocks; raises tminus.WaitTimeout if it takes longer
print(status.state, status.exit_code)

statuses = tminus.wait_all(jobs)
print(tminus.report(jobs=jobs).to_table())

tminus.cleanup.purge(jobs=jobs, policy="on_success")   # delete logs of the jobs that succeeded
```

With asyncio, `await tminus.aio.wait(job)`, `await tminus.aio.wait_all(jobs)`, and `async for job in tminus.aio.as_completed(jobs)` wait without blocking the event loop. Concurrent waits share one batched query per poll.

## How it behaves

- **Gentle on the scheduler.** Waiting polls every outstanding job in one `squeue` call (then one `sacct` call for jobs that have left the queue), backing off from 5 seconds to once a minute.
- **Tags travel with the job.** Tags are written into the job's Slurm comment, so they show up in `squeue` whether or not the job record is on.
- **Careful with deletion.** `clean` only deletes a finished job's log files and any `scratch_paths` the job spec declared. It never deletes working directories, never touches running jobs, skips filename patterns it cannot expand, and refuses the filesystem root and your home directory. Group commands (`cancel --tag`, `clean`, `prune`) are dry runs unless you pass `--yes`.
- **Honest reports.** Efficiency figures come from `sacct`. Anything Slurm did not report, and every figure for a job that has not finished, shows as unavailable rather than zero.

## Finding jobs again later

Set `TMINUS_DB` to a file path before submitting, and tminus records each job it submits (its spec, tags, and submit directory) in that small SQLite file, along with every state change it sees and anything it does to the job:

```bash
export TMINUS_DB=~/.local/share/tminus/jobs.db
```

That is what lets a later shell or script find jobs by tag (`--tag` on the command line, `tminus.jobs(tags=...)` and the `tags=` arguments in Python) and `prune` old records. Without it, those raise an error explaining how to turn the record on; everything else works the same. In Python you can also pass `store=tminus.Store(path)` explicitly. If your home directory is on NFS and you see locking errors, point `TMINUS_DB` at local disk.

## Trying it without a cluster

Set `TMINUS_BACKEND=fake` to use a simulated scheduler that keeps its state next to the tracking database. Jobs submitted to it stay pending unless moved from Python, which makes it most useful for tests:

```python
from tminus import JobSpec, JobState, submit
from tminus.backend.fake import FakeBackend

backend = FakeBackend()
job = submit(JobSpec(command="true"), backend=backend)
backend.set_state(job.job_id, JobState.COMPLETED, exit_code=0)
print(job.refresh().state)   # JobState.COMPLETED
```

## Development

```bash
python -m unittest        # from the repository root; standard library only
```

The suite includes a check that every source file stays compatible with Python 3.7.

## License

[MIT](LICENSE)
