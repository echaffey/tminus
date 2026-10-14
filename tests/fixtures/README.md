# Slurm output fixtures

Recorded `squeue --json` / `sacct --json` output, one directory per Slurm version.

The `slurm-21.08` and `slurm-23.11` sets are **hand-built from the Slurm documentation** and trimmed to the fields tminus reads. They cover the two JSON shapes tminus must handle: 21.08 prints states as plain strings and numbers as plain integers; 23.11 prints states as lists and numbers as `{"set", "infinite", "number"}` objects.

Real output captured from the target cluster (ticket 02) goes in a directory named for its exact version and replaces these as the primary test data.

Every set contains the same jobs:

| Job | Where | State | Comment |
| --- | --- | --- | --- |
| 1001 | squeue | RUNNING | `tminus:exp=exp42,run=1` |
| 1002 | squeue | PENDING (Priority) | an ordinary user comment |
| 1003 | sacct | COMPLETED, exit 0 | `tminus:exp=exp42` |
| 1004 | sacct | FAILED, exit 3 | none |
| 1005 | sacct | TIMEOUT | none |

Array jobs (all tagged `tminus:exp=arrays`):

| Array | Task | Where | State |
| --- | --- | --- | --- |
| 2000 | 0 | sacct | COMPLETED, exit 0 |
| 2000 | 1 | sacct | FAILED, exit 2 |
| 2000 | 2 | squeue and sacct (unique job id 2003) | RUNNING |
| 2000 | 3 | squeue (collapsed pending record `3%2`) and sacct (unstarted `3`) | PENDING (JobArrayTaskLimit) |
| 2010 | 0, 1 | sacct | COMPLETED |
