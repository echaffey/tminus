# Slurm output fixtures

Recorded `squeue --json` / `sacct --json` output, one directory per Slurm version.

The `slurm-21.08` and `slurm-23.11` sets are **hand-built from the Slurm documentation** and trimmed to the fields tminus reads. They cover the two JSON shapes tminus must handle: 21.08 prints states as plain strings and numbers as plain integers; 23.11 prints states as lists and numbers as `{"set", "infinite", "number"}` objects.

Real output captured from the target cluster (ticket 02) goes in a directory named for its exact version and replaces these as the primary test data.

Every set contains the same jobs:

| Job | Where | State |
| --- | --- | --- |
| 1001 | squeue | RUNNING |
| 1002 | squeue | PENDING (Priority) |
| 1003 | sacct | COMPLETED, exit 0 |
| 1004 | sacct | FAILED, exit 3 |
| 1005 | sacct | TIMEOUT |
