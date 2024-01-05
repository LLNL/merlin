# The Specification File

## The `description` Block

## The `environment` Block

## The `global.parameters` Block

## The `batch` Block


### Scheduler Specific Run Options

**Slurm specific run options:**

| Option Name | Description |
| ----------- | ----------- |
|   `slurm`   | Verbatim flags only for the srun parallel launch. This will be expanded as follows for steps that use [`LAUNCHER` or `VLAUNCHER`](./variables.md#the-launcher-and-vlauncher-variables): `srun -N <nodes> -n <procs> ... <slurm>`. |

**Flux specific run options:**

| Option Name | Description |
| ----------- | ----------- |
|    `flux`   | Verbatim flags for the flux parallel launch. This will be expanded as follows for steps that use [`LAUNCHER` or `VLAUNCHER`](./variables.md#the-launcher-and-vlauncher-variables): `flux mini run ... <flux>` |

**LSF specific run options:**

| Option Name           | Description |
| --------------------- | ----------- |
|         `bind`        | Flag for MPI binding of tasks on a node (default: `-b rs`) |
|   `num resource set`  | Number of resource sets |
| `launch_distribution` | The distribution of resources (default: `plane:{procs/nodes}`) |
|         `lsf`         | Verbatim flags only for the lsf parallel launch. This will be expanded as follows for steps that use [`LAUNCHER` or `VLAUNCHER`](./variables.md#the-launcher-and-vlauncher-variables): `jsrun ... <lsf>` |

## The `study` Block

## The `merlin` Block

## The `user` Block

## Full Specification
