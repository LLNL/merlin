# Merlin's Database

When a Merlin study is run with the [`merlin run`](../command_line.md#run-merlin-run) and [`merlin run-workers`](../command_line.md#run-workers-merlin-run-workers) commands, information about the study is stored as data in the [results backend database](../configuration/index.md#what-is-a-results-backend).

See the below sections for [how the database works](#how-it-works) and [why it's needed](#why-is-this-needed). Additional info about Merlin's database can be found at the below pages:

- [The Database Command](./database_cmd.md)
- [Understanding Merlin's Database Entities](./entities.md)

## How it Works

Unless [running locally](../running_studies.md#local-runs), Merlin requires a connection to a [results backend](../configuration/index.md#what-is-a-results-backend) database by default. Therefore, Merlin's database will automatically link to this results backend connection, using it as a store for study-related information.

When a study is converted to Celery tasks and sent to the [broker](../configuration/index.md#what-is-a-broker) with the `merlin run` command, Merlin will create the following entities in your database: a `StudyEntity` (if one does not yet exist), a `RunEntity`, and one or more `LogicalWorkerEntity` instances (if they don't already exist).

Similarly, when workers are started on your allocation with the `merlin run-workers` command, Merlin will create one or more `LogicalWorkerEntity` instances (if they don't already exist) and one or more `PhysicalWorkerEntity` instances.

## Why is this Needed?

The [entities](./entities.md) that are created in the database are used by the [`merlin monitor`](../../command_line.md#monitor-merlin-monitor) command to help keep your workflow alive during the lifetime of your allocation. You can read more about how this process works on the [Monitoring Studies for Persistent Allocations](../monitor_for_allocation.md) page.
