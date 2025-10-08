# Retrieving Data

!!! note "Note on Entities"

    It's highly recommended that you read through [Understanding Merlin's Database Entities](./entities.md) before diving into this page.

!!! note "Note on Examples in this Page"

    See [Setting Up Test Data](./database_cmd.md#setting-up-test-data) to understand how the examples in this command were created.

The `merlin database get` subcommand allows users to query the database for specific information. This subcommand includes several options for retrieving data:

- [`all-studies`](#retrieving-all-studies): Retrieves information about all studies in the database.
- [`study`](#retrieving-specific-studies): Retrieves information about specific studies in the database.
- [`all-runs`](#retrieving-all-runs): Retrieves information about all runs in the database.
- [`run`](#retrieving-specific-runs): Retrieves information about specific runs in the database.
- [`all-logical-workers`](#retrieving-all-logical-workers): Retrieves information about all logical workers in the database.
- [`logical-worker`](#retrieving-specific-logical-workers): Retrieves information about specific logical workers in the database.
- [`all-physical-workers`](#retrieving-all-physical-workers): Retrieves information about all physical workers in the database.
- [`physical-worker`](#retrieving-specific-physical-workers): Retrieves information about specific physical workers in the database.
- [`everything`](#retrieving-everything): Retrieves information about every entry in the database.

The following sections demonstrate how to use each option listed above.

## Retrieving All Studies

Using the `all-studies` option will retrieve and display each study in our database. When displayed, each study will display every field stored in a study entry.

Let's try this out. Executing the following command:

```bash
merlin database get all-studies
```

Will provide output similar to the following:

```bash
Study with ID 75f49c2d-7135-41a8-a858-efad4ff19961
------------------------------------------------
Name: hello
Runs:
  - ID: 0bdbae0b-c321-4178-a5a2-ab1ea6067be7
    Workspace: /path/to/hello_20250508-161150
Additional Data: {}


Study with ID 837fafbe-4f40-4e47-8dd7-abb17142caed
------------------------------------------------
Name: hello_samples
Runs:
  - ID: c735ade0-9b28-4b9e-bb46-d9429d7cf61a
    Workspace: /path/to/hello_samples_20250508-161159
Additional Data: {}
```

The output includes the following fields:

| Field            | Description                                                       |
|------------------|-------------------------------------------------------------------|
| **ID**           | A unique identifier for the study.                                |
| **Name**         | The name assigned to the study.                                   |
| **Runs**         | A list of associated runs, with details such as ID and workspace. |
| **Additional Data** | Any extra metadata stored with the study.                      |

## Retrieving Specific Studies

To obtain information about specific studies, users can pass the name or ID of one or more studies to the `merlin database get study` command.

For example, let's query just the "hello_samples" study:

```bash
merlin database get study hello_samples
```

Which should display just the "hello_samples" study entry:

```bash
Study with ID 837fafbe-4f40-4e47-8dd7-abb17142caed
------------------------------------------------
Name: hello_samples
Runs:
  - ID: c735ade0-9b28-4b9e-bb46-d9429d7cf61a
    Workspace: /path/to/hello_samples_20250508-161159
Additional Data: {}
```

## Retrieving All Runs

Using the `all-runs` option will retrieve and display each run in our database. When displayed, each run will display every field stored in a run entry.

Let's try this out. Executing the following command:

```bash
merlin database get all-runs
```

Will provide output similar to the following:

```bash
Run with ID c735ade0-9b28-4b9e-bb46-d9429d7cf61a
------------------------------------------------
Workspace: /path/to/hello_samples_20250508-161159
Study:
  - ID: 837fafbe-4f40-4e47-8dd7-abb17142caed
    Name: hello_samples
Queues: ['[merlin]_step_1_queue', '[merlin]_step_2_queue']
Workers: ['4b0cd8f6-35a3-b484-4603-fa55eb0e7134']
Parent: None
Child: None
Run Complete: False
Additional Data: {}


Run with ID 0bdbae0b-c321-4178-a5a2-ab1ea6067be7
------------------------------------------------
Workspace: /path/to/hello_20250508-161150
Study:
  - ID: 75f49c2d-7135-41a8-a858-efad4ff19961
    Name: hello
Queues: ['[merlin]_merlin']
Workers: ['2f740737-a727-ea7d-6de4-17dc643183bb']
Parent: None
Child: None
Run Complete: False
Additional Data: {}
```

The output includes the following fields:

| Field              | Description                                    |
|--------------------|------------------------------------------------|
| **ID**             | A unique identifier for the run.               |
| **Workspace**      | The workspace directory for the run.           |
| **Study**          | The unique identifier of the associated study. |
| **Queues**         | A list of queues used by the run.              |
| **Workers**        | A list of workers assigned to the run.         |
| **Parent**         | The parent run ID, if any.                     |
| **Child**          | The child run ID, if any.                      |
| **Run Complete**   | Indicates whether the run is complete.         |
| **Additional Data**| Any extra metadata stored with the run.        |

## Retrieving Specific Runs

To obtain information about specific runs, users can pass the ID or workspace of one or more runs to the `merlin database get run` command.

For example, let's query the run associated with the "hello" study:

```bash
merlin database get run f93eecdf-d573-43d1-a3f9-c728c15802ea
```

Which should display just the one run entry:

```bash
Run with ID 0bdbae0b-c321-4178-a5a2-ab1ea6067be7
------------------------------------------------
Workspace: /path/to/hello_20250508-161150
Study:
  - ID: 75f49c2d-7135-41a8-a858-efad4ff19961
    Name: hello
Queues: ['[merlin]_merlin']
Workers: ['2f740737-a727-ea7d-6de4-17dc643183bb']
Parent: None
Child: None
Run Complete: False
Additional Data: {}
```

## Retrieving All Logical Workers

Using the `all-logical-workers` option will retrieve and display each logical worker in our database. When displayed, each logical worker will display every field stored in a logical worker entry.

Let's try this out. Executing the following command:

```bash
merlin database get all-logical-workers
```

Will provide output similar to the following:

```bash
Logical Worker with ID 4b0cd8f6-35a3-b484-4603-fa55eb0e7134
------------------------------------------------
Name: hello_samples_worker
Runs:
  - ID: c735ade0-9b28-4b9e-bb46-d9429d7cf61a
    Workspace: /path/to/hello_samples_20250508-161159
Queues: {'[merlin]_step_1_queue', '[merlin]_step_2_queue'}
Physical Workers:
  - ID: 9a6b8bec-2ede-4a8c-bb07-0778c5c5f356
    Name: celery@hello_samples_worker.%ruby10
Additional Data: {}


Logical Worker with ID 2f740737-a727-ea7d-6de4-17dc643183bb
------------------------------------------------
Name: hello_worker
Runs:
  - ID: 0bdbae0b-c321-4178-a5a2-ab1ea6067be7
    Workspace: /path/to/hello_20250508-161150
Queues: {'[merlin]_merlin'}
Physical Workers:
  - ID: 8549ed5f-83df-4922-aaac-16f676112322
    Name: celery@hello_worker.%ruby9
Additional Data: {}
```

The output includes the following fields:

| Field                | Description                                                      |
|----------------------|------------------------------------------------------------------|
| **ID**               | A unique identifier for the logical worker.                      |
| **Name**             | The name of the logical worker from the spec.                    |
| **Runs**             | The runs utilizing this logical worker.                          |
| **Queues**           | A list of queues that the logical worker is watching.            |
| **Physical Workers** | A list of physical worker instantiations of this logical worker. |
| **Additional Data**  | Any extra metadata stored with the logical worker.               |

## Retrieving Specific Logical Workers

To obtain information about specific logical workers, users can pass the ID of one or more logical workers to the `merlin database get logical-worker` command.

For example, let's query the logical worker with the name "hello_worker":

```bash
merlin database get logical-worker 2f740737-a727-ea7d-6de4-17dc643183bb
```

Which should display just the one logical worker entry:

```bash
Logical Worker with ID 2f740737-a727-ea7d-6de4-17dc643183bb
------------------------------------------------
Name: hello_worker
Runs:
  - ID: 0bdbae0b-c321-4178-a5a2-ab1ea6067be7
    Workspace: /path/to/hello_20250508-161150
Queues: {'[merlin]_merlin'}
Physical Workers:
  - ID: 8549ed5f-83df-4922-aaac-16f676112322
    Name: celery@hello_worker.%ruby9
Additional Data: {}
```

## Retrieving All Physical Workers

Using the `all-physical-workers` option will retrieve and display each physical worker in our database. When displayed, each physical worker will display every field stored in a physical worker entry.

Let's try this out. Executing the following command:

```bash
merlin database get all-physical-workers
```

Will provide output similar to the following:

```bash
Physical Worker with ID 8549ed5f-83df-4922-aaac-16f676112322
------------------------------------------------
Name: celery@hello_worker.%ruby9
Logical Worker ID: 2f740737-a727-ea7d-6de4-17dc643183bb
Launch Command: None
Args: {}
Process ID: 228105
Status: WorkerStatus.RUNNING
Last Heartbeat: 2025-05-08 16:13:22.793487
Last Spinup: 2025-05-08 16:13:22.793490
Host: ruby9
Restart Count: 0.0
Additional Data: {}


Physical Worker with ID 9a6b8bec-2ede-4a8c-bb07-0778c5c5f356
------------------------------------------------
Name: celery@hello_samples_worker.%ruby10
Logical Worker ID: 4b0cd8f6-35a3-b484-4603-fa55eb0e7134
Launch Command: None
Args: {}
Process ID: 650803
Status: WorkerStatus.RUNNING
Last Heartbeat: 2025-05-08 16:13:25.766678
Last Spinup: 2025-05-08 16:13:25.766681
Host: ruby10
Restart Count: 0.0
Additional Data: {}
```

The output includes the following fields:

| Field                 | Description                                            |
|-----------------------|--------------------------------------------------------|
| **ID**                | A unique identifier for the physical worker.           |
| **Name**              | The name of the physical worker from Celery.           |
| **Logical Worker ID** | The logical worker that this is associated with.       |
| **Launch Command**    | The command used to launch this worker.                |
| **Args**              | The arguments passed to this worker.                   |
| **Process ID**        | The ID of the process that's running this worker       |
| **Status**            | The current status of this worker.                     |
| **Last Heartbeat**    | The last time a heartbeat was received by this worker. |
| **Last Spinup**       | The last time this worker was spun up.                 |
| **Host**              | The host that this worker is running on.               |
| **Restart Count**     | The number of times this worker has been restarted.    |
| **Additional Data**   | Any extra metadata stored with the physical worker.    |

## Retrieving Specific Physical Workers

To obtain information about specific physical workers, users can pass the ID or name of one or more physical workers to the `merlin database get physical-worker` command.

For example, let's query the physical worker for the "hello_samples" workflow:

```bash
merlin database get physical-worker celery@hello_samples_worker.%ruby10
```

Which should display just the one physical worker entry:

```bash
Physical Worker with ID 9a6b8bec-2ede-4a8c-bb07-0778c5c5f356
------------------------------------------------
Name: celery@hello_samples_worker.%ruby10
Logical Worker ID: 4b0cd8f6-35a3-b484-4603-fa55eb0e7134
Launch Command: None
Args: {}
Process ID: 650803
Status: WorkerStatus.RUNNING
Last Heartbeat: 2025-05-08 16:13:25.766678
Last Spinup: 2025-05-08 16:13:25.766681
Host: ruby10
Restart Count: 0.0
Additional Data: {}
```

## Retrieving Everything

!!! tip "Large Databases"

    If you have a large database with many entries, the `everything` option may produce substantial output. Consider using the [`merlin database info`](./info.md) command first to see how many entries exist, or use the specific entity type retrieval commands to get only what you need.

Using the `everything` option will retrieve and display every entry in our database. In our case this would be 8 entries: 2 studies, 2 runs, 2 logical workers, and 2 physical workers.

If you want to give this a shot use:

```bash
merlin database get everything
```