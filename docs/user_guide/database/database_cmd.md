# The Database Command

The [`merlin database`](../command_line.md#database-merlin-database) command provides a straightforward way to interact with the data stored in Merlin's database. For more information on what type of data is stored in Merlin's database, see [Understanding Merlin's Database Entities](./entities.md).

**Usage:**

```bash
merlin database SUBCOMMAND
```

Currently, the `merlin database` command supports three subcommands:

- [`info`](#getting-general-database-information): Displays general information about the database.
- [`get`](#retrieving-and-displaying-entries): Retrieves specific entries from the database and prints them to the console.
- [`delete`](#deleting-entries): Removes entries from the database.

For demonstration purposes, we'll start by running the `hello.yaml` and `hello_samples.yaml` files from the [Hello World Example](../../examples/hello.md) one time each.

However, let's make a slight modification to these files to help distinguish the workers between them. Add the following [`merlin` block](../specification.md#the-merlin-block) to the `hello.yaml` file:

```yaml
merlin:
    resources:
        workers:
            hello_worker:
                args: -l INFO --concurrency 1 --prefetch-multiplier 1
                steps: [all]
```

Similarly, we're going to update the `merlin` block of the `hello_samples.yaml` file as well:

```yaml
merlin:
    samples:
        generate:
            cmd: python3 $(SPECROOT)/make_samples.py --filepath=$(MERLIN_INFO)/samples.csv --number=$(N_SAMPLES)
        file: $(MERLIN_INFO)/samples.csv
        column_labels: [WORLD]
    resources:
        workers:
            hello_samples_worker:
                args: -l INFO --concurrency 1 --prefetch-multiplier 1
                steps: [all]
```

We'll also add task queues to the steps in `hello_samples.yaml`:

```yaml
study:
    - name: step_1
      description: say hello
      run:
          cmd: echo "$(GREET), $(WORLD)!"
          task_queue: step_1_queue

    - name: step_2
      description: print a success message
      run:
          cmd: print("Hurrah, we did it!")
          depends: [step_1_*]
          shell: /usr/bin/env python3
          task_queue: step_2_queue
```

Now let's run these studies:

```bash
merlin run hello.yaml ; merlin run hello_samples.yaml
```

And execute them by submitting worker launch scripts (examples of these scripts can be found in the [Distributed Runs](../running_studies.md#distributed-runs) section).

## Getting General Database Information

The `merlin database info` subcommand displays general information about the database including the type of database that's connected, the version of the database, and some basic information about the entries currently in the database.

Let's execute this command:

```bash
merlin database info
```

This provides us with the following output:

```bash
Merlin Database Information
---------------------------
General Information:
- Database Type: redis
- Database Version: 7.0.12
- Connection String: rediss://:******@server.gov:12345/0

Studies:
- Total: 2

Runs:
- Total: 2

Logical Workers:
- Total: 2

Physical Workers:
- Total: 2
```

## Retrieving and Displaying Entries

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

A **study** represents a collection of related data, while a **run** refers to an individual execution of a study. A study is unique by study name where a run is created each time `merlin run` is executed.

A **logical worker** represents the worker that's defined in a [specification file](../specification.md), while a **physical worker** represents the actual instantiation of the logical worker. Each logical worker entry is unique based upon the worker's name and associated task queues. A physical worker entry will not be created until the `merlin run-workers` command is executed and a worker is started.

The following sections demonstrate how to use each option listed above.

### Retrieving All Studies

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

### Retrieving Specific Studies

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

### Retrieving All Runs

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
| **Study ID**       | The unique identifier of the associated study. |
| **Queues**         | A list of queues used by the run.              |
| **Workers**        | A list of workers assigned to the run.         |
| **Parent**         | The parent run ID, if any.                     |
| **Child**          | The child run ID, if any.                      |
| **Run Complete**   | Indicates whether the run is complete.         |
| **Additional Data**| Any extra metadata stored with the run.        |

### Retrieving Specific Runs

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

### Retrieving All Logical Workers

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

### Retrieving Specific Logical Workers

To obtain information about specific logical workers, users can pass the ID of one or more logical workers to the `merlin database get logical-worker` command.

For example, let's query the logical worker with the name "hello_worker":

```bash
merlin database get logical-worker 2f740737-a727-ea7d-6de4-17dc643183bb
```

Which should display just the one run entry:

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

### Retrieving All Physical Workers

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

### Retrieving Specific Physical Workers

To obtain information about specific physical workers, users can pass the IDl or name of one or more physical workers to the `merlin database get physical-worker` command.

For example, let's query the physical worker for the "hello_samples" workflow:

```bash
merlin database get physical-worker celery@hello_samples_worker.%ruby10
```

Which should display just the one run entry:

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

### Retrieving Everything

Using the `everything` option will retrieve and display every entry in our database. In our case this would be 8 entries: 2 studies, 2 runs, 2 logical workers, and 2 physical workers.

If you want to give this a shot use:

```bash
merlin database get everything
```

## Deleting Entries

The `merlin database delete` subcommand allows users to delete entries from the database. This subcommand includes several options for removing data:

- [`all-studies`](#removing-all-studies): Remove all studies in the database.
- [`study`](#removing-specific-studies): Remove specific studies in the database.
- [`all-runs`](#removing-all-runs): Remove all runs in the database.
- [`run`](#removing-specific-runs): Remove specific runs in the database.
- [`all-logical-workers`](#removing-all-logical-workers): Remove all logical workers in the database.
- [`logical-worker`](#removing-specific-logical-workers): Remove specific logical workers in the database.
- [`all-physical-workers`](#removing-all-physical-workers): Remove all physical workers in the database.
- [`physical-worker`](#removing-specific-physical-workers): Remove specific physical workers in the database.
- [`everything`](#removing-everything): Removes every entry in the database, wiping the whole thing clean.

The following sections explain how to use each option.

### Removing All Studies

!!! warning

    Using the `delete all-studies` option without the `-k` argument will remove *all* of the associated runs for *every* study.

Using the `all-studies` option will delete every study in our database, along with their associated runs. For the purposes of this example, we'll refrain from removing all of the runs as well by utilizing the `-k` option.

Assuming we still have our runs for the "hello" and "hello_samples" studies available to us, we can remove both studies *without* removing their associated runs using the following command:

```bash
merlin database delete all-studies -k
```

This will provide output similar to the following:

```bash
[2025-05-12 16:48:59: INFO] Reading app config from file /path/to/.merlin/app.yaml
[2025-05-12 16:48:59: INFO] Fetching all studys from Redis...
[2025-05-12 16:48:59: INFO] Successfully retrieved 2 studys from Redis.
[2025-05-12 16:48:59: INFO] Deleting study with id or name '8d3935ce-5eff-4f80-b55d-66e9dacd88b2' from the database...
[2025-05-12 16:48:59: INFO] Successfully deleted study with id '8d3935ce-5eff-4f80-b55d-66e9dacd88b2'.
[2025-05-12 16:48:59: INFO] Study '8d3935ce-5eff-4f80-b55d-66e9dacd88b2' has been successfully deleted.
[2025-05-12 16:48:59: INFO] Deleting study with id or name '77eddf31-bce5-4422-8782-e96cf372af43' from the database...
[2025-05-12 16:48:59: INFO] Successfully deleted study with id '77eddf31-bce5-4422-8782-e96cf372af43'.
[2025-05-12 16:48:59: INFO] Study '77eddf31-bce5-4422-8782-e96cf372af43' has been successfully deleted.
```

We can verify that this removed the studies from the database using the [`merlin database get all-studies`](#retrieving-all-studies) command:

```bash
merlin database get all-studies
```

This should provide us with output denoting no studies were found:

```bash
[2025-02-20 18:27:28: INFO] Fetching all studies from Redis...
[2025-02-20 18:27:29: INFO] Successfully retrieved 0 studies from Redis.
```

Similarly, we can verify that this *did not* remove all of the runs by using the [`merlin database get all-runs`](#retrieving-all-runs) command:

```bash
merlin database get all-runs
```

This should still display the two runs we had before:

```bash
Run with ID f93eecdf-d573-43d1-a3f9-c728c15802ea
------------------------------------------------
Workspace: /path/hello_20250220-172743
Study:
  - ID: 849515a9-767c-4104-9ae9-6820ff000b65
    Name: hello
Queues: ['[merlin]_merlin']
Workers: ['0bdbae0b-c321-4178-a5a2-ab1ea6067be7']
Parent: None
Child: None
Run Complete: False
Additional Data: {}


Run with ID c55993e3-c210-4872-902d-e99e250e6f00
------------------------------------------------
Workspace: /path/to/hello_samples_20250220-182655
Study:
  - ID: 794836d9-1797-4b41-b962-d3688b93db52
    Name: hello_samples
Queues: ['[merlin]_merlin']
Workers: ['4b0cd8f6-35a3-b484-4603-fa55eb0e7134']
Parent: None
Child: None
Run Complete: False
Additional Data: {}
```

### Removing Specific Studies

!!! warning

    Using the `delete study` option without the `-k` argument will remove *all* of the associated runs for this study.

To remove specific studies from the database, users can pass the ID or name of a study to the `merlin database delete study` command.

Assuming we still have our runs for the "hello" and "hello_samples" studies available to us, we can remove the "hello_samples" study using the following command:

```bash
merlin database delete study hello_samples
```

This will remove the study and all runs associated with this study from the database. The output will look similar to this:

```bash
[2025-05-12 16:50:31: INFO] Reading app config from file /path/to/.merlin/app.yaml
[2025-05-12 16:50:31: INFO] Attempting to delete run with id '42955780-087b-4b82-b0e3-99f82c1448be' from Redis...
[2025-05-12 16:50:31: INFO] Successfully deleted run '42955780-087b-4b82-b0e3-99f82c1448be' from Redis.
[2025-05-12 16:50:31: INFO] Run with id or workspace '42955780-087b-4b82-b0e3-99f82c1448be' has been successfully deleted.
[2025-05-12 16:50:31: INFO] Deleting study with id or name 'hello_samples' from the database...
[2025-05-12 16:50:31: INFO] Successfully deleted study with name 'hello_samples'.
[2025-05-12 16:50:31: INFO] Study 'hello_samples' has been successfully deleted.
```

We can verify that this removed the study from the database using the [`merlin database get all-studies`](#retrieving-all-studies) command:

```bash
merlin database get all-studies
```

Which provides output similar to the following:

```bash
Study with ID 849515a9-767c-4104-9ae9-6820ff000b65
------------------------------------------------
Name: hello
Runs:
  - ID: f93eecdf-d573-43d1-a3f9-c728c15802ea
    Workspace: /path/to/hello_20250220-172743
Additional Data: {}
```

Here we see that the hello_samples study is no longer showing up. What about the run that was associated with the study though? Using the [`merlin database get all-runs`](#retrieving-all-runs) command, we can check:

```bash
merlin database get all-runs
```

Which gives us:

```bash
Run with ID f93eecdf-d573-43d1-a3f9-c728c15802ea
------------------------------------------------
Workspace: /path/to/hello_20250220-172743
Study ID: 849515a9-767c-4104-9ae9-6820ff000b65
Queues: ['[merlin]_merlin']
Workers: ['4b0cd8f6-35a3-b484-4603-fa55eb0e7134']
Parent: None
Child: None
Run Complete: False
Additional Data: {}
```

There is no longer a run associated with the "hello_samples" study in our database.

To prevent this, run the `merlin database delete study` command with the `-k` option. An example of this is shown in the [above section](#removing-all-studies).

### Removing All Runs

Using the `all-runs` option will delete every run in our database.

Assuming we still have our runs for the "hello" and "hello_samples" studies available to us, we can remove both runs using the following command:

```bash
merlin database delete all-runs
```

This will provide output similar to the following:

```bash
[2025-05-12 16:52:16: INFO] Reading app config from file /path/to/.merlin/app.yaml
[2025-05-12 16:52:16: INFO] Fetching all runs from Redis...
[2025-05-12 16:52:16: INFO] Successfully retrieved 2 runs from Redis.
[2025-05-12 16:52:16: INFO] Attempting to delete run with id '5fb17970-8af6-40b4-a600-305170bca580' from Redis...
[2025-05-12 16:52:16: INFO] Successfully deleted run '5fb17970-8af6-40b4-a600-305170bca580' from Redis.
[2025-05-12 16:52:16: INFO] Run with id or workspace '5fb17970-8af6-40b4-a600-305170bca580' has been successfully deleted.
[2025-05-12 16:52:16: INFO] Attempting to delete run with id '60b60d60-cc3c-440e-ac51-cdf9c8cbd2e6' from Redis...
[2025-05-12 16:52:16: INFO] Successfully deleted run '60b60d60-cc3c-440e-ac51-cdf9c8cbd2e6' from Redis.
[2025-05-12 16:52:16: INFO] Run with id or workspace '60b60d60-cc3c-440e-ac51-cdf9c8cbd2e6' has been successfully deleted.
```

We can verify that this removed the runs from the database using the [`merlin database get all-runs`](#retrieving-all-runs) command:

```bash
merlin database get all-runs
```

This should provide us with output denoting no runs were found:

```bash
[2025-02-20 18:33:21: INFO] Fetching all runs from Redis...
[2025-02-20 18:33:21: INFO] Successfully retrieved 0 runs from Redis.
```

Similarly, utilizing the [`merlin database get all-studies`](#retrieving-all-studies) command:

```bash
merlin database get all-studies
```

We should see that both studies do not have any runs:

```bash
Study with ID c2a8082e-b651-42cc-9d21-c3bafeb7d8ed
------------------------------------------------
Name: hello_samples
Runs:
  No runs found.
Additional Data: {}


Study with ID e94efa15-af1f-41d1-b3fd-947a79f18387
------------------------------------------------
Name: hello
Runs:
  No runs found.
Additional Data: {}
```

### Removing Specific Runs

To remove specific runs from the database, users can pass the ID or workspace of one or more runs to the `merlin database delete run` command.

Assuming we still have our runs for the "hello" and "hello_samples" studies available to us, we can remove the run for the "hello_samples" study using the following command:

```bash
merlin database delete run 62fe3a0f-fc27-4e84-a8ea-cbd19aebc53f
```

This will remove the run from the database. The output will look similar to this:

```bash
[2025-05-12 16:56:53: INFO] Reading app config from file /path/to/.merlin/app.yaml
[2025-05-12 16:56:53: INFO] Attempting to delete run with id '62fe3a0f-fc27-4e84-a8ea-cbd19aebc53f' from Redis...
[2025-05-12 16:56:53: INFO] Successfully deleted run '62fe3a0f-fc27-4e84-a8ea-cbd19aebc53f' from Redis.
[2025-05-12 16:56:53: INFO] Run with id or workspace '62fe3a0f-fc27-4e84-a8ea-cbd19aebc53f' has been successfully deleted.
```

We can verify that this removed the run from the database using the [`merlin database get all-runs`](#retrieving-all-runs) command:

```bash
merlin database get all-runs
```

Which provides output showing only one run:

```bash
Run with ID 97b9b899-f509-4460-9ecd-2ead5629bed3
------------------------------------------------
Workspace: /usr/WS1/gunny/debug/database_testing/hello/hello_20250220-183534
Study ID: e94efa15-af1f-41d1-b3fd-947a79f18387
Queues: ['[merlin]_merlin']
Workers: ['default_worker']
Parent: None
Child: None
Run Complete: False
Additional Data: {}
```

Similarly, utilizing the [`merlin database get all-studies`](#retrieving-all-studies) command:

```bash
merlin database get all-studies
```

We should see that only the "hello" study has a run:

```bash
Study with ID c2a8082e-b651-42cc-9d21-c3bafeb7d8ed
------------------------------------------------
Name: hello_samples
Runs:
  No runs found.
Additional Data: {}


Study with ID e94efa15-af1f-41d1-b3fd-947a79f18387
------------------------------------------------
Name: hello
Runs:
  - ID: 97b9b899-f509-4460-9ecd-2ead5629bed3
    Workspace: /path/to/hello_20250220-183534
Additional Data: {}
```

### Removing All Logical Workers

Using the `all-logical-workers` option will delete every logical-worker in our database.

Assuming we still have our logical workers for the "hello" and "hello_samples" studies available to us, we can remove both logical workers using the following command:

```bash
merlin database delete all-logical-workers
```

This will provide output similar to the following:

```bash
[2025-05-12 16:57:38: INFO] Reading app config from file /path/to/.merlin/app.yaml
[2025-05-12 16:57:38: INFO] Fetching all logical_workers from Redis...
[2025-05-12 16:57:38: INFO] Successfully retrieved 2 logical_workers from Redis.
[2025-05-12 16:57:38: INFO] Attempting to delete logical_worker with id '2f740737-a727-ea7d-6de4-17dc643183bb' from Redis...
[2025-05-12 16:57:38: INFO] Successfully deleted logical_worker '2f740737-a727-ea7d-6de4-17dc643183bb' from Redis.
[2025-05-12 16:57:38: INFO] Worker with id '2f740737-a727-ea7d-6de4-17dc643183bb' has been successfully deleted.
[2025-05-12 16:57:38: INFO] Attempting to delete logical_worker with id '4b0cd8f6-35a3-b484-4603-fa55eb0e7134' from Redis...
[2025-05-12 16:57:38: INFO] Successfully deleted logical_worker '4b0cd8f6-35a3-b484-4603-fa55eb0e7134' from Redis.
[2025-05-12 16:57:38: INFO] Worker with id '4b0cd8f6-35a3-b484-4603-fa55eb0e7134' has been successfully deleted.
```

We can verify that this removed the logical workers from the database using the [`merlin database get all-logical-workers`](#retrieving-all-logical-workers) command:

```bash
merlin database get all-logical-workers
```

This should provide us with output denoting no logical workers were found:

```bash
[2025-05-08 17:11:37: INFO] Fetching all logical workers from Redis...
[2025-05-08 17:11:37: INFO] Successfully retrieved 0 logical workers from Redis.
[2025-05-08 17:11:37: INFO] No logical workers found in the database.
```

Similarly, utilizing the [`merlin database get all-runs`](#retrieving-all-runs) command:

```bash
merlin database get all-runs
```

We should see that both runs do not have any workers:

```bash
Run with ID 0bdbae0b-c321-4178-a5a2-ab1ea6067be7
------------------------------------------------
Workspace: /path/to/hello_20250508-161150
Study:
  - ID: 75f49c2d-7135-41a8-a858-efad4ff19961
    Name: hello
Queues: ['[merlin]_merlin']
Workers: []
Parent: None
Child: None
Run Complete: True
Additional Data: {}


Run with ID c735ade0-9b28-4b9e-bb46-d9429d7cf61a
------------------------------------------------
Workspace: /path/to/hello_samples_20250508-161159
Study:
  - ID: 837fafbe-4f40-4e47-8dd7-abb17142caed
    Name: hello_samples
Queues: ['[merlin]_step_1_queue', '[merlin]_step_2_queue']
Workers: []
Parent: None
Child: None
Run Complete: True
Additional Data: {}
```

### Removing Specific Logical Workers

To remove specific logical workers from the database, users can pass the ID of a logical worker to the `merlin database delete logical-worker` command.

Assuming we still have our logical workers for the "hello" and "hello_samples" studies available to us, we can remove the logical worker for the "hello_samples" study using the following command:

```bash
merlin database delete logical-worker 4b0cd8f6-35a3-b484-4603-fa55eb0e7134
```

This will remove the logical worker from the database. The output will look similar to this:

```bash
[2025-05-12 16:59:16: INFO] Reading app config from file /path/to/.merlin/app.yaml
[2025-05-12 16:59:16: INFO] Attempting to delete logical_worker with id '4b0cd8f6-35a3-b484-4603-fa55eb0e7134' from Redis...
[2025-05-12 16:59:16: INFO] Successfully deleted logical_worker '4b0cd8f6-35a3-b484-4603-fa55eb0e7134' from Redis.
[2025-05-12 16:59:16: INFO] Worker with id '4b0cd8f6-35a3-b484-4603-fa55eb0e7134' has been successfully deleted.
```

We can verify that this removed the logical worker from the database using the [`merlin database get all-logical-workers`](#retrieving-all-logical-workers) command:

```bash
merlin database get all-logical-workers
```

Which provides output showing only one logical worker:

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

Similarly, utilizing the [`merlin database get all-runs`](#retrieving-all-runs) command:

```bash
merlin database get all-runs
```

We should see that only the "hello" run has a worker:

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
Run Complete: True
Additional Data: {}


Run with ID c735ade0-9b28-4b9e-bb46-d9429d7cf61a
------------------------------------------------
Workspace: /path/to/hello_samples_20250508-161159
Study:
  - ID: 837fafbe-4f40-4e47-8dd7-abb17142caed
    Name: hello_samples
Queues: ['[merlin]_step_1_queue', '[merlin]_step_2_queue']
Workers: []
Parent: None
Child: None
Run Complete: True
Additional Data: {}
```

### Removing All Physical Workers

Using the `all-physical-workers` option will delete every physical worker in our database.

Assuming we still have our physical workers for the "hello" and "hello_samples" studies available to us, we can remove both physical workers using the following command:

```bash
merlin database delete all-physical-workers
```

This will provide output similar to the following:

```bash
[2025-05-12 17:04:20: INFO] Reading app config from file /path/to/.merlin/app.yaml
[2025-05-12 17:04:20: INFO] Fetching all physical_workers from Redis...
[2025-05-12 17:04:20: INFO] Successfully retrieved 2 physical_workers from Redis.
[2025-05-12 17:04:20: INFO] Successfully deleted physical_worker with id '04536fdd-d096-4b58-bc26-9b44c489b4c6'.
[2025-05-12 17:04:20: INFO] Worker '04536fdd-d096-4b58-bc26-9b44c489b4c6' has been successfully deleted.
[2025-05-12 17:04:20: INFO] Successfully deleted physical_worker with id '6387b7b9-4bbd-4067-ae0a-e6003b8c1186'.
[2025-05-12 17:04:20: INFO] Worker '6387b7b9-4bbd-4067-ae0a-e6003b8c1186' has been successfully deleted.
```

We can verify that this removed the physical workers from the database using the [`merlin database get all-physical-workers`](#retrieving-all-physical-workers) command:

```bash
merlin database get all-physical-workers
```

This should provide us with output denoting no physical workers were found:

```bash
[2025-05-12 17:06:22: INFO] Fetching all physical_workers from Redis...
[2025-05-12 17:06:23: INFO] Successfully retrieved 0 physical_workers from Redis.
[2025-05-12 17:06:23: INFO] No physical workers found in the database.
```

Similarly, utilizing the [`merlin database get all-logical-workers`](#retrieving-all-logical-workers) command:

```bash
merlin database get all-logical-workers
```

We should see that both logical workers do not have any physical workers:

```bash
Logical Worker with ID 2f740737-a727-ea7d-6de4-17dc643183bb
------------------------------------------------
Name: hello_worker
Runs:
  No runs found.
Queues: {'[merlin]_merlin'}
Physical Workers:
  No physical workers found.
Additional Data: {}


Logical Worker with ID 4b0cd8f6-35a3-b484-4603-fa55eb0e7134
------------------------------------------------
Name: hello_samples_worker
Runs:
  No runs found.
Queues: {'[merlin]_step_1_queue', '[merlin]_step_2_queue'}
Physical Workers:
  No physical workers found.
Additional Data: {}
```

### Removing Specific Physical Workers

To remove specific physical workers from the database, users can pass the ID of a physical worker to the `merlin database delete physical-worker` command.

Assuming we still have our physical workers for the "hello" and "hello_samples" studies available to us, we can remove the physical worker for the "hello_samples" study using the following command (yours will likely be slightly different depending on the worker name):

```bash
merlin database delete physical-worker celery@hello_samples_worker.%rzadams1017
```

This will remove the physical worker from the database. The output will look similar to this:

```bash
[2025-05-12 17:10:12: INFO] Reading app config from file /path/to/.merlin/app.yaml
[2025-05-12 17:10:12: INFO] Successfully deleted physical_worker with name 'celery@hello_samples_worker.%rzadams1017'.
[2025-05-12 17:10:12: INFO] Worker 'celery@hello_samples_worker.%rzadams1017' has been successfully deleted.
```

We can verify that this removed the physical worker from the database using the [`merlin database get all-physical-workers`](#retrieving-all-physical-workers) command:

```bash
merlin database get all-physical-workers
```

Which provides output showing only one physical worker:

```bash
Physical Worker with ID cc6934ab-f6c2-41e8-ab61-03cc7d3e3f5e
------------------------------------------------
Name: celery@hello_worker.%rzadams1017
Logical Worker ID: 2f740737-a727-ea7d-6de4-17dc643183bb
Launch Command: None
Args: {}
Process ID: None
Status: WorkerStatus.STOPPED
Last Heartbeat: 2025-05-12 17:09:14.699762
Last Spinup: 2025-05-12 17:09:14.699767
Host: rzadams1017
Restart Count: 0.0
Additional Data: {}
```

Similarly, utilizing the [`merlin database get all-logical-workers`](#retrieving-all-logical-workers) command:

```bash
merlin database get all-logical-workers
```

We should see that only the "hello" logical worker has a physical worker implementation:

```bash
Logical Worker with ID 2f740737-a727-ea7d-6de4-17dc643183bb
------------------------------------------------
Name: hello_worker
Runs:
  No runs found.
Queues: {'[merlin]_merlin'}
Physical Workers:
  - ID: cc6934ab-f6c2-41e8-ab61-03cc7d3e3f5e
    Name: celery@hello_worker.%rzadams1017
Additional Data: {}


Logical Worker with ID 4b0cd8f6-35a3-b484-4603-fa55eb0e7134
------------------------------------------------
Name: hello_samples_worker
Runs:
  No runs found.
Queues: {'[merlin]_step_2_queue', '[merlin]_step_1_queue'}
Physical Workers:
  No physical workers found.
Additional Data: {}
```

### Removing Everything

Using the `everything` option will delete every entry in our database.

Assuming we still have all of our entries for the "hello" and "hello_samples" studies available to us, we can remove everything with the following command:

```bash
merlin database delete everything
```

This will bring up a prompt asking if you really want to flush the database:

```bash
[2025-05-12 17:16:44: INFO] Reading app config from file /path/to/.merlin/app.yaml
Are you sure you want to flush the entire database? (y/n): 
```

If you answer `n` to this prompt, the command will stop executing and nothing will be deleted. However, if you answer `y` to this prompt, you will see output similar to the following:

```bash
[2025-05-12 17:16:44: INFO] Reading app config from file /path/to/.merlin/app.yaml
Are you sure you want to flush the entire database? (y/n): y
[2025-05-12 17:17:48: INFO] Flushing the database...
[2025-05-12 17:17:48: INFO] Database successfully flushed.
```

We can verify that this removed the every entry from the database using the [`merlin database get everything`](#retrieving-everything) command:

```bash
merlin database get everything
```

This should provide us with output denoting nothing was found:

```bash
[2025-05-12 17:21:24: INFO] Reading app config from file /path/to/.merlin/app.yaml
[2025-05-12 17:21:24: INFO] Fetching all logical_workers from Redis...
[2025-05-12 17:21:24: INFO] Successfully retrieved 0 logical_workers from Redis.
[2025-05-12 17:21:24: INFO] Fetching all physical_workers from Redis...
[2025-05-12 17:21:24: INFO] Successfully retrieved 0 physical_workers from Redis.
[2025-05-12 17:21:24: INFO] Fetching all runs from Redis...
[2025-05-12 17:21:24: INFO] Successfully retrieved 0 runs from Redis.
[2025-05-12 17:21:24: INFO] Fetching all studies from Redis...
[2025-05-12 17:21:24: INFO] Successfully retrieved 0 studies from Redis.
[2025-05-12 17:21:24: INFO] Nothing found in the database.
```

Similarly, utilizing the [`merlin database info`](#getting-general-database-information) command:

```bash
merlin database info
```

We should see every entry should have a total of 0:

```bash
Merlin Database Information
---------------------------
General Information:
- Database Type: redis
- Database Version: 7.0.12
- Connection String: rediss://:******@server.gov:12345/0

Studies:
- Total: 0

Runs:
- Total: 0

Logical Workers:
- Total: 0

Physical Workers:
- Total: 0
```
