# The Database Command

When a Merlin study is run with the [`merlin run`](../command_line.md#run-merlin-run) command, tasks are sent to queues on the central server. At the same time, information about the study is stored as data in the [results backend database](../configuration/index.md#what-is-a-results-backend).

The [`merlin database`](../command_line.md#database-merlin-database) command provides a straightforward way to interact with the data stored in Merlin's database.

**Usage:**

```bash
merlin database SUBCOMMAND
```

Currently, the `merlin database` command supports three subcommands:

- [`info`](#getting-general-database-information): Displays general information about the database.
- [`get`](#retrieving-and-printing-entries): Retrieves specific entries from the database and prints them to the console.
- [`delete`](#deleting-entries): Removes entries from the database.

For demonstration purposes, we'll start by running the `hello.yaml` and `hello_samples.yaml` files from the [Hello World Example](../../examples/hello.md) one time each.

```bash
merlin run hello.yaml ; merlin run hello_samples.yaml
```

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

Studies:
- Total studies: 2

Runs:
- Total runs: 2
```

## Retrieving and Displaying Entries

The `merlin database get` subcommand allows users to query the database for specific information. This subcommand includes several options for retrieving data:

- [`all-studies`](#retrieving-all-studies): Retrieves information about all studies in the database.
- [`study`](#retrieving-specific-studies): Retrieves information about specific studies in the database.
- [`all-runs`](#retrieving-all-runs): Retrieves information about all runs in the database.
- [`run`](#retrieving-specific-runs): Retrieves information about specific runs in the database.

A **study** represents a collection of related data, while a **run** refers to an individual execution of a study. A study is unique by study name where a run is created each time `merlin run` is executed.

The following sections demonstrate how to use each option.

### Retrieving All Studies

Using the `all-studies` option will retrieve and display each study in our database. When displayed, each study will display every field stored in a study entry.

Let's try this out. Executing the following command:

```bash
merlin database get all-studies
```

Will provide output similar to the following:

```bash
Study with ID 849515a9-767c-4104-9ae9-6820ff000b65
------------------------------------------------
Name: hello
Runs:
  - ID: f93eecdf-d573-43d1-a3f9-c728c15802ea
    Workspace: /usr/WS1/gunny/debug/database_testing/hello/hello_20250220-172743
Additional Data: {}


Study with ID 3ad38a9f-1c77-4548-a60a-2c9553408555
------------------------------------------------
Name: hello_samples
Runs:
  - ID: e593ec70-c270-448e-bcf1-4a9ad9d0c864
    Workspace: /usr/WS1/gunny/debug/database_testing/hello/hello_samples_20250220-172749
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

!!! note

    The `merlin database get study` command can currently only take in a single study name at a time. There are plans to modify this in the future so multiple study names or spec files can be taken as input, but for now it is limited to one study name.

To obtain information about specific studies, users can pass the name of a study to the `merlin database get study` command.

For example, let's query just the "hello_samples" study:

```bash
merlin database get study hello_samples
```

Which should display just the "hello_samples" study entry:

```bash
Study with ID 3ad38a9f-1c77-4548-a60a-2c9553408555
------------------------------------------------
Name: hello_samples
Runs:
  - ID: e593ec70-c270-448e-bcf1-4a9ad9d0c864
    Workspace: /usr/WS1/gunny/debug/database_testing/hello/hello_samples_20250220-172749
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
Run with ID e593ec70-c270-448e-bcf1-4a9ad9d0c864
------------------------------------------------
Workspace: /usr/WS1/gunny/debug/database_testing/hello/hello_samples_20250220-172749
Study ID: 3ad38a9f-1c77-4548-a60a-2c9553408555
Queues: ['[merlin]_merlin']
Workers: ['default_worker']
Parent: None
Child: None
Run Complete: False
Additional Data: {}


Run with ID f93eecdf-d573-43d1-a3f9-c728c15802ea
------------------------------------------------
Workspace: /usr/WS1/gunny/debug/database_testing/hello/hello_20250220-172743
Study ID: 849515a9-767c-4104-9ae9-6820ff000b65
Queues: ['[merlin]_merlin']
Workers: ['default_worker']
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

To obtain information about specific runs, users can pass the ID of a run to the `merlin database get run` command.

For example, let's query the run associated with the "hello" study:

```bash
merlin database get run f93eecdf-d573-43d1-a3f9-c728c15802ea
```

Which should display just the one run entry:

```bash
Run with ID f93eecdf-d573-43d1-a3f9-c728c15802ea
------------------------------------------------
Workspace: /usr/WS1/gunny/debug/database_testing/hello/hello_20250220-172743
Study ID: 849515a9-767c-4104-9ae9-6820ff000b65
Queues: ['[merlin]_merlin']
Workers: ['default_worker']
Parent: None
Child: None
Run Complete: False
Additional Data: {}
```

## Deleting Entries

The `merlin database delete` subcommand allows users to delete entries from the database. This subcommand includes several options for removing data:

- [`all-studies`](#removing-all-studies): Remove all studies in the database.
- [`study`](#removing-specific-studies): Remove specific studies in the database.
- [`all-runs`](#removing-all-runs): Remove all runs in the database.
- [`run`](#removing-specific-runs): Remove information about specific runs in the database.

The following sections explain how to use each option.

### Removing All Studies

!!! warning

    Using the `delete all-studies` option without the `-k` argument will remove *all* of the associated runs for *every* study, essentially wiping the database.

Using the `all-studies` option will delete every study in our database, along with their associated runs. For the purposes of this example, we'll refrain from removing all of the runs as well by utilizing the `-k` option.

Assuming we still have our runs for the "hello" and "hello_samples" studies available to us, we can remove both studies *without* removing their associated runs using the following command:

```bash
merlin database delete all-studies -k
```

This will provide output similar to the following:

```bash
[2025-02-20 18:27:16: INFO] Reading app config from file /g/g20/gunny/.merlin/app.yaml
[2025-02-20 18:27:16: INFO] Fetching all studies from Redis...
[2025-02-20 18:27:16: INFO] Successfully retrieved 2 studies from Redis.
[2025-02-20 18:27:16: INFO] Deleting study 'hello' from the database...
[2025-02-20 18:27:16: INFO] Attempting to delete study 'hello' from Redis...
[2025-02-20 18:27:16: INFO] Deleting study hash with key 'study:849515a9-767c-4104-9ae9-6820ff000b65'...
[2025-02-20 18:27:16: INFO] Removing study name-to-ID mapping for 'hello'...
[2025-02-20 18:27:16: INFO] Successfully deleted study 'hello' and all associated data from Redis.
[2025-02-20 18:27:16: INFO] Study 'hello' has been successfully deleted.
[2025-02-20 18:27:16: INFO] Deleting study 'hello_samples' from the database...
[2025-02-20 18:27:16: INFO] Attempting to delete study 'hello_samples' from Redis...
[2025-02-20 18:27:16: INFO] Deleting study hash with key 'study:794836d9-1797-4b41-b962-d3688b93db52'...
[2025-02-20 18:27:16: INFO] Removing study name-to-ID mapping for 'hello_samples'...
[2025-02-20 18:27:16: INFO] Successfully deleted study 'hello_samples' and all associated data from Redis.
[2025-02-20 18:27:16: INFO] Study 'hello_samples' has been successfully deleted.
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
Workspace: /usr/WS1/gunny/debug/database_testing/hello/hello_20250220-172743
Study ID: 849515a9-767c-4104-9ae9-6820ff000b65
Queues: ['[merlin]_merlin']
Workers: ['default_worker']
Parent: None
Child: None
Run Complete: False
Additional Data: {}


Run with ID c55993e3-c210-4872-902d-e99e250e6f00
------------------------------------------------
Workspace: /usr/WS1/gunny/debug/database_testing/hello/hello_samples_20250220-182655
Study ID: 794836d9-1797-4b41-b962-d3688b93db52
Queues: ['[merlin]_merlin']
Workers: ['default_worker']
Parent: None
Child: None
Run Complete: False
Additional Data: {}
```

### Removing Specific Studies

!!! warning

    Using the `delete study` option without the `-k` argument will remove *all* of the associated runs for this study.

To remove specific studies from the database, users can pass the name of a study to the `merlin database delete study` command.

Assuming we still have our runs for the "hello" and "hello_samples" studies available to us, we can remove the "hello_samples" study using the following command:

```bash
merlin database delete study hello_samples
```

This will remove the study and all runs associated with this study from the database. The output will look similar to this:

```bash
[2025-02-20 18:19:05: INFO] Reading app config from file /g/g20/gunny/.merlin/app.yaml
[2025-02-20 18:19:05: INFO] Deleting study 'hello_samples' from the database...
[2025-02-20 18:19:05: INFO] Attempting to delete study 'hello_samples' from Redis...
[2025-02-20 18:19:05: INFO] Attempting to delete run with id 'e593ec70-c270-448e-bcf1-4a9ad9d0c864' from Redis...
[2025-02-20 18:19:05: INFO] Attempting to update study with id '3ad38a9f-1c77-4548-a60a-2c9553408555'...
[2025-02-20 18:19:05: INFO] Successfully updated study with id '3ad38a9f-1c77-4548-a60a-2c9553408555'.
[2025-02-20 18:19:05: INFO] Study with name 'hello_samples' saved to Redis under id '3ad38a9f-1c77-4548-a60a-2c9553408555'.
[2025-02-20 18:19:05: INFO] Successfully deleted run 'e593ec70-c270-448e-bcf1-4a9ad9d0c864' and all associated data from Redis.
[2025-02-20 18:19:05: INFO] Deleting study hash with key 'study:3ad38a9f-1c77-4548-a60a-2c9553408555'...
[2025-02-20 18:19:05: INFO] Removing study name-to-ID mapping for 'hello_samples'...
[2025-02-20 18:19:05: INFO] Successfully deleted study 'hello_samples' and all associated data from Redis.
[2025-02-20 18:19:05: INFO] Study 'hello_samples' has been successfully deleted.
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
    Workspace: /usr/WS1/gunny/debug/database_testing/hello/hello_20250220-172743
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
Workspace: /usr/WS1/gunny/debug/database_testing/hello/hello_20250220-172743
Study ID: 849515a9-767c-4104-9ae9-6820ff000b65
Queues: ['[merlin]_merlin']
Workers: ['default_worker']
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
[2025-02-20 18:32:09: INFO] Reading app config from file /g/g20/gunny/.merlin/app.yaml
[2025-02-20 18:32:09: INFO] Fetching all runs from Redis...
[2025-02-20 18:32:09: INFO] Successfully retrieved 2 runs from Redis.
[2025-02-20 18:32:09: INFO] Deleting run with id '2e20a7f9-84c1-444f-bc17-ba6bc8816c79' from the database...
[2025-02-20 18:32:09: INFO] Attempting to delete run with id '2e20a7f9-84c1-444f-bc17-ba6bc8816c79' from Redis...
[2025-02-20 18:32:09: INFO] Attempting to update study with id 'e94efa15-af1f-41d1-b3fd-947a79f18387'...
[2025-02-20 18:32:09: INFO] Successfully updated study with id 'e94efa15-af1f-41d1-b3fd-947a79f18387'.
[2025-02-20 18:32:09: INFO] Study with name 'hello' saved to Redis under id 'e94efa15-af1f-41d1-b3fd-947a79f18387'.
[2025-02-20 18:32:09: INFO] Successfully deleted run '2e20a7f9-84c1-444f-bc17-ba6bc8816c79' and all associated data from Redis.
[2025-02-20 18:32:09: INFO] Run with id '2e20a7f9-84c1-444f-bc17-ba6bc8816c79' has been successfully deleted.
[2025-02-20 18:32:09: INFO] Deleting run with id 'bd00b3a8-20a9-4cc1-8854-b9e2e1a4dbf0' from the database...
[2025-02-20 18:32:09: INFO] Attempting to delete run with id 'bd00b3a8-20a9-4cc1-8854-b9e2e1a4dbf0' from Redis...
[2025-02-20 18:32:09: INFO] Attempting to update study with id 'c2a8082e-b651-42cc-9d21-c3bafeb7d8ed'...
[2025-02-20 18:32:09: INFO] Successfully updated study with id 'c2a8082e-b651-42cc-9d21-c3bafeb7d8ed'.
[2025-02-20 18:32:09: INFO] Study with name 'hello_samples' saved to Redis under id 'c2a8082e-b651-42cc-9d21-c3bafeb7d8ed'.
[2025-02-20 18:32:09: INFO] Successfully deleted run 'bd00b3a8-20a9-4cc1-8854-b9e2e1a4dbf0' and all associated data from Redis.
[2025-02-20 18:32:09: INFO] Run with id 'bd00b3a8-20a9-4cc1-8854-b9e2e1a4dbf0' has been successfully deleted.
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
Additional Data: {}


Study with ID e94efa15-af1f-41d1-b3fd-947a79f18387
------------------------------------------------
Name: hello
Runs:
Additional Data: {}
```

### Removing Specific Runs

To remove specific runs from the database, users can pass the ID of a run to the `merlin database delete run` command.

Assuming we still have our runs for the "hello" and "hello_samples" studies available to us, we can remove the run for the "hello_samples" study using the following command:

```bash
merlin database delete run f6580a9d-6ae5-4ad5-a5f9-9360dffce1a1
```

This will remove the run from the database. The output will look similar to this:

```bash
[2025-02-20 18:36:21: INFO] Reading app config from file /g/g20/gunny/.merlin/app.yaml
[2025-02-20 18:36:21: INFO] Deleting run with id 'f6580a9d-6ae5-4ad5-a5f9-9360dffce1a1' from the database...
[2025-02-20 18:36:21: INFO] Attempting to delete run with id 'f6580a9d-6ae5-4ad5-a5f9-9360dffce1a1' from Redis...
[2025-02-20 18:36:21: INFO] Attempting to update study with id 'c2a8082e-b651-42cc-9d21-c3bafeb7d8ed'...
[2025-02-20 18:36:21: INFO] Successfully updated study with id 'c2a8082e-b651-42cc-9d21-c3bafeb7d8ed'.
[2025-02-20 18:36:21: INFO] Study with name 'hello_samples' saved to Redis under id 'c2a8082e-b651-42cc-9d21-c3bafeb7d8ed'.
[2025-02-20 18:36:21: INFO] Successfully deleted run 'f6580a9d-6ae5-4ad5-a5f9-9360dffce1a1' and all associated data from Redis.
[2025-02-20 18:36:21: INFO] Run with id 'f6580a9d-6ae5-4ad5-a5f9-9360dffce1a1' has been successfully deleted.
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
Additional Data: {}


Study with ID e94efa15-af1f-41d1-b3fd-947a79f18387
------------------------------------------------
Name: hello
Runs:
  - ID: 97b9b899-f509-4460-9ecd-2ead5629bed3
    Workspace: /usr/WS1/gunny/debug/database_testing/hello/hello_20250220-183534
Additional Data: {}
```
