# The Info Subcommand

The `merlin database info` subcommand displays general information about the database including:

- The type of database that's connected (e.g., redis, sqlite)
- The version of the database
- The connection string (with passwords masked)
- A summary count of each entity type stored in the database
- Previews of entities that are currently stored in the database

## Command Options

| Option                | Type | Default | Description                                                      |
| --------------------- | ---- | ------- | ---------------------------------------------------------------- |
| `-m`, `--max-preview` | int  | 3       | Maximum number of recent entries to preview for each entity type |

## Basic Usage

To view database information:

```bash
merlin database info
```

This provides output similar to the following:

```bash
Merlin Database Information
---------------------------
General Information:
- Database Type: redis
- Database Version: 7.0.12
- Connection String: rediss://:******@server.gov:12345/0

Studies:
- Total: 5
- Recent Studies:
    1. ID: 75f49c2d-7135-41a8-a858-efad4ff19961, Name: hello
    2. ID: 837fafbe-4f40-4e47-8dd7-abb17142caed, Name: hello_samples
    3. ID: 9b7b6a53-52a2-4204-b374-f317017babf5, Name: random_study1

Runs:
- Total: 4
- Recent Runs:
    1. ID: 0bdbae0b-c321-4178-a5a2-ab1ea6067be7, Workspace: /path/to/hello_20250508-161150
    2. ID: c735ade0-9b28-4b9e-bb46-d9429d7cf61a, Workspace: /path/to/hello_samples_20250508-161159
    3. ID: db285285-d37d-461c-9b79-96cfb49a7df2, Workspace: /path/to/random_study1_20251008-113029

Logical Workers:
- Total: 2
- Recent Logical Workers:
    1. ID: 2f740737-a727-ea7d-6de4-17dc643183bb, Name: hello_worker, Queues: ['[merlin]_merlin']
    2. ID: 4b0cd8f6-35a3-b484-4603-fa55eb0e7134, Name: hello_samples_worker, Queues: ['[merlin]_step_1_queue', '[merlin]_step_2_queue']

Physical Workers:
- Total: 4
- Recent Physical Workers:
    1. ID: 8549ed5f-83df-4922-aaac-16f676112322, Name: celery@hello_worker.%ruby9
    2. ID: dbf0a091-6b7f-4ade-b404-6f00a104049a, Name: celery@hello_worker.%ruby11
    3. ID: 9a6b8bec-2ede-4a8c-bb07-0778c5c5f356, Name: celery@hello_samples_worker.%ruby10
```

## Changing the Preview Count

By default, the `info` command shows previews of only 3 entries for each entity type. To see more or less previews of recent entries, use the `--max-preview` option:

```bash
merlin database info --max-preview 5
```

This displays up to 5 recent entries for each entity type:

```bash
Merlin Database Information
---------------------------
General Information:
- Database Type: redis
- Database Version: 7.0.12
- Connection String: rediss://:******@server.gov:12345/0

Studies:
- Total: 5
- Recent Studies:
    1. ID: 75f49c2d-7135-41a8-a858-efad4ff19961, Name: hello
    2. ID: 837fafbe-4f40-4e47-8dd7-abb17142caed, Name: hello_samples
    3. ID: 9b7b6a53-52a2-4204-b374-f317017babf5, Name: random_study1
    4. ID: f863c9b0-9e75-409d-ba11-16a84a18fb38, Name: random_study2
    5. ID: 75fe50e5-6e20-4077-a381-4666af42d24c, Name: random_study3

Runs:
- Total: 4
- Recent Runs:
    1. ID: 0bdbae0b-c321-4178-a5a2-ab1ea6067be7, Workspace: /path/to/hello_20250508-161150
    2. ID: c735ade0-9b28-4b9e-bb46-d9429d7cf61a, Workspace: /path/to/hello_samples_20250508-161159
    3. ID: db285285-d37d-461c-9b79-96cfb49a7df2, Workspace: /path/to/random_study1_20251008-113029
    4. ID: 06745abf-d366-485d-8c66-8ed85f139c72, Workspace: /path/to/random_study2_20251008-113148

Logical Workers:
- Total: 2
- Recent Logical Workers:
    1. ID: 2f740737-a727-ea7d-6de4-17dc643183bb, Name: hello_worker, Queues: ['[merlin]_merlin']
    2. ID: 4b0cd8f6-35a3-b484-4603-fa55eb0e7134, Name: hello_samples_worker, Queues: ['[merlin]_step_1_queue', '[merlin]_step_2_queue']

Physical Workers:
- Total: 4
- Recent Physical Workers:
    1. ID: 8549ed5f-83df-4922-aaac-16f676112322, Name: celery@hello_worker.%ruby9
    2. ID: dbf0a091-6b7f-4ade-b404-6f00a104049a, Name: celery@hello_worker.%ruby11
    3. ID: 9a6b8bec-2ede-4a8c-bb07-0778c5c5f356, Name: celery@hello_samples_worker.%ruby10
    4. ID: bab9a016-9ae9-40ad-8a80-8e46a21e3fb7, Name: celery@hell_samples_worker.%ruby12
```

## Using with Local Database

When working with the local SQLite database, use the `-l` flag:

```bash
merlin database -l info
```

This will show information about your local database file (`~/.merlin/merlin.db`) instead of the configured results backend.

## Common Use Cases

**Quick database health check:**

```bash
merlin database info
```

Use this to verify your database connection is working and see how many entities are stored.

**Detailed inspection before cleanup:**

```bash
merlin database info --max-preview 5
```

Use this before running [garbage collection](./garbage_collection.md) or [deletion operations](./deleting_data.md) to get a sense of what's currently in the database.

**Checking local vs. remote database:**

```bash
# Check remote database
merlin database info

# Check local database
merlin database -l info
```

Compare the two to understand what was ran in a distributed environment vs locally.
