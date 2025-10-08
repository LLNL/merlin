# Database Garbage Collection

The `merlin database gc` subcommand helps maintain database integrity by identifying and optionally removing "stale" entries—database records that no longer have corresponding resources in the filesystem. This is particularly useful for cleaning up entries from runs whose workspace directories have been deleted.

This subcommand can be invoked with the following aliases, as well:

- `merlin database garbage-collect`
- `merlin database cleanup`

## Understanding Garbage Collection

Over time, your Merlin database may accumulate entries for runs whose workspace directories no longer exist. This can happen when:

- Workspace directories are manually deleted from the filesystem
- Storage cleanup operations remove old run directories
- Studies are moved or archived without updating the database

The garbage collection process identifies these orphaned entries and can remove them automatically, keeping your database synchronized with the actual state of your filesystem.

### How Garbage Collection Works

The garbage collection process follows a cascading cleanup approach:

1. **Identifies orphaned runs**: Scans all run entries in the database and checks if their workspace directories still exist
2. **Identifies orphaned workers**:

    - Finds logical workers that only reference runs that no longer exist
    - Finds physical workers that only reference logical workers that no longer exist

3. **Identifies empty studies**: Finds studies that have no remaining runs after cleanup

This cascading approach ensures that removing a run with a missing workspace automatically cleans up its dependent entities.

## Basic Usage

To preview what would be removed without actually deleting anything:

```bash
merlin database gc --dry-run
```

This displays a detailed report showing:

- Which runs have missing workspaces
- Which workers would be removed due to having no valid runs
- Which studies would be removed due to having no remaining runs

??? example "Example Output for Dry Run"

    Let's say we ran the `hello_samples.yaml` example, then eventually removed the workspace that it outputs. Running the garbage collection command with the `--dry-run` option would yield the following results:

    ```bash
    $ merlin database gc --dry-run
    [2025-10-08 10:27:31: INFO] Created component 'redis'
    [2025-10-08 10:27:31: INFO] [GARBAGE COLLECTOR] Scanning database for stale entries...
    [2025-10-08 10:27:31: INFO] [GARBAGE COLLECTOR] Checking run workspaces for validity...
    [2025-10-08 10:27:31: INFO] Fetching all runs from Redis...
    [2025-10-08 10:27:31: INFO] Successfully retrieved 2 runs from Redis.
    [2025-10-08 10:27:31: INFO] [GARBAGE COLLECTOR] Found 1 runs with invalid workspaces.
    [2025-10-08 10:27:31: INFO] [GARBAGE COLLECTOR] Checking for orphaned logical workers...
    [2025-10-08 10:27:31: INFO] Fetching all runs from Redis...
    [2025-10-08 10:27:31: INFO] Successfully retrieved 2 runs from Redis.
    [2025-10-08 10:27:31: INFO] Fetching all logical workers from Redis...
    [2025-10-08 10:27:31: INFO] Successfully retrieved 2 logical workers from Redis.
    [2025-10-08 10:27:31: INFO] [GARBAGE COLLECTOR] Found 1 orphaned logical workers.
    [2025-10-08 10:27:31: INFO] [GARBAGE COLLECTOR] Checking for orphaned physical workers...
    [2025-10-08 10:27:31: INFO] Fetching all logical workers from Redis...
    [2025-10-08 10:27:31: INFO] Successfully retrieved 2 logical workers from Redis.
    [2025-10-08 10:27:31: INFO] Fetching all physical workers from Redis...
    [2025-10-08 10:27:31: INFO] Successfully retrieved 2 physical workers from Redis.
    [2025-10-08 10:27:31: INFO] [GARBAGE COLLECTOR] Found 1 orphaned physical workers.
    [2025-10-08 10:27:31: INFO] [GARBAGE COLLECTOR] Checking for empty studies...
    [2025-10-08 10:27:31: INFO] Fetching all runs from Redis...
    [2025-10-08 10:27:31: INFO] Successfully retrieved 2 runs from Redis.
    [2025-10-08 10:27:31: INFO] Fetching all studies from Redis...
    [2025-10-08 10:27:31: INFO] Successfully retrieved 2 studies from Redis.
    [2025-10-08 10:27:31: INFO] [GARBAGE COLLECTOR] Found 1 empty studies.
    [2025-10-08 10:27:31: INFO] [GARBAGE COLLECTOR] Scan complete.
    [2025-10-08 10:27:31: INFO] 
    ============================================================
    Database Garbage Collection Report
    ============================================================

    Invalid Runs: 1
      - /path/to/hello_samples_20251008-102348

    Orphaned Logical Workers: 1
      - hello_samples_worker (queues: [merlin]_step_1_queue, [merlin]_step_2_queue)

    Orphaned Physical Workers: 1
      - celery@hello_samples_worker.%dane13 (host: dane13)

    Empty Studies: 1
      - hello_samples
    ============================================================
    ```

## Performing Cleanup

To actually remove the stale entries, run without the `--dry-run` flag:

```bash
merlin database gc
```

This will prompt for confirmation before proceeding:

```bash
$ merlin database gc
Class MerlinStatusRendererFactory did not override _discover_builtin_modules(). Built-in module discovery will be skipped.
[2025-10-08 10:32:47: INFO] Created component 'redis'
[2025-10-08 10:32:47: INFO] [GARBAGE COLLECTOR] Starting database garbage collection...
[2025-10-08 10:32:47: INFO] [GARBAGE COLLECTOR] Scanning database for stale entries...
[2025-10-08 10:32:47: INFO] [GARBAGE COLLECTOR] Checking run workspaces for validity...
[2025-10-08 10:32:47: INFO] Fetching all runs from Redis...
[2025-10-08 10:32:47: INFO] Successfully retrieved 2 runs from Redis.
[2025-10-08 10:32:47: INFO] [GARBAGE COLLECTOR] Found 1 runs with invalid workspaces.
[2025-10-08 10:32:47: INFO] [GARBAGE COLLECTOR] Checking for orphaned logical workers...
[2025-10-08 10:32:47: INFO] Fetching all runs from Redis...
[2025-10-08 10:32:47: INFO] Successfully retrieved 2 runs from Redis.
[2025-10-08 10:32:47: INFO] Fetching all logical workers from Redis...
[2025-10-08 10:32:47: INFO] Successfully retrieved 2 logical workers from Redis.
[2025-10-08 10:32:47: INFO] [GARBAGE COLLECTOR] Found 1 orphaned logical workers.
[2025-10-08 10:32:47: INFO] [GARBAGE COLLECTOR] Checking for orphaned physical workers...
[2025-10-08 10:32:47: INFO] Fetching all logical workers from Redis...
[2025-10-08 10:32:47: INFO] Successfully retrieved 2 logical workers from Redis.
[2025-10-08 10:32:47: INFO] Fetching all physical workers from Redis...
[2025-10-08 10:32:47: INFO] Successfully retrieved 2 physical workers from Redis.
[2025-10-08 10:32:47: INFO] [GARBAGE COLLECTOR] Found 1 orphaned physical workers.
[2025-10-08 10:32:47: INFO] [GARBAGE COLLECTOR] Checking for empty studies...
[2025-10-08 10:32:47: INFO] Fetching all runs from Redis...
[2025-10-08 10:32:47: INFO] Successfully retrieved 2 runs from Redis.
[2025-10-08 10:32:47: INFO] Fetching all studies from Redis...
[2025-10-08 10:32:47: INFO] Successfully retrieved 2 studies from Redis.
[2025-10-08 10:32:47: INFO] [GARBAGE COLLECTOR] Found 1 empty studies.
[2025-10-08 10:32:47: INFO] [GARBAGE COLLECTOR] Scan complete.
[2025-10-08 10:32:47: INFO] 
============================================================
Database Garbage Collection Report
============================================================

Invalid Runs: 1
  - /usr/WS1/gunny/debug/disappearing_tasks/hello/hello_samples_20251008-102348

Orphaned Logical Workers: 1
  - default_worker (queues: [merlin]_merlin)

Orphaned Physical Workers: 1
  - celery@default_worker.%dane13 (host: dane13)

Empty Studies: 1
  - hello_samples
============================================================
[2025-10-08 10:32:47: WARNING] [GARBAGE COLLECTOR] WARNING: This will permanently delete stale database entries. Run with --dry-run first to see what would be deleted.

Continue? (yes/no):  
```

Answer "yes" or "y" to proceed with the cleanup. Enter "no" or"n" to cancel.

## Forcing Cleanup Without Confirmation

!!! warning

    Use `--force` with caution, especially in production environments. Always test with `--dry-run` first to ensure you're removing the correct entries.

For automated scripts or CI/CD pipelines, you can bypass the confirmation prompt with the `--force` or `-f` flag:

```bash
merlin database gc --force
```

### Targeted Cleanup Options

You can limit garbage collection to specific entity types:

**Skip cleaning runs with missing workspaces:**

```bash
merlin database gc --skip-runs
```

**Skip cleaning orphaned workers (both logical and physical):**

```bash
merlin database gc --skip-workers
```

**Skip cleaning empty studies:**

```bash
merlin database gc --skip-studies
```

**These options can be combined with --dry-run to preview the targeted cleanup:**

```bash
merlin database gc --skip-runs --dry-run
```

**These options can also be combined with each other:**

```bash
merlin database gc --skip-workers --skip-studies
```

## Limitations

The garbage collection process:

- Only checks for missing workspace directories (it does not validate workspace contents)
- Does not clean up data in the workspace directories themselves (only database entries)
- Requires read access to the filesystem paths referenced in run entries
