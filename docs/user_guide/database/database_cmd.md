# The Database Command

The [`merlin database`](../command_line.md#database-merlin-database) command provides a straightforward way to interact with the data stored in Merlin's database. For more information on what type of data is stored in Merlin's database, see [Understanding Merlin's Database Entities](./entities.md).

**Usage:**

```bash
merlin database [OPTIONS] SUBCOMMAND
```

This command can also be invoked with its `db` alias:

```bash
merlin db [OPTIONS] SUBCOMMAND
```

## Common Options

The `merlin database` command supports the following option that applies to all subcommands:

- `-l`, `--local`: Use the local SQLite database (`~/.merlin/merlin.db`) instead of the configured results backend

!!! note

    The only time Merlin will use the local SQLite database is if you execute a run in local mode.

Example:

```bash
merlin database -l info
```

## Available Subcommands

The `merlin database` command supports the following subcommands:

| Subcommand       |  Description   | Documentation |
| ------------     | ------- | ----------- |
| `info`   | Displays general information about the database | [The Info Subcommand](./info.md) |
| `get`   | Retrieves specific entries from the database and prints them to the console | [Retrieving Data](./retrieving_data.md) |
| `delete`   | Removes entries from the database | [Deleting Data](./deleting_data.md) |
| `gc`   | Runs garbage collection to help keep the database up-to-date | [Garbage Collection](./garbage_collection.md) |

## Setting Up Test Data

For the examples throughout this documentation, we'll use the `hello.yaml` and `hello_samples.yaml` files from the [Hello World Example](../../examples/hello.md).

To help distinguish between the workers in these examples, let's make some modifications to the specification files.

### Modifying hello.yaml

Add the following `merlin` block to the `hello.yaml` file:

```yaml
merlin:
    resources:
        workers:
            hello_worker:
                args: -l INFO --concurrency 1 --prefetch-multiplier 1
                steps: [all]
```

### Modifying hello_samples.yaml

Update the `merlin` block of the `hello_samples.yaml` file:

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

Also add task queues to the steps in `hello_samples.yaml`:

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

### Running the Test Studies

Now let's run these studies:

```bash
merlin run hello.yaml ; merlin run hello_samples.yaml
```

And execute them by submitting worker launch scripts (examples of these scripts can be found in the [Distributed Runs](../running_studies.md#distributed-runs) section).
