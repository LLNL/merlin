# Command Line Interface

The Merlin library defines a number of commands to help configure your server and manage and monitor your workflow.

This module will detail every command available with Merlin.

## Merlin

The entrypoint to everything related to executing Merlin commands.

**Usage:**

```bash
merlin [OPTIONS] COMMAND [ARGS] ...
```

**Options:**

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `-h`, `--help`   | boolean | Show this help message and exit | `False` |
| `--version`  | boolean | Show program's version number and exit  | `False` |
| `-lvl`, `--level` | choice(`ERROR` \| `WARNING` \| `INFO` \| `DEBUG`) | Level of logging messages to be output. The smaller the number in the table below, the more output that's produced: <table>  <thead>  <th></th>  <th>Log Level Choice</th>  </thead>  <tbody>  <tr>  <td>4</td>  <td>ERROR</td>  </tr>  <tr>  <td>3</td>  <td>WARNING</td>  </tr>  <tr>  <td>2</td>  <td>INFO (default)</td>  </tr>  <tr>  <td>1</td>  <td>DEBUG</td>  </tr>  </tbody>  </table> | INFO |

See the [Configuration Commands](#configuration-commands), [Workflow Management Commands](#workflow-management-commands), and [Monitoring Commands](#monitoring-commands) below for more information on every command available with the Merlin library.

## Configuration Commands

Since running Merlin in a distributed manner requires the [configuration](./configuration.md) of a centralized server, Merlin comes equipped with three commands to help users get this set up:

- *[config](#config-merlin-config)*: Create the skeleton `app.yaml` file needed for configuration
- *[info](#info-merlin-info)*: Ensure stable connections to the server(s)
- *[server](#server-merlin-server)*: Spin up containerized servers

### Config (`merlin config`)

Create a default [config (app.yaml) file](./configuration.md#the-appyaml-file) in the `${HOME}/.merlin` directory using the `config` command. This file can then be edited for your system configuration.

See more information on how to set this file up at the [Configuration](./configuration.md) page.

**Usage:**

```bash
merlin config [OPTIONS]
```

**Options:**

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `-h`, `--help`   | boolean | Show this help message and exit | `False` |
| `--task_server`  | string | Select the appropriate configuration for the given task server. Currently only "celery" is implemented. | "celery" |
| `-o`, `--output_dir` | path | Output the configuration in the given directory. This file can then be edited and copied into `${HOME}/.merlin`. | None |
| `--broker` | string | Write the initial `app.yaml` config file for either a `rabbitmq` or `redis` broker. The default is `rabbitmq`. The backend will be `redis` in both cases. The redis backend in the `rabbitmq` config shows the use on encryption for the backend. | "rabbitmq" |

**Examples:**

!!! example "Create an `app.yaml` File at `~/.merlin`"

    ```bash
    merlin config
    ```

!!! example "Create an `app.yaml` File at a Custom Path"

    ```bash
    merlin config -o /Documents/configuration/
    ```

!!! example "Create an `app.yaml` File With a Redis Broker"

    ```bash
    merlin config --broker redis
    ```

### Info (`merlin info`)

Information about your Merlin and Python configuration can be printed out by using the `info` command. This is helpful for debugging. Included in this command is a server check which will check for server connections. The connection check will timeout after 60 seconds.

**Usage:**

```bash
merlin info [OPTIONS]
```

**Options:**

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `-h`, `--help`   | boolean | Show this help message and exit | `False` |

### Server (`merlin server`)

Create a local containerized server for Merlin to connect to. Merlin server creates and configures a server on the current directory. This allows multiple instances of Merlin server to exist for different studies or uses.

Merlin server has a list of commands for interacting with the broker and results server. These commands allow the user to manage and monitor the exisiting server and create instances of servers if needed.

More information on configuring with Merlin server can be found at the [Merlin Server Configuration](./configuration/merlin_server.md) page.

**Usage:**

```
merlin server [OPTIONS] COMMAND [ARGS] ...
```

**Options:**

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `-h`, `--help`   | boolean | Show this help message and exit | `False` |

**Commands:**

| Name             | Description |
| ------------     | ----------- |
| [init](#server-init-merlin-server-init) | Initialize the files needed for Merlin server |
| [status](#server-status-merlin-server-status) | Check the status of your Merlin server |
| [start](#server-start-merlin-server-start) | Start the containerized Merlin server |
| [stop](#server-stop-merlin-server-stop) | Stop the Merlin server |
| [restart](#server-restart-merlin-server-restart) | Restart an instance of the Merlin server |
| [config](#server-config-merlin-server-config) | Configure the Merlin server |

#### Server Init (`merlin server init`)

!!! note

    If there is an exisiting subdirectory containing a merlin server configuration then only missing files will be replaced. However it is recommended that users backup their local configurations prior to running this command.

The `init` subcommand initalizes a new instance of Merlin server by creating configurations for other subcommands.

A main Merlin sever configuration subdirectory is created at `~/.merlin/server/` which contains configuration for local Merlin configuration, and configurations for different containerized services that Merlin server supports, which includes Singularity (Docker and Podman implemented in the future). 

A local Merlin server configuration subdirectory called `merlin_server/` will also be created in your current working directory when this command is run. This will include a container for merlin server and associated configuration files that might be used to start the server. For example, for a redis server a "redis.conf" will contain settings which will be dynamically loaded when the redis server is run. This local configuration will also contain information about currently running containers as well.

**Usage:**

```bash
merlin server init [OPTIONS]
```

**Options:**

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `-h`, `--help`   | boolean | Show this help message and exit | `False` |

#### Server Status (`merlin server status`)

The `status` subcommand checks the status of the Merlin server.

**Usage:**

```bash
merlin server status [OPTIONS]
```

**Options:**

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `-h`, `--help`   | boolean | Show this help message and exit | `False` |

#### Server Start (`merlin server start`)

!!! warning

    Newer versions of Redis have started requiring a global variable `LC_ALL` to be set in order for this to work. To set this properly, run:

    ```bash
    export LC_ALL="C"
    ```

    If this is not set, the `merlin server start` command may seem to hang until you manually terminate it.

The `start` subcommand starts the Merlin server using the container located in the local merlin server configuration.

**Usage:**

```bash
merlin server start [OPTIONS]
```

**Options:**

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `-h`, `--help`   | boolean | Show this help message and exit | `False` |

#### Server Stop (`merlin server stop`)

The `stop` subcommand stops any exisiting container being managed and monitored by Merlin server.

**Usage:**

```bash
merlin server stop [OPTIONS]
```

**Options:**

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `-h`, `--help`   | boolean | Show this help message and exit | `False` |

#### Server Restart (`merlin server restart`)

The `restart` subcommand performs a `stop` command followed by a `start` command on the Merlin server.

**Usage:**

```bash
merlin server restart [OPTIONS]
```

**Options:**

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `-h`, `--help`   | boolean | Show this help message and exit | `False` |

#### Server Config (`merlin server config`)

The `config` subcommand edits configurations for the Merlin server. There are multiple options to allow for different configurations.

**Usage:**

```bash
merlin server config [OPTIONS]
```

**Options:**

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `-h`, `--help`   | boolean | Show this help message and exit | `False` |
| `-ip`, `--ipadress` | string | Set the binded IP address for Merlin server | None |
| `-p`, `--port` | integer | Set the binded port for Merlin server | None |
| `-pwd`, `--password` | filename | Set the password file for Merlin server | None |
| `--add-user` | string string | Add a new user for Merlin server. This requires a space-delimited username and password as input. | None |
| `--remove-user` | string | Remove an existing user from Merlin server | None |
| `-d`, `--directory` | path | Set the working directory for Merlin server | None |
| `-ss`, `--snapshot-seconds` | integer | Set the number of seconds before each snapshot | None |
| `-sc`, `--snapshot-changes` | integer | Set the number of database changes before each snapshot | None |
| `-sf`, `--snapshot-file` | filename | Set the name of the snapshot file | None |
| `-am`, `--append-mode` | choice(`always` \| `everysec` \| `no`) | Set the appendonly mode | None |
| `-af`, `--append-file` | filename | Set the name of the file for the server append/change file | None |

**Examples:**

!!! example "Configure The Port and Password"

    ```bash
    merlin server config -p 5879 -pwd /Documents/redis.pass
    ```

!!! example "Add A User and Set Snapshot File"

    ```bash
    merlin server config --add-user custom_username custom_password -sf /Documents/snapshot
    ```

## Workflow Management Commands

The Merlin library provides several commands for setting up and managing your Merlin workflow:

- *[example](#example-merlin-example)*: Download pre-made workflow specifications that can be modified for your own workflow needs
- *[purge](#purge-merlin-purge)*: Clear any tasks that are currently living in the central server
- *[restart](#restart-merlin-restart)*: Restart a workflow
- *[run](#run-merlin-run)*: Send tasks to the central server
- *[run workers](#run-workers-merlin-run-workers)*: Start up workers that will execute the tasks that exist on the central server
- *[stop workers](#stop-workers-merlin-stop-workers)*: Stop existing workers

### Example (`merlin example`)

If you want to obtain an example workflow, use Merlin's `merlin example` command. First, view all of the example workflows that are available with:

```bash
merlin example list
```

This will list the available example workflows and a description for each one. To select one:

```bash
merlin example <example_name>
```

This will copy the example workflow to the current working directory. It is possible to specify another path to copy to.

```bash
merlin example <example_name> -p path/to/dir
```

If the specified directory does not exist Merlin will automatically create it.

This will generate the example workflow at the specified location, ready to be run.

For more information on these examples, visit the [Examples](../examples/index.md) page.

**Usage:**

```bash
merlin example [OPTIONS] [list | <example_name>]
```

**Options:**

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `-h`, `--help`   | boolean | Show this help message and exit | `False` |
| `-p`, `--path` | path | A directory path to download the example to | Current Working Directory |

### Purge (`merlin purge`)

!!! warning

    Any tasks reserved by workers will not be purged from the queues. All workers must be first stopped so the tasks can be returned to the task server and then they can be purged.
    
    In short, you probably want to use [`merlin stop-workers`](#stop-workers-merlin-stop-workers) before running `merlin purge`.

If you've executed the [`merlin run`](#run-merlin-run) command and sent tasks to the server, this command can be used to remove those tasks from the server. If there are no tasks currently on the server then this command will not do anything.

**Usage:**

```
merlin purge [OPTIONS] SPECIFICATION
```

**Options:**

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `-h`, `--help`   | boolean | Show this help message and exit | `False` |
| `-f` | boolean | Purge tasks without confirmation | `False` |
| `--steps` | List[string] | A space-delimited list of steps from the specification file to purge | `['all']` |
| `--vars` | List[string] | A space-delimited list of variables to override in the spec file. Ex: `--vars MY_QUEUE=hello` | None |

**Examples:**

!!! example "Purge All Queues From Spec File"

    The following command will purge all queues that exist in `my_specification.yaml`:

    ```bash
    merlin purge my_specification.yaml
    ```

!!! example "Purge Specific Steps From Spec File"

    The following command will purge any queues associated with `step_1` and `step_3` in `my_specification.yaml`:

    ```bash
    merlin purge my_specification.yaml --steps step_1 step_3
    ```

!!! example "Purge Queues Without Confirmation"

    The following command will ignore the confirmation prompt that's provided and purge the queues:

    ```bash
    merlin purge -f my_specification.yaml
    ```

### Restart (`merlin restart`)

To restart a previously started Merlin workflow, use the  `restart` command and the path to root of the Merlin workspace that was generated during the previously run workflow. This will define the tasks and queue them on the task server also called the broker.

Merlin currently writes file called `MERLIN_FINISHED` to the directory of each step that was finished successfully. It uses this to determine which steps to skip during execution of a restarted workflow.

**Usage:**

```bash
merlin restart [OPTIONS] WORKSPACE
```

**Options:**

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `-h`, `--help`   | boolean | Show this help message and exit | `False` |
| `--local` | string | Run tasks sequentially in your current shell | "distributed" |

**Examples:**

!!! example "Restart an Existing Workflow"

    ```bash
    merlin restart my_study_20240102-143903/
    ```

!!! example "Restart an Existing Workflow Locally"

    ```bash
    merlin restart my_study_20240102-143903/ --local
    ```

### Run (`merlin run`)

To run a Merlin workflow use the `run` command and the path to the input yaml file `<input.yaml>`. This will define the tasks and queue them on the task server also called the broker.

**Usage:**

```bash
merlin run [OPTIONS] SPECIFICATION
```

**Options:**

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `-h`, `--help`   | boolean | Show this help message and exit | `False` |
| `--local` | string | Run tasks sequentially in your current shell | "distributed" |
| `--vars` | List[string] | A space-delimited list of variables to override in the spec file. This list should be given after the spec file is provided. Ex: `--vars LEARN=/path/to/new_learn.py EPOCHS=3` | None |
| `--samplesfile` | choice(`<filename>.npy` \| `<filename>.csv` \| `<filename>.tab`) | Specify a file containing samples. This file should be given after the spec file is provided. | None |
| `--dry` | boolean | Do a [Dry Run](#dry-run) of your workflow | `False` |
| `--no-errors` | boolean | Silence the errors thrown when flux is not present | `False` |
| `--pgen` | filename | Specify a parameter generator filename to override the `global.parameters` block of your spec file | None |
| `--pargs` | string | A string that represents a single argument to pass a custom parameter generation function. Reuse `--parg` to pass multiple arguments. [Use with `--pgen`] | None |

**Examples:**

!!! example "Basic Run Example"

    ```bash
    merlin run my_specification.yaml
    ```

!!! example "Pass A Parameter Generator File to Run"

    ```bash
    merlin run my_specification.yaml --pgen /path/to/pgen.py
    ```

!!! example "Pass A Samples File to Run"

    ```bash
    merlin run my_specification.yaml --samplesfile /path/to/samplesfile.csv
    ```

!!! example "Do A Dry Run of Your Workflow Locally"

    ```bash
    merlin run my_specification.yaml --dry --local
    ```

#### Dry Run

'Dry run' means telling workers to create a study's workspace and all of its necessary subdirectories and scripts (with variables expanded) without actually executing the scripts.

To dry-run a workflow, use `--dry`:

=== "Locally"

    ```bash
    merlin run --local --dry <input.yaml>
    ```

=== "Distributed"

    ```bash
    merlin run --dry <input.yaml> ; merlin run-workers <input.yaml>
    ```

You can also specify dry runs from the workflow specification file:

```yaml
batch:
    dry_run: True
```

If you wish to execute a workflow after dry-running it, simply use [`merlin restart`](#restart-merlin-restart).


### Run Workers (`merlin run-workers`)

The tasks queued on the broker by the [`merlin run`](#run-merlin-run) command are run by a collection of workers. These workers can be run local in the current shell or in parallel on a batch allocation. The workers are launched using the `run-workers` command which reads the configuration for the worker launch from the `<input.yaml>` file.

Within the `<input.yaml>` file, the `batch` and `merlin.resources.workers` sections are both used to configure the worker launch. The top level `batch` section can be overridden in the `merlin.resources.workers` section. Parallel workers should be scheduled using the system's batch scheduler (see [below](#launching-workers-in-parallel) for more info).

Once the workers are running, tasks from the broker will be processed.

**Usage:**

```bash
merlin run-workers [OPTIONS] SPECIFICATION
```

**Options:**

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `-h`, `--help`   | boolean | Show this help message and exit | `False` |
| `--echo` | boolean | Echo the Celery workers run command to stdout and don't start any workers | `False` |
| `--worker-args` | string | Pass arguments (all wrapped in quotes) to the Celery workers. Should be given after the input spec. | None |
| `--steps` | List[string] | The specific steps in the input spec that you want to run the corresponding workers for. Should be given after the input spec. | `['all']` |
| `--vars` | List[string] | A space-delimited list of variables to override in the spec file. This list should be given after the spec file is provided. Ex: `--vars SIMWORKER=new_sim_worker` | None |
| `--disable-logs` | boolean | Disable logs for Celery workers. **Note:** Having the `-l` flag in your workers' args section will overwrite this flag for that worker. | `False` |

**Examples:**

!!! example "Basic Worker Launch"

    ```bash
    merlin run-workers my_specification.yaml
    ```

!!! example "Worker Launch for Just Certain Steps"

    ```bash
    merlin run-workers my_specification.yaml --steps step_1 step_3
    ```

!!! example "Worker Launch with Worker Args Passed"

    ```bash
    merlin run-workers my_specification.yaml --worker-args "-l INFO --concurrency 4"
    ```

#### Launching Workers in Parallel

An example of launching a simple Celery worker using srun:

```bash
srun -n 1 celery -A merlin worker -l INFO
```

A parallel batch allocation launch is configured to run a single worker process per node. This worker process will then launch a number of worker threads to process the tasks. The number of worker threads that are launched depends on the `--concurrency` value provided to the workers. By default this will be the number of CPUs on the node. The number of threads can be configured by the users (see the [Configuring Celery Wrokers](./celery.md#configuring-celery-workers) section for more details).

A full SLURM batch submission script to run the workflow on 4 nodes is shown below.

```bash
#!/bin/bash
#SBATCH -N 4
#SBATCH -J Merlin
#SBATCH -t 30:00
#SBATCH -p pdebug
#SBATCH --mail-type=ALL
#SBATCH -o merlin_workers_%j.out

# Assumes you are run this in the same dir as the yaml file.
YAML_FILE=input.yaml

# Source the merlin virtualenv
source <path to merlin venv>/bin/activate

# Remove all tasks from the queues for this run.
#merlin purge -f ${YAML_FILE}

# Submit the tasks to the task server
merlin run ${YAML_FILE}

# Print out the workers command
merlin run-workers ${YAML_FILE} --echo

# Run the workers on the allocation
merlin run-workers ${YAML_FILE}

# Delay until the workers cease running
merlin monitor
```  

### Stop Workers (`merlin stop-workers`)

!!! warning

    If you've named workers identically across workflows (you shouldn't) only one might get the signal. In this case, you can send it again.

Send out a stop signal to some or all connected workers. By default, a stop will be sent to all connected workers across all workflows, having them shutdown softly. This behavior can be modified with certain options.

**Usage:**

```bash
merlin stop-workers [OPTIONS]
```

**Options:**

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `-h`, `--help`   | boolean | Show this help message and exit | `False` |
| `--spec` | filename | Target only the workers named in the `merlin` block of the spec file given here | None |
| `--queues` | List[string] | Takes a space-delimited list of specific queues as input and will stop all workers watching these queues | None |
| `--workers` | List[regex] | A space-delimited list of regular expressions representing workers to stop | None |
| `--task_server`  | string | Task server type for which to stop the workers. Currently only "celery" is implemented. | "celery" |

**Examples:**

!!! example "Stop All Workers Across All Workflows"

    ```bash
    merlin stop-workers
    ```

!!! example "Stop Workers for a Certain Specification"

    ```bash
    merlin stop-workers --spec my_specification.yaml
    ```

!!! example "Stop Workers for Certain Queues"

    ```bash
    merlin stop-workers --queues queue_1 queue_2
    ```

!!! example "Stop Specific Workers Using Regex"

    ```bash
    merlin stop-workers --workers ".*@my_other_host*"
    ```

## Monitoring Commands

The Merlin library comes equipped with commands to help monitor your workflow:

- *[monitor](#monitor-merlin-monitor)*: Keep your allocation alive while tasks are being processed
- *[query-workers](#query-workers-merlin-query-workers)*: Communicate with Celery to view information on active workers
- *[status](#status-merlin-status)*: Communicate with Celery to view the status of queues in your workflow(s)

### Monitor (`merlin monitor`)

Batch submission scripts may not keep the batch allocation alive if there is not a blocking process in the submission script. The `merlin monitor` command addresses this by providing a blocking process that checks for tasks in the queues every (sleep) seconds ("sleep" here can be defined with the `--sleep` option). When the queues are empty, the monitor will query Celery to see if any workers are still processing tasks from the queues. If no workers are processing any tasks from the queues and the queues are empty, the blocking process will exit and allow the allocation to end.

The `monitor` functionality will check for Celery workers for up to 10*(sleep) seconds before monitoring begins. The loop happens when the queue(s) in the spec contain tasks, but no running workers are detected. This is to protect against a failed worker launch.

**Usage:**

```bash
merlin monitor [OPTIONS] SPECIFICATION
```

**Options:**

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `-h`, `--help`   | boolean | Show this help message and exit | `False` |
| `--steps` | List[string] | A space-delimited list of steps in the input spec that you want to query. Should be given after the input spec. | `['all']` |
| `--vars` | List[string] | A space-delimited list of variables to override in the spec file. This list should be given after the spec file is provided. Ex: `--vars SIMWORKER=new_sim_worker` | None |
| `--sleep` | integer | The duration in seconds between checks for workers/tasks | 60 |
| `--task_server`  | string | Task server type for which to monitor the workers. Currently only "celery" is implemented. | "celery" |

!!! example "Basic Monitor"

    ```bash
    merlin monitor my_specification.yaml
    ```

!!! example "Monitor Specific Steps"

    ```bash
    merlin monitor my_specification.yaml --steps step_1 step_3
    ```

!!! example "Monitor With a Shortened Sleep Interval"

    ```bash
    merlin monitor my_specification.yaml --sleep 30
    ```

### Query Workers (`merlin query-workers`)

Check which workers are currently connected to the task server.

This will broadcast a command to all connected workers and print the names of any that respond and the queues they're attached to. This is useful for interacting with workers, such as via `merlin stop-workers --workers`.

**Usage:**

```bash
merlin query-workers [OPTIONS]
```

**Options:**

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `-h`, `--help`   | boolean | Show this help message and exit | `False` |
| `--task_server`  | string | Task server type for which to query workers. Currently only "celery" is implemented. | "celery" |
| `--spec` | filename | Query for the workers named in the `merlin` block of the spec file given here | None |
| `--queues` | List[string] | Takes a space-delimited list of queues as input. This will query for workers associated with the names of the queues you provide here. | None |
| `--workers` | List[regex] | A space-delimited list of regular expressions representing workers to query | None |

**Examples:**

!!! example "Query All Active Workers"

    ```bash
    merlin query-workers
    ```

!!! example "Query Workers of Specific Queues"

    ```bash
    merlin query-workers --queues demo merlin
    ```

!!! example "Query Workers From Spec File"

    ```
    merlin query-workers --spec my_specification.yaml
    ```

!!! example "Query Workers Based on Their Name"

    This will query a worker named `step_1_worker`:

    ```
    merlin query-workers --workers step_1_worker
    ```

!!! example "Query Workers Using Regex"

    This will query only workers whose names start with `step`:

    ```bash
    merlin query-workers --workers ^step
    ```

### Status (`merlin status`)

Check the status of the queues in your spec file to see if there are any tasks in them and any active workers watching them.

**Usage:**

```bash
merlin status [OPTIONS] SPECIFICATION
```

**Options:**

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `-h`, `--help`   | boolean | Show this help message and exit | `False` |
| `--steps` | List[string] | A space-delimited list of steps in the input spec that you want to query. Should be given after the input spec. | `['all']` |
| `--vars` | List[string] | A space-delimited list of variables to override in the spec file. This list should be given after the spec file is provided. Ex: `--vars QUEUE_NAME=new_queue_name` | None |
| `--task_server`  | string | Task server type. Currently only "celery" is implemented. | "celery" |
| `--csv` | filename | The name of a csv file to dump the queue status report to | None |

**Examples:**

!!! example "Basic Status Check"

    ```bash
    merlin status my_specification.yaml
    ```

!!! example "Check the Status of Queues for Certain Steps"

    ```bash
    merlin status my_specification.yaml --steps step_1 step_3
    ```

!!! example "Dump the Status to a CSV File"

    ```bash
    merlin status my_specification.yaml --csv status_report.csv
    ```
