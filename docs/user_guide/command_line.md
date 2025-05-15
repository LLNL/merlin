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

Since running Merlin in a distributed manner requires the [configuration](./configuration/index.md) of a centralized server, Merlin comes equipped with three commands to help users get this set up:

- *[config](#config-merlin-config)*: Create, update, or select a configuration file
- *[info](#info-merlin-info)*: Ensure stable connections to the server(s)
- *[server](#server-merlin-server)*: Spin up containerized servers

### Config (`merlin config`)

Create, update, or select a [configuration file](./configuration/index.md#the-configuration-file) that Merlin will use to connect to your server(s).

Merlin config has a list of commands for interacting with configuration files. These commands allow the user to create and update configuration files, and select which one should be the active configuration.

See more information on how to set up the configuration file at the [Configuration](./configuration/index.md) page.

**Usage:**

```bash
merlin config [OPTIONS] COMMAND [ARGS] ...
```

**Options:**

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `-h`, `--help`   | boolean | Show this help message and exit | `False` |

**Commands:**

| Name             | Description |
| ------------     | ----------- |
| [create](#config-create-merlin-config-create) | Create a template configuration file |
| [update-backend](#config-update-backend-merlin-config-update-backend) | Update broker settings in the configuration file |
| [update-broker](#config-update-broker-merlin-config-update-broker) | Update backend settings in the configuration file |
| [use](#config-use-merlin-config-use) | Use a different configuration setup |

#### Config Create (`merlin config create`)

The `merlin config create` command creates a template [configuration file](./configuration/index.md#the-configuration-file) that you can customize to connect to your central server. Detailed instructions for completing this template are available in the [Configuring the Broker and Results Backend](./configuration/index.md#configuring-the-broker-and-results-backend) guide.

By default, the generated configuration file is saved at `$(HOME)/.merlin/app.yaml`. If you prefer to rename the file or save it to a different location, you can use the `-o` option to specify the desired path. Note that the file must have the `.yaml` extension.

The default configuration sets the broker to use a RabbitMQ server and the results backend to Redis. While the Redis results backend is mandatory, the broker can be configured to use either RabbitMQ or Redis. To switch to a Redis broker, use the `--broker` option.

**Options:**

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `-h`, `--help`   | boolean | Show this help message and exit | `False` |
| `--task_server`  | string | Select the appropriate configuration for the given task server. Currently only "celery" is implemented. | "celery" |
| `-o`, `--output-file` | path | Optional yaml file name for your configuration. Default: $(HOME)/.merlin/app.yaml. | None |
| `--broker` | string | Write the initial `app.yaml` config file for either a `rabbitmq` or `redis` broker. The default is `rabbitmq`. The backend will be `redis` in both cases. The redis backend in the `rabbitmq` config shows the use on encryption for the backend. | "rabbitmq" |

**Examples:**

!!! example "Create an `app.yaml` File at `~/.merlin`"

    ```bash
    merlin config create
    ```

!!! example "Create a Configuration File at a Custom Path"

    ```bash
    merlin config create -o /Documents/configuration/merlin_config.yaml
    ```

!!! example "Create a Configuration File With a Redis Broker"

    ```bash
    merlin config create --broker redis
    ```

#### Config Update-Backend (`merlin config update-backend`)

The `merlin config update-backend` command allows you to modify the [`results_backend`](./configuration/index.md#what-is-a-results-backend) section of your configuration file directly from the command line. See the options table below to see exactly what settings can be set.

**Usage:**

```bash
merlin config update-backend -t {redis} [OPTIONS] ...
```

**Options:**

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `-h`, `--help`   | boolean | Show this help message and exit | `False` |
| `-t`, `--type`   | choice(`redis`) | Type of results backend to configure. | None |
| `--cf`, `--config-file` | string | Path to the config file that will be updated. | `$(HOME)/.merlin/app.yaml` |
| `-u`, `--username` | string | The backend username. | None |
| `--pf`, `--password-file` | string | Path to a password file that contains the password to the backend. | None |
| `-s`, `--server` | string | The URL of the backend server. | None |
| `-p`, `--port` | int | The port number that this backend server is using. | None |
| `-d`, `--db-num` | int | The backend database number. | None |
| `-c`, `--cert-reqs` | string | Backend cert requirements. | None |
| `-e`, `--encryption-key` | string | Path to the encryption key file. | None |

**Examples:**

!!! example "Update Every Setting Required for Redis"

    ```bash
    merlin config update-backend -t redis --pf ~/.merlin/redis.pass -s my-redis-server.llnl.gov -p 6379 -d 0 -c none
    ```

    This will create the following `results_backend` section in your `app.yaml` file:

    ```yaml
    results_backend:
        cert_reqs: none
        db_num: 0
        encryption_key: ~/.merlin/encrypt_data_key
        name: rediss
        password: ~/.merlin/redis.pass
        port: 6379
        server: my-redis-server.llnl.gov
        username: ''
    ```

!!! example "Update Just the Port"

    ```bash
    merlin config update-backend -t redis -p 6379
    ```

!!! example "Update a Custom Configuration File Path"

    ```bash
    merlin config update-backend -t redis --cf /path/to/custom_config.yaml -s new-server.gov
    ```

#### Config Update-Broker (`merlin config update-broker`)

The `merlin config update-broker` command allows you to modify the [`broker`](./configuration/index.md#what-is-a-broker) section of your configuration file directly from the command line. See the options table below to see exactly what settings can be set.

**Usage:**

```bash
merlin config update-broker -t {rabbitmq,redis} [OPTIONS] ...
```

**Options:**

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `-h`, `--help`   | boolean | Show this help message and exit | `False` |
| `-t`, `--type`   | choice(`rabbitmq` \| `redis`) | Type of broker to configure. | None |
| `--cf`, `--config-file` | string | Path to the config file that will be updated. | `$(HOME)/.merlin/app.yaml` |
| `-u`, `--username` | string | The broker username (only for `rabbitmq` broker). | None |
| `--pf`, `--password-file` | string | Path to a password file that contains the password to the broker. | None |
| `-s`, `--server` | string | The URL of the broker server. | None |
| `-p`, `--port` | int | The port number that this broker server is using. | None |
| `-v`, `--vhost` | string | The vhost for the broker (only for `rabbitmq` broker). | None |
| `-d`, `--db-num` | int | The backend database number (only for `redis` broker). | None |
| `-c`, `--cert-reqs` | string | Backend cert requirements. | None |

**Examples:**

!!! example "Update Every Setting Required for a Redis Broker"

    ```bash
    merlin config update-broker -t redis --pf ~/.merlin/redis.pass -s my-redis-server.llnl.gov -p 6379 -d 0 -c none
    ```

    This will create the following `broker` section in your `app.yaml` file:

    ```yaml
    broker:
        cert_reqs: none
        db_num: 0
        name: rediss
        password: ~/.merlin/redis.pass
        port: 6379
        server: my-redis-server.llnl.gov
        username: ''
    ```

!!! example "Update Every Setting Required for a RabbitMQ Broker"

    ```bash
    merlin config update-broker -t rabbitmq -u my_rabbit_username --pf ~/.merlin/rabbit.pass -s my-rabbitmq-server.llnl.gov -p 5672 -v host4rabbit -c none
    ```

    This will create the following `broker` section in your `app.yaml` file:

    ```yaml
    broker:
        cert_reqs: none
        name: rabbitmq
        password: ~/.merlin/rabbit.pass
        port: 5672
        server: my-rabbitmq-server.llnl.gov
        username: my_rabbit_username
        vhost: host4rabbit
    ```

!!! example "Update Just the Username"

    ```bash
    merlin config update-broker -t rabbitmq -u my_new_username
    ```

!!! example "Update a Custom Configuration File Path"

    ```bash
    merlin config update-broker -t redis --cf /path/to/custom_config.yaml -s new-server.gov
    ```

#### Config Use (`merlin config use`)

The `merlin config use` command allows you to switch which configuration file to use as your active configuration.

**Usage:**

```bash
merlin config use [OPTIONS] CONFIG_FILE
```

**Options:**

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `-h`, `--help`   | boolean | Show this help message and exit | `False` |

**Examples:**

!!! example "Use a Custom Configuration"

    ```bash
    merlin config use /path/to/custom_config.yaml
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

More information on configuring with Merlin server can be found at the [Containerized Server Configuration](./configuration/containerized_server.md) page.

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
| `--dry` | boolean | Do a [Dry Run](./running_studies.md#dry-runs) of your workflow | `False` |
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

### Run Workers (`merlin run-workers`)

The tasks queued on the broker by the [`merlin run`](#run-merlin-run) command are run by a collection of workers. These workers can be run local in the current shell or in parallel on a batch allocation. The workers are launched using the `run-workers` command which reads the configuration for the worker launch from the `<input.yaml>` file.

Within the `<input.yaml>` file, the `batch` and `merlin.resources.workers` sections are both used to configure the worker launch. The top level `batch` section can be overridden in the `merlin.resources.workers` section. Parallel workers should be scheduled using the system's batch scheduler (see the section describing [Distributed Runs](./running_studies.md#distributed-runs) for more info).

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

The Merlin library comes equipped with several commands to help monitor your workflow:

- *[detailed-status](#detailed-status-merlin-detailed-status)*: Display task-by-task status information for a study
- *[monitor](#monitor-merlin-monitor)*: Keep your allocation alive while tasks are being processed
- *[query-workers](#query-workers-merlin-query-workers)*: Communicate with Celery to view information on active workers
- *[queue-info](#queue-info-merlin-queue-info)*: Communicate with Celery to view the status of queues in your workflow(s)
- *[status](#status-merlin-status)*: Display a summary of the status of a study

More information on all of these commands can be found below and in the [Monitoring documentation](./monitoring/index.md).

### Detailed Status (`merlin detailed-status`)

!!! warning

    For the pager opened by this command to work properly the `MANPAGER` or `PAGER` environment variable must be set to `less -r`. This can be set with:

    === "MANPAGER"

        ```bash
        export MANPAGER="less -r"
        ```
    
    === "PAGER"

        ```bash
        export PAGER="less -r"
        ```

Display the task-by-task status of a workflow.

This command will open a pager window with task statuses. Inside this pager window, you can search and scroll through task statuses for every step of your workflow.

For more information, see the [Detailed Status documentation](./monitoring/status_cmds.md#the-detailed-status-command).

**Usage:**

```bash
merlin detailed-status [OPTIONS] WORKSPACE_OR_SPECIFICATION
```

**Options:**

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `-h`, `--help`   | boolean | Show this help message and exit | `False` |
| `--dump` | filename | The name of a csv or json file to dump the status to | None |
| `--task_server`  | string | Task server type. Currently only "celery" is implemented. | "celery" |
| `-o`, `--output-path` | dirname | Specify a location to look for output workspaces. Only used when a spec file is passed as the argument to `status`. | None |

**Filter Options:**

The `detailed-status` command comes equipped with several options to help filter the output of your status query.

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `--max-tasks` | integer | Sets a limit on how many tasks can be displayed. | None |
| `--return-code` | List[string] | Filter which tasks to display based on their return code. Multiple return codes can be provided using a space-delimited list. Options: `SUCCESS`, `SOFT_FAIL`, `HARD_FAIL`, `STOP_WORKERS`, `RETRY`, `DRY_SUCCESS`, `UNRECOGNIZED`. | None |
| `--steps` | List[string] | Filter which tasks to display based on the steps that they're associated with. Multiple steps can be provided using a space-delimited list. | `['all']` |
| `--task-queues` | List[string] | Filter which tasks to display based on a the task queues that they were/are in. Multiple task queues can be provided using a space-delimited list. | None |
| `--task-status` | List[string] | Filter which tasks to display based on their status. Multiple statuses can be provided using a space-delimited list. Options: `INITIALIZED`, `RUNNING`, `FINISHED`, `FAILED`, `CANCELLED`, `DRY_RUN`, `UNKNOWN`. | None |
| `--workers` | List[string] | Filter which tasks to display based on which workers are processing them. Multiple workers can be provided using a space-delimited list. | None |

**Display Options:**

There are multiple options to modify the way task statuses are displayed.

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `--disable-pager` | boolean | Turn off the pager functionality when viewing the task-by-task status. **Caution:** This option is *not* recommended for large workflows as you could freeze your terminal with thousands of task statuses. | `False` |
| `--disable-theme` | boolean | Turn off styling for the status layout. | `False` |
| `--layout` | string | Alternate task-by-task status display layouts. Options: `table`, `default`. | `default` |
| `--no-prompts` | boolean | Ignore any prompts provided. This cause the `detailed-status` command to default to the latest study if you provide a spec file as input. | `False` |

**Examples:**

!!! example "Check the Detailed Status Using Workspace as Input"

    ```bash
    merlin detailed-status study_name_20240129-123452/
    ```

!!! example "Check the Detailed Status Using a Specification as Input"

    This will look in the `OUTPUT_PATH` [Reserved Variable](./variables.md#reserved-variables) defined within the spec file to try to find existing workspace directories associated with this spec file. If more than one are found, a prompt will be displayed for you to select a workspace directory. 

    ```bash
    merlin detailed-status my_specification.yaml
    ```

!!! example "Dump the Status Report to a JSON File"

    ```bash
    merlin detailed-status study_name_20240129-123452/ --dump status_report.json
    ```

!!! example "Only Display Failed Tasks"

    ```bash
    merlin detailed-status study_name_20240129-123452/ --task-status FAILED
    ```

!!! example "Display the First 8 Successful Tasks"

    ```bash
    merlin detailed-status study_name_20240129-123452/ --return-code SUCCESS --max-tasks 8
    ```

!!! example "Disable the Theme"

    ```bash
    merlin detailed-status study_name_20240129-123452/ --disable-theme
    ```

!!! example "Use the Table Layout"

    ```bash
    merlin detailed-status study_name_20240129-123452/ --layout table
    ```

### Monitor (`merlin monitor`)

Batch submission scripts may not keep the batch allocation alive if there is not a blocking process in the submission script. The `merlin monitor` command addresses this by providing a blocking process that checks for tasks in the queues every (sleep) seconds ("sleep" here can be defined with the `--sleep` option). When the queues are empty, the monitor will query Celery to see if any workers are still processing tasks from the queues. If no workers are processing any tasks from the queues and the queues are empty, the blocking process will exit and allow the allocation to end.

The `monitor` functionality will check for Celery workers for up to 10*(sleep) seconds before monitoring begins. The loop happens when the queue(s) in the spec contain tasks, but no running workers are detected. This is to protect against a failed worker launch.

For more information, see the [Monitoring Studies for Persistent Allocations documentation](./monitoring/monitor_for_allocation.md).

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

For more information, see the [Query Workers documentation](./monitoring/queues_and_workers.md#query-workers).

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

### Queue Info (`merlin queue-info`)

!!! note

    Prior to Merlin v1.12.0 the `merlin status` command would produce the same output as `merlin queue-info --spec <spec_file>`

Check the status of queues to see if there are any tasks in them and/or any workers watching them.

If used without the `--spec` option, this will query any active queues. Active queues are queues that have a worker watching them.

For more information, see the [Queue Information documentation](./monitoring/queues_and_workers.md#queue-information).

**Usage:**

```bash
merlin queue-info [OPTIONS]
```

**Options:**

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `-h`, `--help`   | boolean | Show this help message and exit | `False` |
| `--dump` | filename | The name of a csv or json file to dump the queue information to | None |
| `--specific-queues` | List[string] | A space-delimited list of queues to get information on | None |
| `--task_server`  | string | Task server type. Currently only "celery" is implemented. | "celery" |

**Specification Options:**

These options all *must* be used with the `--spec` option if used.

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `--spec` | filename | Query for the queues named in each step of the spec file given here | None |
| `--steps` | List[string] | A space-delimited list of steps in the input spec that you want to query. Should be given after the input spec. | `['all']` |
| `--vars` | List[string] | A space-delimited list of variables to override in the spec file. This list should be given after the spec file is provided. Ex: `--vars QUEUE_NAME=new_queue_name` | None |

**Examples:**

!!! example "Query All Active Queues"

    ```bash
    merlin queue-info
    ```

!!! example "Check the Status of Specific Queues"

    ```bash
    merlin queue-info --specific-queues queue_1 queue_3
    ```

!!! example "Check the Status of Queues in a Spec File"

    **This is the same as running `merlin status <spec_file>` prior to Merlin v1.12.0**

    ```bash
    merlin queue-info --spec my_specification.yaml
    ```

!!! example "Check the Status of Queues for Specific Steps"

    ```bash
    merlin queue-info --spec my_specification.yaml --steps step_1 step_3
    ```

!!! example "Dump the Queue Information to a JSON File"

    ```bash
    merlin queue-info --dump queue_report.json
    ```

### Status (`merlin status`)

!!! note

    To obtain the same functionality as the `merlin status` command prior to Merlin v1.12.0 use [`merlin queue-info`](#queue-info-merlin-queue-info) with the `--spec` option:

    ```bash
    merlin queue-info --spec <spec_file>
    ```

Display a high-level status summary of a workflow.

This will display the progress of each step in your workflow using progress bars and brief summaries. In each summary you can find how many tasks there are in total for a step, how many tasks are in each state, the average run time and standard deviation of run times of the tasks in the step, the task queue, and the worker that is watching the step.

For more information, see the [Status documentation](./monitoring/status_cmds.md#the-status-command).

**Usage:**

```bash
merlin status [OPTIONS] WORKSPACE_OR_SPECIFICATION
```

**Options:**

| Name             |  Type   | Description | Default |
| ------------     | ------- | ----------- | ------- |
| `-h`, `--help`   | boolean | Show this help message and exit | `False` |
| `--cb-help` | boolean | Colorblind help option. This will utilize different symbols for each state of a task. | `False` |
| `--dump` | filename | The name of a csv or json file to dump the status to | None |
| `--no-prompts` | boolean | Ignore any prompts provided to the command line. This will default to the latest study if you provide a spec file rather than a study workspace. | `False` |
| `--task_server`  | string | Task server type. Currently only "celery" is implemented. | "celery" |
| `-o`, `--output-path` | dirname | Specify a location to look for output workspaces. Only used when a spec file is passed as the argument to `status`. | None |

**Examples:**

!!! example "Check the Status Using Workspace as Input"

    ```bash
    merlin status study_name_20240129-123452/
    ```

!!! example "Check the Status Using a Specification as Input"

    This will look in the `OUTPUT_PATH` [Reserved Variable](./variables.md#reserved-variables) defined within the spec file to try to find existing workspace directories associated with this spec file. If more than one are found, a prompt will be displayed for you to select a workspace directory. 

    ```bash
    merlin status my_specification.yaml
    ```

!!! example "Check the Status Using a Specification as Input & Ignore Any Prompts"

    If multiple workspace directories associated with the spec file provided are found, the `--no-prompts` option will ignore the prompt and select the most recent study that was ran based on the timestamps. 

    ```bash
    merlin status my_specification.yaml --no-prompts
    ```

!!! example "Dump the Status Report to a CSV File"

    ```bash
    merlin status study_name_20240129-123452/ --dump status_report.csv
    ```

!!! example "Look For Workspaces at a Certain Location"

    ```bash
    merlin status my_specification.yaml -o new_output_path/
    ```

!!! example "Utilize the Colorblind Functionality"

    ```bash
    merlin status study_name_20240129-123452/ --cb-help
    ```
