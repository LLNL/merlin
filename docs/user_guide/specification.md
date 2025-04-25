# The Specification File

At the core of Merlin is the specification (spec) file. This file is used to define how workflows should be created and executed by Merlin, and is also utilized as a way for users to keep records of their studies.

Merlin enables several blocks in the spec file, each with their own purpose:

| Block Name | Required? | Description |
| ---------- | --------- | ----------- |
| [`description`](#the-description-block) | Yes | General information about the study |
| [`env`](#the-env-block) | No | Fixed constants and other values that are globally set and referenced |
| [`global.parameters`](#the-globalparameters-block) | No | Parameters that are user varied and applied to the workflow |
| [`batch`](#the-batch-block) | No | Settings for submission to batch systems |
| [`study`](#the-study-block) | Yes | Steps that the study is composed of and are executed in a defined order |
| [`merlin`](#the-merlin-block) | No | Worker settings and sample generation handling |
| [`user`](#the-user-block) | No | YAML anchor definitions |

This module will go into detail on every block and the properties available within each.

## The `description` Block

Since Merlin is built as extension of [Maestro](https://maestrowf.readthedocs.io/en/latest/index.html), most of the behavior of the `description` block is inherited directly from Maestro. Therefore, we recommend reading [Maestro's documentation on the `description` Block](https://maestrowf.readthedocs.io/en/latest/Maestro/specification.html#description-description) for the most accurate description of how it should be used.

There is one difference between Merlin and Maestro when it comes to the `description` block: the use of variables. With Merlin, the `description` block can use variables defined in [the `env` block](#the-env-block).

!!! example "Using Variables in the `description` Block"

    ```yaml
    description:
        name: $(STUDY_NAME)
        description: An example showcasing how variables can be used in the description block

    env:
        variables:
            STUDY_NAME: variable_study_name
    ```

## The `env` Block

Since Merlin is built as extension of [Maestro](https://maestrowf.readthedocs.io/en/latest/index.html), the behavior of the `env` block is inherited directly from Maestro. Therefore, we recommend reading [Maestro's documentation on the `env` block](https://maestrowf.readthedocs.io/en/latest/Maestro/specification.html#environment-env) for the most accurate description of how it should be used.

For more information on how variables defined in this block can be used, check out the [Variables](./variables.md) page (specifically the [Token Syntax](./variables.md#token-syntax), [User Variables](./variables.md#user-variables), and [Environment Variables](./variables.md#environment-variables) sections).

!!! example "A Basic `env` Block"

    ```yaml
    env:
        variables:
            N_SAMPLES: 10
            OUTPUT_PATH: /path/to/study_output/
    ```

## The `global.parameters` Block

Since Merlin is built as extension of [Maestro](https://maestrowf.readthedocs.io/en/latest/index.html), the behavior of the `global.parameters` block is inherited directly from Maestro. Therefore, we recommend reading [Maestro's documentation on the `global.parameters` block](https://maestrowf.readthedocs.io/en/latest/Maestro/specification.html#parameters-globalparameters) for the most accurate description of how it should be used.

It would also be a good idea to read through [Specifying Study Parameters](https://maestrowf.readthedocs.io/en/latest/Maestro/parameter_specification.html) from Maestro which goes into further detail on how to use parameters in your study. There you will also find details how to programmatically generate parameters using [`pgen`](https://maestrowf.readthedocs.io/en/latest/Maestro/parameter_specification.html#parameter-generator-pgen).

!!! example "A Basic `global.parameters` Block"

    ```yaml
    global.parameters:
        RADIUS:
            values: [2, 5, 10, 20]
            label: RADIUS.%%
        HEIGHT:
            values: [5, 10, 30, 60]
            label: HEIGHT.%%
    ```

## The `batch` Block

!!! warning

    Although the `batch` block exists in both Maestro and Merlin spec files, this block will differ slightly in Merlin.

!!! tip

    This block is frequently used in conjunction with the [`LAUNCHER` and `VLAUNCHER` variables](./variables.md#the-launcher-and-vlauncher-variables).

The `batch` block is an optional block that enables specification of HPC scheduler information to enable writing steps that are decoupled from particular machines and thus more portable/reusable. Below are the base properties for this block.

| Property Name | Required? | Type | Description |
| ------------- | --------- | ---- | ----------- |
| `bank` | Yes | str | Account to charge computing time to |
| `dry_run` | No | bool | Execute a [dry run](./running_studies.md#dry-runs) of the study |
| `launch_args` | No | str | Extra arguments for the parallel launch command |
| `launch_pre` | No | str | Any configuration needed before the scheduler launch command (`srun`, `jsrun`, etc.) |
| `nodes` | No | int | The number of nodes to use for all workers. This can be overridden in [the `resources` property of the `merlin` block](#resources). If this is unset the number of nodes will be queried from the environment, failing that, the number of nodes will be set to 1. |
| `queue` | Yes | str | Scheduler queue/partition to submit jobs (study steps) to |
| `shell` | No | str | Optional specification path to the shell to use for execution. Defaults to `/bin/bash` |
| `type` | Yes | str | Type of scheduler managing execution. One of: `local`, `flux`, `slurm`, `lsf`, `pbs` |
| `walltime` | No | str | The total walltime of the batch allocation (hh\:mm:ss or mm:ss or ss) |
| `worker_launch` | No | str | Override the parallel launch defined in Merlin |

If using `flux` as your batch type, there are a couple more properties that you can define here:

| Property Name | Type | Description |
| ------------- | ---- | ----------- |
| `flux_exec` | str | Optional flux exec command to launch workers on all nodes if `flux_exec_workers` is True |
| `flux_exec_workers` | bool | Optional flux argument to launch workers on all nodes |
| `flux_path` | str | Optional path to flux bin |
| `flux_start_opts` | str | Optional flux start options |

Below are examples of different scheduler set ups. The only required keys in each of these examples are: `type`, `queue`, and `bank`.

=== "Local"

    ```yaml
    batch:
        type: local
        queue: pbatch
        bank: baasic
    ```

=== "Slurm"

    ```yaml
    batch:
        type: slurm
        queue: pbatch
        bank: baasic
        walltime: "08:30:00"
    ```

=== "LSF"

    ```yaml
    batch:
        type: lsf
        queue: pbatch
        bank: baasic
        nodes: 2
    ```

=== "Flux"

    ```yaml
    batch:
        type: flux
        queue: pbatch
        bank: baasic
        launch_pre: export FLUX_STRT_TIME=`date -u +%Y-%m-%dT%H:%M:%S.%6NZ`
    ```

## The `study` Block

!!! warning

    Although the `study` block exists in both Maestro and Merlin spec files, this block will differ slightly in Merlin.

The `study` block is where the steps to be executed in the Merlin study are defined. The steps that are defined here will ultimately create the [DAG](../faq.md#what-is-a-dag) that's executed by Merlin.

This block represents the unexpanded set of steps that the study is composed of. Here, unexpanded means no parameter nor sample substitution; the steps only contain references to the parameters and/or samples. Steps are given as a list (- prefixed) of properties:

| Property Name | Required? | Type | Description |
| ------------- | --------- | ---- | ----------- |
| `name` | Yes | str | Unique name for identifying and referring to a step |
| `description` | Yes | str | A general description of what this step is intended to do |
| [`run`](#the-run-property) | Yes | dict | Properties that describe the actual specification of the step |

### The `run` Property

The `run` property contains several subproperties that define what a step does and how it relates to other steps. This is where you define the concrete shell commands the task needs to execute, step dependencies that dictate the topology of the DAG, and any `parameter`, `env`, or `sample` tokens to inject.

| Property Name | Required? | Type | Description |
| ------------- | --------- | ---- | ----------- |
| `cmd` | Yes | str | The actual commands to be executed for this step |
| `depends` | No | List[str] | List of other steps which must successfully execute before this task can be executed |
| `max_retries` | No | int | The maximum number of retries allowed for this step |
| `restart` | No | str | Similar to `cmd`, providing optional alternate commands to run upon restarting, e.g. after a scheduler timeout |
| `retry_delay` | No | int | The time in seconds to delay a retry by |
| `shell` | No | str | The shell to execute `cmd` in (e.g. `/bin/bash`, `/usr/bin/env`, `python`) (default: `/bin/bash`) |
| `task_queue` | No | str | The name of the task queue to assign this step to. Workers will watch task queues to find tasks to execute. (default: `merlin`) |

!!! example

    ```yaml
    study:
        - name: create_data
          description: Use a python script to create some data
          run:
            cmd: |  # (1)
                echo "Creating data..."
                python create_data.py --outfile data.npy
                echo "Data created"
            restart: |  # (2)
                echo "Restarted the data creation..."
                python create_data.py --outfile data_restart.npy
                echo "Data created upon restart"
            max_retries: 3  # (3)
            retry_delay: 5  # (4)
            task_queue: create  # (5)
        
        - name: transpose_data
          description: Use python to transpose the data
          run:
            cmd: |  # (6)
                import numpy as np
                import os

                data_file = "$(create_data.workspace)/data.npy"
                if not os.path.exists(data_file):
                    data_file = "$(create_data.workspace)/data_restart.npy"

                initial_data = np.load(data_file)
                transposed_data = np.transpose(initial_data)
                np.save("$(WORKSPACE)/transposed_data.npy", transposed_data)
            shell: /usr/bin/env python3  # (7)
            task_queue: transpose
            depends: [create_data]  # (8)
    ```

    1. The `|` character allows the `cmd` to become a multi-line string
    2. The `restart` command will be ran if the initial execution of `cmd` exits with a `$(MERLIN_RESTART)` [Return Code](./variables.md#step-return-variables)
    2. Only allow this step to retry itself 3 times
    3. Delay by 5 seconds on each retry
    4. All tasks created by this step will get sent to the `create` queue. They will live in this queue on the [broker](./configuration/index.md#what-is-a-broker) until a worker picks them up for execution.
    5. This step uses two variables `$(create_data.workspace)` and `$(WORKSPACE)`. They point to `create_data`'s output directory and `transpose_data`'s output directory, respectively. Read the section on [Reserved Variables](./variables.md#reserved-variables) for more information on these and other variables.
    6. Setting our shell to be `python3` allows us to write python in the `cmd` rather than bash scripting
    7. Since this step depends on the `create_data` step, it will not be ran until `create_data` finishes processing

There are also a few optional properties for describing resource requirements to pass to the scheduler and associated [`$(LAUNCHER)`](./variables.md#the-launcher-and-vlauncher-variables) tokens used to execute applications on HPC systems.

| Property Name | Required? | Type | Description |
| ------------- | --------- | ---- | ----------- |
| `batch` | No | dict | Override the `batch` block for this step |
| `nodes` | No | int | Number of nodes to reserve for executing this step: primarily used by `$(LAUNCHER)` expansion |
| `procs` | No | int | Number of processors needed for step execution: primarily used by `$(LAUNCHER)` expansion |
| `walltime` | No | str | Specifies maximum amount of time to reserve HPC resources for |

!!! example

    ```yaml
    batch:
        type: flux
        queue: pbatch
        bank: baasic

    study:
        - name: create_data
          description: Use a python script to create some data
          run:
            cmd: 
                echo "Creating data..."
                $(LAUNCHER) python create_data.py  # (1)
                echo "Data created"
            nodes: 2
            procs: 4
            walltime: "30:00"
            task_queue: create
    ```

    1. The [`$(LAUNCHER)`](./variables.md#the-launcher-and-vlauncher-variables) token here will be expanded to `flux run -N 2 -n 4 -t 1800.0s`

Additionally, there are scheduler specific properties that can be used. The sections below will highlight these properties.

#### Slurm Specific Properties

Merlin supports the following properties for Slurm:

| Property Name | Equivalent `srun` Option | Type | Description | Default |
| ------------- | ------------------------ | ---- | ----------- | ------- |
| `cores per task` | `-c`, `--cpus-per-task` | int | Number of cores to use for each task | 1 |
| `reservation` | `--reservation` | str | Reservation to schedule this step to; overrides batch block | None |
|   `slurm`   | N/A | str | Verbatim flags only for the srun parallel launch. This will be expanded as follows for steps that use [`LAUNCHER` or `VLAUNCHER`](./variables.md#the-launcher-and-vlauncher-variables): `srun -N <nodes> -n <procs> ... <slurm>`. | None |

!!! example

    The following example will run `example_slurm_step` with Slurm specific options `cores per task` and `slurm`. This will tell Merlin that this step needs 2 nodes, 4 cores per task, and to begin this at noon.

    ```yaml
    batch:
        type: slurm
        queue: pbatch
        bank: baasic
    
    study:
        - name: example_slurm_step
          description: A step using slurm specific options
          run:
            cmd: |
                $(LAUNCHER) python3 do_something.py
            nodes: 2
            cores per task: 4
            slurm: --begin noon
    ```

    Here, `$(LAUNCHER)` will become `srun -N 2 -c 4 --begin noon`.

#### Flux Specific Properties

Merlin supports the following Flux properties:

| Property Name | Equivalent `flux run` Option | Type | Description | Default |
| ------------- | ---------------------------- | ---- | ----------- | ------- |
| `cores per task` | `-c`, `--cores-per-task` | int | Number of cores to use for each task | 1 |
| `gpus per task` | `-g`, `--gpus-per-task` | int | Number of gpus to use for each task | 0 |
| `flux` | N/A | str | Verbatim flags for the flux parallel launch. This will be expanded as follows for steps that use [`LAUNCHER` or `VLAUNCHER`](./variables.md#the-launcher-and-vlauncher-variables): `flux mini run ... <flux>` | None |

!!! example

    The following example will run `example_flux_step` with Flux specific options `cores per task` and `gpus per task`. This will tell Merlin that this step needs 2 nodes, 4 cores per task, and 1 gpu per task.

    ```yaml
    batch:
        type: flux
        queue: pbatch
        bank: baasic
    
    study:
        - name: example_flux_step
          description: A step using flux specific options
          run:
            cmd: |
                $(LAUNCHER) python3 do_something.py
            nodes: 2
            cores per task: 4
            gpus per task: 1
    ```

    Here, `$(LAUNCHER)` will become `flux run -N 2 -c 4 -g 1`.

#### LSF Specific Properties

Merlin supports the following properties for LSF:

| Property Name         | Equivalent `jsrun` Option | Type | Description | Default |
| --------------------- | ------------------------- | ---- | ----------- | ------- |
| `bind` | `-b`, `--bind` | str | Flag for MPI binding of tasks on a node | `rs` |
| `cores per task` | `-c`, `--cpu_per_rs` | int | Number of cores to use for each task | 1 |
| `exit_on_error` | `-X`, `--exit_on_error` | int | Flag to exit on error. A value of `1` enables this and `0` disables it. | 1 |
| `gpus per task` | `-g`, `--gpu_per_rs` | int | Number of gpus to use for each task | 0 |
| `num resource set` | `-n`, `--nrs` | int | Number of resource sets. The `nodes` property will set this same flag for LSF so only do one or the other. | 1 |
| `launch_distribution` | `-d`, `--launch_distribution` | str | The distribution of resources | `plane:{procs/nodes}` |
| `lsf` | N/A | str | Verbatim flags only for the lsf parallel launch. This will be expanded as follows for steps that use [`LAUNCHER` or `VLAUNCHER`](./variables.md#the-launcher-and-vlauncher-variables): `jsrun ... <lsf>` | None |

!!! example

    The following example will run `example_lsf_step` with LSF specific options `exit_on_error` and `bind`. This will tell Merlin that this step needs 2 nodes, to not exit on error, and to not have any binding.

    ```yaml
    batch:
        type: lsf
        queue: pbatch
        bank: baasic
    
    study:
        - name: example_lsf_step
          description: A step using lsf specific options
          run:
            cmd: |
                $(LAUNCHER) python3 do_something.py
            nodes: 2
            exit_on_error: 0
            bind: none
    ```

    Here, `$(LAUNCHER)` will become `jsrun -N 2 -X 0 -b none`.


## The `merlin` Block

The `merlin` block is where you can customize Celery workers and generate samples to be used throughout the workflow.

This block is split into two main properties:

| Property Name | Required? | Type | Description |
| ------------- | --------- | ---- | ----------- |
| [`resources`](#resources) | No | dict | Define the task server configuration and workers to run the tasks |
| [`samples`](#samples) | No | dict | Define samples to be referenced in your study steps |

Both of these properties have multiple subproperties so we'll take a deeper dive into each one below.

### Resources

!!! note

    Currently the only task server that Merlin supports is Celery.

The `resources` property of the `merlin` block allows users to customize task server configuration and create custom workers to run tasks. This property has the following subproperties:

| Property Name | Required? | Type | Description |
| ------------- | --------- | ---- | ----------- |
| `task_server` | No | str | The type of task server to use. **Currently "celery" is the only option.** (default: celery) |
| `overlap` | No | bool | Flag to determine if multiple workers can pull tasks from overlapping queues. (default: False) |
| `workers` | No | List[dict] | A list of worker definitions. |

The `workers` subproperty is where you can create custom workers to process your workflow. The keys that you provide under this property will become the names of your custom workers.

!!! example

    The following `merlin` block will create two workers named `data_creation_worker` and `data_transpose_worker`.

    ```yaml
    merlin:
        resources:
            workers:
                data_creation_worker:
                    <custom worker settings (we'll get to this next)>
                data_transpose_worker:
                    <custom worker settings (we'll get to this next)>
    ```

Each worker can be customized with the following settings:

| Setting Name | Type | Description |
| ------------ | ---- | ----------- |
| `args` | str | Arguments to provide to the worker. Check out [Configuring Celery Workers](./celery.md#configuring-celery-workers) and/or [Celery's worker options](https://docs.celeryq.dev/en/main/reference/cli.html#celery-worker) for more info on what can go here. <div class="admonition tip"><p class="admonition-title">Tip</p><p>The most common arguments used with `args` are `--concurrency`, `--prefetch-multiplier`, `-O fair`, and `-l`.</p></div> |
| `batch` | dict | Override the main `batch` config for this worker. <div class="admonition tip"><p class="admonition-title">Tip</p><p>This setting is useful if other workers are running flux, but some component of the workflow requires the native scheduler or cannot run under flux.</p><p>Another possibility is to have the default `batch` type as `local` and then define workers needed for flux or slurm steps.</p></div> |
| `machines` | List[str] | A list of machines to run the given steps provided in the `steps` setting here. **A full `OUTPUT_PATH` and the `steps` argument are both _required_ for this setting. Currently all machines in the list must have access to the `OUTPUT_PATH`.** <div class="admonition note"><p class="admonition-title">Note</p><p>You'll need an allocation on any machine that you list here. You'll then have to run [`merlin run-workers`](./command_line.md#run-workers-merlin-run-workers) from any machine listed here. The `merlin run` command will only have to be ran once from any machine in order to send the tasks to the broker.</p></div> |
| `nodes` | int | Number of nodes for this worker to run on. (defaults to all nodes on your allocation) |
| `steps` | List[str] | A list of step names for this worker to "watch". The worker will *actually* be watching the `task_queue` associated with the steps listed here. (default: `[all]`) |

??? example "Custom Worker for Each Step"

    This example showcases how to define custom workers that watch different steps in your workflow. Here, `data_creation_worker` will execute tasks created from the `create_data` step that are sent to the `create` queue, and `data_transpose_worker` will execute tasks created from the `transpose_data` step that are sent to the `transpose` queue.
    
    We're also showing how to vary worker arguments using some of the most common arguments for workers.

    ```yaml
    study:
        - name: create_data
          description: Use a python script to create some data
          run:
            cmd: |
                echo "Creating data..."
                python create_data.py
                echo "Data created"
            task_queue: create  # (1)
        
        - name: transpose_data
          description: Use python to transpose the data
          run:
            cmd: |
                import numpy as np
                initial_data = np.load("$(create_data.workspace)/data.npy")
                transposed_data = np.transpose(initial_data)
                np.save("$(WORKSPACE)/transposed_data.npy", transposed_data)
            shell: /usr/bin/env python3
            task_queue: transpose
            depends: [create_data]

    merlin:
        resources:
            workers:
                data_creation_worker:
                    args: -l INFO --concurrency 4 --prefetch-multiplier 1 -O fair  # (2)
                    steps: [create_data]  # (3)

                data_transpose_worker:
                    args: -l INFO --concurrency 1 --prefetch-multiplier 1
                    steps: [transpose_data]
    ```

    1. The name of the queue for this step is important as that is where the tasks required to execute this step will be stored on the [broker](./configuration/index.md#what-is-a-broker) until a worker (in this case `data_creation_worker`) pulls the tasks and executes them.
    2. Arguments here can be broken down as follows:
        - `-l` sets the log level
        - `--concurrency` sets the number of worker processes to spin up on each node that this worker is running on (Celery's default is to set `--concurrency` to be the number of CPUs on your node). More info on this can be found on [Celery's concurrency documentation](https://docs.celeryq.dev/en/stable/userguide/workers.html#concurrency).
        - `--prefetch-multiplier` sets the number of messages to prefetch at a time multiplied by the number of concurrent processes (Celery's default is to set `--prefetch-multiplier` to 4). More info on this can be found on [Celery's prefetch multiplier documentation](https://docs.celeryq.dev/en/stable/userguide/configuration.html#std-setting-worker_prefetch_multiplier).
        - `-O fair` sets the scheduling algorithm to be fair. This aims to distribute tasks more evenly based on the current workload of each worker.
    3. Here we tell `data_creation_worker` to watch the `create_data` step. What this *actually* means is that the `data_creation_worker` will go monitor the `task_queue` associated with the `create_data` step, which in this case is `create`. Any tasks sent to the `create` queue will be pulled and executed by the `data_creation_worker`.

??? example "Custom Workers to Run Across Multiple Machines"

    This example showcases how you can define custom workers to be able to run on multiple machines. Here, we're assuming that both machines `quartz` and `ruby` have access to our `OUTPUT_PATH`.

    ```yaml
    env:
        variables:
            OUTPUT_PATH: /path/to/shared/filespace/
            CONCURRENCY: 1
    
    merlin:
        resources:
            workers:
                cross_machine_worker:
                    args: -l INFO --concurrency $(CONCURRENCY)  # (1)
                    machines: [quartz, ruby]  # (2)
                    nodes: 2  # (3)
    ```

    1. Variables can be used within worker customization. They can even be used to name workers!
    2. This worker will be able to start on both `quartz` and `ruby` so long as you have an allocation on both and execute [`merlin run-workers`](./command_line.md#run-workers-merlin-run-workers) from both machines.
    3. This worker will only start on 2 nodes of our allocation.

### Samples

The `samples` property of the `merlin` block allows users to generate, store, and create references to samples that can be used throughout a workflow.

This property comes with several subproperties to assist with the handling of samples:

| Property Name | Type | Description |
| ------------- | ---- | ----------- |
| `column_labels` | List[str] | The names of the samples stored in `file`. This will be how you reference samples in your workflow using [token syntax](./variables.md#token-syntax). |
| `file` | str | The name of the samples file where your samples are stored. **Must be either .npy, .csv, or .tab.** |
| `generate` | dict | Properties that describe how the samples should be generated |
| `level_max_dirs` | int | The number of sample output directories to generate at each level in the sample hierarchy of a step. See the "Modifying The Hierarchy Structure" example in [The Sample Hierarchy](./interpreting_output.md#the-sample-hierarchy) section for an example of how this is used. |

Currently, within the `generate` property there is only one subproperty:

| Property Name | Type | Description |
| ------------- | ---- | ----------- |
| `cmd` | str | The command to execute that will generate samples |

!!! example "Basic Sample Generation & Usage"

    ```yaml
    study:
        - name: echo_samples
          description: Echo the values of our samples
          run:
            cmd: echo "var1 - $(VAR_1) ; var2 - $(VAR_2)"  # (1)

    merlin:
        samples:
            generate:
                cmd: spellbook make-samples -n 25 -outfile=$(MERLIN_INFO)/samples.npy  # (2)
            file: $(MERLIN_INFO)/samples.npy  # (3)
            column_labels: [VAR_1, VAR_2]  # (4)
    ```

    1. Samples are referenced in steps using [token syntax](./variables.md#token-syntax)
    2. Generate 25 samples using [Merlin Spellbook](https://pypi.org/project/merlin-spellbook/)
    3. Tell Merlin where the sample files are stored
    4. Label the samples so that we can use them in our study with [token syntax](./variables.md#token-syntax)

## The `user` Block

!!! warning

    Any anchors/aliases you wish to use *must* be defined *before* you use them. For instance, if you want to use an alias in your `study` block then you must put the `user` block containing the anchor definition before the `study` block in your spec file.

!!! tip

    This block is especially useful if you have a large chunk of code that's re-used in multiple steps.

The `user` block allows other variables in the workflow file to be propogated through to the workflow. This block uses [YAML Anchors and Aliases](https://smcleod.net/2022/11/yaml-anchors-and-aliases/); anchors define a chunk of configuration and their alias is used to refer to that specific chunk of configuration elsewhere.

To define an anchor, utilize the `&` syntax. For example, the following user block will define an anchor `python3_run`. The `python3_run` anchor creates a shorthand for running a simple print statement in Python 3:

```yaml
user:
    python3:
        run: &python3_run
            cmd: |
                print("OMG is this in python3?")
            shell: /usr/bin/env python3
```

You can reference an anchor by utilizing the `<<: *` syntax to refer to its alias. Continuing with the example above, the following study block will reference the `python3_run` anchor:

```yaml
study:
    - name: *step_name
      description: do something in python
      run:
        <<: *python3_run
        task_queue: pyth3_q
```

Here we're merging the anchor `run` value with the existing values of `run`. Therefore, this step will be expanded to:

```yaml
study:
    - name: python3_hello
      description: do something in python
      run:
        cmd: |
            print("OMG is this in python3?")
        shell: /usr/bin/env python3
        task_queue: pyth3_q
```

Notice that the existing `task_queue` value was not overridden.

## Full Specification

Below is a full YAML specification file for Merlin. To fully understand what's going on in this example spec file, see the [Feature Demo](../examples/feature_demo.md) page.

```yaml
description:
    name: $(NAME)
    description: Run 10 hello worlds.

batch:
    type: local

env:
    variables:
        OUTPUT_PATH: ./studies
        N_SAMPLES: 10
        WORKER_NAME: demo_worker
        VERIFY_QUEUE: default_verify_queue
        NAME: feature_demo

        SCRIPTS: $(MERLIN_INFO)/scripts
        HELLO: $(SCRIPTS)/hello_world.py
        FEATURES: $(SCRIPTS)/features.json

user:
    study:
        run:
            hello: &hello_run
                cmd: |
                  python3 $(HELLO) -outfile hello_world_output_$(MERLIN_SAMPLE_ID).json $(X0) $(X1) $(X2)
                max_retries: 1
    python3:
        run: &python3_run
            cmd: |
              print("OMG is this in python?")
              print("Variable X2 is $(X2)")
            shell: /usr/bin/env python3
    python2:
        run: &python2_run
            cmd: |
              print "OMG is this in python2? Change is bad."
              print "Variable X2 is $(X2)"
            shell: /usr/bin/env python2

study:
    - name: hello
      description: |
         process a sample with hello world
      run:
        <<: *hello_run
        task_queue: hello_queue

    - name: collect
      description: |
         process the output of the hello world samples, extracting specific features;
      run:
         cmd: |
             echo $(MERLIN_GLOB_PATH)
             echo $(hello.workspace)
             ls $(hello.workspace)/X2.$(X2)/$(MERLIN_GLOB_PATH)/hello_world_output_*.json > files_to_collect.txt
             spellbook collect -outfile results.json -instring "$(cat files_to_collect.txt)"
         depends: [hello_*]
         task_queue: collect_queue

    - name: translate
      description: |
         process the output of the hello world samples some more
      run:
         cmd: spellbook translate -input $(collect.workspace)/results.json -output results.npz -schema $(FEATURES)
         depends: [collect]
         task_queue: translate_queue

    - name: learn
      description: |
         train a learner on the results
      run:
         cmd: spellbook learn -infile $(translate.workspace)/results.npz
         depends: [translate]
         task_queue: learn_queue

    - name: make_new_samples
      description: |
         make a grid of new samples to pass to the predictor
      run:
         cmd: spellbook make-samples -n $(N_NEW) -sample_type grid -outfile grid_$(N_NEW).npy
         task_queue: make_samples_queue

    - name: predict
      description: |
         make a new prediction from new samples
      run:
         cmd: spellbook predict -infile $(make_new_samples.workspace)/grid_$(N_NEW).npy -outfile prediction_$(N_NEW).npy -reg $(learn.workspace)/random_forest_reg.pkl
         depends: [learn, make_new_samples]
         task_queue: predict_queue

    - name: verify
      description: |
         if learn and predict succeeded, output a dir to signal study completion
      run:
         cmd: |
            if [[ -f $(learn.workspace)/random_forest_reg.pkl && -f $(predict.workspace)/prediction_$(N_NEW).npy ]]
            then
                touch FINISHED
                exit $(MERLIN_SUCCESS)
            else
                exit $(MERLIN_SOFT_FAIL)
            fi
         depends: [learn, predict]
         task_queue: $(VERIFY_QUEUE)

    - name: python3_hello
      description: |
          do something in python
      run:
          <<: *python3_run
          task_queue: pyth3_q

    - name: python2_hello
      description: |
          do something in python2, because change is bad
      run:
          <<: *python2_run
          task_queue: pyth2_hello

global.parameters:
    X2:
        values : [0.5]
        label  : X2.%%
    N_NEW:
        values : [10]
        label  : N_NEW.%%

merlin:
    resources:
        task_server: celery
        overlap: False
        workers:
            $(WORKER_NAME):
                args: -l INFO --concurrency 3 --prefetch-multiplier 1 -Ofair
    samples:
        generate:
            cmd: |
                cp -r $(SPECROOT)/scripts $(SCRIPTS)

                spellbook make-samples -n $(N_SAMPLES) -outfile=$(MERLIN_INFO)/samples.npy
        # can be a file glob of numpy sample files.
        file: $(MERLIN_INFO)/samples.npy
        column_labels: [X0, X1]
        level_max_dirs: 25
```