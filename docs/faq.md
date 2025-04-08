---
hide:
  - navigation
---

# Frequently Asked Questions

## General

### What is Merlin?

Merlin is a distributed task queue system designed to facilitate the large-scale execution of HPC ensembles, like those needed to build machine learning models of complex simulations.

Read more on what Merlin is and how Merlin works at the [User Guide](./user_guide/index.md).

### Where can I get help with Merlin?

In addition to this [documentation](./index.md), you can reach the Merlin developers by email, on Microsoft Teams, or via GitHub. See the [Contact](./contact.md) page for more information.

### Where can I learn more about Merlin?

Check out the rest of the [Merlin Documentation](./index.md) and [our paper on arXiv](https://arxiv.org/abs/1912.02892).

## Setup & Installation

### How can I build Merlin?

Merlin can be installed via [pip in a python virtual environment](./user_guide/installation.md#installing-with-virtual-environments-pip) or via [spack](./user_guide/installation.md#installing-with-spack).

See the [Installation](./user_guide/installation.md) page for full installation instructions.

### Do I have to build Merlin?

If you're at LLNL and want to run on LC, you can use the WEAVE team's common environment to run Merlin. For more information, see [WEAVE's Common Environment Docs](https://lc.llnl.gov/weave/environment.html).

### What are the setup instructions at LLNL?

See *[Do I have to build Merlin?](#do-i-have-to-build-merlin)* above or visit the [Installation](./user_guide/installation.md) and [Configuration](./user_guide/configuration/index.md) pages of the [User Guide](./user_guide/index.md).

### How do I reconfigure for different servers?

The server configuration is set in `~/.merlin/app.yaml`. Details can be found at the [Configuration](./user_guide/configuration/index.md) page, specifically at [Configuring the Broker and Results Backend](./user_guide/configuration/index.md#configuring-the-broker-and-results-backend).

## Component Technology

### What underlying libraries does Merlin use?

- Celery: see *[What is Celery?](#what-is-celery)* below for more information
- Maestro: see *[What is Maestro?](#what-is-maestro)* below for more information

### What security features are in Merlin?

Merlin encrypts network traffic of step results, implying that all results are encrypted with a unique user-based key, which is auto-generated and placed in the `~/.merlin/` directory. This allows for multiple users to share a results database. This is important since some backends, like Redis do not allow for multiple distinct users.

### What is Celery?

Celery is an asynchronous task/job queue based on distributed message passing. It is focused on real-time operation, but supports scheduling as well. See [Celery's GitHub page](https://github.com/celery/celery) and [Celery's documentation](http://www.celeryproject.org/) for more details.

### What is Maestro?

Maestro is a tool and library for specifying and conducting general workflows. See [Maestro's GitHub page](https://github.com/LLNL/maestrowf) and [Maestro's documentation](https://maestrowf.readthedocs.io/en/latest/index.html) for more details.

## Designing and Building Workflows

Most of these questions can be answered by reading through the docs on [The Specification File](./user_guide/specification.md) and/or by running through the [Examples](./examples/index.md) that Merlin provides.

### Where are some example workflows?

You can see all of Merlin's built-in example workflows with:

```bash
merlin example list
```

See the docs on these [Examples](./examples/index.md) for more information.

### How do I launch a workflow?

To launch a workflow locally, use the [`merlin run`](./user_guide/command_line.md#run-merlin-run):

```bash
merlin run --local <spec>
``` 

To launch a distributed workflow, first visit the [Configuration](./user_guide/configuration/index.md) page and configure your broker and results servers. Then use both the [`merlin run`](./user_guide/command_line.md#run-merlin-run) and the [`merlin run-workers`](./user_guide/command_line.md#run-workers-merlin-run-workers) commands in any order you choose:

=== "Run the Workers"

    ```bash
    merlin run-workers <spec>
    ```

=== "Launch the Tasks"

    ```bash
    merlin run <spec>
    ```

### How do I describe workflows in Merlin?

A Merlin workflow is described with [The Specification File](./user_guide/specification.md).

### What is a DAG?

DAG is an acronym for 'directed acyclic graph'. This is the way your workflow steps are represented as tasks.

### What if my workflow can't be described by a DAG?

There are certain workflows that cannot be explicitly defined by a single DAG; however, in our experience, many can. Furthermore, those workflows that cannot usually do employ DAG sub-components. You probably can gain much of the functionality you want by combining a DAG with control logic return features (like step restart and additional calls to [`merlin run`](./user_guide/command_line.md#run-merlin-run)).

### How do I implement workflow looping/iteration?

**Single Step Looping:**

Combining `exit $(MERLIN_RETRY)` with `max_retries` can allow you to loop a single step.

**Entire Workflow Looping/Iteration:**

Entire workflow looping/iteration can be accomplished by finishing off your DAG with a final step that makes another call to [`merlin run`](./user_guide/command_line.md#run-merlin-run). See the [Iterative Demo](./examples/iterative.md) for a detailed example of an iterative workflow:

```bash
merlin example iterative_demo
```

### Can steps be restarted?

Yes. To build this into a workflow, use `exit $(MERLIN_RETRY)` within a step to retry a failed `cmd` section. The max number of retries in a given step can be specified with the `max_retries` field.

Alternatively, use `exit $(MERLIN_RESTART)` to run the optional `<step>.run.restart` section.

To delay a retry or restart directive, add the `retry_delay` field to the step.

!!! note

    `retry_delay` only works in server mode (ie not `--local` mode).

To restart failed steps after a workflow is done running, see *[How do I re-rerun failed steps in a workflow?](#how-do-i-re-run-failed-steps-in-a-workflow)*

### How do I put a time delay in before a restart or retry?

Add the `retry_delay` field to the step. This specifies how many seconds before the task gets run after the restart. Set this value to large enough for your problem to finish.

See the [restart_delay example](./examples/restart.md) for syntax:

```bash
merlin example restart_delay
```

!!! note

    `retry_delay` only works in server mode (ie not `--local` mode).

### I have a long running batch task that needs to restart, what should I do?

Before your allocation ends, use `$(MERLIN_RESTART)` or `$(MERLIN_RETRY)` but with a `retry_delay` on your step for longer than your allocation has left. The server will hold onto the step for that long (in seconds) before releasing it, allowing your batch allocation to end without the worker grabbing the step right away.

!!! example

    Here's an example study step that terminates a long running task 1 minute before the allocation ends, then tells Merlin to wait a full 2 minutes before retrying this step. The allocation will then end prior to the step being restarted.

    ```yaml
    study:
        - name: batch_task
          description: A long running task that needs to restart
          run:
            cmd: |
                # Run my code, but end 60 seconds before my allocation
                my_code --end_early 60s
                if [ -e restart_needed_flag ]; then
                    exit $(MERLIN_RESTART)
                fi
            retry_delay: 120 # wait at least 2 minutes before restarting 
    ```

### How do I mark a step failure?

Each step is ultimately designated as:

- a success `$(MERLIN_SUCCESS)` -- writes a `MERLIN_FINISHED` file to the step's workspace directory
- a soft failure `$(MERLIN_SOFT_FAIL)` -- allows the workflow to continue
- a hard failure `$(MERLIN_HARD_FAIL)` -- stops the whole workflow by shutting down all workers on that step

Normally this happens behinds the scenes, so you don't need to worry about it. To hard-code this into your step logic, use a shell command such as `exit $(MERLIN_HARD_FAIL)`.

!!! note

    The `$(MERLIN_HARD_FAIL)` exit code will shutdown all workers connected to the queue associated with the failed step. To shutdown *all* workers use the `$(MERLIN_STOP_WORKERS)` exit code.

To rerun all failed steps in a workflow, see *[How do I re-rerun failed steps in a workflow?](#how-do-i-re-run-failed-steps-in-a-workflow)* If you really want a previously successful step to be re-run, you can manually remove the `MERLIN_FINISHED` file prior to running [`merlin restart`](./user_guide/command_line.md#restart-merlin-restart).

### What fields can be added to steps?

Steps have a `name`, `description`, and `run` field, as shown below.

```yaml
study:
    - name: <string>
      description: <string>
      run:
        cmd: <shell command for this step>
```

Also under `run`, the following fields are optional:

```yaml
run:
    depends: <list of step names>
    task_queue: <task queue name for this step>
    shell: <e.g., /bin/bash, /usr/bin/env python3>
    max_retries: <integer>
    retry_delay: <integer (representing seconds)>
    nodes: <integer>
    procs: <integer>
```

For more details on the options that can be used for steps, see [The Specification File](./user_guide/specification.md).

### How do I specify the language used in a step?

You can add the field `shell` under the `run` portion of your step to change the language you write your step in. The default is `/bin/bash`, but you can do things like `/usr/bin/env python` as well.

Use the [feature_demo example](./examples/feature_demo.md) to see an example of this:

```bash
merlin example feature_demo
```

## Running Workflows

Workflows can be ran with the [`merlin run`](./user_guide/command_line.md#run-merlin-run) and the [`merlin run-workers`](./user_guide/command_line.md#run-workers-merlin-run-workers) commands.

=== "Send Tasks to the Broker"

    ```bash
    merlin run <spec>
    ```

=== "Start Workers to Execute the Tasks"

    ```bash
    merlin run-workers <spec>
    ```

See the docs on all [Merlin Commands](./user_guide/command_line.md) that are available for more info on what Merlin is capable of.

### How do I set up a workspace without executing step scripts?

Use [Merlin's Dry Run](./user_guide/running_studies.md#dry-runs) capability:

=== "Locally"

    ```bash
    merlin run --local --dry <input.yaml>
    ```

=== "Distributed"

    ```bash
    merlin run --dry <input.yaml> ; merlin run-workers <input.yaml>
    ```

### How do I start workers?

Ensure you have a stable connection to your broker and results servers with the [`merlin info`](./user_guide/command_line.md#info-merlin-info) command:

```bash
merlin info
```

This should show an "OK" message next to both servers. If instead you see "ERROR" next to either server, visit the [Configuring the Broker and Results Backend](./user_guide/configuration/index.md#configuring-the-broker-and-results-backend) documentation to get your servers set up properly.

Once connection to your servers is established, use the [`merlin run-workers`](./user_guide/command_line.md#run-workers-merlin-run-workers) command to start your workers:

```bash
merlin run-workers <spec>
```

### How do I see what workers are connected?

You can query which workers are active with the [`merlin query-workers`](./user_guide/command_line.md#query-workers-merlin-query-workers) command:

```bash
merlin query-workers
```

This command gives you fine control over which workers you're looking for via a regex on their name, the queue names associated with workers, or even by providing the name of a spec file where workers are defined.

### How do I stop workers?

**Interactively:**

Interactively outside of a workflow (e.g. at the command line), you can stop workers with the [`merlin stop-workers`](./user_guide/command_line.md#stop-workers-merlin-stop-workers) command:

```bash
merlin stop-workers
```

This gives you fine control over which kinds of workers to stop, for instance via a regex on their name, or the queue names you'd like to stop.

**Within a step:**

From within a step, you can exit with the `$(MERLIN_STOP_WORKERS)` code, which will issue a time-delayed call to stop all of the workers, or with the `$(MERLIN_HARD_FAIL)` directive, which will stop all workers connected to the current step. This helps prevent the *suicide race condition* where a worker could kill itself before removing the step from the workflow, causing the command to be left there for the next worker and creating a really bad loop.

You can of course call `merlin stop-workers` from within a step, but be careful to make sure the worker executing it won't be stopped too.

### How do I re-run failed steps in a workflow?

Workflows can be restarted with the [`merlin restart`](./user_guide/command_line.md#restart-merlin-restart) command:

```bash
merlin restart <workspace>
```

This will only re-run steps in your workflow that do not contain a `MERLIN_FINISHED` file in their output directory.

### What tasks are in my queue?

Tasks are created by Merlin as it processes the [DAG](#what-is-a-dag) that is defined by the steps in your workflow. For each step, Merlin will create multiple [Celery tasks](https://docs.celeryq.dev/en/stable/userguide/tasks.html) in order to accomplish what you've defined. These tasks are what are sent to the queue on your broker and eventually executed by the workers that you spin up.

### How do I purge tasks?

!!! warning

    Tasks that are currently being executed by workers will *not* be purged with the `merlin purge` command.

    It's best to use the [`merlin stop-workers`](./user_guide/command_line.md#stop-workers-merlin-stop-workers) command prior to running `merlin purge`.

To remove tasks from your queue, you can use the [`merlin purge`](./user_guide/command_line.md#purge-merlin-purge) command:

```bash
merlin purge <spec>
```

### Why is stuff still running after I purge?

You probably have workers executing tasks. Purging removes them from the server queue, but any currently running or reserved tasks are being held by the workers. You need to shut down these workers first with the [`merlin stop-workers`](./user_guide/command_line.md#stop-workers-merlin-stop-workers) command:

```bash
merlin stop-workers
```

...and then run the [`merlin purge`](./user_guide/command_line.md#purge-merlin-purge) command:

```bash
merlin purge <spec>
```

### Why am I running old tasks?

You might have old tasks in your queues. Try [purging your queues](#how-do-i-purge-tasks).

You might also have rogue workers. Try [checking which workers are connected](#how-do-i-see-what-workers-are-connected).

### Where do tasks get run?

When you [spin up workers](#how-do-i-start-workers) with Merlin, these worker processes will live on the node(s) in your allocation. When these workers pull tasks from your broker's queue, the tasks will be executed in the worker processes. Therefore, the tasks are ran on the node(s) in your allocation.

### Can I run different steps from my workflow on different machines?

Yes. Under the [`merlin` block](./user_guide/specification.md#the-merlin-block) you can specify which machines your workers are allowed on. In order for this to work, you must then use [`merlin run-workers`](./user_guide/command_line.md#run-workers-merlin-run-workers) separately on each of the specified machines.

```yaml
merlin:
    resources:
        workers:
            worker_name:
                machines: [hostA, hostB, hostC]
```

### What is Slurm?

A job scheduler. See the [Slurm documentation](https://slurm.schedmd.com/documentation.html) for more info.

### What is LSF?

A job scheduler. See [IBM's LSF documentation](https://www.ibm.com/support/knowledgecenter/en/SSWRJV_10.1.0/lsf_welcome/lsf_welcome.html) for more info.

### What is Flux?

Flux is a hierarchical scheduler and launcher for parallel simulations. It allows the user to specify the same launch command that will work on different HPC clusters with different default schedulers such as [Slurm](#what-is-slurm) or [LSF](#what-is-lsf). Merlin versions earlier than 1.9.2 used the non-Flux native scheduler to launch a Flux instance. Subsequent Merlin versions can launch the Merlin workers using a native Flux scheduler.

More information can be found at the [Flux web page](http://flux-framework.org/docs/home/).

Older versions of Flux may need the `--mpi=none` argument if Flux is launched on a system using the Slurm scheduler. This argument can be added in the `launch_args` variable in the [`batch` block](./user_guide/specification.md#the-batch-block) of your spec file.

!!! example

    ```yaml
    batch:
        type: flux
        launch_args: --mpi=none
    ```

### What is PBS?

!!! note

    The PBS functionality is only available to launch a [Flux scheduler](#what-is-flux).

A job scheduler. See [Portable Batch System](https://en.wikipedia.org/wiki/Portable_Batch_System) for more info.

### How do I use Flux on LC?

The `--mpibind=off` option is currently required when using Flux with a Slurm launcher on LC toss3 systems. Set this in the [`batch` block](./user_guide/specification.md#the-batch-block) of your spec as shown in the example below.

!!! example

    ```yaml
    batch:
        type: flux
        launch_args: --mpibind=off
    ```

### What is `LAUNCHER`?

`LAUNCHER` is a reserved variable that may be used in a step command. It serves as an abstraction to launch a job with parallel schedulers like [Slurm](#what-is-slurm), [LSF](#what-is-lsf), and [Flux](#what-is-flux).

See [The `LAUNCHER` and `VLAUNCHER` Variables](./user_guide/variables.md#the-launcher-and-vlauncher-variables) section for more information.

### How do I use `LAUNCHER`?

Instead of this:

```yaml
run:
    cmd: srun -N 1 -n 3 python script.py
```

Do something like this:

```yaml
batch:
    type: slurm

run:
    cmd: $(LAUNCHER) python script.py
    nodes: 1
    procs: 3
```

See [The `LAUNCHER` and `VLAUNCHER` Variables](./user_guide/variables.md#the-launcher-and-vlauncher-variables) and [The Run Property](./user_guide/specification.md#the-run-property) sections for more information.

### What is `level_max_dirs`?

`level_max_dirs` is an optional field that goes under the `merlin.samples` section of a yaml spec. It caps the number of sample directories that can be generated at a single level of a study's sample hierarchy. This is useful for getting around filesystem constraints when working with massive amounts of data.

Defaults to 25.

### What is `pgen`?

`pgen` stands for "parameter generator". It's a way to override the parameters in the [`global.parameters` block](./user_guide/specification.md#the-globalparameters-block) of the spec, instead generating them programatically with a python script. Merlin offers the same pgen functionality as [Maestro](https://maestrowf.readthedocs.io/en/latest/).

See [Maestro's pgen guide](https://maestrowf.readthedocs.io/en/latest/Maestro/parameter_specification.html#parameter-generator-pgen) for details on using `pgen`. It's a Maestro doc, but the exact same flags can be used in conjunction with [`merlin run`](./user_guide/command_line.md#run-merlin-run).
