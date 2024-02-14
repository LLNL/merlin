# Port Your Own Application

!!! info "Prerequisites"

    - [2. Installation](./2_installation.md)
    - [3. Hello, World!](./3_hello_world.md)
    - [4. Run a Real Simulation](./4_run_simulation.md)

!!! info "Estimated Time"

    15 minutes

!!! abstract "You Will Learn"

    - Tips for building workflows
    - Tips for scaling
    - Debugging

## Tips For Porting Your App, Building Workflows

The first step of building a new workflow, or porting an existing app to a workflow, is to describe it as a set of discrete, and ideally focused steps. Decoupling the steps and making them generic when possible will facilitate more rapid composition of future workflows. This will also require mapping out the dependencies and parameters that get passed between/shared across these steps.

Setting up a template using tools such as [cookiecutter](https://cookiecutter.readthedocs.io/en/stable/) can be useful for more production style workflows that will be frequently reused.  Additionally, make use of the built-in examples accessible from the Merlin command line with `merlin example`.

Use dry runs `merlin run --dry --local` to prototype without actually populating task broker's queues. Similarly, once the dry run prototype looks good, try it on a small number of parameters before throwing millions at it.

Merlin inherits much of the input language and workflow specification philosophy from [Maestro](https://maestrowf.readthedocs.io/en/latest/). Thus a good first step is to learn to use that tool.
   
Make use of [exit keys](../user_guide/variables.md#step-return-variables) such as `MERLIN_RESTART` or `MERLIN_RETRY` in your step logic.

## Tips For Debugging Your Workflows

The scripts defined in the workflow steps are also written to the output directories; this is a useful debugging tool as it can both catch parameter and variable replacement errors, as well as provide a quick way to reproduce, edit, and retry the step offline before fixing the step in the workflow specification. The `<stepname>.out` and `<stepname>.err` files log all of the output to catch any runtime errors.

If you launch your workers using a bash script with an output file defined, or provide the `--logfile` argument to your workers, the logs from your workers will be sent to a log file. These logs will show the execution logs for every step in your workflow that the celery workers process. If you're having issues with your workflow, you may need to grep for `WARNING` and `ERROR` in the worker logs to identify the problem.

When a bug crops up in a running study with many parameters, there are a few other commands to make use of. Rather than trying to spam `Ctrl-c` to kill all the workers, you will want to instead use Merlin's built-in command to stop the workers for that workflow:

```bash
merlin stop-workers --spec <workflow_name>.yaml
```

This should then be followed up with Merlin's built-in command to clear out the task queue:

```bash
merlin purge <workflow_name>.yaml
```

This will prevent the same buggy tasks from continuing to run the next time `run-workers` is invoked.

## Tips For Scaling Workflows

Most of the worst bottlenecks you'll encounter when scaling up your workflow are caused by the file system. This can be caused by using too much space or too many files, even in a single workflow if you're not careful. There is a certain number of inodes created just based upon the sample counts even without accounting for the steps being executed. This can be mitigated by avoiding reading/writing to the file system when possible. If file creation is unavoidable, you may need to consider adding cleanup steps to your workflow: dynamically pack up the previous step in a tarball, transfer to another file system or archival system, or even just delete files.

## Misc Tips

Avoid reliance upon storage at the `$(SPECROOT)` level. This is particularly dangerous if using symlinks as it can violate the provenance of what was run, possibly ruining the utility of the dataset that was generated. It is preferred to make local copies of any input decks and supporting scripts and data sets inside the workflows' workspace. This of course has limits, regarding shared/system libraries that any programs running in the steps may need; alternate means of recording this information in a log file or something similar may be needed in this case.
