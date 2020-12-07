Workflow Specification
======================

The merlin input file or spec file is separated into several sections. An
annotated version is given below.

.. note:: The Merlin input file is a yaml file and must adhere to yaml
    syntax. The yaml spec relies on the indentation in the file.

The input file can take a number of variables, beyond the examples shown here.
For a complete list and descriptions of the variables,
see :doc:`./merlin_variables`.

.. code-block:: yaml

  ####################################
  # Description Block (Required)
  ####################################
  # The description block is where the description of the study is placed. This
  # section is meant primarily for documentation purposes so that when a
  # specification is passed to other users they can glean a general understanding
  # of what this study is meant to achieve.
  #-------------------------------
  # Required keys:
  #   name - Name of the study
  #   description - Description of what this study does.
  #-------------------------------
  # NOTE: You can add other keys to this block for custom documentation. Merlin
  # currently only looks for the required set.
  ####################################
  description:
    description: Run a scan through Merlin
    name: MERLIN

  ####################################
  # Batch Block (Required)
  ####################################
  # The batch system to use for each allocation
  #-------------------------------
  # Required keys:
  #   type - The scheduler type to use (local|slurm|flux|lsf)
  #   bank - The allocation bank
  #   queue - The batch queue
  ####################################
  batch:
     type: flux
     bank: testbank
     queue: pbatch
     flux_path: <optional path to flux bin>
     flux_start_opts: <optional flux start options>
     flux_exec_workers: <optional, flux argument to launch workers on 
                         all nodes. (True)> 
     launch_pre: <Any configuration needed before the srun or jsrun launch>
     launch_args: <Optional extra arguments for the parallel launch command>
     worker_launch: <Override the parallel launch defined in merlin>
     shell: <the interpreter to use for the script after the shebang>
            # e.g. /bin/bash, /bin/tcsh, python, /usr/bin/env perl, etc.
     nodes: <num nodes> # The number of nodes to use for all workers
                          This can be overridden in the workers config.
                          If this is unset the number of nodes will be
                          queried from the environment, failing that, the
                          number of nodes will be set to 1.
     walltime: The total walltime of the batch allocation (hh:mm:ss)


  #####################################
  # Environment Block
  ####################################
  # The environment block is where items describing the study's environment are
  # defined. This includes static information that the study needs to know about
  # and dependencies that the workflow requires for execution.
  #-------------------------------
  # NOTE: This block isn't strictly required as a study may not depend on anything.
  ########################################################################
  env:
    #-------------------------------
    # Variables
    #-------------------------------
    # Values that the workflow substitutes into steps and are similar in
    # concept to Unix environment variables. These variables are not dependent
    # on values in the environment and so are more portable.
    #
    # Note that variables defined here can alter the runtime shell
    # variable definitions. 
    # Do not define a variable named "shell" here.
    #-------------------------------
    variables:
      # Set a custom output path for the study workspace. This path is where
      # Merlin will place all temporary files, state files, and any output.
      # The resulting path is usually a timestamped folder within OUTPUT_PATH
      # and in this case would be
      # './sample_output/merlin/merlin_sample1_<timestamp>'.
      # NOTE: If not specified,
      # OUTPUT_PATH is assumed to be the path where Merlin was launched from.
      OUTPUT_PATH: ./sample_output/merlin # OUTPUT_PATH is a keyword
                                          # variable that Merlin looks for
                                          # to replace with the study
                                          # directory created for the
                                          # ensemble

  ####################################
  # Study Block (Required)
  ####################################
  # The study block is where the steps in the workflow are defined. This section
  # of the specification represents the unexpanded set of tasks that the study
  # is composed of.
  #
  #
  # A description of what gets turned into tasks and what type of task
  # would be a good addition
  #
  # study lists the various steps, each of which has these fields
  # name: step name
  # description: what the step does
  # run:
  #   cmd: the command to run for multilines use cmd: | 
  #        The $(LAUNCHER) macro can be used to substitute a parallel launcher 
  #        based on the batch:type:.
  #        It will use the nodes and procs values for the task.
  #   task_queue: the queue to assign the step to (optional. default: merlin)
  #   shell: the shell to use for the command (eg /bin/bash /usr/bin/env python)
  #          (optional. default: /bin/bash)
  #   depends: a list of steps this step depends upon (ie parents)
  #   procs: The total number of MPI tasks
  #   nodes: The total number of MPI nodes
  #   walltime: The total walltime of the run (hh:mm:ss) (not available in lsf)
  #   cores per task: The number of hardware threads per MPI task
  #   gpus per task: The number of GPUs per MPI task
  #   SLURM specific run flags:
  #   slurm: Verbatim flags only for the srun parallel launch (srun -n <nodes> -n <procs> <slurm>)
  #   FLUX specific run flags:
  #   flux: Verbatim flags for the flux parallel launch (flux mini run <flux>)
  #   LSF specific run flags:
  #   bind: Flag for MPI binding of tasks on a node
  #   num resource set: Number of resource sets
  #   launch_distribution : The distribution of resources (default: plane:{procs/nodes})
  #   exit_on_error: Flag to exit on error (default: 1)
  #   lsf: Verbatim flags only for the lsf parallel launch (jsrun ... <lsf>
  #######################################################################
   study:
    - name: runs1
      description: Run on alloc1
      run:
       cmd: $(LAUNCHER) echo "$(VAR1) $(VAR2)" > simrun.out
       nodes: 1
       procs: 1
       task_queue: queue1
       shell: /bin/bash

    - name: post-process
      description: Post-Process runs on alloc1
      run:
        cmd: |
          cd $(runs1.workspace)/$(MERLIN_SAMPLE_PATH)
          <post-process>
        nodes: 1
        procs: 1
        depends: [runs1]
        task_queue: queue1

    - name: runs2
      description: Run on alloc2
      run:
        cmd: |
          touch learnrun.out
          $(LAUNCHER) echo "$(VAR1) $(VAR2)" >> learnrun.out
          exit $(MERLIN_RETRY) # some syntax to send a retry error code
        nodes: 1
        procs: 1
        task_queue: lqueue
        batch:
          type: <override the default batch type>

    - name: monitor
      description: Monitor on alloc1
      run:
        cmd: date > monitor.out
        nodes: 1
        procs: 1
        task_queue: mqueue

  ####################################
  # Parameter Block (Required)
  ####################################
  # The parameter block contains all the things we'd like to vary in the study.
  # Currently, there are two modes of operating in the specification:
  # 1. If a parameter block is specified, the study is expanded and considered a
  #   parameterized study.
  # 2. If a parameter block is not specified, the study is treated as linear and
  #    the resulting study is not expanded.
  #
  # There are three keys per parameter:
  # 1. A list of values that the parameter takes.
  # 2. A label that represents a "pretty printed" version of the parameter. The
  #    parameter values is specified by the '%%' moniker (for example, for SIZE --
  #    when SIZE is equal to 10, the label will be 'SIZE.10'). To access the label
  #    for SIZE, for example, the token '$(SIZE.label)' is used.
  #    Labels can take one of two forms: A single string with the '%%' marker or
  #    a list of per value labels (must be the same length as the list of values).
  #
  # NOTE: A specified parameter does not necessarily have to be used in every step
  # or at all. If a parameter is specified and not used, it simply will not be
  # factored into expansion or the naming of expanded steps or their workspaces.
  # NOTE: You can also specify custom generation of parameters using a Python
  # file containing the definition of a function as follows:
  #
  # 'def get_custom_generator():'
  #
  # The 'get_custom_generator' function is required to return a ParameterGenerator
  # instance populated with custom filled values. In order to use the file, simply
  # call Merlin using 'merlin run <specification path>'.
  ########################################################################
  global.parameters:
    STUDY:
      label: STUDY.%%
      values: [MERLIN1, MERLIN2]
    SIZE:
       values  : [10, 20]
       label   : SIZE.%%
    ITERATIONS:
       values  : [10, 20]
       label   : ITER.%%

  ####################################
  # Merlin Block (Required)
  ####################################
  # The merlin specific block will add any required configuration to
  # the DAG created by the study description.
  # including task server config, data management and sample definitions.
  #
  # merlin will replace all SPECROOT instances with the directory where
  # the input yaml was run.
  #######################################################################
  merlin:

    ####################################
    # Resource definitions
    #
    # Define the task server configuration and workers to run the tasks.
    #
    ####################################
    resources:
      task_server: celery

      # Flag to determine if multiple workers can pull tasks
      # from overlapping queues. (default = False)
      overlap: False

      # Customize workers. Workers can have any user-defined name (e.g., simworkers, learnworkers).
      workers:
          simworkers:
              args: <celery worker args> <optional>
              steps: [runs1, post-process, monitor]  # [all] when steps is omitted
              nodes: <Number of nodes for this worker or batch num nodes>
              # A list of machines to run the given steps can be specified
              # in the machines keyword. <optional>
              # A full OUTPUT_PATH and the steps argument are required
              # when using this option. Currently all machines in the
              # list must have access to the OUTPUT_PATH. 
              machines: [host1, host2]

          learnworkers:
              args: <celery worker args> <optional>
              steps: [runs2]
              nodes: <Number of nodes for this worker or batch num nodes>
              # An optional batch section in the worker can override the
              # main batch config. This is useful if other workers are running
              # flux, but some component of the workflow requires the native
              # scheduler or cannot run under flux. Another possibility is to 
              # have the default type as local and workers needed for flux or
              # slurm steps.
              batch:
                 type: local
              machines: [host3]

    ###################################################
    # Sample definitions
    #
    # samples file can be one of
    #    .npy (numpy binary)
    #    .csv (comma delimited: '#' = comment line)
    #    .tab (tab/space delimited: '#' = comment line)
    ###################################################
    samples:
      column_labels: [VAR1, VAR2]
      file: $(SPECROOT)/samples.npy
      generate:
        cmd: |
        python $(SPECROOT)/make_samples.py -dims 2 -n 10 -outfile=$(INPUT_PATH)/samples.npy "[(1.3, 1.3, 'linear'), (3.3, 3.3, 'linear')]"
      level_max_dirs: 25
