Advanced Topics
===============
.. admonition:: Prerequisites

      * :doc:`Module 2: Installation<installation>`
      * :doc:`Module 3: Hello World<hello_world/hello_world>`
      * :doc:`Module 4: Running a Real Simulation<run_simulation>`

.. admonition:: Estimated time

      * 15 minutes

.. admonition:: You will learn

      * Run workflows using HPC batch schedulers
      * Distribute workflows across multiple batch allocations and machines
      * Spawn new workflows and choose study parameters/samples based upon already executed studies

Interfacing with HPC systems
++++++++++++++++++++++++++++

Another block is added to the merlin workflow specification when running on HPC systems,
the `batch` block.  This block contains information about the batch scheduler system such
as batch type, batch queue to use, and banks to charge to.  There are additional optional
arguments for addressing any special configurations or launch command arguments, varying
based on batch type.  In addition, the shell type used by each steps `cmd` scripts can
be specified here.  The number of nodes in a batch allocation can be defined here, but it
will be overridden in the worker config.

.. code-block:: yaml

   batch:
      # Required keys:
   
      type: flux
      bank: testbank
      queue: pbatch

      # Optional keys:   
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


NOTE FOR CODE MEETING: what is this data management comment in the merlin block header
in the commented specification file?

NOTE FOR CODE MEETING: why not use task queue name in worker resources blocks instead
of step names -> nominally seem to be different terms for the intended functionality

NOTE FOR ME: test out monitor step type from examnple spec -> anything interesting
to do here? can this be started first on the command line to enable an actual monitor
process?

Inside the study step specifications are a few additional keys that become more useful
on HPC systems: nodes, procs, and task_queue.  Adding on the actual study steps to the
above batch block specifies the actual resources each steps processes will take.

.. code-block:: yaml
                
   study:
      - name: sim-runs
        description: Run sumulations
        run:
           cmd: $(LAUNCHER) echo "$(VAR1) $(VAR2)" > simrun.out
           nodes: 4
           procs: 144
           task_queue: sim_queue
  
      - name: post-process
        description: Post-Process simulations on second allocation
        run:
           cmd: |
             cd $(runs1.workspace)/$(MERLIN_SAMPLE_PATH)
             $(LAUNCHER) <parallel-post-proc-script>
           nodes: 1
           procs: 36
           depends: [sim-runs]
           task_queue: post_proc_queue

NOTE FOR ME TO TRY: run various post proc scripts, both with concurrent futures
and mpi4py executors to demo the different calls -> $(LAUNCHER) likely not appropriate here

In addition to the `batch` block is the `resources` section inside the `merlin` block.
This can be used to put together custom celery workers.  Here you can override batch
types and node counts on a per worker basis to accomodate steps with different
resource requirements.  In addition, this is where the `task_queue` becomes useful, as
it groups the different allocaiton types, which can be assigned to each worker here
by specifying step names (why not specify queue instead of step names here?).

.. code-block::yaml

  merlin:

    resources:
      task_server: celery

      # Flag to determine if multiple workers can pull tasks
      # from overlapping queues. (default = False)
      overlap: False

      # Customize workers. Workers can have any user-defined name
      #  (e.g., simworkers, learnworkers, ...)
      workers:
          simworkers:
              args: <celery worker args> # <optional>
              steps: [sim-runs]          # <optional> [all] if none specified
              nodes: 4                   # optional
              machines: [host1]          # <optional>

Arguments to celery itself can also be defined here with the `args` key.  Of particular
interest will be:

=========================  =============
`-\\-concurrency`          <num_threads>
                           
`-\\-prefetch-multiplier`  <num_tasks>
                           
`-0`                       fair
=========================  =============

Concurrency can be used to run multiple workers in an allocation, thus is recommended to be
set to the number of simulations or step work items that fit into the number of nodes in the
batch allocation in which these workers are spawned.

The prefetch multiplier is more related to packing in tasks into the time of the allocation.
For long running tasks it is recommended to set this to 1.  For short running tasks, this
can reduce overhead from talking to the rabbit servers by requesting <num_threads>x<num_tasks>
tasks at a time from the server.

The `-0 fair` option enables workers running tasks from different queues to run on the same
allocation.

The example block below extends the previous with  workers configured for long running
simulation jobs as well as shorter running post processing tasks that can cohabit an allocation

NOTE: verify this is how the celery args work -> docs show raw celery commands, not yaml spec!!

.. code-block:: yaml
                
  merlin:

    resources:
      task_server: celery

      overlap: False

      # Customize workers
      workers:
          simworkers:
              args: --concurrency 1
              steps: [sim-runs]      
              nodes: 4               
              machines: [host1]      

          postworkers:
              args: --concurrency 4 --prefetch-multiplier 2
              steps: [post-proc-runs]
              nodes: 1               
              machines: [host1]      

              
NOTE FOR CODE MEETING/ME TO TRY: nodes, either in batch or workers, behaves differently from
maestro, meaning it's meant to be nodes per step instantiation, not batch allocation size..

NOTE FOR CODE MEETING: clarify what overlap key does if turned on.  Just multiple named workers
pulling from same queues?  is this a requirement for making it work cross machine?
Also: what about procs per worker instead of just nodes?

Putting it all together with the parameter blocks we have an HPC batch enabled study specification

.. code-block:: yaml

   description:
      name: Sample HPC specification
      description: demo batch system and multiple worker configs for HPC workflows
 
   batch:
      type: flux
      bank: testbank
      queue: pbatch
      shell: /bin/bash
      nodes: 1

   ########################################
   # Study definition
   ########################################
   study:
      - name: sim-runs
        description: Run sumulations
        run:
           cmd: $(LAUNCHER) echo "$(VAR1) $(VAR2)" > simrun.out
           nodes: 4
           procs: 144
           task_queue: sim_queue
  
      - name: post-process
        description: Post-Process simulations on second allocation
        run:
           cmd: |
             cd $(runs1.workspace)/$(MERLIN_SAMPLE_PATH)
             $(LAUNCHER) <parallel-post-proc-script>
           nodes: 1
           procs: 36
           depends: [sim-runs]
           task_queue: post_proc_queue
           
   ########################################
   # Worker and sample configuration
   ########################################  
   merlin:
  
     resources:
       task_server: celery
  
       overlap: False
  
       # Customize workers
       workers:
           simworkers:
               args: --concurrency 1
               steps: [sim-runs]      
               nodes: 4               
               machines: [host1]      
  
           postworkers:
               args: --concurrency 4 --prefetch-multiplier 2
               steps: [post-proc-runs]
               nodes: 1               
               machines: [host1]
  
     ###################################################
     samples:
       column_labels: [VAR1, VAR2]
       file: $(SPECROOT)/samples.npy
       generate:
         cmd: |
         python $(SPECROOT)/make_samples.py -dims 2 -n 10 -outfile=$(INPUT_PATH)/samples.npy "[(1.3, 1.3, 'linear'), (3.3, 3.3, 'linear')]"

NOTE FOR ME: replace samples/step cmds with something else that's more interesting
maybe use faker and use post-process to look at statistics of the names generated off of
10k samples or something? -> could extend it to multiple sample counts, scaling up until
repeats start showing up to estimate total number of names in the dict it uses?
Also could do something with monte carlo methods or fractals?

The actual invocation of this workflow can be handled multiple ways: manually launch batch
allocations before starting workers, or use Maestro to automate everything:

...

NOTES: encode virtual envs in the spec/workflow: only the first call to merlin run will
get the host venv, subsequent ones

RECURSIVE WORKFLOWS: if exit condition isn't working, terminating workers can be difficult
- have another shell open at least to purge the queues and stop the workers

When running new workflows, be careful with the path: otherwise it will run it in that step
Can info message spam be reduced?  -> nice to see just the echo/print output in the commands...

Multi-machine workflows
+++++++++++++++++++++++

Spreading this workflow across multiple machines is a simple modification of the above workflow:
simply add additional host names to machines list in the worker config.  The caveats for this
distribution is that all systems will need to have access to the same workspace/filesystem, as
well as use the same scheduler types (VERIFY THIS).  The following resource block demonstrates
using one host for larger simulation steps, and a second host for the smaller post processing
steps.

.. code-block::yaml

   ########################################
   # Worker and sample configuration
   ########################################  
   merlin:
  
     resources:
       task_server: celery
  
       overlap: False
  
       # Customize workers
       workers:
           simworkers:
               args: --concurrency 1
               steps: [sim-runs]      
               nodes: 4               
               machines: [host1]      
  
           postworkers:
               args: --concurrency 4 --prefetch-multiplier 2
               steps: [post-proc-runs]
               nodes: 1               
               machines: [host2]


Dynamic task queueing and sampling
++++++++++++++++++++++++++++++++++

Plan:
 - using recursion, call merlin run again
 - also spawn additional batch allocations with maestro
 - key feature: use maestro param to control iterations
   - each recursive call uses pgen to subtract one from it

Updated plan:
 - have post step call merlin run again
 - workers shoudl continue pulling from the same queues
 - verify that run-workers doesn't need to be called again in the first merlin spec
 - use restart functionality on the maestro spec to reopen batch allocations
 - use of exit keys to control the logic?
 - use command line variable overriding to control iterations, not maestro params
   
