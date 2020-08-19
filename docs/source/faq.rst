.. _faq:

FAQ
===
.. contents:: Frequently Asked Questions
  :local:

General
-------
What is Merlin?
~~~~~~~~~~~~~~~
Merlin is a distributed task queue system
designed to facilitate the large scale
execution of HPC ensembles, like those
needed to build machine learning models
of complex simulations.

Where can I get help with Merlin?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
In addition to this :doc:`documentation <./index>`,
the Merlin developers can be reached at
merlin@llnl.gov.
You can also reach out to the merlin user
group mailing list: merlin-users@listserv.llnl.gov.

Setup & Installation
--------------------

How can I build Merlin?
~~~~~~~~~~~~~~~~~~~~~~~
Merlin can be installed via
`pip <https://pypi.org/project/pip/>`_ in a python
:doc:`virtual environment <./virtualenv>`
or via :doc:`spack <./spack>`.

See :doc:`Getting started <./getting_started>`.

Do I have to build Merlin?
~~~~~~~~~~~~~~~~~~~~~~~~~~
If you're at LLNL and want to run on LC, you
can use one of the public deployments.
For more information, check out the LLNL access page
in confluence.

What are the setup instructions at LLNL?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
See "Do I have to build Merlin"

How do I reconfigure for different servers?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The server configuration is set in ``~/.merlin/app.yaml``.
Details can be found :doc:`here <./merlin_config>`.

Component Technology
--------------------
What underlying libraries does Merlin use?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* Celery
    * :ref:`what-is-celery`
* Maestro
    * :ref:`what-is-maestro`

What security features are in Merlin?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Merlin encrypts network traffic of step results,
implying that all results are encrypted with a
unique user-based key, which is auto-generated
and placed in ``~/.merlin/``. This allows
for multiple users to share a results database.
This is important since some backends, like
redis do not allow for multiple distinct users.

.. _what-is-celery:

What is celery?
~~~~~~~~~~~~~~~
Celery is an asynchronous task/job queue based on distributed message passing.
It is focused on real-time operation, but supports scheduling as well.
See `Celery's GitHub page
<https://github.com/celery/celery>`_
and `Celery's website
<http://www.celeryproject.org/>`_ for more details.

.. _what-is-maestro:

What is maestro?
~~~~~~~~~~~~~~~~
Maestro is a tool and library for specifying and conducting
general workflows.
See `Maestro's GitHub page
<https://github.com/LLNL/maestrowf>`_
for more details.

Designing and Building Workflows
--------------------------------
:doc:`yaml specification file <./merlin_specification>`

Where are some example workflows?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

   $ merlin example --help

How do I launch a workflow?
~~~~~~~~~~~~~~~~~~~~~~~~~~~
To launch a workflow locally, use ``merlin run --local <spec>``.
To launch a distributed workflow, use ``merlin run-workers <spec>``,
and ``merlin run <spec>``.
These may be done in any order.

How do I describe workflows in Merlin?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
A Merlin workflow is described with a :doc:`yaml specification file <./merlin_specification>`.

What is a DAG?
~~~~~~~~~~~~~~
DAG is an acronym for 'directed acyclic graph'.
This is the way your workflow steps are represented as tasks.

What if my workflow can't be described by a DAG?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
There are certain workflows that cannot be explicitly defined by a single DAG; however, in our experience, many can.
Furthermore, those workflows that cannot usually do employ DAG sub-components.
You probably can gain much of the functionality you want by combining a DAG with control logic return features (like step restart and additional calls to ``merlin run``).


How do I implement workflow looping / iteration?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Combining ``exit $(MERLIN_RETRY)`` with ``max_retries`` can allow you to loop a single step.
Entire workflow looping / iteration can be accomplished by finishing off your DAG with a final step that makes another call to ``merlin run``.


Can steps be restarted?
~~~~~~~~~~~~~~~~~~~~~~~
Yes. To build this into a workflow, use ``exit $(MERLIN_RETRY)`` within a step to retry a failed ``cmd`` section.
The max number of retries in given step can be specified with the ``max_retries`` field.

Alternatively, use ``exit $(MERLIN_RESTART)`` to run the optional ``<step>.run.restart`` section.

To restart failed steps after a workflow is done running, see :ref:`restart`.


How do I mark a step failure?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Each step is ultimately designated as:
* a success ``$(MERLIN_SUCCESS)`` -- writes a ``MERLIN_FINISHED`` file to the step's workspace directory
* a soft failure ``$(MERLIN_SOFT_FAIL)`` -- allows the workflow to continue
* a hard failure ``$(MERLIN_HARD_FAIL)`` -- stops the whole workflow by shutting down all workers on that step

Normally this happens behinds the scenes, so you don't need to worry about it.
To hard-code this into your step logic, use a shell command such as ``exit $(MERLIN_HARD_FAIL)``.

.. note:: ``$(MERLIN_HARD_FAIL)``
   The ``$(MERLIN_HARD_FAIL)`` exit code will shutdown all workers connected to the queue associated
   with the failed step. To shutdown *all* workers use the ``$(MERLIN_STOP_WORKERS)`` exit code

To rerun all failed steps in a workflow, see :ref:`restart`.
If you really want a previously successful step to be re-run, you can first manually remove the ``MERLIN_FINISHED`` file.


What fields can be added to steps?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Steps have a ``name``, ``description``, and ``run`` field, as shown below.

.. code:: yaml

    name: <string>
    description: <string>
    run:
        cmd: <shell command for this step>

Also under ``run``, the following fields are optional:

.. code:: yaml

    run:
        depends: <list of step names>
        task_queue: <task queue name for this step>
        shell: <e.g., /bin/bash, /usr/bin/env python3>
        max_retries: <integer>
        nodes: <integer>
        procs: <integer>

How do I specify the language used in a step?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
You can add the field ``shell`` under the ``run`` portion of your step
to change the language you write your step in. The default is ``/bin/bash``,
but you can do things like ``/usr/bin/env python`` as well.
Use ``merlin example feature_demo`` to see an example of this.

Running Workflows
-----------------

.. code:: bash

   $ merlin run <yaml file>

For more details, see :doc:`Merlin commands<./merlin_commands>`.

How do I set up a workspace without executing step scripts?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

   $ merlin run --dry <yaml file>

How do I start workers?
~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

   $ merlin run-workers <yaml file>

How do I see what workers are connected?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

   $ merlin query-workers

How do I stop workers?
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Interactively outside of a workflow (e.g. at the command line), you can do this with

.. code:: bash

   $ merlin stop-workers

This gives you fine control over which kinds of workers to stop, for instance via
a regex on their name, or the queue names you'd like to stop.

From within a step, you can exit with the ``$(MERLIN_STOP_WORKERS)`` code, which will
issue a time-delayed call to stop all of the workers, or with the ``$(MERLIN_HARD_FAIL)``
directive, which will stop all workers connected to the current step. This helps prevent
the *suicide race condition* where a worker could kill itself before removing the step
from the workflow, causing the command to be left there for the next worker and creating
a really bad loop.

You can of course call ``merlin stop-workers`` from within a step, but be careful to make
sure the worker executing it won't be stopped too.

For more tricks, see :ref:`stop-workers`.

.. _restart:

How do I re-run failed steps in a workflow?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

   $ merlin restart <spec>

What tasks are in my queue?
~~~~~~~~~~~~~~~~~~~~~~~~~~~

How do I purge tasks?
~~~~~~~~~~~~~~~~~~~~~

.. code:: bash

   $ merlin purge <yaml file>

Why is stuff still running after I purge?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
You probably have workers executing tasks. Purging
removes them from the server queue, but any currently
running or reserved tasks are being held by the workers.
You need to shut down these workers first:

.. code:: bash

   $ merlin stop-workers
   $ merlin purge <yaml file>

Why am I running old tasks?
~~~~~~~~~~~~~~~~~~~~~~~~~~~
You might have old tasks in your queues. Try ``merlin purge <yaml>``.
You might also have rogue workers. To find out, try ``merlin query-workers``.

Where do tasks get run?
~~~~~~~~~~~~~~~~~~~~~~~

Can I run different steps from my workflow on different machines?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Yes. Under the ``merlin`` block you can specify which machines your workers are allowed on.
In order for this to work, you must then use ``merlin run-workers`` separately on each of
the specified machines.

.. code:: yaml

   merlin:
      resources:
         workers:
            worker_name:
               machines: [hostA, hostB, hostC]

.. _slurm:

What is Slurm?
~~~~~~~~~~~~~~
A job scheduler. See `Slurm documentation
<https://slurm.schedmd.com/documentation.html>`_
.

.. _lsf:

What is LSF?
~~~~~~~~~~~~
Another job scheduler. See `IBM's LSF documentation
<https://www.ibm.com/support/knowledgecenter/en/SSWRJV_10.1.0/lsf_welcome/lsf_welcome.html>`_
.

.. _flux:

What is flux?
~~~~~~~~~~~~~
Flux is a hierarchical scheduler and launcher for parallel simulations. It allows the user
to specify the same launch command that will work on different HPC clusters with different 
default schedulers such as SLURM or LSF.
More information can be found at the `Flux web page <http://flux-framework.org/docs/home/>`_.

How do I use flux on LC?
~~~~~~~~~~~~~~~~~~~~~~~~
The ``--mpibind=off`` option is currently required when using flux with a slurm launcher
on LC toss3 systems. Set this in the batch section as shown in the example below.

.. code:: yaml

   batch:
     type: flux
     launch_args: --mpibind=off

What is ``LAUNCHER``?
~~~~~~~~~~~~~~~~~~~~~
``$LAUNCHER`` is a reserved word that may be used in a step command. It serves as an abstraction to launch a job with parallel schedulers like :ref:`slurm`, :ref:`lsf`, and :ref:`flux`.

How do I use ``LAUNCHER``?
~~~~~~~~~~~~~~~~~~~~~~~~~~
Instead of this:

.. code:: yaml

    run:
        cmd: srun -N 1 -n 3 python script.py

Do something like this:

.. code:: yaml

    batch:
        type: slurm

    run:
        cmd: $(LAUNCHER) python script.py
        nodes: 1
        procs: 3

The arguments the LAUNCHER syntax will use:

procs: The total number of MPI tasks
nodes: The total number of MPI nodes
walltime: The total walltime of the run (hh:mm:ss) (not available in lsf)
cores per task: The number of hardware threads per MPI task
gpus per task: The number of GPUs per MPI task

SLURM specific run flags:
slurm: Verbatim flags only for the srun parallel launch (srun -n <nodes> -n <procs> <slurm>)

FLUX specific run flags:
flux: Verbatim flags for the flux parallel launch (flux mini run <flux>)

LSF specific run flags:
bind: Flag for MPI binding of tasks on a node (default: -b rs)
num resource set: Number of resource sets
launch_distribution : The distribution of resources (default: plane:{procs/nodes})
lsf: Verbatim flags only for the lsf parallel launch (jsrun ... <lsf>)

What is level_max_dirs?
~~~~~~~~~~~~~~~~~~~~~~~
``level_max_dirs`` is an optional field that goes under the ``merlin.samples`` section
of a yaml spec. It caps the number of sample directories that can be generated
at a single level of a study's sample hierarchy. This is useful for getting around
filesystem constraints when working with massive amounts of data.

Defaults to 25.

What is pgen?
~~~~~~~~~~~~~
``pgen`` stands for "parameter generator". It's a way to override the parameters in the
``global.parameters`` spec section, instead generating them programatically with a python script.
Merlin offers the same pgen functionality as Maestro.

See `this guide <https://maestrowf.readthedocs.io/en/latest/parameters.html#parameter-generator-pgen>`_ for details on using ``pgen``.
It's a Maestro doc, but the exact same flags can be used in conjunction with ``merlin run``.

Where can I learn more about merlin?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Check out `our paper <https://arxiv.org/abs/1912.02892>`_ on arXiv.
