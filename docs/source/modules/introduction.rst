Introduction
============
This module introduces you to Merlin, some of the technology behind it,
and how it works.

.. admonition:: Prerequisites

      * Curiosity

.. admonition:: Estimated time

      * 20 minutes

.. admonition:: You will learn

      * What Merlin is and why you might consider it
      * Why it was built and what are some target use cases
      * How it is designed and what the underlying tech is

.. contents:: Table of Contents:
  :local:

What is Merlin?
+++++++++++++++

.. admonition:: Summary

    Merlin is a toolkit designed to enable HPC-focused simulation workflows
    with distributed cloud compute technologies. This helps simulation workflows
    push to immense scale. (Like `100 million`__.)

__ https://arxiv.org/abs/1912.02892

At its core, Merlin translates a text-based, command-line focused workflow
description into a set of discrete tasks. These tasks live on a centralized
broker (e.g. a separate server) that persists outside of your HPC
batch allocation. Autonomous workers in different allocations (even
on different machines) can then connect
to this server, pull off and execute these tasks asynchronously.

Why Merlin? What's the need?
++++++++++++++++++++++++++++

That sounds complicated. Why would you care to do this?

The short answer: machine learning

The longer answer: machine learning and data science are becoming
an integral part of scientific inquiry. The problem is that machine learning
models are data hungry: it takes lots and lots of simulations to train machine
learning models on their outputs. Unfortunately HPC systems were designed to execute
a few large hero simulations, not many smaller simulations. Naively pushing
standard HPC workflow tools to hundreds of thousands and millions of simulations
can lead to some serious problems.

Workflows, applications and machines are becoming more complex, but
subject matter experts need to devote time and attention to their applications
and often require fine command-line level control. Furthermore,
they rarely have the time to devote to learning workflow systems.

With the expansion of data-driven computing, the HPC scientist needs to be able
to run more simulations through complex multi-component workflows.

**Merlin targets HPC workflows that require many simulations**. These include:


.. list-table:: Merlin Targeted Use Cases
  :widths: 25 75

  * - Emulator building
    - Running enough simulations to build an emulator (or "surrogate model")
      of an expensive computer code, such as needed for uncertainty quantification
  * - Iterative sampling
    - Executing some simulations and then choosing new ones to run
      based on the results obtained thus far
  * - Active learning
    - Iteratively sampling coupled with emulator building to efficiently train
      a machine learning model
  * - Design optimization
    - Using a computer code to optimize a model design, perhaps robustly or under
      uncertainty
  * - Reinforcement learning
    - Building a machine learning model by subsequently exposing it to lots of
      trials, giving it a reward/penalty for the outcomes of those trials
  * - Hierarchical simulation
    - Running low-fidelity simulations to inform which higher fidelity simulations
      to execute
  * - Heterogeneous workflows
    - Workflows that require different steps to execute on different hardware and/or
      systems

Many scientific and engineering problems require running lots of simulations.
But accomplishing these tasks
effectively in an unstable bleeding edge HPC environment can be dicey. The tricks
that work for 100 simulations won't work for
`10 thousand <https://doi.org/10.1063/1.4977912>`_, let alone
`100 million <https://arxiv.org/abs/1912.02892>`_.

We made Merlin to make high-frequency extreme scale computing easy.


How can Merlin run so many simulations?
+++++++++++++++++++++++++++++++++++++++

The good news is that distributed cloud compute technology has really pushed the
frontier of scalability. Merlin helps bring this tech to traditional scientific HPC.

Traditionally, HPC workflow systems tie workflow steps to HPC resources and
coordinate the execution of tasks and management of resources one of two ways:

.. |ext-img| image:: ../../images/external_coordination.png


.. |int-img| image:: ../../images/internal_coordination.png

.. table:: Traditional HPC Workflow Philosophies

   +------------------------------+-------------------------------------------------------+
   | External Coordination        + - Separate batch jobs for each task                   |
   | |ext-img|                    + - External daemon tracks dependencies and jobs        |
   |                              + - Progress monitored with periodic polling            |
   |                              +   (of files or batch system)                          |
   +------------------------------+-------------------------------------------------------+
   | Internal Coordination        + - Multiple tasks bundled into larger batch jobs       |
   | |int-img|                    + - Internal daemon tracks dependencies and resources   |
   |                              + - Progress monitored via polling                      |
   |                              +   (of filesystem or message passing)                  |
   +------------------------------+-------------------------------------------------------+



**External coordination** ties together independent batch jobs each executing workflow
sub-tasks with an external monitor. This monitor could be a daemon
or human that monitors either the batch or file system via periodic polling and orchestrates task launch dependencies.

External coordination can tailor the resources to the task, but cannot easily
run lots of concurrent simulations (since batch systems usually limit the number
of jobs a user can queue at once).

**Internal coordination** puts the monitor within a larger batch job that allocates
resources inside that job for the specific tasks at hand.

Internal coordination can run many more
concurrent tasks by bundling smaller jobs into larger jobs, but cannot tailor the
resources to the task at hand. This precludes workflows that, for instance, require
one step on CPU hardware and another on a GPU machine.

Instead of tying resources to tasks, Merlin does this:


.. |cent-img| image:: ../../images/central_coordination.png

.. table:: Merlin's Workflow Philosophy


   +------------------------------+-----------------------------------------------+
   + Centralized Coordination     + - Batch jobs and workers decoupled from tasks +
   + of Producers & Consumers     + - Centralized queues visible to multiple jobs +
   + |cent-img|                   + - Progress and dependencies handled via       +
   +                              +   direct worker connections to central        +
   +                              +   message server and results database         +
   +------------------------------+-----------------------------------------------+

Merlin decouples workflow tasks from workflow resources.

Merlin avoids a command-and-control approach to HPC resource
management for a workflow. Instead of having the workflow coordinator
ask for and manage HPC resources and tasks, the Merlin coordinator just manages
tasks. Task-agnostic resources can then independently connect (and
disconnect) to the coordinator.

In Merlin, this **producer-consumer workflow** happens through two commands:

``merlin run <workflow file>`` (producer)

and

``merlin run-worker <workflow file>`` (consumer).

The ``merlin run`` command populates the central queue(s) with work to do
and the ``merlin run-worker`` command drains the queue(s) by executing the
task instructions. Each new instance of ``merlin run-worker`` creates a new
consumer. These consumers can exist on different machines in different
batch allocations, anywhere that can see the central server. Likewise
``merlin run`` can populate the queue from any system that can see the
queue server, including other workers. In principle, this means a
researcher can push new work onto an already running batch allocation of workers,
or re-direct running jobs to work on higher-priority work.

.. admonition:: The benefits of producer-consumer workflows

   The increased flexibility that comes from
   decoupling *what* HPC applications you run from *where* you run them
   can be extremely enabling.

   Merlin allows you to

   * Scale to very large number of simulations by avoiding common HPC bottlenecks
   * Automatically take advantage of free nodes to process your workflow faster
   * Create iterative workflows, like as needed for active machine learning
   * Dynamically add more tasks to already-running jobs
   * Have cross-machine and cross-batch-job workflows, with different steps
     executing on different resources, but still coordinated

The producer-consumer approach to workflows
allows for increased flexibility and scalability. For this
reason it has become a mainstay of cloud-compute microservices, which
allow for extremely distributed asynchronous computing.

Many asynchronous task and workflow systems exist, but the majority are
focused around this microservices model, where a system is set up (and
managed) by experts that build a single workflow. This static workflow
gets tested and hardened and exists as a service for their users
(e.g. an event on a website triggers a discrete set of tasks).
HPC, and in particular *scientific* HPC
brings its own set of challenges that make a direct application of microservices
to HPC workflows challenging.


.. list-table:: Challenges for bringing microservices to scientific HPC Workflows
  :widths: 50 50
  :header-rows: 1

  * - Challenge
    - Requirement
  * - Workflows can change from day-to-day as researchers explore new simulations,
      configurations, and questions.
    - *Workflows need to be dynamic, not static.*
  * - Workflow components are usually different executables,
      pre- and post-processing scripts and data aggregation steps
      written in different languages.
    - *Workflows need to intuitively support multiple languages.*
  * - These components often need command-line-level control of task instructions.
    - *Workflows need to support shell syntax and environment variables.*
  * - Components frequently require calls to a batch system scheduler for parallel job
      execution.
    - *Workflows need a natural way to launch parallel jobs that use more resources
      then a single worker.*
  * - Tasks can independently create large quantities of data.
    - *Dataflow models could be bottlenecks. Workflows should take advantage of
      parallel file systems.*
  * - HPC systems (in particular leadership class machines) can experience unforeseen
      outages.
    - *Workflows need to be able to restart, retry and rerun failed steps without
      needing to run the entire workflow.*

Merlin was built specifically to address the challenges of porting microservices
to HPC simulations.

So what exactly does Merlin do?
+++++++++++++++++++++++++++++++

Merlin wraps a heavily tested and well used asynchronous task queuing library in
a skin and syntax that is natural for HPC simulations. In essence, we extend
`maestro <https://github.com/LLNL/maestrowf>`_ by hooking it up to
`celery <https://docs.celeryproject.org/en/latest/index.html>`_. We leverage
maestro's HPC-friendly workflow description language and translate it to
discrete celery tasks.

Why not just plain celery?

Celery is extremely powerful, but this power can be a barrier for many science
and engineering subject matter experts,
who might not be python coders. While this may not be
an issue for web developers, it presents a serious challenge to many scientists
who are used to running their code from a shell command line. By wrapping celery
commands in maestro steps, we not only create a familiar environment for users
(since maestro steps look like shell commands), but we also create structure
around celery dependencies. Maestro also has interfaces to common batch schedulers
(e.g. `slurm <https://slurm.schedmd.com/documentation.html>`_
and `flux <http://flux-framework.org>`_)[*]_ for parallel job control.

So why Merlin and not just plain maestro?

The main reason: to run lots of simulations for machine learning
applications. Basically **Merlin scales maestro.**

Maestro follows an external coordinator model. Maestro workflow DAGs
(directed acyclic graphs) need to be unrolled (concretized)
ahead of time, so that batch dependencies can be calculated and managed.
This graph problem becomes very expensive as the number of tasks approaches
a few hundred. (Not to mention most batch systems will prevent a user
from queuing more than a few hundred concurrent batch jobs.) In other words,
using maestro alone to run thousands of simulations is not practical.

But with celery, we can *dynamically* create additional
tasks. This means that the DAG can get unrolled by the very
same workers that will execute the tasks, offering a natural parallelism
(i.e. much less waiting before starting the work).

What does this mean in practice?

*Merlin can quickly queue a lot of simulations.*

How quickly? The figure below shows task queuing rates when pushing
:doc:`a simple workflow<./hello_world/hello_world>` on the
`Quartz Supercomputer <https://hpc.llnl.gov/hardware/platforms/Quartz>`_
to 40 million samples. This measures how quickly simulation ensembles of various
sample sizes can get enqueued.

.. image:: ../../images/task_creation_rate.png

As you can see, by exploiting celery's dynamic task queuing (tasks that create
tasks), Merlin can enqueue hundreds of thousands of
simulations per second. These jobs can then be consumed in parallel,
at a rate that depends on the number of workers you have.

Furthermore, this ability to dynamically add tasks to the queue means
that workflows can become more flexible and responsive. A worker executing
a step can launch additional workflows without having to stand up resources
to execute and monitor the execution of those additional steps.

The only downside to being able to enqueue work this quickly is the inability
of batch schedulers to keep up. This is why we recommend pairing Merlin with
`flux <http://flux-framework.org>`_, which results in a scalable but easy-to-use
workflow system:

- Maestro describes the workflow tasks
- Merlin orchestrates the task executions
- Flux schedules the HPC resources

Here's an example of how Merlin, maestro and flux can all work together
to launch a workflow on multiple machines.

.. image:: ../../images/merlin_arch.png

The scientist describes her workflow with a maestro-like ``<workflow file>``. Her workflow
consists of two steps:

1. Run many parallel CPU-only jobs, varying her simulation parameters of interest
2. Use a GPU to train a deep learning model on the results of those simulations

She then types ``merlin run <workflow file>``, which translates that maestro file
into celery commands and
sends those tasks to two separate queues on a centralized server (one for CPU work and
one for GPU work).

She then launches a batch allocation on the CPU machine, which contains the command
``merlin run-workers <workflow file> --steps 1``.
Workers start up under flux, pull work from the server's CPU queue and call flux to
launch the parallel simulations asynchronously.

She also launches a separate batch request on the GPU machine with
``merlin run-workers <workflow file> --steps 2``. These workers connect to the central
queue associated with the GPU step.

When the simulations in step 1 finish, step 2 will automatically start. In this fashion,
Merlin allows the scientist to coordinate a highly scalable asynchronous multi-machine
heterogeneous workflow.

This is of course a simple example, but it does show how the producer-consumer
philosophy in HPC workflows can be quite enabling. Merlin's goal is to make it easy
for HPC-focused subject matter experts to take advantage of the advances in cloud
computing.


How is it designed?
+++++++++++++++++++

Merlin leverages a number of open source technologies, developed and battle-hardened
in the world of distributed computing. We decided to do this instead of
having to build, test and maintain
stand-alone customized (probably buggy) versions of software that will probably not
be as fully featured.

There are differing philosophies on how much third-party software to rely upon.
On the one hand, building our system off ubiquitous open source message passing libraries
increases the confidence in our
software stack's performance, especially at scale (for instance,
celery is robust enough to `keep Instagram running <https://blogs.vmware.com/vfabric/2013/04/how-instagram-feeds-work-celery-and-rabbitmq.html>`_).
However, doing so means that when something breaks deep down, it can
be difficult to fix (if at all). Indeed if there's an underlying "feature" that we'd
like to work around, we could be stuck. Furthermore, the complexity of the software
stack can be quite large, such that our team couldn't possibly keep track of it all.
These are valid concerns; however, we've found it much easier to quickly develop a
portable system with a small team by treating (appropriately chosen) third party
libraries as underlying infrastructure. (Sure you *could* build and use your own
compiler, but *should* you?)

Merlin manages the increased risk that comes with relying on software that is out of
our control by:

1. Building modular software that can easily be reconfigured / swapped for other tech
2. Participating as developers for those third-party packages upon which rely
   (for instance we often kick enhancements and bug fixes to maestro)
3. Using continuous integration and testing to catch more errors as they occur

This section talks about some of those underlying technologies, what they are, and
why they were chosen.

*A brief technical dive into some underlying tech*

Merlin extends `maestro <https://github.com/LLNL/maestrowf>`_ with
`celery <https://docs.celeryproject.org/en/latest/index.html>`_, which in turn can
be configured to interface with a variety of `message queue brokers <https://docs.celeryproject.org/en/latest/getting-started/brokers/index.html#broker-overview>`_ and `results backends <https://docs.celeryproject.org/en/latest/userguide/configuration.html#result-backend>`_. In practice, we like to use
`RabbitMQ <https://www.rabbitmq.com>`_ and `Redis <https://redis.io>`_ for our broker
and backend respectively, because of their features and reliability, especially at scale.

.. list-table:: Key Merlin Tech Components
  :widths: 25 75
  :header-rows: 1

  * - Component
    - Reasoning
  * - `maestro <https://github.com/LLNL/maestrowf>`_
    - shell-like workflow descriptions, batch system interfaces
  * - `celery <https://docs.celeryproject.org/en/latest/index.html>`_
    - highly scalable, supports multiple brokers and backends
  * - `RabbitMQ <https://www.rabbitmq.com>`_
    - resilience, support for multiple users and queues
  * - `Redis <https://redis.io>`_
    - database speed, scalability
  * - `cryptography <https://github.com/pyca/cryptography>`_
    - secure Redis results
  * - `flux <http://flux-framework.org>`_ (optional)
    - portability and scalability of HPC resource allocation

The different components interact to populate and drain the message queue broker of
workflow tasks.

.. image:: ../../images/merlin_run.png
   :align: center

When a call is made to ``merlin run``, maestro turns the workflow description (composed of "steps" with "parameters" and "samples") into a task
dependency graph. Merlin translates this graph into discrete celery task commands [*]_

Calls to ``merlin run-worker`` cause celery workers to connect to both the message broker
and results database. The workers pull tasks from the broker and begin to execute
the instructions therein.
When finished, a worker posts the results (task status
metadata, such as "SUCCESS" or "FAIL") to the results database and
automatically grabs another task from the queue.
When additional workers come along (through other explicit calls to ``merlin run-worker``),
they connect to the broker and help out with the workflow.

*Multiple vs. Single Queues*

RabbitMQ brokers can have multiple distinct queues. To take advantage of this feature,
Merlin lets you assign workflow steps and workers to different queues. (Steps must be assigned to a single queue, but workers
can connect to multiple queues at once.) The advantage of a single queue is simplicity,
both in workflow design and scalability. However, having multiple queues allows for
prioritization of work (the express checkout lane at the grocery store) and customization
of workers (specialized assembly line workers tailored for a specific task).


What is in this Tutorial?
+++++++++++++++++++++++++

This tutorial will show you how to:


* :doc:`Install Merlin<./installation/installation>`
  and test that it works correctly
* :doc:`Build a basic workflow<./hello_world/hello_world>`
  and scale it up, introducing you to
  Merlin's syntax and how it differs from maestro.
* :doc:`Run a "real" physics simulation<./run_simulation/run_simulation>`
  based workflow, with post-processing of results, visualization
  and machine learning.
* :doc:`Use Merlin's advanced features<./advanced_topics/advanced_topics>`
  to do things like interface with batch systems, distribute a workflow across
  machines and dynamically add new samples to a running workflow.
* :doc:`Contribute to Merlin<./contribute>`,
  through code enhancements and bug reports.
* :doc:`Port your own application<./port_your_application>`,
  with tips and tricks for building and scaling up workflows.


.. rubric:: Footnotes

.. [*] The flux and slurm interfaces used by Merlin differ
       from the versions bundled with maestro to decouple job launching from
       batch submission.
.. [*] Technically Merlin creates celery tasks that will break up the graph into
       subsequent tasks (tasks to create tasks). This improves scalability with parallel
       task creation.
