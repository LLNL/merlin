Introduction
============
.. admonition:: Prerequisites

      * Curiosity

.. admonition:: Estimated time

      * 20 minutes

.. admonition:: You will learn

      * What Merlin is and why you might consider it
      * Why it was built and what are some target use cases
      * How it is designed and what the underlying tech is

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

*Why Merlin? What's the need?*

So what? Why would you care to do this?

The short answer: machine learning

The longer answer: machine learning and data science are becoming
an integral part of scientific inquiry. The problem is that machine learning
models are data hungry; it takes lots and lots of simulations to train machine
learning models on their outputs. Unfortunately HPC systems were designed to execute
a few large hero simulations, not many smaller simulations. Naively pushing
standard HPC workflow tools to hundreds of thousands and millions of simulations
can lead to some serious problems.

*How does Merlin help solve this?*

The good news is that distributed cloud compute technology has really pushed the
frontier of scalability. Merlin helps bring this tech to traditional scientific HPC.

Traditionally, HPC workflow systems tie workflow steps to HPC resources and
coordinate the execution of tasks and management of resources one of two ways:

.. |ext-img| image:: ../../images/external_coordination.png


.. |int-img| image:: ../../images/internal_coordination.png


   
.. table:: Traditional HPC Workflow Philosophies

   +--------------+--------------+
   | External     + Internal     |
   | Coordination + Coordination |
   | |ext-img|    + |int-img|    | 
   +--------------+--------------+


**External coordination** ties together independent batch jobs each executing workflow
sub-tasks with an external monitor. This monitor could be a daemon
or human that monitors either the batch or file system via periodic polling and orchestrates task launch dependencies.


**Internal coordination** puts the monitor with a larger batch job that allocates
resources inside that job for the specific tasks at hand.

   
External coordination can tailor the resources to the task, but cannot easily
run lots of concurrent simulations (since batch systems usually limit the number
of jobs a user can queue at once).


Internal coordination can run many more
concurrent tasks by bundling smaller jobs into larger jobs, but cannot tailor the
resources to the task at hand. This precludes workflows that, for instance, require
one step on CPU hardware and another on a GPU machine.

Instead of tying resources to tasks, Merlin does this:

.. image:: ../../images/central_coordination.png
   :width: 75 %
   :align: center

Merlin avoids a centralized command-and-control approach to HPC resource
management for a workflow. Instead of having the workflow coordinator
ask for and manage HPC resources and tasks, the Merlin coordinator just manages
tasks. Task-agnostic resources can then independently connect (and
disconnect) to the coordinator. This *producer-consumer* approach to workflows
allows for increased flexibility and scalability.



An example workflow setup
-------------------------

.. image:: ../../images/merlin_arch.png

*Benefit*

The increased flexibility that comes from
decoupling *what* HPC simulations you run from *where* you run them
can be extremely enabling. In particular Merlin allows you to

* Scale to very large number of simulations by avoiding common HPC bottlenecks
* Automatically take advantage of free nodes to process your workflow faster
* Create iterative workflows, like as needed for active machine learning
* Dynamically add more tasks to already-running jobs
* Have cross-machine and cross-batch-job workflows, with different steps
  executing on different resources, but still coordinated


*Competition*



Why was it built?
+++++++++++++++++

* More Data, More Problems

  ML & data-driven science are data hungry, but HPC systems typically
  target single large jobs, not many smaller jobs. Naively pushing existing
  solutions to large scales can lead to serious issues.

* Do more with less

  Workflows, applications and machines are becoming more complex.
  SMEs need to devote time and attention to their applications
  and often require fine command-line level control. Furthermore,
  they rarely have the time to devote to learning workflow systems.

* Bring distributed compute to HPC

  Current WF systems target one or the other, but not both

How is it designed?
+++++++++++++++++++

* Tech under the hood

.. image:: ../../images/merlin_run.png
   :width: 75 %
   :align: center


* Components and reasoning




