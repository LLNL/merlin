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

.. admonition:: tldr

    Merlin is a toolkit designed to enable HPC-focused simulation workflows
    with distributed cloud compute technologies. This helps simulation workflows
    push to immense scale. (Like `100 million`__.)

__ https://arxiv.org/abs/1912.02892

At its core, Merlin translates a text-based, command-line focused workflow
descrption into a set of discrete tasks. These tasks live on a centralized
broker (e.g. a separate server) that persists outside of your HPC
batch allocation. Autonomous workers in different allocations (even
on different machines) can then connect
to this server, pull off and execute these tasks asyncronously.

So what? Why would you care to do this?

The short answer: machine learning

The longer answer: machine learning and data science are becoming
an integral part of scientific inquiry. The problem is that machine learning
models are data hungry; it takes lots and lots of simulations to train machine
learning models on their outputs. Unfortunately HPC systems were designed to execute
a few large hero simulations, not many smaller simulations. Naively pushing
standard HPC workflow tools to hundreds of thousands and millions of simulations
can lead to some serious problems.

The good news is that distributed cloud compute technology has really pushed the
frontier of scalabilty. Merlin helps bring this tech to traditional scientific HPC.

The increased flexibility that comes from
decoupling *what* HPC simulations you run from *where* you run them
can be extremely enabling. In particular Merlin allows you to

* Scale to very large number of simulations by avoiding common HPC bottlenecks
* Automatically take advantage of free nodes to process your workflow faster
* Create iterative workflows, like as needed for active machine learning
* Dynamically add more tasks to already-running jobs
* Have cross-machine and cross-batch-job workflows, with different steps
  executing on different resources, but still coordinated

An example workflow setup
-------------------------

.. image:: ../../images/merlin_arch.png

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
* Components and reasoning




