Merlin Workflows
================

The Merlin package provides a few example workflows. These may be useful in
seeing how the software works, and in designing your own workflow. This section
provides documentation on running these Merlin workflow examples.

Overview
--------

Merlin workflow ensembles are located in the ``workflows`` directory of the
Merlin repo.

.. code:: bash

    workflows/
        feature_demo/
        flux/
        simple_chain/
        slurm/

The Merlin team is working on adding a more diverse array of example workflows
like these.

In particular, look at the ``.yaml`` files within these directories. These
are known as Merlin specs, and are foundational to determining a workflow.


Get started with the demo ensemble
-----------------------------------


Merlin provides a demo workflow that highlights some features of the software.

.. tip::

    Have at least two terminals open; one to monitor workers, and the other to
    provide them tasks.

To run the distributed version of ``feature_demo``, run the following:

.. code:: bash

    (merlin3_7) $ merlin run workflows/feature_demo/feature_demo.yaml

This will queue the tasks to the configured broker. To process the queued 
tasks, use the ``run-workers`` Merlin CLI command. Adding this command
to a parallel batch submission script will launch the workers in parallel.

.. code:: bash

    (merlin3_7) $ merlin run-workers workflows/feature_demo/feature_demo.yaml


An example of launching a simple Celery worker using srun:

.. code:: bash

    (merlin3_7) $ srun -n 1 celery worker -A merlin -l DEBUG

