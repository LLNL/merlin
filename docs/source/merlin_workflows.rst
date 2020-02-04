Workflows
=========

The Merlin package provides a few example workflows. These may be useful in
seeing how the software works, and in designing your own workflow. This section
provides documentation on running these Merlin workflow examples.

Overview
--------

List the built-in Merlin workflow examples with ``merlin example --help``.

The Merlin team is working on adding a more diverse array of example workflows
like these.

In particular, look at the ``.yaml`` files within these directories. These
are known as Merlin specifications, and are foundational to determining a workflow.


Get started with the demo ensemble
-----------------------------------

Merlin provides a demo workflow that highlights some features of the software.

.. tip::

    Have at least two terminals open; one to monitor workers, and the other to
    provide them tasks.

Create your workflow example:

.. code:: bash

    $ merlin example feature_demo

To run the distributed version of ``feature_demo``, run the following:

.. code:: bash

    $ merlin run feature_demo/feature_demo.yaml

This will queue the tasks to the configured broker. To process the queued 
tasks, use the ``run-workers`` Merlin CLI command. Adding this command
to a parallel batch submission script will launch the workers in parallel.

.. code:: bash

    $ merlin run-workers feature_demo/feature_demo.yaml

