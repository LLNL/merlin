# Examples

The Merlin package provides a few example workflows. These may be useful in seeing how the software works, and in designing your own workflow. This section provides documentation on running these Merlin workflow examples.

## Overview

There are countless ways to write workflows using Merlin but it helps to have some examples as a starting point. This is made even easier with the `merlin example` command. By providing this command the name of a built-in Merlin example, it will download the folder with everything needed to run that example.

To see a list of all the built-in Merlin examples, run:

```bash
merlin example list
```

Each example will contain at least one `.yaml` file. These are known as Merlin specifications, and are foundational to determining a workflow.

The Merlin team is working on adding a more diverse array of example workflows like these.


<!-- ## Get started with the demo ensemble

Merlin provides a demo workflow that highlights some features of the software.

.. tip::

    Have at least two terminals open; one to monitor workers, and the other to
    provide them tasks.

Create your workflow example:

.. code:: bash

    $ merlin example feature_demo

To run the distributed version of `feature_demo`, run the following:

.. code:: bash

    $ merlin run feature_demo/feature_demo.yaml

This will queue the tasks to the configured broker. To process the queued 
tasks, use the `run-workers` Merlin CLI command. Adding this command
to a parallel batch submission script will launch the workers in parallel.

.. code:: bash

    $ merlin run-workers feature_demo/feature_demo.yaml -->

