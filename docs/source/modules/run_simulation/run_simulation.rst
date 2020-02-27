Run a Real Simulation
=====================
.. admonition:: Prerequisites

      * :doc:`Module 2: Installation<installation>`
      * :doc:`Module 3: Hello World<hello_world/hello_world>`
      * merlin github repository

      .. code:: bash

        git clone https://github.com/LLNL/merlin.git

      * Redis and OpenFOAM docker images

      .. code:: bash

        docker pull cfdengine/openfoam
        docker pull redis

.. admonition:: Estimated time

      * 60 minutes

.. admonition:: You will learn

      * How to run the simulation code OpenFOAM.
      * How to run OpenFOAM using merlin.
      * How to use machine learning on your results.

.. contents::
  :local:

Setting Up
++++++++++

Merlin
~~~~~~
We will need to activate the merlin virtual environment created in :doc:`Module 2: Installation<installation>`

.. code:: bash

  source merlin_venv/bin/activate

Configuring redis
~~~~~~~~~~~~~~~~~
When that is done, we will need to set up the redis server using docker.
This is done by using this command:

.. code:: bash

  docker run --detach --name my-redis -p 6379:6379 redis
  merlin config --broker redis

This sets up the redis server using a docker container without the hassle of
downloading the tar file and making it.

Copying the Module Scripts
~~~~~~~~~~~~~~~~~~~~~~~~~~
This module contains scripts that are expected to be cloned in order to avoid
confusion and debugging non-merlin items. All the required for this module are
available in the merlin github repository.

.. code:: bash

  cp -r merlin/docs/source/modules/run_simulation .

Specification File
++++++++++++++++++

This module aims to do a parameter study on a  well-known benchmark problem for
viscous incompressible fluid flow. We will be setting up our inputs, running
multiple simulations in parallel, combining the outputs, and finally doing some
predictive modeling and visualization using the outputs of these runs.
