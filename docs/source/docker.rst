Docker
======

Merlin has a simple Dockerfile description for running a container
with all requirements installed.


Build the container
*******************

The docker container can be built by building in the top level
merlin directory.

.. code:: bash

  docker build -t merlin .


This will create a merlin:latest image in your docker image
collection.

.. code:: bash

  docker images


Run the container
*****************

The container can be run as if merlin were installed on the command line
by using the docker run command.

.. code:: bash

  docker run --rm -ti merlin

This is the same as running merlin if it were installed locally. Merlin
will run using the local container file system. The output can be written
to the local file system using the ``--volume`` and ``--workdir`` docker
arguments.

.. code:: bash

  docker run --rm -ti --volume "$PWD":/wd --workdir /wd merlin example slurm_test 

  cd slurm

  docker run --rm -ti --volume "$PWD":/wd --workdir /wd merlin run slurm_tets.yaml --dry --local

