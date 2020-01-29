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
collection with a user "merlinu" and a WORKDIR set to /home/merlinu.

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
to the local file system using the ``--volume`` docker arguments. It is
recommended that a fixed directory be used for the ``--volume`` argument.

.. code:: bash

  # Create local working directory
  mkdir $HOME/merlinu
  cd $HOME/merlinu

  # Create a config in the local volume
  docker run --rm -ti --volume "$HOME/merlinu":/home/merlinu merlin config

  # Copy an example to the local dir
  docker run --rm -ti --volume "$HOME/merlinu":/home/merlinu merlin example slurm_test 

  cd slurm

  docker run --rm -ti --volume "$HOME/merlinu":/home/merlinu merlin slurm_tets.yaml --dry --local


A script can be created to handle the docker run command, ``merlin.sh``.

.. code:: bash

  #/bin/sh
  # A script to run the docker merlin

  # Create local working directory if it does not exist
  LWKDIR=$HOME/merlinu
  if [ ! -e ${LWKDIR} ] ;then
    mkdir $LWKDIR
  fi

  cd $LWKDIR

  docker run --rm -ti --volume "$LWKDIR":/home/merlinu merlin $@


The merlin command can be overriden in teh continaer by using a different 
``--entrypoint`` command. If the user would like to examine the container 
contents, they can use a shell as the entry pint.

.. code:: bash

  docker run --rm -ti --volume "$HOME/merlinu":/home/merlinu --entrypoint="/bin/bash" merlin
