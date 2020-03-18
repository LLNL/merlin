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

The container can be run in detached mode to provide both the ``merlin``
and ``celery`` commands

.. code:: bash

  docker run --rm -td --name my-merlin merlin
  alias merlin="docker exec my-merlin merlin"
  alias celery="docker exec my-merlin celery"

Examples can be run through docker containers by first starting a server
for the broker and backend. The server can be a redis or rabbitmq , for this
demonstration a redis server will be used. The backend will always be a 
redis server.

.. code:: bash

  docker pull redis
  docker run -d -p 6379:6379 --name my-redis redis

A local output directory can be defined 
by using the ``--volume`` docker arguments. It is
recommended that a fixed directory be used for the ``--volume`` argument.
The merlin docker container is linked to the redis server above by using
the ``--link`` option.

.. code:: bash

  # Create local working directory
  mkdir $HOME/merlinu
  cd $HOME/merlinu

  docker pull llnl/merlin
  docker run --rm -td --name my-merlin --link my-redis --volume "$HOME/merlinu":/home/merlinu llnl/merlin

  alias merlin="docker exec my-merlin merlin"
  alias celery="docker exec my-merlin celery"

  # Create the $HOME/merlinu/.merlin/app.yaml using redis
  merlin config --broker redis

  <edit $HOME/merlinu/.merlin/app.yaml and change the broker and backend server: variables to  my-redis>

  # Copy an example to the local dir
  merlin example feature_demo

  # Run a test run without workers
  merlin run feature_demo/feature_demo.yaml --dry --local

  # Define the tasks and load them on the broker
  merlin run feature_demo/feature_demo.yaml

  # Start workers to pull tasks from the server and run them in the container
  merlin run-workers feature_demo/feature_demo.yaml


A shell can started in the container by using the
``--entrypoint`` command. If the user would like to examine the container 
contents, they can use a shell as the entry point.

.. code:: bash

  docker run --rm -ti --volume "$HOME/merlinu":/home/merlinu --entrypoint="/bin/bash" merlin


