Installation
============
Prerequisites:
  * shell (bash, csh, etc, if running on Windows, use a linux container)
  * python3 >= python3.6
  * python3 pip 
  * wget
  * build tools (make, C-compiler for local-redis)
  * docker (optional)
  * file editor if using docker

Estimated time: 20 minutes

You will learn:
  * How to install merlin in a virtual environment using pip.
  * How to install a local redis server.
  * How to install merlin using docker (optional).
  * How to start the docker containers, including redis (optional).
  * How to configure merlin.
  * How to test/verify the installation.


This section details the steps necessary to install merlin and its dependencies.
Merlin will then be configured and this configuration checked to ensure a proper installation.
Merlin can be installed using pip or through docker containers.  The pip method is 
recommended for this tutorial.


Pip
+++

Create a virtualenv using python3 to install merlin.

.. code:: bash

  python3 -m venv --prompt merlin merlin_venv

Activate the virtualenv.

.. code:: bash

  source merlin_venv/bin/activate
  or
  source merlin_venv/bin/activate.csh


The ``(merlin) <shell prompt>`` will appear after activating.

You should upgrade pip and setuptools before proceeding.

.. code:: bash

  pip3 install setuptools pip -U

Install merlin through pip.

.. code:: bash

  pip3 install merlinwf

When you are done with the virtualenv you can deactivate it using ``deactivate``.

.. code:: bash

  deactivate


redis local server
^^^^^^^^^^^^^^^^^^

A redis server is required for the celery results backend server, this same server
can also be used for the celery broker. This method will be called local-redis.

.. code:: bash

  # Download redis
  wget http://download.redis.io/releases/redis-5.0.7.tar.gz

  # Untar
  tar xvf redis*.tar.gz

  # cd into redis dir
  cd redis*/

  # make redis
  make

  # make test (~3.5 minutes)
  make test

  # run redis with default config, server is at localhost port 6379
  ./src/redis-server &

Docker
++++++

The docker containers used in this tutorial are all located on dockerhub.

All of this commands are available in a shell script called ``setup_merlin_docker.(c)sh`` 
in the merlin github. See below for instructions. 

.. note::
  When using the docker method the celery workers will run inside the merlin container. This
  means that any workflow tools that are also from docker containers must be installed in, or
  otherwise made available to the merlin container.

The first step is to download all the necessary containers.

.. code:: bash

  docker pull llnl/merlin
  docker pull redis
  # optional
  docker pull rabbitmq


The redis server is used for the celery broker and results backend server in this tutorial,
the redis server will be started  in detached mode, ``--detach``, to provide these servers. 
For the server configuration step below this will be referred to as 
docker-redis. The ``--publish`` or ``-p`` option will link a localhost port to the docker container port 
``-p <localhost port>:<docker container port>``.

.. code:: bash

  docker run --detach --name my-redis -p 6379:6379 redis
  or
  docker run -d --name my-redis -p 6379:6379 redis

Next we will start the merlin container and define some aliases to run
the merlin and celery commands. The merlin docker run has a few new options,
the ``-t`` option will allocate a pseudo-tty. The ``--link`` option will
connect the redis server started above to the merlin container. The ``--volume``
or ``-v`` option will link the local ``$HOME/merlinu`` directory to the ``/home/merlinu``
directory in the container.

.. code:: bash

  docker -dt --name my-merlin --link my-redis --volume "$HOME/merlinu":/home/merlinu llnl/merlin
  or 
  docker -dt --name my-merlin --link my-redis -v "$HOME/merlinu":/home/merlinu llnl/merlin

  # define some aliases for the merlin and celery commands (assuming Bourne shell)
  alias merlin="docker exec my-merlin merlin"
  alias celery="docker exec my-merlin celery"
  alias python3="docker exec my-merlin python3"


A shell script is available for all these commands. 

.. code:: bash

  # Download the setup_merlin_docker.sh file <FIXME: URL>
  wget https:/github.com/LLNL/merlin/<path>/setup_merlin_docker.sh
  source ./setup_merlin_docker.sh

  #For (t)csh based shells <FIXME: URL>
  wget https:/github.com/LLNL/merlin/<path>/setup_merlin_docker.csh
  source ./setup_merlin_docker.csh

A rabbitmq server can be started to provide the broker, the redis server will 
still be required for the backend. 

The celery rabbitmq server interaction requires ssl for encrypted communication,
for this tutorial self-signed certificates can be used. Information on this process
can be found here ``provide link``.

The ``hostname`` option provides the server location for the merlin container. The
rabbitmq server must be configured for merlin environment. This entails setting
the user, vhost and ssl certificates for the connection.


.. code:: bash

  docker run -d --hostname my-rabbit --name some-rabbit \ 
        -v "${HOME}/merlinu/cert_rabbitmq:/cert_rabbitmq
        -e RABBIT_DEFAULT_USER=merlinu \
        -e RABBIT_DEFAULT_VHOST=merlinu \
        -e RABBITMQ_SSL_CERTFILE=/cert_rabbitmq/server_cert.pem \
        -e RABBITMQ_SSL_KEYFILE=/cert_rabbitmq/server_key.pem \
        -e RABBITMQ_SSL_CACERTFILE=/cert_rabbitmq/ca_cert.pem \
        rabbitmq:3

When you are done with the containers you can stop them using ``docker container stop``.

.. code:: bash

  docker container stop my-redis
  docker container stop my-merlin

  docker container stop some-rabbit


Configuring merlin
++++++++++++++++++

Merlin requires a configuration script for the celery interface and optional
passwords for the redis server and encryption.

.. code:: bash

  merlin config --broker redis

If you are using local-redis then you are all set, look in your ``~/.merlin/app.yaml`` file
to see the configuration.

.. code:: bash
    broker:
        name: redis
        server: localhost
        port: 6379
        db_num: 0

    results_backend:
        name: redis
        server: localhost
        port: 6379
        db_num: 0

If you are using the docker-redis server then the ``~/merlinu/.merlin/app.yaml`` file must be edited to 
add the server from the redis docker container my-redis. Change the ``server: localhost`` in both the broker and
backend config definitions to ``server: my-redis``, the port will remain the same.

.. code:: bash
    broker:
        name: redis
        server: my-redis
        port: 6379
        db_num: 0

    results_backend:
        name: redis
        server: my-redis
        port: 6379
        db_num: 0


If you are runing the optional rabbitmq server then the config can be created with the normal
config command. If you have already run the previous command then remove the ``~/.merlin/app.yaml`` or
``~/merlinu/.merlin/app.yaml`` file , then run the command below.

.. code:: bash

  merlin config

The app.yaml file will need to be edited to add the rabbitmq settings in the broker section
of the app.yaml file. The ``server:`` should be changed to ``my-rabbit``. The rabbitmq will
server be accessed on its TLS port, 5671.

.. code:: bash
    broker:
        name: rabbitmq
        server: my-rabbit

    results_backend:
        name: redis
        server: <localhost | my-redis>
        port: 6379
        db_num: 0


Checking/Verifying installation
+++++++++++++++++++++++++++++++

The ``info`` command will check that the configuration file is installed 
correctly, display the server configuration strings, and check server access.

.. code:: bash

  merlin info

If everything is set up correctly, you should see (assuming local-redis servers):

.. code:: bash

  .
  .
  .

  Merlin Configuration
  -------------------------

   config_file        | <user home>/.merlin/app.yaml
   is_debug           | False
   merlin_home        |  <user home>/.merlin
   merlin_home_exists | True
   broker             | redis://localhost:6379/0
   backend            | redis://localhost:6379/0
  

  Checking server connections:
  ----------------------------
  broker connection: OK
  backend connection: OK

  Python Configuration
  -------------------------
  .
  .
  .
