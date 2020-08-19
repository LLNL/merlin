Installation
============
.. admonition:: Prerequisites

  * shell (bash, csh, etc, if running on Windows, use a linux container)
  * python3 >= python3.6
  * pip3
  * wget
  * build tools (make, C/C++ compiler for local-redis)
  * docker (required for :doc:`Module 4: Run a Real Simulation<../run_simulation/run_simulation>`)
  * file editor for docker config file editing

.. admonition:: Estimated time

  * 20 minutes

.. admonition:: You will learn

  * How to install merlin in a virtual environment using pip.
  * How to install a local redis server.
  * How to install merlin using docker (optional).
  * How to start the docker containers, including redis (optional).
  * How to configure merlin.
  * How to test/verify the installation.

.. contents:: Table of Contents:
  :local:

This section details the steps necessary to install merlin and its dependencies.
Merlin will then be configured and this configuration checked to ensure a proper installation.


Installing merlin
-----------------

A merlin installation is required for the subsequent modules of this tutorial. You can choose between the pip method or the docker method. Choose one or the other but
do not use both unless you are familiar with redis servers run locally and through docker.
**The pip method is recommended.**

Once merlin is installed, it requires servers to operate.
The pip section will inform you how to setup a
local redis server to use in merlin.  An alternative method for setting up a
redis server can be found in the docker section. Only setup one redis server either
local-redis or docker-redis.
Your computer/organization  may already have a redis server available, please check
with your local system administrator.

Pip (recommended)
+++++++++++++++++

Create a virtualenv using python3 to install merlin.

.. code-block:: bash

  python3 -m venv --prompt merlin merlin_venv

Activate the virtualenv.

.. code-block:: bash

  source merlin_venv/bin/activate
  or
  source merlin_venv/bin/activate.csh


The ``(merlin) <shell prompt>`` will appear after activating.

You should upgrade pip and setuptools before proceeding.

.. code-block:: bash

  pip3 install setuptools pip -U

Install merlin through pip.

.. code-block:: bash

  pip3 install merlin

When you are done with the virtualenv you can deactivate it using ``deactivate``,
but leave the virtualenv activated for the subsequent steps.

.. code-block:: bash

  deactivate


redis local server
^^^^^^^^^^^^^^^^^^

A redis server is required for the celery results backend server, this same server
can also be used for the celery broker. This method will be called local-redis.

.. code-block:: bash

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


The redis server is started by calling the ``redis-server`` command located in
the src directory.
This should be run in a separate terminal in the top-level source
directory so the output can be examined.
The redis server will use the default ``redis.conf`` file in the top-level
redis directory.

.. code:: bash

  # run redis with default config, server is at localhost port 6379
  ./src/redis-server &

You can shutdown the local-redis server by using the ``redis-cli shutdown`` command
when you are done with the tutorial.

.. code-block:: bash

  #cd to redis directory
  cd <path to>/redis*/
  ./src/redis-cli shutdown


Docker
++++++

Merlin and the servers required by merlin are all available as docker containers on dockerhub. Do not use this method if you have already set up a virtualenv through
the pip installation method.

.. note::

  When using the docker method the celery workers will run inside the
  merlin container. This
  means that any workflow tools that are also from docker containers must
  be installed in, or
  otherwise made available to, the merlin container.


To run a merlin docker container with a docker redis server, cut
and paste the commands below into a new file called ``docker-compose.yml``.
This file can be placed anywhere in your filesystem but you may want to put it in
a directory ``merlin_docker_redis``.

.. literalinclude:: ./docker-compose.yml
   :language: yaml

This file can then be run with the ``docker-compose`` command in same directory
as the ``docker-compose.yml`` file.

.. code-block:: bash

  docker-compose up -d

The ``volume`` option in the ``docker-compose.yml`` file
will link the local ``$HOME/merlinu`` directory to the ``/home/merlinu``
directory in the container.

Some aliases can be defined for convenience.

.. code-block:: bash

  # define some aliases for the merlin and celery commands (assuming Bourne shell)
  alias merlin="docker exec my-merlin merlin"
  alias celery="docker exec my-merlin celery"
  alias python3="docker exec my-merlin python3"

When you are done with the containers you can stop them using ``docker-compose down``.
We will be using the containers in the subsequent modules so leave them running.

.. code-block:: bash

  docker-compose down

Any required python modules can be installed in the running ``my-merlin`` container
through ``docker exec``. When using docker-compose, these changes will persist
if you stop the containers with ``docker-compose down`` and restart them with 
``docker-compose up -d``.

.. code-block:: bash

     docker exec my-merlin pip3 install pandas faker

Configuring merlin
------------------

Merlin configuration is slightly different between the pip and docker methods.
The fundamental differences include the app.yaml file location and the server name.

Merlin requires a configuration script for the celery interface and optional
passwords for the redis server and encryption. Run this configuration method
to create the ``app.yaml`` configuration file.

.. code-block:: bash

  merlin config --broker redis

Pip
+++

The ``merlin config`` command above will create a file called ``app.yaml``
in the ``~/.merlin`` directory.
If you are using local-redis then you are all set, look in the ``~/.merlin/app.yaml`` file
to see the configuration, it should look like the configuration below.

.. literalinclude:: ./app_local_redis.yaml
   :language: yaml

Docker
++++++

If you are using the docker merlin with docker-redis server then the
``~/merlinu/.merlin/app.yaml`` will be created by the ``merlin config``
command above.
This file must be edited to
add the server from the redis docker container my-redis. Change the ``server: localhost``, in both the
broker and backend config definitions, to ``server: my-redis``, the port will remain the same.

.. note::
  You can use the docker redis server, instead of the local-redis server,
  with the virtualenv installed merlin by using the local-redis
  ``app.yaml`` file above.

.. literalinclude:: ./app_docker_redis.yaml
   :language: yaml

.. _Verifying installation:

Checking/Verifying installation
-------------------------------

The ``merlin info`` command will check that the configuration file is
installed correctly, display the server configuration strings, and check server
access. This command works for both the pip and docker installed merlin.

.. code-block:: bash

  merlin info

If everything is set up correctly, you should see (assuming local-redis servers):

.. code-block:: bash

  .
  .
  .

  Merlin Configuration
  -------------------------

   config_file        | <user home>/.merlin/app.yaml
   is_debug           | False
   merlin_home        | <user home>/.merlin
   merlin_home_exists | True
   broker server      | redis://localhost:6379/0
   results server     | redis://localhost:6379/0


  Checking server connections:
  ----------------------------
  broker server connection: OK
  results server connection: OK

  Python Configuration
  -------------------------
  .
  .
  .


Docker Advanced Installation
----------------------------

RabbitMQ server
+++++++++++++++

This optional section details the setup of a rabbitmq server for merlin.
A rabbitmq server can be started to provide the broker, the redis
server will still be required for the backend. Merlin is configured
to use ssl encryption for all communication with the rabbitmq server.
An ssl server requires ssl certificates to encrypt the communication through
the python ssl module `python ssl <https://docs.python.org/3/library/ssl.html>`_ .
This tutorial can use self-signed certificates created by the user for use
in the rabbitmq server.
The rabbitmq server uses Transport Layer Security (TLS)
(often known as "Secure Sockets Layer").
Information on rabbitmq
with TLS can be found here: `rabbit TLS <https://www.rabbitmq.com/ssl.html>`_

A set of self-signed keys is created through the ``tls-gen`` package.
These keys are then copied to a common directory for use in the rabbitmq
server and python.

.. code-block:: bash

 git clone https://github.com/michaelklishin/tls-gen.git
 cd tls-gen/basic
 make CN=my-rabbit CLIENT_ALT_NAME=my-rabbit SERVER_ALT_NAME=my-rabbit
 make verify
 mkdir -p ${HOME}/merlinu/cert_rabbitmq
 cp result/* ${HOME}/merlinu/cert_rabbitmq


The rabbitmq docker service can be added to the previous
``docker-compose.yml`` file.

.. literalinclude:: ./docker-compose_rabbit.yml
   :language: yaml


When running the rabbitmq broker server, the config can be created with
the default ``merlin config`` command.
If you have already run the previous command then remove the
``~/.merlin/app.yaml`` or
``~/merlinu/.merlin/app.yaml`` file , and run the ``merlin config``
command again.

.. code-block:: bash

  merlin config

The app.yaml file will need to be edited to add the rabbitmq settings
in the broker section
of the app.yaml file. The ``server:`` should be changed to ``my-rabbit``.
The rabbitmq server will be accessed on the default TLS port, 5671.

.. literalinclude:: ./app_docker_rabbit.yaml
   :language: yaml

To complete the config create a file ``~/merlinu/.merlin/rabbit.pass``
and add the password ``guest``.

The aliases defined previously can be used with this set of docker containers.

Redis TLS server
++++++++++++++++

This optional section details the setup of a redis server with TLS for merlin.
The reddis TLS configuration can be found in the :ref:`broker_redis_ssl` section.
A newer redis (version 6 or greater) must be used to enable TLS.

A set of self-signed keys is created through the ``tls-gen`` package.
These keys are then copied to a common directory for use in the redis
server and python.

.. code-block:: bash

     git clone https://github.com/michaelklishin/tls-gen.git
     cd tls-gen/basic
     make CN=my-redis CLIENT_ALT_NAME=my-redis SERVER_ALT_NAME=my-redis
     make verify
     mkdir -p ${HOME}/merlinu/cert_redis
     cp result/* ${HOME}/merlinu/cert_redis

The ``redis:6.0-rc2`` docker service is exchanged for the previous
``redis:latest`` service. The configuration below does not use client
verification ``--tls-auth-clients no`` so the ssl files do not need to 
be defined as shown in the :ref:`broker_redis_ssl` section.

.. literalinclude:: ./docker-compose_rabbit_redis_tls.yml
      :language: yaml

Once this docker-compose file is run, the merlin ``app.yaml`` file is changed
to use the redis TLS server ``rediss`` instead of ``redis``.

