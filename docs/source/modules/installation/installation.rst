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
  * How to install a container platform eg. singularity, docker, or podman.
  * How to configure merlin.
  * How to test/verify the installation.

.. contents:: Table of Contents:
  :local:

This section details the steps necessary to install merlin and its dependencies.
Merlin will then be configured and this configuration checked to ensure a proper installation.


Installing merlin
-----------------

A merlin installation is required for the subsequent modules of this tutorial.

Once merlin is installed, it requires servers to operate. While you are able to host your own server
we will be using the containerized solution that merlin provides. For the sake of this tutorial we will
be using redis servers.
Your computer/organization may already have a redis server available, please check
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


redis server
^^^^^^^^^^^^

A redis server is required for the celery results backend server, this same server
can also be used for the celery broker. We will be using merlin's containerized solution
however we will need to download one of the supported container platforms avaliable. For 
the purpose of this tutorial we will be using singularity.

.. code-block:: bash

  # Download redis
  wget http://download.redis.io/releases/redis-6.0.5.tar.gz

  # Update and install singularity dependencies
  apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    uuid-dev \
    libgpgme11-dev \
    squashfs-tools \
    libseccomp-dev \
    pkg-config
  
  # Download dependency go
  wget https://go.dev/dl/go1.18.1.linux-amd64.tar.gz

  # Extract go into local
  tar -C /usr/local -xzf go1.18.1.linux-amd64.tar.gz

  # Remove go tar file
  rm go1.18.1.linux-amd64.tar.gz

  # Update PATH to include go
  export PATH=$PATH:/usr/local/go/bin

  # Download singularity
  wget https://github.com/sylabs/singularity/releases/download/v3.9.9/singularity-ce-3.9.9.tar.gz

  # Extract singularity
  tar -xzf singularity-ce-3.9.9.tar.gz

  # Configure and install singularity
  cd singularity-ce-3.9.9
  ./mconfig && \
    make -C ./builddir && \
    sudo make -C ./builddir install

Configuring merlin
------------------
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


The configuration below does not use client
verification ``--tls-auth-clients no`` so the ssl files do not need to 
be defined as shown in the :ref:`broker_redis_ssl` section.

.. literalinclude:: ./docker-compose_rabbit_redis_tls.yml
      :language: yaml

The ``rabbitmq.conf`` file contains the configuration, including ssl, for
the rabbitmq server.

.. code-block:: bash

  default_vhost = /merlinu
  default_user = merlinu
  default_pass = guest
  listeners.ssl.default = 5671
  ssl.options.ccertfile = /cert_rabbitmq/ca_certificate.pem
  ssl.options.certfile = /cert_rabbitmq/server_certificate.pem
  ssl.options.keyfile = /cert_rabbitmq/server_key.pem
  ssl.options.verify = verify_none
  ssl.options.fail_if_no_peer_cert = false

Once this docker-compose file is run, the merlin ``app.yaml`` file is changed
to use the redis TLS server ``rediss`` instead of ``redis``.

