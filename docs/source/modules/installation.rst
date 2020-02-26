Installation
============
.. admonition:: Prerequisites

  * shell (bash, csh, etc, if running on Windows, use a linux container)
  * python3 >= python3.6
  * python3 pip 
  * wget
  * build tools (make, C/C++ compiler for local-redis)
  * docker (optional)
  * file editor if using docker

.. admonition:: Estimated time

  * 20 minutes

.. admonition:: You will learn

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

Merlin and the servers required by merlin are all available as docker containers on dockerhub.

.. note::

  When using the docker method the celery workers will run inside the
  merlin container. This
  means that any workflow tools that are also from docker containers must 
  be installed in, or
  otherwise made available to, the merlin container.


To run a merlin docker container with a docker redis server cut
and paste the commands below in to a ``docker-compose.yml`` file.

.. code:: bash

  version: '3'
  
  networks:
    mernet:
      driver: bridge
  
  services:
    redis:
      image: 'redis:latest'
      container_name: my-redis
      ports:
        - "6379:6379"
      networks:
        - mernet
  
    merlin:
      image: 'llnl/merlin'
      container_name: my-merlin
      tty: true
      volumes:
        - ~/merlinu/:/home/merlinu
      networks:
        - mernet


This file can then be run with the ``docker-compose`` command.

.. code:: bash

  docker-compose up -d

The ``volume`` option in the ``docker-compose.yml`` file
will link the local ``$HOME/merlinu`` directory to the ``/home/merlinu``
directory in the container.

Some aliases can be defined for convenience.

.. code:: bash

  # define some aliases for the merlin and celery commands (assuming Bourne shell)
  alias merlin="docker exec my-merlin merlin"
  alias celery="docker exec my-merlin celery"
  alias python3="docker exec my-merlin python3"

When you are done with the containers you can stop them using ``docker-compose down``.

.. code:: bash

  docker-compose down


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

If you are using the docker-redis server then the 
``~/merlinu/.merlin/app.yaml`` file must be edited to 
add the server from the redis docker container my-redis. Change the ``server: localhost``, in both the 
broker and backend config definitions, to ``server: my-redis``, the port will remain the same. 

.. note::
  You can use the docker redis server, instead of the local-redis server,
  with the pi installed merlin by leaving the server locations as localhost.
  ``server: localhost``

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


Checking/Verifying installation
+++++++++++++++++++++++++++++++

The ``merlin info`` command will check that the configuration file is 
installed correctly, display the server configuration strings, and check server access.

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
++++++++++++++++++++++++++++

A rabbitmq server can be started to provide the broker, the redis 
server will still be required for the backend. Merlin is configured
to use ssl encryption for all communication with the rabbitmq server.
This tutorial ca use self-signed certificates . Information on TLS
can be found here ``provide link``.

A set of self-signed keys is created through the ``tls-gen`` package.
These keys are then copied to a common directory for use in the rabbitmq
server and python.

.. code:: bash

 git clone https://github.com/michaelklishin/tls-gen.git 
 cd tls-gen/basic
 make
 mkdir -p ${HOME}/merlinu/cert_rabbitmq
 cp server/*.pem testca/*.pem ${HOME}/merlinu/cert_rabbitmq


The rabbitmq docker microservice can be added to the previous 
``docker-compose.yml`` file.

.. code:: bash

  version: '3'

  networks:
    mernet:
      driver: bridge
     
  services:
    redis:
      image: 'redis:latest'
      container_name: my-redis
      ports:
        - "6379:6379"
      networks:
        - mernet
     
    rabbitmq:
      image: rabbitmq:3-management
      container_name: some-rabbit
      hostname: my-rabbit
      tty: true
      ports:
        - "15672:15672"
        - "15671:15671"
        - "5672:5672"
        - "5671:5671"
      environment:
        - RABBITMQ_SSL_CACERTFILE=/cert_rabbitmq/cacert.pem
        - RABBITMQ_SSL_KEYFILE=/cert_rabbitmq/key.pem
        - RABBITMQ_SSL_CERTFILE=/cert_rabbitmq/cert.pem
        - RABBITMQ_DEFAULT_USER=merlinu
        - RABBITMQ_DEFAULT_VHOST=merlinu
        - RABBITMQ_DEFAULT_PASS=guest
      volumes:
        - ~/merlinu/cert_rabbitmq:/cert_rabbitmq
      networks:
        - mernet
     
    merlin:
      image: 'llnl/merlin'
      container_name: my-merlin
      tty: true
      environment:
        - SSL_CERT_FILE=/home/merlinu/cert_rabbitmq/cert.pem
        - SSL_CERT_DIR=/home/merlinu/cert_rabbitmq
        - REQUESTS_CA_BUNDLE=/home/merlinu/cert_rabbitmq/cert.pem
      volumes:
        - ~/merlinu/:/home/merlinu
      networks:
        - mernet

When running the rabbitmq broker server, the config can be created with 
the default ``merlin config`` command.
If you have already run the previous command then remove the 
``~/.merlin/app.yaml`` or
``~/merlinu/.merlin/app.yaml`` file , and run the ``merlin config``
command again.

.. code:: bash

  merlin config

The app.yaml file will need to be edited to add the rabbitmq settings 
in the broker section
of the app.yaml file. The ``server:`` should be changed to ``my-rabbit``. 
The rabbitmq server will be accessed on the default TLS port, 5671.

.. code:: bash

   broker:
       name: rabbitmq
       server: my-rabbit

   results_backend:
       name: redis
       server: my-redis
       port: 6379
       db_num: 0

The aliases defined previously can be used with this set of docker containers.
