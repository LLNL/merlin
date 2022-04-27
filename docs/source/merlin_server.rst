Merlin Server
=============
The merlin server command allows users easy access to containerized broker 
and results servers for merlin workflows. This allowsusers to run merlin without
a dedicated external server.

The main configuration will be stored in the subdirectory called "server/" by 
default in the main merlin configuration "~/.merlin". However different server
images can be created for different use cases or studies just by simplying creating
a new directory to store local configuration files for merlin server instances.

Below is an example of how merlin server can be utilized.

First create and navigate into a directory to store your local merlin 
configuration for a specific use case or study.

.. code-block:: bash

   mkdir study1/
   cd study1/

Afterwards you can instantiate merlin server in this directory by running

.. code-block:: bash

   merlin server init

A main server configuration will be created in the ~/.merlin/server and a local
configuration will be created in a subdirectory called "merlin_server/"

We should expect the following files in each directory

.. code-block:: bash

   ~/study1$ ls ~/.merlin/server/
   docker.yaml  merlin_server.yaml  podman.yaml  singularity.yaml

   ~/study1$ ls
   merlin_server

   ~/study1$ ls merlin_server/
   redis.conf  redis_latest.sif

The main configuration in "~/.merlin/server" deals with defaults and 
technical commands that might be used for setting up the merlin server
local configuration and its containers. Each container has their own
configuration file to allow users to be able to switch between different
containerized services freely.

The local configuration "merlin_server" folder contains configuration files 
specific to a certain use case or run. In the case above you can see that we have a 
redis singularity container called "redis_latest.sif" with the redis configuration
file called "redis.conf". This redis configuration will allow the user to
configurate redis to their specified needs without have to manage or edit
the redis container. When the server is run this configuration will be dynamically
read, so settings can be changed between runs if needed.

Once the merlin server has been initialized in the local directory the user will be allowed
to run other merlin server commands such as "run, status, stop" to interact with the 
merlin server. A detailed list of commands can be found in the `Merlin Server Commands <./modules/server/commands.html>`_ page.

Note: Running "merlin server init" again will NOT override any exisiting configuration
that the users might have set or edited. By running this command again any missing files 
will be created for the users with exisiting defaults. HOWEVER it is highly advised that 
users back up their configuration in case an error occurs where configuration files are override.

.. toctree::
   :maxdepth: 1
   :caption: Merlin Server Settings:

   modules/server/configuration
   modules/server/commands