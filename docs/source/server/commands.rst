Merlin Server Commands
======================

Merlin server has a list of commands for interacting with the broker and results server.
These commands allow the user to manage and monitor the exisiting server and create 
instances of servers if needed.

Initializing Merlin Server (``merlin server init``)
---------------------------------------------------
The merlin server init command creates configurations for merlin server commands.

A main merlin sever configuration subdirectory is created in "~/.merlin/server" which contains 
configuration for local merlin configuration, and configurations for different containerized
services that merlin server supports, which includes singularity (docker and podman implemented
in the future). 

A local merlin server configuration subdirectory called "merlin_server/" will also
be created when this command is run. This will contain a container for merlin server and associated
configuration files that might be used to start the server. For example, for a redis server a "redis.conf"
will contain settings which will be dynamically loaded when the redis server is run. This local configuration
will also contain information about currently running containers as well.

Note: If there is an exisiting subdirectory containing a merlin server configuration then only 
missing files will be replaced. However it is recommended that users backup their local configurations. 


Checking Merlin Server Status (``merlin server status``)
--------------------------------------------------------

Displays the current status of the merlin server.

Starting up a Merlin Server (``merlin server start``)
-----------------------------------------------------

Starts the container located in the local merlin server configuration.

Stopping an exisiting Merlin Server (``merlin server stop``)
------------------------------------------------------------

Stop any exisiting container being managed and monitored by merlin server.

Restarting a Merlin Server instance (``merlin server restart``)
------------------------------------------------------------

Restarting an existing container that is being managed and monitored by merlin server.

Configurating Merlin Server instance (``merlin server config``)
------------------------------------------------------------
Place holder for information regarding merlin server config command

Possible Flags
.. code:: none
    -ip IPADDRESS, --ipaddress IPADDRESS
                            Set the binded IP address for the merlin server
                            container. (default: None)
    -p PORT, --port PORT  Set the binded port for the merlin server container.
                            (default: None)
    -pwd PASSWORD, --password PASSWORD
                            Set the password file to be used for merlin server
                            container. (default: None)
    --add-user ADD_USER ADD_USER
                            Create a new user for merlin server instance. (Provide
                            both username and password) (default: None)
    --remove-user REMOVE_USER
                            Remove an exisiting user. (default: None)
    -d DIRECTORY, --directory DIRECTORY
                            Set the working directory of the merlin server
                            container. (default: None)
    -ss SNAPSHOT_SECONDS, --snapshot-seconds SNAPSHOT_SECONDS
                            Set the number of seconds merlin server waits before
                            checking if a snapshot is needed. (default: None)
    -sc SNAPSHOT_CHANGES, --snapshot-changes SNAPSHOT_CHANGES
                            Set the number of changes that are required to be made
                            to the merlin server before a snapshot is made.
                            (default: None)
    -sf SNAPSHOT_FILE, --snapshot-file SNAPSHOT_FILE
                            Set the snapshot filename for database dumps.
                            (default: None)
    -am APPEND_MODE, --append-mode APPEND_MODE
                            The appendonly mode to be set. The avaiable options
                            are always, everysec, no. (default: None)
    -af APPEND_FILE, --append-file APPEND_FILE
                            Set append only filename for merlin server container.
                            (default: None)
