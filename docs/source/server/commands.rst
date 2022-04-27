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

Stop any exisiting container being managaed and monitored by merlin server.
