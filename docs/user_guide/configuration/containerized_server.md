# Containerized Server Configuration

!!! warning

    It's recommended that you read through the [Configuration Overview](./index.md) page before proceeding with this module.

The `merlin server` command allows users easy access to containerized broker and results servers for Merlin workflows. This allows users to run Merlin without a dedicated external server.

The main configuration will be stored in the subdirectory called `server/` by default in the main Merlin configuration directory `~/.merlin`. However, different server images can be created for different use cases or studies by simplying creating a new directory to store local configuration files for Merlin server instances.

This module will walk through how to initalize the server, start it, and ensure it's linked to Merlin.

## Initializing the Server

First create and navigate into a directory to store your local Merlin configuration for a specific use case or study:

```bash
mkdir study1/ ; cd study1/
```

Afterwards you can instantiate Merlin server in this directory by running:

```bash
merlin server init
```

A main server configuration will be created in the `~/.merlin/server/` directory. This will have the following files:

- docker.yaml
- merlin_server.yaml
- podman.yaml
- singularity.yaml

The main configuration in `~/.merlin/server/` deals with defaults and technical commands that might be used for setting up the Merlin server local configuration and its containers. Each container has their own configuration file to allow users to be able to switch between different containerized services freely.

In addition to the main server configuration, a local server configuration will be created in your current working directory in a folder called `merlin_server/`. This directory will contain:

- `redis.conf`: The Redis configuration file that contains all of the settings to be used for our Redis server
- `redis.pass`: A password for the Redis server that we'll start up next
- `redis.users`: A file defining the users that are allowed to access the Redis server and their permissions
- `redis_latest.sif`: A singularity file that contains the latest Redis Docker image that was pulled behind the scenes by Merlin

The local configuration `merlin_server/` folder contains configuration files specific to a certain use case or run. In the case above you can see that we have a Redis singularity container called `redis_latest.sif` with the Redis configuration file called `redis.conf`. This Redis configuration will allow the user to configure Redis to their specified needs without have to manage or edit the Redis container. When the server is run this configuration will be dynamically read, so settings can be changed between runs if needed.

Once the Merlin server has been initialized in the local directory the user will be allowed to run other Merlin server commands such as `start`, `status`, and `stop` to interact with the Merlin server. A detailed list of commands can be found in the [Merlin Server](../command_line.md#server-merlin-server) section of the [Command Line](../command_line.md) page.

!!! note

    Running `merlin server init` again will *not* override any exisiting configuration that the users might have set or edited. By running this command again any missing files will be created for the users with exisiting defaults. *However,* it is highly advised that users back up their configuration in case an error occurs where configuration files are overriden.

## Starting the Server and Linking it to Merlin

!!! bug

    For LC users, servers cannot be started outside your home (`~/`) directory.

!!! warning

    Newer versions of Redis have started requiring a global variable `LC_ALL` to be set in order for this to work. To set this properly, run:

    ```bash
    export LC_ALL="C"
    ```

    If this is not set, the `merlin server start` command may seem to run forever until you manually terminate it.

After initializing the server, starting the server is as simple as running:

```bash
merlin server start
```

You can check that the server was started properly with:

```bash
merlin server status
```

The `merlin server start` command will add new files to the local configuration `merlin_server/` folder:

- `merlin_server.pf`: A process file containing information regarding the Redis process
- `app.yaml`: A new app.yaml file configured specifically for the containerized Redis server that we just started

To have Merlin read this server configuration:

=== "Copy Configuration to CWD"

    ```bash
    cp merlin_server/app.yaml .
    ```

=== "Make This Server Configuration Your Main Configuration"

    If you're going to use the server configuration as your main configuration, it's a good idea to make a backup of your current server configuration (if you have one):

    ```bash
    mv ~/.merlin/app.yaml ~/.merlin/app.yaml.bak
    ```

    From here, simply copy the server configuration to your `~/.merlin/` folder:

    ```bash
    cp merlin_server/app.yaml ~/.merlin/app.yaml
    ```

You can check that Merlin recognizes the containerized server connection with:

```bash
merlin info
```

If your servers are running and set up properly, this should output something similar to this:

???+ success

    ```bash
           *      
       *~~~~~                                       
      *~~*~~~*      __  __           _ _       
     /   ~~~~~     |  \/  |         | (_)      
         ~~~~~     | \  / | ___ _ __| |_ _ __  
        ~~~~~*     | |\/| |/ _ \ '__| | | '_ \ 
       *~~~~~~~    | |  | |  __/ |  | | | | | |
      ~~~~~~~~~~   |_|  |_|\___|_|  |_|_|_| |_|
     *~~~~~~~~~~~                                    
       ~~~*~~~*    Machine Learning for HPC Workflows                                 



    Merlin Configuration
    -------------------------

    config_file        | /path/to/app.yaml
    is_debug           | False
    merlin_home        | /path/to/.merlin
    merlin_home_exists | True
    broker server      | redis://default:******@127.0.0.1:6379/0
    broker ssl         | False
    results server     | redis://default:******@127.0.0.1:6379/0
    results ssl        | False

    Checking server connections:
    ----------------------------
    broker server connection: OK
    results server connection: OK

    Python Configuration
    -------------------------

    $ which python3
    /path/to/python3

    $ python3 --version
    Python x.y.z

    $ which pip3
    /path/to/pip3

    $ pip3 --version
    pip x.y.x from /path/to/pip (python x.y)

    "echo $PYTHONPATH"
    ```

## Stopping the Server

Once you're done using your containerized server, it can be stopped with:

```bash
merlin server stop
```

You can check that it's no longer running with:

```bash
merlin server status
```