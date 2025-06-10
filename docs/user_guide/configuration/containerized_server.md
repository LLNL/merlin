# Containerized Server Configuration

!!! warning

    It's recommended that you read through the [Configuration Overview](./index.md) page before proceeding with this module.

!!! warning

    It is not possible to run [cross-machine workflows](../../tutorial/5_advanced_topics.md#multi-machine-workflows) with the containerized servers created with the `merlin server` command.

The `merlin server` command allows users easy access to containerized broker and results servers for Merlin workflows. This allows users to run Merlin without a dedicated external server.

The main configuration will be stored in the subdirectory called `server/` by default in the main Merlin configuration directory `~/.merlin`. However, different server images can be created for different use cases or studies by simplying creating a new directory to store local configuration files for Merlin server instances.

This module will walk through how to initalize the server, start it, and ensure it's linked to Merlin. There will also be an explanation of how to set the containerized server up to run across multiple nodes.

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

- `app.yaml`: The configuration file that Merlin will eventually read from
- `redis.conf`: The Redis configuration file that contains all of the settings to be used for our Redis server
- `redis.pass`: A password for the Redis server that we'll start up next
- `redis.users`: A file defining the users that are allowed to access the Redis server and their permissions
- `redis_latest.sif`: A singularity file that contains the latest Redis Docker image that was pulled behind the scenes by Merlin

The local configuration `merlin_server/` folder contains configuration files specific to a certain use case or run. In the case above you can see that we have a Redis singularity container called `redis_latest.sif` with the Redis configuration file called `redis.conf`. This Redis configuration will allow the user to configure Redis to their specified needs without have to manage or edit the Redis container. When the server is run this configuration will be dynamically read, so settings can be changed between runs if needed.

Once the Merlin server has been initialized in the local directory the user will be allowed to run other Merlin server commands such as `start`, `status`, and `stop` to interact with the Merlin server. A detailed list of commands can be found in the [Merlin Server](../command_line.md#server-merlin-server) section of the [Command Line](../command_line.md) page.

!!! note

    Running `merlin server init` again will *not* override any exisiting configuration that the users might have set or edited. By running this command again any missing files will be created for the users with exisiting defaults. *However,* it is highly advised that users back up their configuration in case an error occurs where configuration files are overriden.

## Starting the Server and Linking it to Merlin

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

The `merlin server start` command will add a new file to the local configuration `merlin_server/` folder: `merlin_server.pf`. This is a process file containing information regarding the Redis process. Additionally, the `merlin server start` command will update the `merlin_server/app.yaml` file that was created when we ran `merlin server init`, so that it has `broker` and `results_backend` sections that point to our started server.

To have Merlin read this server configuration:

=== "Make This Server Configuration Your Main Configuration"

    !!! Tip

        The `merlin config use` command allows you to have multiple server configuration files that you can easily swap between.

    ```bash
    merlin config use merlin_server/app.yaml
    ```

=== "Copy Configuration to CWD"

    ```bash
    cp merlin_server/app.yaml .
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
                

    v. 1.13.0

    [2025-05-28 11:57:14: INFO] Reading app config from file merlin_server/app.yaml
    Merlin Configuration
    -------------------------

    config_file        | merlin_server/app.yaml
    is_debug           | False
    merlin_home        | ~/.merlin
    merlin_home_exists | True
    broker server      | redis://default:******@127.0.0.1:4000/0
    broker ssl         | False
    results server     | redis://default:******@127.0.0.1:4000/0
    results ssl        | False

    Checking server connections:
    ----------------------------
    broker server connection: OK
    results server connection: OK

    Python Configuration
    -------------------------

    Python Packages

    Package    Version    Location
    ---------  ---------  ----------------------------------------------------------------------
    python     3.13.2     /path/to/python3
    pip        25.0.1     /path/to/python3.13/site-packages
    merlin     1.12.2     /path/to/merlin
    maestrowf  1.1.10     /path/to/python3.13/site-packages
    celery     5.5.2      /path/to/python3.13/site-packages
    kombu      5.5.3      /path/to/python3.13/site-packages
    amqp       5.3.1      /path/to/python3.13/site-packages
    redis      5.2.1      /path/to/python3.13/site-packages

    $PYTHONPATH:
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

## Running Cross-Node Workflows with a Containerized Server

By default, the container will be started using `localhost` as the location to route network traffic through. For a cross-node workflow, we'll have to modify this since `localhost` will only be reachable on the same node that the container is running on.

Instead of using `localhost`, we'll do two things:

1. Configure the server to point to the IP address of the node that's hosting the server with `merlin server config`
2. Modify the `app.yaml` file generated by the `merlin server start` command so that the `server` setting points to the name of the node hosting the server

### Creating the Cross-Node Server Launch Script

We'll automate the process described in the beginning of this section by creating a batch script. We can start with a typical header:

```bash title="server.sbatch" linenums="1"
#!/bin/bash
#SBATCH -N 1
#SBATCH -J Merlin
#SBATCH -t 00:20:00
#SBATCH -p pdebug
#SBATCH -A wbronze
#SBATCH -o merlin_server_%j.out
```

These settings can easily be modified as you see fit for your workflow. Specifically, you'll want to modify the walltime to run for however long your workflow should take. You should also modify the queue and the bank to fit your needs.

Next, similar to what's common for worker launch scripts (like the one shown in [Distributed Runs](../running_studies.md#distributed-runs)), we'll add a variable to define a path to the virtual environment where Merlin is installed and a statement to activate this environment:

```bash title="server.sbatch" linenums="9"
# Turn off core files to work aroung flux exec issue.
ulimit -c 0

# Path to virtual environment containing Merlin
VENV=/path/to/your/merlin_venv  # UPDATE THIS PATH

# Activate the virtual environment
source ${VENV}/bin/activate
```

You'll need to modify the `VENV` variable to point to your Merlin virtual environment.

Now that we've got Merlin activated, let's get started with initializing the server (for more information on what's happening in this process, [see above](#initializing-the-server)):

```bash title="server.sbatch" linenums="18"
#########################################
#          Starting the Server          #
#########################################

# Necessary for the Redis server to spin up
export LC_ALL="C"

# Initialize the server files
merlin server init

# Check to make sure the server initialized properly
MERLIN_SERVER_DIR=`pwd`/merlin_server
echo "merlin_server_dir: $MERLIN_SERVER_DIR"
if ! [ -d $MERLIN_SERVER_DIR ]; then
    echo "The server directory '$MERLIN_SERVER_DIR' doesn't exist. Likely a problem with 'merlin server init'"
    exit 1
fi
```

When `merlin server init` is executed, the server files should all be initialized in a folder named `merlin_server` located in your current working directory. If there was a problem with this process, the check after should output an error message and exit gracefully.

The server files now exist so it's time to handle our first major task listed at the start of this section: configuring the server to point to the IP of the current node. We can accomplish this by adding the following lines to `server.sbatch`:

```bash title="server.sbatch" linenums="36"
# Obtain the ip of the current node and set the server to point to it
ip=`getent hosts $(hostname) | awk '{ print $1 }'`
echo "ip: $ip"
merlin server config -ip $ip
```

The server is now configured to use the IP address of the current node.

We'll now start up the server so that we can obtain the `app.yaml` file that we'll need to modify for the second major task listed at the start of this section:

```bash title="server.sbatch" linenums="41"
# Start the server (this creates the app.yaml file that we need)
merlin server start

# Check to make sure the app.yaml file was created properly
APP_YAML_PATH=$MERLIN_SERVER_DIR/app.yaml
echo "app_yaml_path: $APP_YAML_PATH"
if ! [ -f $APP_YAML_PATH ]; then
    echo "The app.yaml file '$APP_YAML_PATH' doesn't exist. Likely a problem with 'merlin server start'"
    exit 1
fi
```

From here, we'll pause on creating the server launch script so that we can create a python script to assist us in updating the `app.yaml` programmatically.

### Creating a Script to Update the Server in `app.yaml`

At this point in the execution of our server launch script, our server should be started and an `app.yaml` file should exist in the `merlin_server` directory that was created by our call to `merlin server init`. In this subsection, we'll create a python script `update_app_hostname.py` that updates the `server` settings in the `app.yaml` file so that they point to the name of the node that's hosting the server.

This `update_app_hostname.py` script will need to take in a hostname and a path to the `app.yaml` file that we need to update. Therefore, we'll start this script by establishing arguments with Python's built-in [argparse library](https://docs.python.org/3/library/argparse.html):

```python title="update_app_hostname.py"
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("hostname", help="the hostname to set in the app.yaml file")
    parser.add_argument("app_yaml", help="the path to the app.yaml file to update")
    args = parser.parse_args()
```

Before updating the `app.yaml` file using the path that was passed in, let's ensure the path exists just to be safe:

```python title="update_app_hostname.py" hl_lines="3 11"
import argparse

from merlin.utils import verify_filepath

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("hostname", help="the hostname to set in the app.yaml file")
    parser.add_argument("app_yaml", help="the path to the app.yaml file to update")
    args = parser.parse_args()

    app_yaml_path = verify_filepath(args.app_yaml)
```

Finally, we'll add a function `update_app_yaml` to do the actual updating of the `app.yaml` file. This function will load in the current contents of the `app.yaml` file, update the necessary `server` settings, and dump the updated settings back to the `app.yaml` file.

```python title="update_app_hostname.py" hl_lines="2-3 7-32 42"
import argparse
import logging
import yaml

from merlin.utils import verify_filepath

LOG = logging.getLogger(__name__)

def update_app_yaml(hostname, app_yaml):
    """
    Read in the app.yaml contents, update them, then write the updated
    contents back to the file.

    :param hostname: The hostname to set our broker and results server to
    :param app_yaml: The path to the app.yaml file to update
    """
    with open(app_yaml, "r") as yaml_file:
        try:
            contents = yaml.load(yaml_file, yaml.FullLoader)
        except AttributeError:
            print(
                "PyYAML is using an unsafe version with a known "
                "load vulnerability. Please upgrade your installation "
                "to a more recent version!"
            )
            contents = yaml.load(yaml_file, yaml.Loader)

    contents["broker"]["server"] = hostname
    contents["results_backend"]["server"] = hostname

    with open(app_yaml, "w") as yaml_file:
        yaml.dump(contents, yaml_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("hostname", help="the hostname to set in the app.yaml file")
    parser.add_argument("app_yaml", help="the path to the app.yaml file to update")
    args = parser.parse_args()

    app_yaml_path = verify_filepath(args.app_yaml)
    update_app_yaml(args.hostname, app_yaml_path)
```

This script is now complete. If you'd like to test it out to make sure it works:

1. Create a copy of your current `app.yaml` file
2. Call this script with:

    ```
    python update_app_hostname.py test /path/to/app_copy.yaml
    ```

3. Open your `app_copy.yaml` file and verify that the `server` settings are now `server: test`

From here, we'll pick up where we left off with creating the server launch script.

### Finishing the Cross-Node Server Launch Script

We now have a fully complete python script to update our `app.yaml` file and a partially complete batch script to launch our server. All that's left now is to finish up the server launch script.

To accomplish this, we'll use the `hostname` command to obtain the name of the host that the server is currently on and pass that in to a call to our `update_app_hostname.py` script that was created in the [previous subsection](#creating-a-script-to-update-the-server-in-appyaml):

```bash title="server.sbatch" linenums="52"
# Update the app.yaml file generated by merlin server start to point to the hostname of this node
python update_app_hostname.py `hostname` ${APP_YAML_PATH}
```

Now let's make sure Merlin is pointing to this `app.yaml` file:

```bash title="server.sbatch" linenums="55"
# Tell Merlin to use this app.yaml file
merlin config use ${APP_YAML_PATH}
```

<!-- When Merlin reads in the `app.yaml` file, it will search for this file in two locations: your current working directory and `~/.merlin`. In order for Merlin to read in this `app.yaml` file that we just updated, we need to copy it to the directory where you'll launch your study from (AKA the current working directory):

```bash title="server.sbatch" linenums="55"
# Move the app.yaml to the project directory
PROJECT_DIR=`pwd`
cp ${MERLIN_SERVER_DIR}/app.yaml ${PROJECT_DIR}
```

In these lines, we're assuming that this file is located in the same place as your spec file. If you place this file elsewhere you'll need to modify the `PROJECT_DIR` variable to point to where your spec file will be launched from. -->

Finally, let's add in a statement to see if our server is connected properly (this will help with debugging) and a call to sleep forever so that this server stays up and running until our allocation terminates:

```bash title="server.sbatch" linenums="59"
# Check the server connection
merlin info

# Keeping the allocation alive so that the server remains up for as long as possible
sleep inf
```

This file is now complete. The full versions of the `server.sbatch` and the `update_app_hostname.py` files can be found in the section below. 

### Full Scripts

The cross-node containerized server configuration requires two scripts:

1. `server.sbatch` - the script needed to launch the server
2. `update_app_hostname.py` - the script needed to update the `app.yaml` file

Below are the full scripts:

=== "server.sbatch"

    ```bash title="server.sbatch"
    #!/bin/bash
    #SBATCH -N 1
    #SBATCH -J Merlin
    #SBATCH -t 00:20:00
    #SBATCH -p pdebug
    #SBATCH -A wbronze
    #SBATCH -o merlin_server_%j.out

    # Turn off core files to work aroung flux exec issue.
    ulimit -c 0

    # Path to virtual environment containing Merlin
    VENV=/path/to/your/merlin_venv  # UPDATE THIS PATH

    # Activate the virtual environment
    source ${VENV}/bin/activate

    #########################################
    #          Starting the Server          #
    #########################################

    # Necessary for the Redis server to spin up
    export LC_ALL="C"

    # Initialize the server files
    merlin server init

    # Check to make sure the server initialized properly
    MERLIN_SERVER_DIR=`pwd`/merlin_server
    echo "merlin_server_dir: $MERLIN_SERVER_DIR"
    if ! [ -d $MERLIN_SERVER_DIR ]; then
        echo "The server directory '$MERLIN_SERVER_DIR' doesn't exist. Likely a problem with 'merlin server init'"
        exit 1
    fi

    # Obtain the ip of the current node and set the server to point to it
    ip=`getent hosts $(hostname) | awk '{ print $1 }'`
    echo "ip: $ip"
    merlin server config -ip $ip

    # Start the server (this creates the app.yaml file that we need)
    merlin server start

    # Check to make sure the app.yaml file was created properly
    APP_YAML_PATH=$MERLIN_SERVER_DIR/app.yaml
    echo "app_yaml_path: $APP_YAML_PATH"
    if ! [ -f $APP_YAML_PATH ]; then
        echo "The app.yaml file '$APP_YAML_PATH' doesn't exist. Likely a problem with 'merlin server start'"
        exit 1
    fi

    # Update the app.yaml file generated by merlin server start to point to the hostname of this node
    python update_app_hostname.py `hostname` ${APP_YAML_PATH}

    # Tell Merlin to use this app.yaml file
    merlin config use ${APP_YAML_PATH}

    # Check the server connection
    merlin info

    # Keeping the allocation alive so that the server remains up for as long as possible
    sleep inf
    ```

=== "update_app_hostname.py"

    ```python title="update_app_hostname.py"
    import argparse
    import logging
    import yaml

    from merlin.utils import verify_filepath

    LOG = logging.getLogger(__name__)

    def update_app_yaml(hostname, app_yaml):
        """
        Read in the app.yaml contents, update them, then write the updated
        contents back to the file.

        :param hostname: The hostname to set our broker and results server to
        :param app_yaml: The path to the app.yaml file to update
        """
        with open(app_yaml, "r") as yaml_file:
            try:
                contents = yaml.load(yaml_file, yaml.FullLoader)
            except AttributeError:
                print(
                    "PyYAML is using an unsafe version with a known "
                    "load vulnerability. Please upgrade your installation "
                    "to a more recent version!"
                )
                contents = yaml.load(yaml_file, yaml.Loader)

        contents["broker"]["server"] = hostname
        contents["results_backend"]["server"] = hostname

        with open(app_yaml, "w") as yaml_file:
            yaml.dump(contents, yaml_file)


    if __name__ == "__main__":
        parser = argparse.ArgumentParser()
        parser.add_argument("hostname", help="the hostname to set in the app.yaml file")
        parser.add_argument("app_yaml", help="the path to the app.yaml file to update")
        args = parser.parse_args()

        app_yaml_path = verify_filepath(args.app_yaml)
        update_app_yaml(args.hostname, app_yaml_path)
    ```

### How to Use the Scripts

Using the scripts is as easy as:

1. Copying the `update_app_hostname.py` and `server.sbatch` files to the same location as your spec file
2. Updating the `VENV` variable in `servers.sbatch` to point to your venv with Merlin installed
3. Starting the server by submitting the script with `sbatch server.sbatch`

Once your allocation is granted, the server should spin up. You can check that it's been started by executing `merlin info`. This should output a message like is shown at the end of [Starting the Server and Linking it to Merlin](#starting-the-server-and-linking-it-to-merlin). Specifically, you should see that it's pointing to the server we just spun up.

From here, you should be able to start your workers by submitting a `workers.sbatch` script like is shown in [Distributed Runs](../running_studies.md#distributed-runs). To ensure that this script doesn't start prior to your server spinning up, you should submit this script with:

```bash
sbatch -d after:<job id of server.sbatch> workers.sbatch
```

This will make it so that workers.sbatch cannot start until the server job has been running for 1 minute.

You can also submit tasks to the server with:

```bash
merlin run <spec file>
```

### Example Usage

For this example, we'll use Merlin's built-in [Hello Samples Example](../../examples/hello.md#the-hello-samples-example). The files for this example can be downloaded with:

```bash
merlin example hello_samples
```

We'll then move into the directory that was downloaded with:

```bash
cd hello/
```

From here, let's copy over the `update_app_hostname.py` and `server.sbatch` files from the [Full Scripts section above](#full-scripts). We'll also add in the following `workers.sbatch` file:

```bash title="workers.sbatch"
#!/bin/bash
#SBATCH -N 2
#SBATCH -J Merlin
#SBATCH -t 00:20:00
#SBATCH -p pdebug
#SBATCH -A wbronze
#SBATCH -o merlin_workers_%j.out

# Turn off core files to work aroung flux exec issue.
ulimit -c 0

YAML=hello_samples.yaml 

VENV=/path/to/your/merlin_venv

# Activate the virtual environment
source ${VENV}/bin/activate

# Check the server connection
merlin info

#########################################
#          Running the Workers          #
#########################################

# Show the workers command
merlin run-workers ${YAML} --echo

# Start workers to run the tasks in the broker
merlin run-workers ${YAML}

# Keep the allocation alive until all workers stop
merlin monitor ${YAML}
sleep inf  # If you're using merlin v1.12.0+ you can comment out this line
```

Now update the `VENV` variable in both `server.sbatch` and `workers.sbatch` to point to the virtual environment where Merlin is installed.

From here, all we need to do is:

1. Start the containerized server with:

    ```bash
    sbatch server.sbatch
    ```

2. Start the workers with:

    ```bash
    sbatch -d after:<job id of server.sbatch> workers.sbatch
    ```

3. Wait for the server to start (step 1), then queue the tasks with:

    ```bash
    merlin run hello_samples.yaml
    ```

You can check that everything ran properly with:

```bash
merlin status hello_samples.yaml
```

Or, if you're using a version of Merlin prior to v1.12.0, you can ensure that the `hello_samples_<timestamp>/` output workspace was created. More info on the expected output can be found in [the Hello World Examples page](../../examples/hello.md#expected-output_1).

Congratulations, you just ran a cross-node workflow with a containerized server!
