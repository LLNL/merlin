<img src="docs/images/merlin.png" width="400"
alt="
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
">

Welcome to the Merlin README, a condensed guide. For more in-depth Merlin
 information, try our [web docs here](https://merlin.readthedocs.io/).

See the [CHANGELOG](CHANGELOG.md) for up-to-date details about features,
 fixes, etc.


# Quick Start

Note: Merlin only supports Python 3.6+.


To install the project and set up its virtualenv with dependencies, run:

    $ make all
    $ source venv_merlin_$SYS_TYPE_py$(PYVERSION)/bin/activate  
    # activate.csh for cshrc (venv_merlin_$SYS_TYPE_py$(PYVERSION)) $

That's it.

To update the project:

    $ make update

To run something a little more like what you're interested in,
namely a demo workflow that has simulation and machine-learning:

    (venv) $ merlin run workflows/feature_demo/feature_demo.yaml
    (venv) $ merlin run-workers workflows/feature_demo/feature_demo.yaml

More documentation on the example workflows can be found under
'Running the Examples'.


# Running the Examples
Example workflows can be found in the `workflows/` directory.
They can be run with the command line interface (CLI).

    # This processes the workflow and creates tasks on the server
    (venv) $ merlin run workflows/feature_demo/feature_demo.yaml
    # This launches workers that can process those tasks
    (venv) $ merlin run-workers workflows/feature_demo/feature_demo.yaml


# Using the CLI
A good way to use merlin is through the command line interface (CLI).
This allows you to both create tasks to be run, as well as stand up workers
for those tasks.

For more information see:

    (venv) $ merlin --help
    (venv) $ merlin run --help
    (venv) $ merlin run-workers --help
    (venv) $ merlin purge --help

Run a workflow specified by the given file.

`(venv) $ merlin run <my_workflow.yaml>`

Stand up celery workers with queues specified in the workflow file.

`(venv) $ merlin run-workers <my_workflow.yaml> [--echo] [--worker-args "celery args"] [--steps step1 stepN]`

A note on arguments:

`[--echo]` Just process the file and print the appropriate command.

`[--worker-args "celery args"]` Passes arguments to the workers

`[--steps step1 ... stepN]` Just give workers for specific steps in the file.


To remove tasks from the task server, use the purge option:

`(venv) $ merlin purge <my_workflow.yaml>`

More information can be obtained by running:

    (venv) $ merlin purge --help

    usage: merlin purge [-h] [-f] [--steps PURGE_STEPS [PURGE_STEPS ...]]
                        specification

    positional arguments:
      specification         Path to a Merlin YAML spec file

    optional arguments:`
      -h, --help            Show this help message and exit
      -f, --force           Purge the tasks without confirmation (default: False)
      --steps PURGE_STEPS [PURGE_STEPS ...]
                            The specific steps in the YAML file from which you
                            want to purge the queues. The input is a space
                            separated list. (default: None)

## Some real-life examples:

Run workers for the feature_demo.yaml file:

    (venv) $ merlin run-workers workflows/feature_demo/feature_demo.yaml --worker-args "--prefetch-multiplier 1"
    $ celery worker -A merlin --prefetch-multiplier 1 -Q merlin,hello_queue,post_process_queue

Just run workers for the hello step in the file:

    (venv) $ merlin run-workers workflows/feature_demo/feature_demo.yaml --steps hello
    $ celery worker -A merlin -Q hello_queue

Adding `--echo` to a command will just print out the command, so you can move this command into more complex workflows.

For instance, to put into a batch script, or run many workers you can do stuff like this:

    $ srun -n 5 `merlin run-workers workflows/feature_demo/feature_demo.yaml --echo --steps hello`

Which is equivalent to

    $ srun -n 5 celery worker -A merlin -Q hello_queue

Generate a template spec file.

`(venv)` $ merlin template <path/for/spec>

Show pip and python versions and locations. This is useful for troubleshooting.

`(venv) $ merlin info`

Display version number.

`(venv) $ merlin -v` or `(venv) $ merlin --version`

Display these CLI options in-console.

`(venv) $ merlin -h` or `(venv) $ merlin --help`

# Custom Setup

##  Create and activate a Virtual Environment

    $ python -m virtualenv venv
    $ source venv/bin/activate  # Or activate.csh for cshrc.
    (venv) $

## Upgrade Pip

    (venv) $ pip install -U pip

## Add requirements to your environment

    # This will install all package dependencies into the virtualenv.
    (venv) $ pip install -r requirements.txt
    (venv) $ pip install -e .

## Adding MySQL packages

If using MySQL, then the following requirements should also be installed:

    (venv) $ pip install -r requirements/mysql.txt

## Celery

Celery is a distributed task queue that helps spread work over threads and machines.
Before running a distributed job, use:

    (venv) $ celery worker -A merlin -l INFO

For very small tests this may done on a login node, but otherwise celery workers should
be scheduled via the system's batch.
Some useful flags are: `--concurrency N`, `-Ofair`, `--prefetch-multiplier M`.
For more details, see the batch scripts in `merlin/examples/` or type `celery -h` for help.

# redis

The redis system is currently being used to implement the backend server on
rabbit.llnl.gov. This same redis system can be used as the frontend broker
and can also be run on the local allocation instead of a remote server. The
instructions below detail the method for implementing a local broker or
backend.

## Build redis

Download the code from:
    https://redis.io/download

untar the code

    tar xvf redis-4.0.11.tar.gz

Type make in the top level redis code directory.

    make

The executables will be in src.

## Local redis server

In this example, the same redis server is used as the broker and
backend at port 6397 of localhost.

### Start the server

Run redis-server <file> on an allocation node using the default config.

    redis-server redis.conf

### Configure Merlin to use the local redis server

Edit the app.yaml file and use these configurations

    broker:
        name: redis
        server: localhost
        port: 6379

    results_backend:
        name: redis
        server: localhost
        port: 6379

## Local redis+socket

This configuration will use redis+socket for the broker and the same redis as the backend
server connection to port 6397 on the localhost.

Edit the redis.conf file to turn on the socket interface, this method
only works on a single node:

    unixsocket /<socket path>/redis.sock
    unixsocketperm 700

### Start the server

Run redis-server <file> on an allocation node.

    redis-server redis.conf

### Configure Merlin to use the local redis server

Edit the `app.yaml` file and use these configurations:

    broker:
        name: redis+socket
        path: /<socket path>/redis.sock
        socketname: redis.sock

    results_backend:
        name: redis
        server: localhost
        port: 6379

# Testing
## Unit tests
* `(venv) $ make version-tests` or `(venv) $ tox` runs Merlin tests in different Python version environments. See `tox.ini` file for details on what this runs.
* From the main directory, `(venv) $ py.tests merlin/` runs Merlin unit tests.
## Style checks
* `(venv) $ make check-style` or `(venv) $ pylint merlin/` checks for PEP8 infractions in our Python code. 
To customize what is tested for, see `.pylintrc` and `tox.ini` files.

