# Docker

Merlin has a simple Dockerfile description for running a container with all requirements installed.

## Build the Container

The docker container can be built by building in the top level Merlin directory.

```bash
docker build -t merlin .
```

This will create a `merlin:latest` image in your docker image collection with a user "merlinu" and a WORKDIR set to `/home/merlinu`.

```bash
docker images
```

## Run the Container

Here we'll discuss:

- Starting a broker and results backend server
- Pulling and running Merlin
- Linking Merlin to the broker and results backend server
- Aliasing the docker `merlin` and `celery` commands
- Running an example

### Starting a Broker/Results Server

Before we can run Merlin we first need to start servers for the broker and results backend (see [Why is Configuration Necessary?](./configuration/index.md#why-is-configuration-necessary)). The broker server can be either Redis or RabbitMQ. For this demonstration a Redis server will be used. The results backend will always be a Redis server.

Pull the Redis image:

```bash
docker pull redis
```

Name and run the Redis container:

```bash
docker run -d -p 6379:6379 --name my-redis redis
```

Our Redis server that we'll use for both the broker and results backend is now running.

### Starting Merlin

Now that our Redis server is set up, all that's left is to start Merlin and link the server.

First, let's create a local working directory:

```bash
mkdir $HOME/merlinu ; cd $HOME/merlinu
```

Next, pull the Merlin image:

```bash
docker pull llnl/merlin
```

!!! tip

    A shell can be started in the container by using the `--entrypoint` command. If the user would like to examine the container contents, they can use a shell as the entry point.

    ```bash
    docker run --rm -ti --volume "$HOME/merlinu":/home/merlinu --entrypoint="/bin/bash" merlin
    ```

Now we'll run Merlin in detached mode to provide both the `merlin` and `celery` commands. Here we'll also link the Redis server we started with the `--link` option and provide a local output directory with the `--volume` argument (it's recommended that a fixed directory be used for `--volume`).

```bash
docker run --rm -td --name my-merlin --link my-redis --volume "$HOME/merlinu":/home/merlinu llnl/merlin
```

To finish linking the server, edit the `$HOME/merlinu/.merlin/app.yaml` so that the broker and results backend variables are `my-redis`.

Finally, we can alias the docker commands for `merlin` and `celery` to make things easier for us:

```bash
alias merlin="docker exec my-merlin merlin" ; alias celery="docker exec my-merlin celery"
```

Congratulations, Merlin is now running!

### Running an Example

To test that Merlin is running properly, we can grab a [built-in Merlin example](../examples/index.md) and try running it. For this demonstration we'll get the [feature_demo example](../examples/feature_demo.md) with:

```bash
merlin example feature_demo
```

We can first do a dry run without workers:

```bash
merlin run feature_demo/feature_demo.yaml --dry --local
```

If this ran successfully you should see an output directory named `studies/feature_demo_<timestamp>`. Then, running

```bash
tree studies/feature_demo_<timestamp>
```

...should provide the following output:

???+ success

    ```bash
    studies/feature_demo_<timestamp>/
    ├── collect
    │   └── X2.0.5
    │       └── collect_X2.0.5.sh
    ├── hello
    │   └── X2.0.5
    │       ├── 00
    │       │   └── hello_X2.0.5.sh
    │       ├── 01
    │       │   └── hello_X2.0.5.sh
    │       ├── 02
    │       │   └── hello_X2.0.5.sh
    │       ├── 03
    │       │   └── hello_X2.0.5.sh
    │       ├── 04
    │       │   └── hello_X2.0.5.sh
    │       ├── 05
    │       │   └── hello_X2.0.5.sh
    │       ├── 06
    │       │   └── hello_X2.0.5.sh
    │       ├── 07
    │       │   └── hello_X2.0.5.sh
    │       ├── 08
    │       │   └── hello_X2.0.5.sh
    │       └── 09
    │           └── hello_X2.0.5.sh
    ├── learn
    │   └── X2.0.5
    │       └── learn_X2.0.5.sh
    ├── make_new_samples
    │   └── N_NEW.10
    │       └── make_new_samples_N_NEW.10.sh
    ├── merlin_info
    │   ├── cmd.err
    │   ├── cmd.out
    │   ├── cmd.sh
    │   ├── feature_demo.expanded.yaml
    │   ├── feature_demo.orig.yaml
    │   ├── feature_demo.partial.yaml
    │   ├── samples.npy
    │   └── scripts
    │       ├── features.json
    │       ├── hello_world.py
    │       ├── pgen.py
    │       └── __pycache__
    │           └── pgen.cpython-310.pyc
    ├── predict
    │   └── N_NEW.10.X2.0.5
    │       └── predict_N_NEW.10.X2.0.5.sh
    ├── python2_hello
    │   └── X2.0.5
    │       └── python2_hello_X2.0.5.sh
    ├── python3_hello
    │   └── X2.0.5
    │       └── python3_hello_X2.0.5.sh
    ├── translate
    │   └── X2.0.5
    │       └── translate_X2.0.5.sh
    └── verify
        └── N_NEW.10.X2.0.5
            └── verify_N_NEW.10.X2.0.5.sh
    ```

Now that we know a dry run works properly, we can try a real run with workers. To do this, run the following two commands ([`merlin run`](./command_line.md#run-merlin-run) and [`merlin run-workers`](./command_line.md#run-workers-merlin-run-workers)) in any order you choose:

=== "Queue Tasks"

    Define the tasks and load them on the broker with:

    ```bash
    merlin run feature_demo/feature_demo.yaml
    ```

=== "Start Workers"

    Start workers to pull tasks from the server and run them in the container with:

    ```bash
    merlin run-workers feature_demo/feature_demo.yaml
    ```

Once all tasks are done processing, running the tree command on the workspace should show:

???+ success

    ```bash
    studies/feature_demo_20240108-091708/
    ├── collect
    │   └── X2.0.5
    │       ├── collect_X2.0.5.err
    │       ├── collect_X2.0.5.out
    │       ├── collect_X2.0.5.sh
    │       ├── files_to_collect.txt
    │       ├── MERLIN_FINISHED
    │       └── results.json
    ├── hello
    │   └── X2.0.5
    │       ├── 00
    │       │   ├── hello_world_output_0.json
    │       │   ├── hello_X2.0.5.err
    │       │   ├── hello_X2.0.5.out
    │       │   ├── hello_X2.0.5.sh
    │       │   └── MERLIN_FINISHED
    │       ├── 01
    │       │   ├── hello_world_output_1.json
    │       │   ├── hello_X2.0.5.err
    │       │   ├── hello_X2.0.5.out
    │       │   ├── hello_X2.0.5.sh
    │       │   └── MERLIN_FINISHED
    │       ├── 02
    │       │   ├── hello_world_output_2.json
    │       │   ├── hello_X2.0.5.err
    │       │   ├── hello_X2.0.5.out
    │       │   ├── hello_X2.0.5.sh
    │       │   └── MERLIN_FINISHED
    │       ├── 03
    │       │   ├── hello_world_output_3.json
    │       │   ├── hello_X2.0.5.err
    │       │   ├── hello_X2.0.5.out
    │       │   ├── hello_X2.0.5.sh
    │       │   └── MERLIN_FINISHED
    │       ├── 04
    │       │   ├── hello_world_output_4.json
    │       │   ├── hello_X2.0.5.err
    │       │   ├── hello_X2.0.5.out
    │       │   ├── hello_X2.0.5.sh
    │       │   └── MERLIN_FINISHED
    │       ├── 05
    │       │   ├── hello_world_output_5.json
    │       │   ├── hello_X2.0.5.err
    │       │   ├── hello_X2.0.5.out
    │       │   ├── hello_X2.0.5.sh
    │       │   └── MERLIN_FINISHED
    │       ├── 06
    │       │   ├── hello_world_output_6.json
    │       │   ├── hello_X2.0.5.err
    │       │   ├── hello_X2.0.5.out
    │       │   ├── hello_X2.0.5.sh
    │       │   └── MERLIN_FINISHED
    │       ├── 07
    │       │   ├── hello_world_output_7.json
    │       │   ├── hello_X2.0.5.err
    │       │   ├── hello_X2.0.5.out
    │       │   ├── hello_X2.0.5.sh
    │       │   └── MERLIN_FINISHED
    │       ├── 08
    │       │   ├── hello_world_output_8.json
    │       │   ├── hello_X2.0.5.err
    │       │   ├── hello_X2.0.5.out
    │       │   ├── hello_X2.0.5.sh
    │       │   └── MERLIN_FINISHED
    │       └── 09
    │           ├── hello_world_output_9.json
    │           ├── hello_X2.0.5.err
    │           ├── hello_X2.0.5.out
    │           ├── hello_X2.0.5.sh
    │           └── MERLIN_FINISHED
    ├── learn
    │   └── X2.0.5
    │       ├── learn_X2.0.5.err
    │       ├── learn_X2.0.5.out
    │       ├── learn_X2.0.5.sh
    │       ├── MERLIN_FINISHED
    │       └── random_forest_reg.pkl
    ├── make_new_samples
    │   └── N_NEW.10
    │       ├── grid_10.npy
    │       ├── make_new_samples_N_NEW.10.err
    │       ├── make_new_samples_N_NEW.10.out
    │       ├── make_new_samples_N_NEW.10.sh
    │       └── MERLIN_FINISHED
    ├── merlin_info
    │   ├── cmd.err
    │   ├── cmd.out
    │   ├── cmd.sh
    │   ├── feature_demo.expanded.yaml
    │   ├── feature_demo.orig.yaml
    │   ├── feature_demo.partial.yaml
    │   ├── samples.npy
    │   └── scripts
    │       ├── features.json
    │       ├── hello_world.py
    │       ├── pgen.py
    │       └── __pycache__
    │           └── pgen.cpython-310.pyc
    ├── predict
    │   └── N_NEW.10.X2.0.5
    │       ├── MERLIN_FINISHED
    │       ├── prediction_10.npy
    │       ├── predict_N_NEW.10.X2.0.5.err
    │       ├── predict_N_NEW.10.X2.0.5.out
    │       └── predict_N_NEW.10.X2.0.5.sh
    ├── python2_hello
    │   └── X2.0.5
    │       ├── MERLIN_FINISHED
    │       ├── python2_hello_X2.0.5.err
    │       ├── python2_hello_X2.0.5.out
    │       └── python2_hello_X2.0.5.sh
    ├── python3_hello
    │   └── X2.0.5
    │       ├── MERLIN_FINISHED
    │       ├── python3_hello_X2.0.5.err
    │       ├── python3_hello_X2.0.5.out
    │       └── python3_hello_X2.0.5.sh
    ├── translate
    │   └── X2.0.5
    │       ├── MERLIN_FINISHED
    │       ├── results.npz
    │       ├── translate_X2.0.5.err
    │       ├── translate_X2.0.5.out
    │       └── translate_X2.0.5.sh
    └── verify
        └── N_NEW.10.X2.0.5
            ├── FINISHED
            ├── MERLIN_FINISHED
            ├── verify_N_NEW.10.X2.0.5.err
            ├── verify_N_NEW.10.X2.0.5.out
            └── verify_N_NEW.10.X2.0.5.sh
    ```

For more information on what's going on in this example, see the [Feature Demo Example page](../examples/feature_demo.md).
