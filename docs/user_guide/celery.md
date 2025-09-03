# Celery With Merlin

Merlin uses [Celery](http://www.celeryproject.org), a Python based distributed task management system. Merlin uses Celery to queue work which is processed by Celery workers.

Merlin queues tasks to the broker which receives and routes tasks. Merlin by default is configured to use [RabbitMQ](https://www.rabbitmq.com/) as the broker but [Redis](https://redis.io/) can be used as well.

Celery has many functions, it defines the interface to the task broker, the backend results database and the workers that will run the tasks.

As discussed in the [Configuration](./configuration/index.md) page, the broker and backend are configured through [the configuration file](./configuration/index.md#the-configuration-file). A configuration for the rabbit AMQP server is shown below.

???+ abstract "Config File for RabbitMQ Broker and Redis Backend"

    ```yaml title="app.yaml"
    celery:
        # directory where Merlin looks for the following:
        # mysql-ca-cert.pem rabbit-client-cert.pem  rabbit-client-key.pem redis.pass
        certs: /path/to/celery/config

    broker:
        # can be rabbitmq, redis, rediss, redis+sock, amqps, or amqp
        name: rabbitmq
        #username: # defaults to your username unless changed here
        password: ~/.merlin/rabbit-password
        # server URL
        server: server.domain.com

        ### for rabbitmq connections ###
        #vhost: # defaults to your username unless changed here

        ### for redis+sock connections ###
        #socketname: the socket name your redis connection can be found on.
        #path: The path to the socket.

        ### for redis/rediss connections ###
        #port: The port number redis is listening on (default 6379)
        #db_num: The data base number to connect to.
        
        # ssl security
        #keyfile: /var/ssl/private/client-key.pem
        #certfile: /var/ssl/amqp-server-cert.pem
        #ca_certs: /var/ssl/myca.pem
        # This is optional and can be required, optional or none
        # (required is the default)
        #cert_reqs: required


    results_backend:
        # Can be redis,rediss, mysql, db+ or memcached server
        # Only a few of these are directly configured by merlin
        name: redis

        dbname: dbname
        username: username
        # name of file where redis password is stored.
        password: redis.pass
        server: server.domain.com
        # merlin will generate this key if it does not exist yet,
        # and will use it to encrypt all data over the wire to
        # your redis server.
        encryption_key: ~/.merlin/encrypt_data_key
        port: 6379
        db_num: 0

        # ssl security
        #keyfile: /var/ssl/private/client-key.pem
        #certfile: /var/ssl/amqp-server-cert.pem
        #ca_certs: /var/ssl/myca.pem
        # This is optional and can be required, optional or none
        # (required is the default)
        #cert_reqs: required
    ```

## Using Celery Commands With Merlin

Typically Merlin will handle all interactions with Celery behind the scenes. However, if you'd like to run Celery commands directly you can.

The Celery command needs application configuration for the specific module that includes Celery, this is specified using the `-A <module>` syntax. All Celery commands should include the `-A` argument. The correct syntax for interacting with your Merlin module is:

```bash
celery -A merlin
```

The merlin run command will define the tasks from the steps in the yaml file and then send them to the broker through the Celery broker interface. If these tasks are no longer needed or are incorrect, they can be purged by using one of these commands:

=== "General Purge"

    The following is equivalent to the [`merlin purge`](./command_line.md#purge-merlin-purge) command.

    ```bash
    celery -A merlin -Q <queue list> purge
    ```

    !!! example

        ```bash
        celery -A merlin -Q merlin,queue2,queue3 purge
        ```

=== "RabbitMQ Purge"

    This will *not* work if you're using a broker other than Rabbit AMQP.

    ```bash
    celery -A merlin amqp queue.purge <queue name>
    ```

    !!! example

        ```bash
        celery -A merlin amqp queue.purge merlin
        ```

=== "RabbitMQ Queue Deletion"

    It's recommended to save this as a last resort if purging does not work for some reason.

    ```bash
    celery -A merlin amqp queue.delete <queue>
    ```

    !!! example

        ```bash
        celery -A merlin amqp queue.delete merlin
        ```

## Configuring Celery Workers

The common configurations used for the Celery workers in the [Celery Workers Guide](https://docs.celeryproject.org/en/latest/userguide/workers.html) are not the best for HPC applications. Here are some parameters you may want to use for HPC specific workflows.

These options can be altered by setting the args for an entry of type worker in the `merlin.resources` section of your yaml spec file.

!!! note

    Merlin uses Celery's [prefork pool](https://celery.school/celery-worker-pools#heading-prefork) so modifying the `--concurrency` value will modify the number of concurrent processes that are started, *not* the number of threads.

    *Celery will set the concurrency to be the number of CPUs on the node by default.*

The number of processes to use on each node of the HPC allocation is set through the `--concurrency` keyword. A good choice for this is the number of simulations that can be run per node or the number of cores on the machine, whichever is smaller.

```bash
celery -A merlin worker --concurrency <num processes>
```

!!! example "Concurrency for Simple 1D Short Running Sim"

    If the HPC simulation is a simple 1D short running sim then on Lassen you might want to use all cores on a node:

    ```bash
    celery -A merlin worker --concurrency 44
    ```

!!! example "Concurrency for Limited Processes"

    If the HPC simulation will take the whole node, you may want to limit this to only a few processes:

    ```bash
    celery -A merlin worker --concurrency 2
    ```

The `--prefetch-multiplier` argument sets how many tasks are requested from the task server per worker process. If `--concurrency` is 2 and `--prefetch-multiplier` is 3, then 6 tasks will be requested from the task server by the worker processes. Since HPC tasks are generally not short running tasks, the recommendation is to set this to 1.

```bash
celery -A merlin worker --prefetch-multiplier <num_tasks>
```

!!! example

    ```bash
    celery -A merlin worker --prefetch-multiplier 1
    ```

The `-O fair` option is another parameter used for long running Celery tasks. With this set, Celery will only send tasks to processes that are available to run them.

```bash
celery -A merlin worker -O fair
```

The `-n` option allows the workers to be given a unique name so multiple workers running tasks from different queues may share the allocation resources. The names are automatically set to `<queue name>.%h`, where `<queue name>` is from the task_queue config or merlin (default) and `%h` will resolve to the hostname of the compute node.

```bash
celery -A merlin worker -n <name>
```

!!! example "Naming the Worker"

    ```bash
    celery -A merlin worker -n merlin.%h
    ```

!!! example "Naming the Worker After a Queue"

    ```bash
    celery -A merlin worker -n queue_1.%h
    ```

On the toss3 nodes, the CPU affinity can be set for the worker processes. This is enabled by setting the environment variable `CELERY_AFFINIITY` to the number of CPUs to skip.

!!! example

    Setting `CELERY_AFFINITY` to 4 will skip 4 CPUs between each Celery worker process.

    ```bash
    export CELERY_AFFINITY=4
    ```
