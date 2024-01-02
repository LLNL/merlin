# Configuration

!!! warning "LC Users"

    We recommend reading [Why is Configuration Necessary?](#why-is-configuration-necessary), [The app.yaml File](#the-appyaml-file), and [The Celery Section](#the-celery-section) below. After that, Livermore Computing (LC) users should visit the [LaunchIT Configuration](https://lc.llnl.gov/confluence/display/MERLIN/LaunchIT+Configuration) page on Merlin's Confluence space.

!!! note

    Merlin works best configuring [Celery](https://docs.celeryq.dev/en/stable/index.html) to run with a [RabbitMQ](https://www.rabbitmq.com/) broker and a [Redis](https://redis.io/) backend. Merlin uses Celery chords which require a results backend to be configured. The Amqp (rpc RabbitMQ) server does not support chords but the Redis, Database, Memcached and more, support chords.

The [Celery](https://docs.celeryq.dev/en/stable/index.html) library provides several ways to configure both your broker and your results backend. This page will go over why configuration is necessary and will detail the different configurations that Merlin supports.

## Why is Configuration Necessary?

As explained in the [User Guide Landing Page](./index.md), Merlin uses a central server to store tasks in a queue which workers will manage. To establish this functionality, Merlin uses the [Celery](https://docs.celeryq.dev/en/stable/index.html) library. Because of this, Merlin requires users to configure a broker and results backend.

### What is a Broker?

A broker is a message queue that acts as an intermediary between the sender of a task and the worker processes that execute the task. It facilitates the communication between different parts of a distributed system by passing messages (tasks) from producers (the code that generates tasks) to consumers (worker processes that execute tasks).

The broker is responsible for queuing the tasks and delivering them to the appropriate worker processes. It allows for the decoupling of the task producer and the task consumer, enabling a more scalable and flexible architecture.

[Celery supports various message brokers](https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/index.html), including [RabbitMQ](https://www.rabbitmq.com/), [Redis](https://redis.io/), and others. You can configure Celery to use a specific broker based on your requirements (although we suggest using RabbitMQ). See the [Broker](#the-broker-section) section below for more information on configuring your broker.

### What is a Results Backend?

The results backend is a storage system where the results of executed tasks are stored. After a task is executed by a worker, the result is stored in the result backend, and the original task sender can retrieve the result later.

The results backend enables the asynchronous nature of Celery. Instead of blocking and waiting for a task to complete, the sender can continue with other work and later retrieve the result from the results backend.

[Celery supports various results backends](https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/index.html), including databases (such as SQLAlchemy, Django ORM), message brokers (Redis, RabbitMQ), and others. You can configure Celery to use a specific broker based on your requirements (although we suggest using Redis). However, since Merlin utilizes [Celery chords](https://docs.celeryq.dev/en/stable/userguide/canvas.html#chords) and the amqp (rpc RabbitMQ) server does not support chords, we cannot use RabbitMQ as a results backend. See the [Results Backend](#the-results-backend-section) section below for more information on configuring your results backend.

## The app.yaml File

In order to read in configuration options for your Celery settings, broker, and results backend, Merlin utilizes an app.yaml file.

There's a built-in command with Merlin to set up a skeleton app.yaml for you:

```bash
merlin config
```

This command will create an app.yaml file in the `~/.merlin/` directory that looks like so:

```yaml
celery:
    # see Celery configuration options
    # https://docs.celeryproject.org/en/stable/userguide/configuration.html
    override:
        visibility_timeout: 86400

broker:
    # can be redis, redis+sock, or rabbitmq
    name: rabbitmq
    #username: # defaults to your username unless changed here
    password: ~/.merlin/jackalope-password
    # server URL
    server: jackalope.llnl.gov

    ### for rabbitmq, redis+sock connections ###
    #vhost: # defaults to your username unless changed here

    ### for redis+sock connections ###
    #socketname: the socket name your redis connection can be found on.
    #path: The path to the socket.

    ### for redis connections ###
    #port: The port number redis is listening on (default 6379)
    #db_num: The data base number to connect to.

results_backend:
    # must be redis
    name: redis
    dbname: mlsi
    username: mlsi
    # name of file where redis password is stored.
    password: redis.pass
    server: jackalope.llnl.gov
    # merlin will generate this key if it does not exist yet,
    # and will use it to encrypt all data over the wire to
    # your redis server.
    encryption_key: ~/.merlin/encrypt_data_key
    port: 6379
    db_num: 0
```

As you can see there are three key sections to Merlin's app.yaml file: `celery`, `broker`, and `results_backend`. The rest of this page will go into more depth on each.

## The Celery Section

In the `celery` section of your app.yaml you can override any Celery settings that you may want to change.

Merlin's default Celery configurations are as follows:

??? abstract "Default Celery Configuration"

    ```yaml
    accept_content: ['pickle']  # DO NOT MODIFY
    result_accept_content: None  # DO NOT MODIFY
    enable_utc: True
    imports: ()
    include: ()
    timezone: None
    beat_max_loop_interval: 0
    beat_schedule: {}
    beat_scheduler: celery.beat:PersistentScheduler
    beat_schedule_filename: celerybeat-schedule
    beat_sync_every: 0
    beat_cron_starting_deadline: None
    broker_url: <set in the broker section of app.yaml>
    broker_read_url: None
    broker_write_url: None
    broker_transport: None
    broker_transport_options: {'visibility_timeout': 86400, 'max_connections': 100}
    broker_connection_timeout: 4
    broker_connection_retry: True
    broker_connection_retry_on_startup: None
    broker_connection_max_retries: 100
    broker_channel_error_retry: False
    broker_failover_strategy: None
    broker_heartbeat: 120
    broker_heartbeat_checkrate: 3.0
    broker_login_method: None
    broker_pool_limit: 0
    broker_use_ssl: <set in the broker section of app.yaml>
    broker_host: <set in the broker section of app.yaml>
    broker_port: <set in the broker section of app.yaml>
    broker_user: <set in the broker section of app.yaml>
    broker_password: <set in the broker section of app.yaml>
    broker_vhost: <set in the broker section of app.yaml>
    cache_backend: None
    cache_backend_options: {}
    cassandra_entry_ttl: None
    cassandra_keyspace: None
    cassandra_port: None
    cassandra_read_consistency: None
    cassandra_servers: None
    cassandra_bundle_path: None
    cassandra_table: None
    cassandra_write_consistency: None
    cassandra_auth_provider: None
    cassandra_auth_kwargs: None
    cassandra_options: {}
    s3_access_key_id: None
    s3_secret_access_key: None
    s3_bucket: None
    s3_base_path: None
    s3_endpoint_url: None
    s3_region: None
    azureblockblob_container_name: celery
    azureblockblob_retry_initial_backoff_sec: 2
    azureblockblob_retry_increment_base: 2
    azureblockblob_retry_max_attempts: 3
    azureblockblob_base_path: 
    azureblockblob_connection_timeout: 20
    azureblockblob_read_timeout: 120
    control_queue_ttl: 300.0
    control_queue_expires: 10.0
    control_exchange: celery  # DO NOT MODIFY
    couchbase_backend_settings: None
    arangodb_backend_settings: None
    mongodb_backend_settings: None
    cosmosdbsql_database_name: celerydb
    cosmosdbsql_collection_name: celerycol
    cosmosdbsql_consistency_level: Session
    cosmosdbsql_max_retry_attempts: 9
    cosmosdbsql_max_retry_wait_time: 30
    event_queue_expires: 60.0
    event_queue_ttl: 5.0
    event_queue_prefix: celeryev
    event_serializer: json  # DO NOT MODIFY
    event_exchange: celeryev  # DO NOT MODIFY
    redis_backend_use_ssl: <set in results_backend section of app.yaml>
    redis_db: <set in results_backend section of app.yaml>
    redis_host: <set in results_backend section of app.yaml>
    redis_max_connections: 100000
    redis_username: <set in results_backend section of app.yaml>
    redis_password: <set in results_backend section of app.yaml>
    redis_port: <set in results_backend section of app.yaml>
    redis_socket_timeout: 120.0
    redis_socket_connect_timeout: None
    redis_retry_on_timeout: False
    redis_socket_keepalive: False
    result_backend: <set in results_backend section of app.yaml>
    result_cache_max: -1
    result_compression: None
    result_exchange: celeryresults
    result_exchange_type: direct
    result_expires: 1 day, 0:00:00
    result_persistent: None
    result_extended: False
    result_serializer: pickle  # DO NOT MODIFY
    result_backend_transport_options: {}
    result_chord_retry_interval: 1.0
    result_chord_join_timeout: 3.0
    result_backend_max_sleep_between_retries_ms: 10000
    result_backend_max_retries: inf
    result_backend_base_sleep_between_retries_ms: 10
    result_backend_always_retry: False
    elasticsearch_retry_on_timeout: None
    elasticsearch_max_retries: None
    elasticsearch_timeout: None
    elasticsearch_save_meta_as_text: True
    security_certificate: None
    security_cert_store: None
    security_key: None
    security_key_password: None
    security_digest: sha256
    database_url: None
    database_engine_options: None
    database_short_lived_sessions: False
    database_table_schemas: None
    database_table_names: None
    task_acks_late: True  # DO NOT MODIFY
    task_acks_on_failure_or_timeout: True  # DO NOT MODIFY
    task_always_eager: False  # DO NOT MODIFY
    task_annotations: None  # DO NOT MODIFY
    task_compression: None  # DO NOT MODIFY
    task_create_missing_queues: True  # DO NOT MODIFY
    task_inherit_parent_priority: False  # DO NOT MODIFY
    task_default_delivery_mode: 2  # DO NOT MODIFY
    task_default_queue: merlin  # DO NOT MODIFY
    task_default_exchange: None  # DO NOT MODIFY
    task_default_exchange_type: direct  # DO NOT MODIFY
    task_default_routing_key: None  # DO NOT MODIFY
    task_default_rate_limit: None
    task_default_priority: 5  # DO NOT MODIFY
    task_eager_propagates: False
    task_ignore_result: False
    task_store_eager_result: False
    task_protocol: 2  # DO NOT MODIFY
    task_publish_retry: True
    task_publish_retry_policy: {'interval_start': 10, 'interval_step': 10, 'interval_max': 60}
    task_queues: None  # DO NOT MODIFY
    task_queue_max_priority: 10  # DO NOT MODIFY
    task_reject_on_worker_lost: True  # DO NOT MODIFY
    task_remote_tracebacks: False  
    task_routes: (<function route_for_task at 0x0123456789ab>,)  # DO NOT MODIFY
    task_send_sent_event: False
    task_serializer: pickle  # DO NOT MODIFY
    task_soft_time_limit: None
    task_time_limit: None
    task_store_errors_even_if_ignored: False
    task_track_started: False
    task_allow_error_cb_on_chord_header: False
    worker_agent: None  # DO NOT MODIFY
    worker_autoscaler: celery.worker.autoscale:Autoscaler  # DO NOT MODIFY
    worker_cancel_long_running_tasks_on_connection_loss: True
    worker_concurrency: None  # DO NOT MODIFY; this will be set on a worker-by-worker basis that you can customize in your spec file
    worker_consumer: celery.worker.consumer:Consumer  # DO NOT MODIFY
    worker_direct: False  # DO NOT MODIFY
    worker_disable_rate_limits: False
    worker_deduplicate_successful_tasks: False
    worker_enable_remote_control: True
    worker_hijack_root_logger: True
    worker_log_color: True
    worker_log_format: [%(asctime)s: %(levelname)s] %(message)s
    worker_lost_wait: 10.0
    worker_max_memory_per_child: None
    worker_max_tasks_per_child: None
    worker_pool: prefork
    worker_pool_putlocks: True  # DO NOT MODIFY
    worker_pool_restarts: False
    worker_proc_alive_timeout: 4.0
    worker_prefetch_multiplier: 4  # this can be modified on a worker-by-worker basis in your spec file
    worker_redirect_stdouts: True  # DO NOT MODIFY
    worker_redirect_stdouts_level: WARNING
    worker_send_task_events: False
    worker_state_db: None
    worker_task_log_format: [%(asctime)s: %(levelname)s] [%(task_name)s(%(task_id)s)] %(message)s
    worker_timer: None
    worker_timer_precision: 1.0
    deprecated_settings: set()
    visibility_timeout: 86400
    ```

See [Celery's Configuration Settings](https://docs.celeryq.dev/en/stable/userguide/configuration.html#new-lowercase-settings) for more information on each of these settings.

Overriding these settings is as simple as listing a new key-value pair in the `celery.override` section of your app.yaml.

!!! example

    To change the `visibility_timeout` and `broker_pool_limit` settings, we'd modify the `celery.override` section of our app.yaml like so:

    ```yaml
    celery:
        override:
            broker_pool_limit: 10
            visibility_timeout: 75000
    ```

## The Broker Section

In the `broker` section of your app.yaml you will provide all of the necessary settings for Merlin to establish a connection with your broker.

Here, we'll discuss:

- The different [Broker Options](#broker-options) that are allowed
- How the [password field is resolved](#resolving-the-broker-password-field)
- The [URL](#broker-url) option

### Broker Options

Merlin allows for several different broker configurations. This section will detail each of these options and how to configure them. **We recommend using RabbitMQ as your broker.**

#### RabbitMQ, AMQPS, and AMQP

See the [RabbitMQ Documentation](https://rabbitmq.com/) for instructions on how to create a RabbitMQ server.

Once your server is set up, we'll need six keys in the `broker` section of the app.yaml file:

1. `name`: The name of the broker (options are `rabbitmq`, `amqps`, or `amqp`)
2. `username`: Username for RabbitMQ user that will be used for accessing the service
3. `password`: The path to the file that's storing your RabbitMQ password
4. `server`: A URL to the server that you're connecting
5. `port`: The port that your server is running on. If left undefined and the `name` setting is:
    - `rabbitmq` or `amqps` the default RabbitMQ TLS port (5671) will be used
    - `amqp` the default port will be 5672
6. `vhost`: The vhost for your RabbitMQ service

Using these settings, Merlin will construct a connection string of the form:

```bash
{conn}://{username}:{password}@{server}:{port}/{vhost}
```

Here `conn` is `amqps` (with ssl) when the `name` field is `rabbitmq` or `amqps`, and `amqp` (without ssl) when the `name` field is `amqp`. If you're using a connection option with ssl, see the [Security With RabbitMQ](#security-with-rabbitmq) section below.

!!! example

    Let's say we create a file in our `~/.merlin/` directory called `rabbit.pass`. In this file we'll store the password for our RabbitMQ server:

    ```bash title="~/.merlin/rabbit.pass" linenums="1"
    my-super-secret-password
    ```

    Now we'll update the `broker` section of our app.yaml file to be:

    ```yaml title="~/.merlin/app.yaml"
    broker:
        name: rabbitmq
        username: rabbit-username
        password: ~/.merlin/rabbit.pass
        server: server.domain.com
        vhost: rabbit-vhost
    ```

    The connection string that Merlin generates will then become:

    ```bash
    amqps://rabbit-username:my-super-secret-password@server.domain.com:5671/rabbit-vhost
    ```

##### Security With RabbitMQ

Merlin can only be configured to communicate with [RabbitMQ over an SSL connection](https://www.rabbitmq.com/ssl.html) and does not permit use of a RabbitMQ server configured without SSL. Therefore, the default value of the `broker_use_ssl` celery argument is `True` for RabbitMQ.

The keys can be given in the broker config as shown below:

```yaml
broker:
    name: rabbitmq
    username: rabbit-username
    password: ~/.merlin/rabbit.pass
    server: server.domain.com
    vhost: rabbit-vhost

    # ssl security
    keyfile: /var/ssl/private/client-key.pem
    certfile: /var/ssl/amqp-server-cert.pem
    ca_certs: /var/ssl/myca.pem
    # This is optional and can be required, optional or none
    # (required is the default)
    cert_reqs: required
```

The ssl config with rabbitmq/amqps in the broker is then placed in the `broker_use_ssl` celery argument.

```py
broker_use_ssl = {
  'keyfile': '/var/ssl/private/client-key.pem',
  'certfile': '/var/ssl/amqp-server-cert.pem',
  'ca_certs': '/var/ssl/myca.pem',
  'cert_reqs': ssl.CERT_REQUIRED
}
```

#### Redis and Rediss

!!! note

    If you're using Redis v6.0.0+ and would like to configure with ssl, you'll need to view the [Security With Rediss](#security-with-rediss) section below after completing this section.

See the [Redis Documentation](https://redis.io/) for instructions on how to create a Redis server.

Once your server is set up, we'll need five keys in the `broker` section of the app.yaml file:

1. `name`: The name of the broker (here it will be `redis` if running *without* ssl, or `rediss` if running *with* ssl)
2. `password`: The path to the file that's storing your Redis password
3. `server`: A URL to the server that you're connecting
4. `port`: The port that your server is running on. Default is 6379.
5. `db_num`: The database index (this will likely be 0).

Using these settings, Merlin will construct a connection string of the form:

```bash
{name}://:{password}@{server}:{port}/{db_num}
```

If using ssl, see [Security With Rediss](#security-with-rediss) for additional setup instructions.

!!! example

    Let's say we create a file in our `~/.merlin/` directory called `redis.pass`. In this file we'll store the password for our Redis server:

    ```bash title="~/.merlin/redis.pass" linenums="1"
    my-super-secret-password
    ```

    Now we'll update the `broker` section of our app.yaml file to be:

    ```yaml title="~/.merlin/app.yaml"
    broker:
        name: redis
        password: ~/.merlin/redis.pass
        server: server.domain.com
        port: 6379
        db_num: 0
    ```

    The connection string that Merlin generates will then become:

    ```bash
    redis://:my-super-secret-password@server.domain.com:6379/0
    ```

##### Security With Rediss

When using Redis with ssl, aka rediss, (only available with Redis v6.0.0+), there are some additional keys that you'll need to add to your `broker` section:

```yaml
broker:
    name: rediss
    password: ~/.merlin/redis.pass
    server: server.domain.com
    port: 6379
    db_num: 0


    # ssl security
    keyfile: /var/ssl/private/client-key.pem
    certfile: /var/ssl/amqp-server-cert.pem
    ca_certs: /var/ssl/myca.pem
    # This is optional and can be required, optional or none
    # (required is the default)
    cert_reqs: required
```

The ssl config with redis (rediss) in the broker is then placed in the `broker_use_ssl` celery argument.

```python
broker_use_ssl = {
  'ssl_keyfile': '/var/ssl/private/client-key.pem',
  'ssl_certfile': '/var/ssl/amqp-server-cert.pem',
  'ssl_ca_certs': '/var/ssl/myca.pem',
  'ssl_cert_reqs': ssl.CERT_REQUIRED
}
```

#### Redis+Socket

Celery supports Redis connections using Unix domain sockets. For this setup, three keys are required in the `broker` section:

1. `name`: The name of the broker. This will be `redis+socket` here.
2. `path`: The path to the Unix domain socket file for Redis
3. `db_num`: The database index

Using these settings, Merlin will construct a connection string of the form:

```bash
redis+socket://{path}?virtual_host={db_num}
```

!!! example

    Let's set the `broker` configuration to be:

    ```yaml title="~/.merlin/app.yaml"
    broker:
        name: redis+socket
        path: /tmp/username/redis.sock
        db_num: 0
    ```

    The connection string that Merlin generates will then become:

    ```bash
    redis+socket:///tmp/username/redis.sock?virtual_host=0
    ```

### Resolving The Broker Password Field

The `broker/password` is simply the full path to a file containing your password for the user defined by `broker/username`.

!!! example

    Say the password to our server is `my-super-secret-password`. We'd simply take this password, place it in a file, and then link the path to the file in the `broker` section.

    ```bash title="~/.merlin/password-file.pass" linenums="1"
    my-super-secret-password
    ```

    ```yaml title="~/.merlin/app.yaml"
    broker:
        password: ~/.merlin/password-file.pass
    ```

### Broker URL

A `url` option is available to specify the broker connection url, in which case the server name is ignored. The url must include the entire connection url except the ssl if the broker name is recognized by the ssl processing system. Currently the ssl system will only configure the Rabbitmq and Redis servers.

Using the `url` setting, Merlin will construct a connection string of the form:

```bash
{url}
```

!!! example

    Say we use the default Redis server for our broker:

    ```yaml title="~/.merlin/app.yaml"
    broker:
        url: redis://localhost:6379/0
    ```

    The connection string that Merlin generates will then become:

    ```bash
    redis://localhost:6379/0
    ```

## The Results Backend Section

In the `results_backend` section of your app.yaml you will provide all of the necessary settings for Merlin to establish a connection with your results backend.

Here, we'll discuss:

- The different [Results Backend Options](#results-backend-options) that are allowed
- How the [password field is resolved](#resolving-the-results-backend-password-field)
- The [URL](#results-backend-url) option

### Results Backend Options

Merlin allows for several different results backend configurations. This section will detail each of these options and how to configure them. **We recommend using Redis as your results backend.**

#### Redis and Rediss

!!! note

    If you're using Redis v6.0.0+ and would like to configure with ssl, you'll need to view the [Security With Rediss](#security-with-rediss_1) section below after completing this section.

The recommended option to use for your results backend is a Redis server. See the [Redis Documentation](https://redis.io/) for instructions on how to create a Redis server.

Once your server is set up, we'll need six keys in the `results_backend` section of the app.yaml file:

1. `name`: The name of the results backend (here it will be `redis` if running *without* ssl, or `rediss` if running *with* ssl)
2. `password`: The path to the file that's storing your Redis password
3. `server`: A URL to the server that you're connecting
4. `port`: The port that your server is running on. Default is 6379.
5. `db_num`: The database index (this will likely be 0).
6. `encryption_key`: The path to the encryption key (this is automatically generated by `merlin config`)

Using these settings, Merlin will construct a connection string of the form:

```bash
{name}://:{password}{server}:{port}/{db_num}
```

To further understand what the `encryption_key` is for, see [Security With Redis](#security-with-redis).

If using ssl, see [Security With Rediss](#security-with-rediss_1) for additional setup instructions.

!!! example

    Let's say we create a file in our `~/.merlin/` directory called `redis.pass`. In this file we'll store the password for our Redis server:

    ```bash title="~/.merlin/redis.pass" linenums="1"
    my-super-secret-password
    ```

    Now we'll update the `results_backend` section of our app.yaml file to be:

    ```yaml title="~/.merlin/app.yaml"
    results_backend:
        name: redis
        password: ~/.merlin/redis.pass
        server: server.domain.com
        port: 6379
        db_num: 0
        encryption_key: ~/.merlin/encrypt_data_key
    ```

    The connection string that Merlin generates will then become:

    ```bash
    redis://:my-super-secret-password@server.domain.com:6379/0
    ```

##### Security With Redis

Redis versions less than 6 do not natively support multiple users or SSL. We address security concerns here by redefining the core Celery routine that communicates with
redis to encrypt all data it sends to redis and then decrypt anything it receives. Each user should have their own encryption key as defined by 
`results_backend/encryption_key` in the app.yaml file. Merlin will generate a key if that key does not yet exist.

##### Security With Rediss

When using Redis with ssl, aka rediss, (only available with Redis v6.0.0+), there are some additional keys that you'll need to add to your `results_backend` section:

```yaml
results_backend:
    name: rediss
    password: ~/.merlin/redis.pass
    server: server.domain.com
    port: 6379
    db_num: 0
    encryption_key: ~/.merlin/encrypt_data_key


    # ssl security
    keyfile: /var/ssl/private/client-key.pem
    certfile: /var/ssl/amqp-server-cert.pem
    ca_certs: /var/ssl/myca.pem
    # This is optional and can be required, optional or none
    # (required is the default)
    cert_reqs: required
```

The ssl config with redis (rediss) in the results backend is then placed in the `redis_backend_use_ssl` celery argument.

```python
redis_backend_use_ssl = {
  'ssl_keyfile': '/var/ssl/private/client-key.pem',
  'ssl_certfile': '/var/ssl/amqp-server-cert.pem',
  'ssl_ca_certs': '/var/ssl/myca.pem',
  'ssl_cert_reqs': ssl.CERT_REQUIRED
}
```

#### MySQL

Coming soon!

### Resolving The Results Backend Password Field

The `results_backend/password` is interpreted in the following way. First, it is treated as an absolute path to a file containing your backend password. If that path doesn't exist, it then looks for a file of that name under the directory defined under `celery/certs`. If that file doesn't exist, it then treats `results_backend/password` as the password itself.

### Results Backend URL

A `url` option is available to specify the results backend connection url, in which case the server name is ignored. The url must include the entire connection url including the ssl configuration.

Using the `url` setting, Merlin will construct a connection string of the form:

```bash
{url}
```

!!! example

    Say we use the default Redis server for our results backend:

    ```yaml title="~/.merlin/app.yaml"
    results_backned:
        url: redis://localhost:6379/0
    ```

    The connection string that Merlin generates will then become:

    ```bash
    redis://localhost:6379/0
    ```

The `url` option can also be used to define a server that is not explicitly handled by the merlin configuration system.

!!! example

    Say we have a PostgreSQL database that we want to connect. We can simply copy/paste the url to the `results_backend` section:

    ```yaml title="~/.merlin/app.yaml"
    results_backend:
        url: db+postgresql://scott:tiger@localhost/mydatabase
    ```
