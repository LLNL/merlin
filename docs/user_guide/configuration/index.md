# Configuration

!!! note

    Merlin works best configuring [Celery](https://docs.celeryq.dev/en/stable/index.html) to run with a [RabbitMQ](https://www.rabbitmq.com/) broker and a [Redis](https://redis.io/) backend. Merlin uses Celery chords which require a results backend to be configured. The Amqp (rpc RabbitMQ) server does not support chords but the Redis, Database, Memcached and more, support chords.

The [Celery](https://docs.celeryq.dev/en/stable/index.html) library provides several ways to configure both your broker and your results backend. This page will go over why configuration is necessary and will detail the different configurations that Merlin supports.

## Why is Configuration Necessary?

As explained in the [User Guide Landing Page](../index.md), Merlin uses a central server to store tasks in a queue which workers will manage. To establish this functionality, Merlin uses the [Celery](https://docs.celeryq.dev/en/stable/index.html) library. Because of this, Merlin requires users to configure a broker and results backend.

### What is a Broker?

A broker is a message queue that acts as an intermediary between the sender of a task and the worker processes that execute the task. It facilitates the communication between different parts of a distributed system by passing messages (tasks) from producers (the code that generates tasks) to consumers (worker processes that execute tasks).

The broker is responsible for queuing the tasks and delivering them to the appropriate worker processes. It allows for the decoupling of the task producer and the task consumer, enabling a more scalable and flexible architecture.

[Celery supports various message brokers](https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/index.html), including [RabbitMQ](https://www.rabbitmq.com/), [Redis](https://redis.io/), and others. You can configure Celery to use a specific broker based on your requirements (although we suggest using RabbitMQ).

See the [Configuring the Broker and Results Backend](#configuring-the-broker-and-results-backend) section below for more information on configuring your broker.

### What is a Results Backend?

The results backend is a storage system where the results of executed tasks are stored. After a task is executed by a worker, the result is stored in the result backend, and the original task sender can retrieve the result later.

The results backend enables the asynchronous nature of Celery. Instead of blocking and waiting for a task to complete, the sender can continue with other work and later retrieve the result from the results backend.

[Celery supports various results backends](https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/index.html), including databases (such as SQLAlchemy, Django ORM), message brokers (Redis, RabbitMQ), and others. You can configure Celery to use a specific broker based on your requirements (although we suggest using Redis). However, since Merlin utilizes [Celery chords](https://docs.celeryq.dev/en/stable/userguide/canvas.html#chords) and the amqp (rpc RabbitMQ) server does not support chords, we cannot use RabbitMQ as a results backend.

See the [Configuring the Broker and Results Backend](#configuring-the-broker-and-results-backend) section below for more information on configuring your results backend.

## The Configuration File

!!! note

    Prior to Merlin v1.13.0, this configuration file had to be named `app.yaml`. If you hear any references to `app.yaml`, this is why.

In order to read in configuration options for your Celery settings, broker, and results backend, Merlin utilizes a configuration file.

There's a built-in command with Merlin to set up a skeleton configuration file for you:

```bash
merlin config create
```

_*Note:* If you want this file to be at a different path or if you want it to have a different name, use the `-o` option._

This command will create an app.yaml file in the `~/.merlin/` directory that looks like so:

<!--codeinclude-->
[app.yaml](../../../merlin/data/celery/app.yaml)
<!--/codeinclude-->

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
    broker_transport_options: {'visibility_timeout': 86400, 'max_connections': 100, 'socket_timeout': 300, 'retry_policy': {'timeout': 600}}
    broker_connection_timeout: 60
    broker_connection_retry: True
    broker_connection_retry_on_startup: None
    broker_connection_max_retries: 100
    broker_channel_error_retry: False
    broker_failover_strategy: None
    broker_heartbeat: 120
    broker_heartbeat_checkrate: 3.0
    broker_login_method: None
    broker_native_delayed_delivery_queue_type: quorum
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
    gcs_bucket: None
    gcs_project: None
    gcs_base_path: 
    gcs_ttl: 0
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
    redis_socket_timeout: 300
    redis_socket_connect_timeout: 300
    redis_retry_on_timeout: True
    redis_socket_keepalive: True
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
    result_backend_max_retries: 20
    result_backend_base_sleep_between_retries_ms: 10
    result_backend_always_retry: True
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
    database_create_tables_at_setup: True
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
    task_publish_retry_policy: {'interval_start': 10, 'interval_step': 10, 'interval_max': 300}
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
    worker_soft_shutdown_timeout: 0.0
    worker_enable_soft_shutdown_on_idle: False
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
    worker_detect_quorum_queues: True
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

## Configuring the Broker and Results Backend

When it comes to configuring the `broker` and `results_backend` sections of your `app.yaml` file, configuration will depend on the type of user you are and what type of servers you wish to use.

For Livermore Computing (LC) users we recommend configuring with either:

- [Dedicated LaunchIT Servers](https://lc.llnl.gov/confluence/display/MERLIN/LaunchIT+Configuration)
- [Containerized Servers](./containerized_server.md)

For all other users, we recommend configuring with either:

- [Dedicated External Servers](./external_server.md)
- [Containerized Servers](./containerized_server.md)

With any server setup that you use, it is possible to configure everything in your server from the command line. See [`merlin config update-broker`](../command_line.md#config-update-broker-merlin-config-update-broker) and [`merlin config update-backend`](../command_line.md#config-update-backend-merlin-config-update-backend) for more details on how this can be done.

## Switching Between Configurations

It's not uncommon to have two different configuration files with settings to connect to different servers. To switch between servers you can utilize the [`merlin config use`](../command_line.md#config-use-merlin-config-use) command.

For example, you may have one configuration file that uses a RabbitMQ broker located at `/path/to/rabbitmq_config.yaml` and another that uses Redis as a broker located at `/path/to/redis_config.yaml`. If you want to switch to your Redis configuration, use:

```bash
merlin config use /path/to/redis_config.yaml
```
