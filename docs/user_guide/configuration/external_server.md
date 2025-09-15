# External Server Configuration

!!! warning

    It's recommended that you read through the [Configuration Overview](./index.md) page before proceeding with this module.

## Configuring the Broker

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

## Configuring the Results Backend

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
6. `encryption_key`: The path to the encryption key (this is automatically generated by [`merlin config create`](../command_line.md#config-create-merlin-config-create))

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
