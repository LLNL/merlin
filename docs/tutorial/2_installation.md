# Installation

!!! info "Prerequisites"

    - shell (bash, csh, etc, if running on Windows, use a linux container)
    - python3 >= python3.8
    - pip3
    - wget
    - build tools (make, C/C++ compiler)
    - (OPTIONAL) docker (required for [4. Run a Real Simulation](4_run_simulation.md))
    - (OPTIONAL) file editor for docker config file editing

!!! info "Estimated Time"

    20 minutes

!!! abstract "You Will Learn"

    - How to install Merlin in a virtual environment using pip.
    - How to install a container platform eg. [Singularity](https://docs.sylabs.io/guides/latest/user-guide/), [Docker](https://www.docker.com/), or [Podman](https://podman.io/).
    - How to configure Merlin.
    - How to test/verify the installation.

This section details the steps necessary to install Merlin and its dependencies. Merlin will then be configured for the local machine and the configuration will be checked to ensure a proper installation.


## Installing Merlin

A Merlin installation is required for the subsequent modules of this tutorial.

Once Merlin is installed, it requires servers to operate. While you are able to host your own servers, we will use Merlin's containerized servers in this tutorial. However, if you prefer to host your own servers you can host a Redis server that is accessible to your current machine. Your computer/organization may already have a Redis server available you can use, please check with your local system administrator.

Create a virtualenv using python3 to install Merlin.

```bash
python3 -m venv --prompt merlin merlin_venv
```

Activate the virtualenv.

=== "bash"

    ```bash
    source merlin_venv/bin/activate
    ```

=== "csh"

    ```bash
    source merlin_venv/bin/activate.csh
    ```

The ``(merlin) <shell prompt>`` will appear after activating.

You should upgrade pip and setuptools before proceeding.

```bash
pip3 install setuptools pip -U
```

Install Merlin through pip.

=== "Latest Version"

    ```bash
    pip3 install merlin
    ```

=== "Specific Version"

    ```bash
    pip3 install merlin==x.y.z
    ```

Check to make sure Merlin installed correctly.

```bash
which merlin
```

You should see that it was installed in your virtualenv, like so:

!!! success

    ```bash
    <path_to_virtualenv>/merlin_venv/bin/merlin
    ```

If this is not the output you see, you may need to restart your virtualenv and try again. 

You'll need the virtualenv activated for the subsequent steps in the tutorial. Once you've finished you can deactivate the virtual environment with:

```bash
deactivate
```

## Redis Server

A [Redis](https://redis.io/) server is required for the [Celery](https://docs.celeryq.dev/en/stable/index.html) results backend server, this same server can also be used for the Celery broker. We will be using Merlin's containerized server however we will need to download one of the supported container platforms avaliable. For the purpose of this tutorial we will be using [Singularity](https://docs.sylabs.io/guides/latest/user-guide/).

### Installing Singularity

Update and install Singularity dependencies:

```bash
apt-get update && apt-get install -y build-essential libssl-dev uuid-dev libgpgme11-dev squashfs-tools libseccomp-dev pkg-config
```

Download dependency [go](https://go.dev/):

```bash
wget https://go.dev/dl/go1.18.1.linux-amd64.tar.gz
```

Extract `go` into `local`:

```bash
tar -C /usr/local -xzf go1.18.1.linux-amd64.tar.gz
```

Remove `go` tar file:

```bash
rm go1.18.1.linux-amd64.tar.gz
```

Update `PATH` to include `go`:

```bash
export PATH=$PATH:/usr/local/go/bin
```

Download Singularity:

```bash
wget https://github.com/sylabs/singularity/releases/download/v3.9.9/singularity-ce-3.9.9.tar.gz
```

Extract Singularity:

```bash
tar -xzf singularity-ce-3.9.9.tar.gz
```

Configure and Install Singularity:

```bash
cd singularity-ce-3.9.9 ./mconfig && make -C ./builddir && sudo make -C ./builddir install
```

## Configuring Merlin

Merlin requires a configuration script for the Celery interface in order to know which server(s) to connect to. Run this configuration method to create the `app.yaml` configuration file.

```bash
merlin config --broker redis
```

The `merlin config` command above will create a file called `app.yaml` in the `~/.merlin` directory. If you are running a Redis server locally then you are all set, look in the `~/.merlin/app.yaml` file to see the configuration, it should look like the configuration below.

???+ abstract "app.yaml"

    ```yaml
    broker:
      name: redis
      server: localhost
      port: 6379
      db_num: 0

    results_backend:
      name: redis
      server: localhost
      port: 6379
      db_num: 0
    ```

More detailed information on configuring Merlin can be found in the [Configuration](../user_guide/configuration/index.md) page.

## Checking/Verifying Installation

First launch the Merlin server containers by using the `merlin server` commands.

Initialize the server files:

```bash
merlin server init
```

This will create a `merlin_server/` folder in the current run directory. The structure of this folder will look like so:

```bash
merlin_server/
|-- redis.conf
|-- redis.pass
|-- redis.users
`-- redis_latest.sif
```

The files in this folder are:

1. `redis.conf`: The Redis configuration file that contains all of the settings to be used for our Redis server
2. `redis.pass`: A password for the Redis server that we'll start up next
3. `redis.users`: A file defining the users that are allowed to access the Redis server and their permissions
4. `redis_latest.sif`: A singularity file that contains the latest Redis docker image that was pulled behind the scenes by Merlin

If you'd like to modify the configuration of your server, you can either modify the files directly or use:

```bash
merlin server config
```

Now that we have the necessary server files initialized, start the server:

```bash
merlin server start
```

With this command, the containerized server should now be started. Notice that two new files were added to the `merlin_server` folder:

1. `merlin_server.pf`: A process file containing information regarding the Redis process
2. `app.yaml`: A new `app.yaml` file configured specifically for the containerized Redis server that we just started

To have Merlin read this configuration, copy it to your current run directory:

```bash
cp merlin_server/app.yaml .
```

You can also make this server container your main server configuration by replacing the one located in your home directory. Make sure you make back-ups of your current `app.yaml` file in case you want to use your previous configurations.

```bash
mv ~/.merlin/app.yaml ~/.merlin/app.yaml.bak
```

```bash
cp ./merlin_server/app.yaml ~/.merlin/
```

!!! note

    Since Merlin servers are created locally on your run directory you are allowed to create multiple instances of Merlin server with their unique configurations for different studies. Simply create different directories for each study and run the following command in each directory to create an instance for each:

    ```bash
    merlin server init
    ```

The `merlin info` command will check that the configuration file is installed correctly, display the server configuration strings, and check server access.

```bash
merlin info
```

If everything is set up correctly, you should see:

???+ success "Expected Output for Successful Config"

    ```
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

## Docker Advanced Installation (Optional)

This optional section details the setup of a RabbitMQ server and a Redis TLS (Transport Layer Security) server for Merlin. For this section, we'll start with the following `docker-compose.yml` file:

???+ abstract "Initial Docker Compose"

    ```yaml title="docker-compose.yml"
    version: '3'

    networks:
      mernet:
        driver: bridge

    services:
      redis:
        image: 'redis:latest'
        container_name: my-redis
        ports:
          - "6379:6379"
        networks:
          - mernet

      merlin:
        image: 'llnl/merlin'
        container_name: my-merlin
        tty: true
        volumes:
          - ~/merlinu/:/home/merlinu
        networks:
          - mernet
    ```

### RabbitMQ Server

A RabbitMQ server can be started to provide the broker, the Redis server will still be required for the backend. Merlin is configured to use ssl encryption for all communication with the RabbitMQ server. An ssl server requires ssl certificates to encrypt the communication through the python ssl module [python ssl](https://docs.python.org/3/library/ssl.html). This tutorial can use self-signed certificates created by the user for use in the RabbitMQ server. The RabbitMQ server uses TLS (often known as "Secure Sockets Layer"). Information on RabbitMQ with TLS can be found here: [RabbitMQ TLS](https://www.rabbitmq.com/ssl.html).

A set of self-signed keys is created through the `tls-gen` package. These keys are then copied to a common directory for use in the RabbitMQ server and python.

```bash
git clone https://github.com/michaelklishin/tls-gen.git
cd tls-gen/basic
make CN=my-rabbit CLIENT_ALT_NAME=my-rabbit SERVER_ALT_NAME=my-rabbit
make verify
mkdir -p ${HOME}/merlinu/cert_rabbitmq
cp result/* ${HOME}/merlinu/cert_rabbitmq
```

The RabbitMQ docker service can be added to the previous `docker-compose.yml` file:

??? abstract "RabbitMQ Docker Compose"

    ```yaml title="docker-compose.yml"
    version: '3'

    networks:
    mernet:
    driver: bridge

    services:
    redis:
    image: 'redis:latest'
    container_name: my-redis
    ports:
      - "6379:6379"
    networks:
      - mernet

    rabbitmq:
    image: rabbitmq:3-management
    container_name: my-rabbit
    tty: true
    ports:
      - "15672:15672"
      - "15671:15671"
      - "5672:5672"
      - "5671:5671"
    environment:
      - RABBITMQ_SSL_CACERTFILE=/cert_rabbitmq/ca_certificate.pem
      - RABBITMQ_SSL_KEYFILE=/cert_rabbitmq/server_key.pem
      - RABBITMQ_SSL_CERTFILE=/cert_rabbitmq/server_certificate.pem
      - RABBITMQ_SSL_VERIFY=verify_none
      - RABBITMQ_SSL_FAIL_IF_NO_PEER_CERT=false
      - RABBITMQ_DEFAULT_USER=merlinu
      - RABBITMQ_DEFAULT_VHOST=/merlinu
      - RABBITMQ_DEFAULT_PASS=guest
    volumes:
      - ~/merlinu/cert_rabbitmq:/cert_rabbitmq
    networks:
      - mernet

    merlin:
    image: 'llnl/merlin'
    container_name: my-merlin
    tty: true
    volumes:
      - ~/merlinu/:/home/merlinu
    networks:
      - mernet

    ```

When running the RabbitMQ broker server, the config can be created with the default `merlin config` command. If you have already run the previous command then remove the `~/.merlin/app.yaml` or `~/merlinu/.merlin/app.yaml` file , and run the `merlin config` command again.

```bash
merlin config
```

The `app.yaml` file will need to be edited to add the RabbitMQ settings in the broker section of the `app.yaml` file. The `server:` should be changed to `my-rabbit`. The RabbitMQ server will be accessed on the default TLS port, 5671.

???+ abstract "RabbitMQ app.yaml"

    ```yaml title="app.yaml"
    broker:
      name: rabbitmq
      server: my-rabbit
      password: ~/.merlin/rabbit.pass

    results_backend:
      name: redis
      server: my-redis
      port: 6379
      db_num: 0
    ```

To complete the config create a password file:

```bash
touch ~/merlinu/.merlin/rabbit.pass
```

Then open the file and add the password `guest`.

The aliases defined previously can be used with this set of docker containers.

### Redis TLS Server

This optional section details the setup of a Redis server with TLS for Merlin. The Redis TLS configuration can be found in the [Security With Redis](../user_guide/configuration/external_server.md#security-with-rediss_1) section. A newer Redis (version 6 or greater) must be used to enable TLS.

A set of self-signed keys is created through the `tls-gen` package. These keys are then copied to a common directory for use in the Redis server and python.

```bash
git clone https://github.com/michaelklishin/tls-gen.git
cd tls-gen/basic
make CN=my-redis CLIENT_ALT_NAME=my-redis SERVER_ALT_NAME=my-redis
make verify
mkdir -p ${HOME}/merlinu/cert_redis
cp result/* ${HOME}/merlinu/cert_redis
```

The configuration below does not use client verification `--tls-auth-clients no` so the ssl files do not need to be defined as shown in the [Security With Redis](../user_guide/configuration/external_server.md#security-with-rediss_1) section.

??? abstract "RabbitMQ & Redis TLS Docker Compose"

    ```yaml title="docker-compose.yml"
    version: '3'

    networks:
      mernet:
        driver: bridge

    services:
      redis:
        image: 'redis'
        container_name: my-redis
        command:
          - --port 0
          - --tls-port 6379
          - --tls-ca-cert-file /cert_redis/ca_certificate.pem
          - --tls-key-file /cert_redis/server_key.pem
          - --tls-cert-file /cert_redis/server_certificate.pem
          - --tls-auth-clients no
        ports:
          - "6379:6379"
        volumes:
          - "~/merlinu/cert_redis:/cert_redis"
        networks:
          - mernet

      rabbitmq:
        image: rabbitmq:3-management
        container_name: my-rabbit
        tty: true
        ports:
          - "15672:15672"
          - "15671:15671"
          - "5672:5672"
          - "5671:5671"
        volumes:
          - "~/merlinu/rabbbitmq.conf:/etc/rabbitmq/rabbitmq.conf"
          - "~/merlinu/cert_rabbitmq:/cert_rambbitmq"
        networks:
          - mernet
    ```

The `rabbitmq.conf` file contains the configuration, including ssl, for the RabbitMQ server.

???+ abstract "RabbitMQ Config with SSL"

    ```title="rabbitmq.conf"
    default_vhost = /merlinu
    default_user = merlinu
    default_pass = guest
    listeners.ssl.default = 5671
    ssl.options.ccertfile = /cert_rabbitmq/ca_certificate.pem
    ssl.options.certfile = /cert_rabbitmq/server_certificate.pem
    ssl.options.keyfile = /cert_rabbitmq/server_key.pem
    ssl.options.verify = verify_none
    ssl.options.fail_if_no_peer_cert = false
    ```

Once this docker-compose file is run, the Merlin `app.yaml` file is changed to use the Redis TLS server `rediss` instead of `redis`.
