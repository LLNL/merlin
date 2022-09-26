Merlin Server Configuration
===========================

Below are a sample list of configurations for the merlin server command

Main Configuration ``~/.merlin/server/``
----------------------------------------

merlin_server.yaml

.. code-block:: yaml

    container:
        # Select the format for the recipe e.g. singularity, docker, podman (currently singularity is the only working option.)
        format: singularity
        # The image name
        image: redis_latest.sif
        # The url to pull the image from
        url: docker://redis
        # The config file
        config: redis.conf
        # Subdirectory name to store configurations Default: merlin_server/
        config_dir: merlin_server/
        # Process file containing information regarding the redis process
        pfile: merlin_server.pf

    process:
        # Command for determining the process of the command
        status: pgrep -P {pid} #ps -e | grep {pid}
        # Command for killing process
        kill: kill {pid}


singularity.yaml

.. code-block:: yaml

    singularity:
        command: singularity
        # init_command: \{command} .. (optional or default)
        run_command: \{command} run {image} {config}
        stop_command: kill # \{command} (optional or kill default)
        pull_command: \{command} pull {image} {url}


Local Configuration ``merlin_server/``
--------------------------------------

redis.conf

.. code-block:: yaml

    bind 127.0.0.1 -::1
    protected-mode yes
    port 6379
    logfile ""
    dir ./
    ...

see documentation on redis configuration `here <https://redis.io/docs/manual/config/>`_ for more detail

merlin_server.pf

.. code-block:: yaml

    bits: '64'
    commit: '00000000'
    hostname: ubuntu
    image_pid: '1111'
    mode: standalone
    modified: '0'
    parent_pid: 1112
    port: '6379'
    version: 6.2.6

