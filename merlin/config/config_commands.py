"""
Functions to handle `merlin config` functionality.
"""
import logging
import os
from argparse import Namespace
from importlib import resources
from typing import Dict, List, Union

import yaml

from merlin.study.celeryadapter import create_celery_config

# TODO when we create the task server interface, update these functions to use a setup
# that's closer to what we have for the database functionality rather than using the
# router.py file
# - probably want a base MerlinConfig class (or maybe we call it TaskServerInterface for
#   consistency with the internship?)
#   - this will define abstract methods that all task servers will need
# - can have CeleryInterface that's an inherited class
#   - this can utilize the ResultsBackend class we already have maybe?
#     - right now the ResultsBackend class just handles database interactions; does it make
#       sense to add configuration functionality to that class as well?
#   - will need to create a Broker class too
# - can have TaskVineInterface as an inherited class
#   - not sure if this will need Broker but we'll need to utilize ResultsBackend still for
#     study monitoring and status

LOG = logging.getLogger(__name__)

def create_template_config(task_server: str, config_dir: str, broker: str):
    """
    Create a config for the given task server.

    Args:
        task_server: The task server from which to stop workers.
        config_dir: Optional directory to install the config.
        broker: string indicated the broker, used to check for redis.
    """
    LOG.info("Creating config ...")

    if not os.path.isdir(config_dir):
        os.makedirs(config_dir)

    if task_server == "celery":
        config_file = "app.yaml"
        data_config_file = "app.yaml"
        if broker == "redis":
            data_config_file = "app_redis.yaml"
        with resources.path("merlin.data.celery", data_config_file) as data_file:
            create_celery_config(config_dir, config_file, data_file)
    else:
        LOG.error("Only celery can be configured currently.")


def update_config(args: Namespace, config: Dict[str, Union[str, int]], required_fields: List[str]) -> None:
    """
    Generic function to update configuration fields based on provided arguments.

    Args:
        args: The argparse namespace with arguments from the command line.
        config: The configuration dictionary to update.
        required_fields: List of required field names for the configuration.
    """
    for field in required_fields:
        value = getattr(args, field, None)
        if value is not None:
            if field == "password_file":
                field = "password"
            config[field] = value


def update_redis_config(args: Namespace, config: Dict[str, Union[str, int]], config_type: str) -> None:
    """
    Update the Redis-specific configuration for either broker or backend.

    Args:
        args: The argparse namespace with arguments from the command line.
        config: The configuration dictionary to update.
        config_type: Type of configuration ("broker" or "backend").
    """
    LOG.warning(f"Redis does not use the 'username' or 'vhost' arguments for {config_type}. Ignoring these if provided.")
    config["name"] = "rediss"
    config["username"] = ""  # Redis doesn't use a username

    required_fields = ["password_file", "server", "port", "db_num", "cert_reqs"]
    update_config(args, config, required_fields)


def update_rabbitmq_config(args: Namespace, config: Dict[str, Union[str, int]]) -> None:
    """
    Update the RabbitMQ-specific broker configuration.

    Args:
        args: The argparse namespace with arguments from the command line.
        config: The configuration dictionary to update.
    """
    LOG.warning("RabbitMQ does not use the 'db_num' argument.")
    config["name"] = "rabbitmq"

    required_fields = ["username", "password_file", "server", "port", "vhost", "cert_reqs"]
    update_config(args, config, required_fields)


def update_broker(args: Namespace, config_file: str):
    """
    Update the broker section of the app.yaml file.

    Args:
        args: The argparse namespace with arguments from the command line.
        config_file: Path to the app.yaml file.
    """
    LOG.info(f"Updating broker settings in '{config_file}'...")

    with open(config_file, "r") as app_yaml_file:
        config = yaml.safe_load(app_yaml_file)

    broker_config = config.get("broker", {})
    if args.type == "redis":
        update_redis_config(args, broker_config, "broker")
    elif args.type == "rabbitmq":
        update_rabbitmq_config(args, broker_config)
    else:
        LOG.error("Invalid broker type. Use 'redis' or 'rabbitmq'.")
        return

    config["broker"] = broker_config

    with open(config_file, "w") as app_yaml_file:
        yaml.dump(config, app_yaml_file, default_flow_style=False)

    LOG.info("Broker settings successfully updated. Check your new connection with `merlin info`.")


def update_backend(args: Namespace, config_file: str):
    """
    Update the results backend section of the app.yaml file.

    Args:
        args: The argparse namespace with arguments from the command line.
        config_file: Path to the app.yaml file.
    """
    LOG.info(f"Updating results backend settings in '{config_file}'...")

    with open(config_file, "r") as app_yaml_file:
        config = yaml.safe_load(app_yaml_file)

    backend_config = config.get("results_backend", {})
    if args.type == "redis":
        update_redis_config(args, backend_config, "backend")
    else:
        LOG.error("Invalid backend type. Use 'redis'.")
        return

    config["results_backend"] = backend_config

    with open(config_file, "w") as app_yaml_file:
        yaml.dump(config, app_yaml_file, default_flow_style=False)

    LOG.info("Results backend settings successfully updated.  Check your new connection with `merlin info`.")
