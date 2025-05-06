"""
This module provides the `MerlinConfigManager` class, which is responsible for managing
Merlin configuration files. It allows users to initialize, create, and update configuration
files for the Merlin application, including broker and results backend settings.
"""

import logging
import os
import shutil
from argparse import Namespace
from importlib import resources
from typing import Dict, List, Union

import yaml

from merlin.config.config_filepaths import CONFIG_PATH_FILE, MERLIN_HOME


LOG = logging.getLogger(__name__)


class MerlinConfigManager:
    """
    A class to manage the configuration of the Merlin application.

    This class provides functionality to initialize, create, and update configuration files
    for the Merlin application. It supports updating broker and results backend settings
    for Redis and RabbitMQ, and allows users to switch between multiple configurations
    using a `config_path.txt` file.

    Attributes:
        args (Namespace): Parsed command-line arguments.
        output_dir (str): The directory where configuration files are stored.
        config_file (str): The path to the current configuration file.

    Methods:
        ensure_directory_exists:
            Ensures the output directory exists, creating it if necessary.

        save_config_path:
            Saves the path to the configuration file in `config_path.txt`.

        create_template_config:
            Creates a template configuration file based on user input.

        update_broker:
            Updates the broker section of the configuration file.

        update_backend:
            Updates the results backend section of the configuration file.

        update_redis_config:
            Updates Redis-specific configuration fields for broker or backend.

        update_rabbitmq_config:
            Updates RabbitMQ-specific configuration fields for broker.

        update_config:
            Generic method to update configuration fields based on user input.
    """

    def __init__(self, args: Namespace):
        """
        Initialize the configuration manager.

        Args:
            args: Parsed command-line arguments.
        """
        self.args = args
        self.output_dir = self._get_output_directory()
        self.config_file = os.path.join(self.output_dir, "app.yaml")

    def _get_output_directory(self) -> str:
        """
        Determine the output directory for configuration files.

        Returns:
            The absolute path to the output directory.
        """
        if self.args.output_dir is None:
            return MERLIN_HOME
        return os.path.abspath(self.args.output_dir)

    def ensure_directory_exists(self):
        """
        Ensure the output directory exists, creating it if necessary.
        """
        if not os.path.isdir(self.output_dir):
            os.makedirs(self.output_dir)

    def save_config_path(self):
        """
        Save the path to the configuration file in `config_path.txt`.
        """
        with open(CONFIG_PATH_FILE, "w") as f:
            f.write(self.config_file)
        LOG.info(f"Configuration path saved to '{CONFIG_PATH_FILE}'.")

    def create_template_config(self):
        """
        Create a template configuration file.
        """
        LOG.info("Creating config ...")

        if self.args.task_server == "celery":
            template_config = "app.yaml"
            if self.args.broker == "redis":
                template_config = "app_redis.yaml"
        else:
            LOG.error("Only celery can be configured currently.")

        with resources.path("merlin.data.celery", template_config) as template_config_file:
            self._create_config(template_config_file)

    def _create_config(self, template_config_file: str):
        """
        Internal method to create the Celery configuration.
        """
        # Create the configuration file if it doesn't already exist
        if not os.path.isfile(self.config_file):
            shutil.copy(template_config_file, self.config_file)

            # Check to make sure the copy worked
            if not os.path.isfile(self.config_file):
                LOG.error(f"Cannot create config file '{self.config_file}'.")
            else:
                LOG.info(f"The file '{self.config_file}' is ready to be edited for your system.")
        # Log a message if the configuration file already exists
        else:
            LOG.info(f"The config file already exists, '{self.config_file}'.")

        from merlin.common.security import encrypt  # pylint: disable=import-outside-toplevel

        encrypt.init_key()

    def update_broker(self):
        """
        Update the broker section of the app.yaml file.
        """
        LOG.info(f"Updating broker settings in '{self.config_file}'...")

        with open(self.config_file, "r") as app_yaml_file:
            config = yaml.safe_load(app_yaml_file)

        broker_config = config.get("broker", {})
        if self.args.type == "redis":
            self.update_redis_config(broker_config)
        elif self.args.type == "rabbitmq":
            self.update_rabbitmq_config(broker_config)
        else:
            LOG.error("Invalid broker type. Use 'redis' or 'rabbitmq'.")
            return

        config["broker"] = broker_config

        with open(self.config_file, "w") as app_yaml_file:
            yaml.dump(config, app_yaml_file, default_flow_style=False)

        LOG.info("Broker settings successfully updated. Check your new connection with `merlin info`.")

    def update_backend(self):
        """
        Update the results backend section of the app.yaml file.
        """
        LOG.info(f"Updating results backend settings in '{self.config_file}'...")

        with open(self.config_file, "r") as app_yaml_file:
            config = yaml.safe_load(app_yaml_file)

        backend_config = config.get("results_backend", {})
        if self.args.type == "redis":
            self.update_redis_config(backend_config)
        else:
            LOG.error("Invalid backend type. Use 'redis'.")
            return

        config["results_backend"] = backend_config

        with open(self.config_file, "w") as app_yaml_file:
            yaml.dump(config, app_yaml_file, default_flow_style=False)

        LOG.info("Results backend settings successfully updated. Check your new connection with `merlin info`.")

    def update_redis_config(self, config: Dict[str, Union[str, int]]):
        """
        Update the Redis-specific configuration for either broker or backend.

        Args:
            config: The configuration dictionary to update.
        """
        LOG.warning("Redis does not use the 'username' or 'vhost' arguments. Ignoring these if provided.")
        config["name"] = "rediss"
        config["username"] = ""  # Redis doesn't use a username

        required_fields = ["password_file", "server", "port", "db_num", "cert_reqs"]
        self.update_config(config, required_fields)

    def update_rabbitmq_config(self, config: Dict[str, Union[str, int]]):
        """
        Update the RabbitMQ-specific broker configuration.

        Args:
            config: The configuration dictionary to update.
        """
        LOG.warning("RabbitMQ does not use the 'db_num' argument.")
        config["name"] = "rabbitmq"

        required_fields = ["username", "password_file", "server", "port", "vhost", "cert_reqs"]
        self.update_config(config, required_fields)

    def update_config(self, config: Dict[str, Union[str, int]], required_fields: List[str]):
        """
        Generic function to update configuration fields based on provided arguments.

        Args:
            config: The configuration dictionary to update.
            required_fields: List of required field names for the configuration.
        """
        for field in required_fields:
            value = getattr(self.args, field, None)
            if value is not None:
                if field == "password_file":
                    field = "password"
                config[field] = value if field != "port" else int(value)
