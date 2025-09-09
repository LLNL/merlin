##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""This module represents everything that goes into server configuration"""

import enum
import logging
import os
import random
import string
import subprocess
from importlib import resources
from io import BufferedReader
from typing import Dict, Tuple

import yaml

from merlin.config.config_filepaths import MERLIN_HOME
from merlin.server.server_util import (
    CONTAINER_TYPES,
    MERLIN_SERVER_CONFIG,
    MERLIN_SERVER_SUBDIR,
    AppYaml,
    RedisConfig,
    RedisUsers,
    ServerConfig,
)


LOG = logging.getLogger("merlin")

# Default values for configuration
IMAGE_NAME = "redis_latest.sif"
PROCESS_FILE = "merlin_server.pf"
CONFIG_FILE = "redis.conf"
REDIS_URL = "docker://redis"
LOCAL_APP_YAML = "./app.yaml"

PASSWORD_LENGTH = 256


class ServerStatus(enum.Enum):
    """
    Represents different states that a server can be in.

    Attributes:
        RUNNING (int): Indicates the server is running and operational. Numeric value: 0.
        NOT_INITIALIZED (int): Indicates the server has not been initialized yet. Numeric value: 1.
        MISSING_CONTAINER (int): Indicates the server is missing a required container. Numeric value: 2.
        NOT_RUNNING (int): Indicates the server is not currently running. Numeric value: 3.
        ERROR (int): Indicates the server encountered an error. Numeric value: 4.
    """

    RUNNING = 0
    NOT_INITIALIZED = 1
    MISSING_CONTAINER = 2
    NOT_RUNNING = 3
    ERROR = 4


def generate_password(length, pass_command: str = None) -> str:
    """
    Generates a password for a Redis container.

    If a specific command is provided, the password will be generated using the output
    of the given command. Otherwise, a random password will be created by combining
    characters (letters, digits, and special symbols) based on the specified length.

    Args:
        length (int): The desired length of the password.
        pass_command (str, optional): A shell command to generate the password.
            If provided, the command's output will be used as the password.

    Returns:
        The generated password.
    """
    if pass_command:
        process = subprocess.run(pass_command, shell=True, capture_output=True, text=True)
        return process.stdout.strip()

    characters = list(string.ascii_letters + string.digits + "!@#$%^&*()")

    random.shuffle(characters)

    password = []
    for _ in range(length):
        password.append(random.choice(characters))

    random.shuffle(password)
    return "".join(password)


def parse_redis_output(redis_stdout: BufferedReader) -> Tuple[bool, str]:
    """
    Parses the Redis output from a Redis container.

    This function processes the Redis container's output to extract necessary information,
    such as configuration details and server state. It determines whether the server was
    successfully initialized and ready to accept connections, or if an error occurred.

    Args:
        redis_stdout (BufferedReader): A buffered reader object containing the Redis container's output.

    Returns:
        A tuple containing:\n
            - A boolean indicating whether the server was successfully initialized and ready.
            - A dictionary containing parsed configuration values if successful, or an error message otherwise.
    """
    if redis_stdout is None:
        return False, "None passed as redis output"
    server_init = False
    redis_config = {}
    line = redis_stdout.readline()
    while line != b"" and line is not None:
        if not server_init:
            values = [ln for ln in line.split() if b"=" in ln]
            for val in values:
                key, value = val.split(b"=")
                redis_config[key.decode("utf-8")] = value.strip(b",").strip(b".").decode("utf-8")
            if b"Server initialized" in line:
                server_init = True
        if b"Ready to accept connections" in line:
            return True, redis_config
        if b"aborting" in line or b"Fatal error" in line:
            return False, line.decode("utf-8")
        line = redis_stdout.readline()

    return False, "Reached end of redis output without seeing 'Ready to accept connections'"


def copy_container_command_files(config_dir: str) -> bool:
    """
    Copies YAML files containing command instructions for container types to the specified configuration directory.

    Args:
        config_dir (str): The path to the configuration directory where the YAML files will be copied.

    Returns:
        True if all files are successfully copied or already exist. False otherwise.
    """
    files = [i + ".yaml" for i in CONTAINER_TYPES]
    for file in files:
        file_path = os.path.join(config_dir, file)
        if os.path.exists(file_path):
            LOG.info(f"{file} already exists.")
            continue
        LOG.info(f"Copying file {file} to configuration directory.")
        try:
            with resources.path("merlin.server", file) as config_file:
                with open(file_path, "w") as outfile, open(config_file, "r") as infile:
                    outfile.write(infile.read())
        except OSError:
            LOG.error(f"Destination location {config_dir} is not writable.")
            return False
    return True


def create_server_config() -> bool:
    """
    Creates the main configuration file for the Merlin server in the Merlin configuration directory.

    This function checks for the existence of the Merlin configuration directory and creates a default
    server configuration if none exists. It also copies necessary container command files, applies the
    server configuration to `app.yaml`, and initializes the server configuration directory. If the
    configuration already exists, it will not overwrite it.

    Returns:
        True if the configuration is successfully created and applied. False otherwise.
    """
    # Check for ~/.merlin/ directory
    if not os.path.exists(MERLIN_HOME):
        LOG.error(f"Unable to find main merlin configuration directory at {MERLIN_HOME}")
        return False

    # Create ~/.merlin/server/ directory if it doesn't already exist
    config_dir = os.path.join(MERLIN_HOME, MERLIN_SERVER_SUBDIR)
    if not os.path.exists(config_dir):
        LOG.info("Unable to find exisiting server configuration.")
        LOG.info(f"Creating default configuration in {config_dir}")
        try:
            os.mkdir(config_dir)
        except OSError as err:
            LOG.error(err)
            return False

    # Copy container-specific yaml files to ~/.merlin/server/
    if not copy_container_command_files(config_dir):
        return False

    # Load Merlin Server Configuration and apply it to merlin_server/app.yaml
    with resources.path("merlin.server", MERLIN_SERVER_CONFIG) as merlin_server_config:
        with open(merlin_server_config) as f:  # pylint: disable=C0103
            main_server_config = yaml.load(f, yaml.Loader)

            server_config = load_server_config(main_server_config)
            if not server_config:
                LOG.error('Try to run "merlin server init" again to reinitialize values.')
                return False

            if not os.path.exists(server_config.container.get_config_dir()):
                LOG.info("Creating merlin server directory.")
                os.mkdir(server_config.container.get_config_dir())

            LOG.info("Applying merlin server configuration to ./merlin_server/app.yaml")
            server_app_yaml = AppYaml()  # By default this will point to ./merlin_server/app.yaml
            server_app_yaml.update_data(main_server_config)
            server_app_yaml.write()

    return True


def config_merlin_server():
    """
    Configures the Merlin server with necessary settings, including username and password.

    This function sets up the Merlin server by generating and storing a password file, creating a user file,
    and configuring Redis settings. If the password or user files already exist, it skips the respective
    setup steps. The function ensures that default and environment-specific users are added to the user file.
    """

    server_config = pull_server_config()
    if not server_config:
        LOG.error('Try to run "merlin server init" again to reinitialize values.')
        return False

    pass_file = server_config.container.get_pass_file_path()
    if os.path.exists(pass_file):
        LOG.info("Password file already exists. Skipping password generation step.")
    else:
        password = generate_password(PASSWORD_LENGTH)

        with open(pass_file, "w+") as f:  # pylint: disable=C0103
            f.write(password)

        LOG.info("Creating password file for merlin server container.")

    user_file = server_config.container.get_user_file_path()
    if os.path.exists(user_file):
        LOG.info("User file already exists.")
    else:
        redis_users = RedisUsers(user_file)
        redis_config = RedisConfig(server_config.container.get_config_path())
        redis_config.set_password(server_config.container.get_container_password())
        redis_users.add_user(user="default", password=server_config.container.get_container_password())
        redis_users.add_user(user=os.environ.get("USER"), password=server_config.container.get_container_password())
        redis_users.write()
        redis_config.write()

        LOG.info(f"User {os.environ.get('USER')} created in user file for merlin server container")

    return None


def load_server_config(server_config: Dict) -> ServerConfig:
    """
    Given a dictionary containing server configuration values, load them into a
    [`ServerConfig`][server.server_util.ServerConfig] instance.

    Args:
        server_config: A dictionary containing server configuration values. Should have
            a 'container' entry and a 'process' entry.

    Returns:
        A `ServerConfig` object containing the loaded server configuration.
    """
    return_data = {}
    return_data.update(server_config)
    format_needed_keys = ["command", "run_command", "stop_command", "pull_command"]
    process_needed_keys = ["status", "kill"]

    config_dir = os.path.join(MERLIN_HOME, MERLIN_SERVER_SUBDIR)

    if "container" in server_config:
        if "format" in server_config["container"]:
            format_file = os.path.join(config_dir, server_config["container"]["format"] + ".yaml")
            with open(format_file, "r") as ff:  # pylint: disable=C0103
                format_data = yaml.load(ff, yaml.Loader)
                for key in format_needed_keys:
                    if key not in format_data[server_config["container"]["format"]]:
                        LOG.error(f'Unable to find necessary "{key}" value in format config file {format_file}')
                        return None
                return_data.update(format_data)
        else:
            LOG.error('Unable to find "format" in server_config object.')
            return None
    else:
        LOG.error('Unable to find "container" in server_config object.')
        return None

    # Checking for process values that are needed for main functions and defaults
    if "process" not in server_config:
        LOG.error('Unable to find "process" in server_config object.')
        return None

    for key in process_needed_keys:
        if key not in server_config["process"]:
            LOG.error(f'Process necessary "{key}" command configuration not found in server_config object.')
            return None

    return ServerConfig(return_data)


def pull_server_config(app_yaml_path: str = None) -> ServerConfig:
    """
    Retrieves the main configuration file and its corresponding format configuration file for the Merlin server.

    This function reads the `app.yaml` configuration file and additional format-specific configuration files
    to construct a complete configuration dictionary. It validates the presence of required keys in the format
    and process configurations. If any required configuration is missing, an error is logged and `None` is returned.

    Returns:
        An instance of [`ServerConfig`][server.server_util.ServerConfig] containing all necessary configuration values.
    """
    app_yaml_file = app_yaml_path if app_yaml_path is not None else LOCAL_APP_YAML
    merlin_app_yaml = AppYaml(app_yaml_file)
    server_config = merlin_app_yaml.get_data()
    return load_server_config(server_config)


def pull_server_image() -> bool:
    """
    Fetches the server image and ensures the necessary configuration files are in place.

    This function retrieves the server image from a specified URL and saves it locally if it does not already exist.
    Additionally, it copies the default Redis configuration file to the appropriate location if it is missing.
    The function relies on the server configuration to determine the image URL, image path, and configuration file details.

    Returns:
        True if the server image and configuration file are successfully set up, False if an error occurs.
    """
    server_config = pull_server_config()
    if not server_config:
        LOG.error('Try to run "merlin server init" again to reinitialize values.')
        return False

    config_dir = server_config.container.get_config_dir()
    config_file = server_config.container.get_config_name()
    image_url = server_config.container.get_image_url()
    image_path = server_config.container.get_image_path()

    if not os.path.exists(image_path):
        LOG.info(f"Fetching redis image from {image_url}")
        subprocess.run(
            server_config.container_format.get_pull_command()
            .strip("\\")
            .format(command=server_config.container_format.get_command(), image=image_path, url=image_url)
            .split(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    else:
        LOG.info(f"{image_path} already exists.")

    if not os.path.exists(os.path.join(config_dir, config_file)):
        LOG.info("Copying default redis configuration file.")
        try:
            with resources.path("merlin.server", config_file) as file:
                with open(os.path.join(config_dir, config_file), "w") as outfile, open(file, "r") as infile:
                    outfile.write(infile.read())
        except OSError as exc:
            LOG.error(f"Destination location {config_dir} is not writable. Raised from:\n{exc}")
            return False
    else:
        LOG.info("Redis configuration file already exist.")

    return True


def get_server_status() -> ServerStatus:
    """
    Determines the current status of the server.

    This function checks the server's state by verifying the existence of necessary files,
    including configuration files, the container image, and the process file. It also checks
    if the server process is actively running.

    Returns:
        An enum value representing the server's current state:\n
            - `ServerStatus.NOT_INITIALIZED`: The server has not been initialized.
            - `ServerStatus.MISSING_CONTAINER`: The server container image is missing.
            - `ServerStatus.NOT_RUNNING`: The server process is not running.
            - `ServerStatus.RUNNING`: The server is actively running.
    """
    server_config = pull_server_config()
    if not server_config:
        return ServerStatus.NOT_INITIALIZED

    if not os.path.exists(server_config.container.get_config_dir()):
        return ServerStatus.NOT_INITIALIZED

    if not os.path.exists(server_config.container.get_image_path()):
        return ServerStatus.MISSING_CONTAINER

    if not os.path.exists(server_config.container.get_pfile_path()):
        return ServerStatus.NOT_RUNNING

    pf_data = pull_process_file(server_config.container.get_pfile_path())
    parent_pid = pf_data["parent_pid"]

    check_process = subprocess.run(
        server_config.process.get_status_command().strip("\\").format(pid=parent_pid).split(),
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )

    if check_process.stdout == b"":
        return ServerStatus.NOT_RUNNING

    return ServerStatus.RUNNING


def check_process_file_format(data: Dict) -> bool:
    """
    Validates the format of a process file.

    This function checks if the given process file data (in dictionary format) contains all the
    required keys: "parent_pid", "image_pid", "port", and "hostname".

    Args:
        data (Dict): The process file data to validate.

    Returns:
        True if the process file contains all required keys, False otherwise.
    """
    required_keys = ["parent_pid", "image_pid", "port", "hostname"]
    for key in required_keys:
        if key not in data:
            return False
    return True


def pull_process_file(file_path: str) -> Dict:
    """
    Reads and parses data from a process file.

    This function attempts to load the contents of a process file located at the specified
    file path. If the file exists and its format is valid, the data is returned as a dictionary.
    If the format is invalid or the file cannot be processed, `None` is returned.

    Args:
        file_path (str): The path to the process file.

    Returns:
        A dictionary containing the data from the process file if the format is valid.
    """
    with open(file_path, "r") as f:  # pylint: disable=C0103
        data = yaml.load(f, yaml.Loader)
        if check_process_file_format(data):
            return data
    return None


def dump_process_file(data: Dict, file_path: str) -> bool:
    """
    Writes process data to a specified file.

    This function takes a dictionary containing process data and writes it to the specified
    file path in YAML format. Before writing, the function validates the format of the data.
    If the data format is invalid, the function returns `False`. If the operation is successful,
    it returns `True`.

    Args:
        data (Dict): The process data to be written to the file.
        file_path (str): The path to the file where the data will be written.

    Returns:
        True if the data is successfully written to the file, False if the data
            format is invalid or the operation fails.
    """
    if not check_process_file_format(data):
        return False
    with open(file_path, "w+") as f:  # pylint: disable=C0103
        yaml.dump(data, f, yaml.Dumper)
    return True
