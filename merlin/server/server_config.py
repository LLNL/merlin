###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.11.0.
#
# For details, see https://github.com/LLNL/merlin.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################
"""This module represents everything that goes into server configuration"""

import enum
import logging
import os
import random
import string
import subprocess
from io import BufferedReader
from typing import Tuple

import yaml

from merlin.server.server_util import (
    CONTAINER_TYPES,
    MERLIN_CONFIG_DIR,
    MERLIN_SERVER_CONFIG,
    MERLIN_SERVER_SUBDIR,
    AppYaml,
    RedisConfig,
    RedisUsers,
    ServerConfig,
)


try:
    from importlib import resources
except ImportError:
    import importlib_resources as resources


LOG = logging.getLogger("merlin")

# Default values for configuration
CONFIG_DIR = os.path.abspath("./merlin_server/")
IMAGE_NAME = "redis_latest.sif"
PROCESS_FILE = "merlin_server.pf"
CONFIG_FILE = "redis.conf"
REDIS_URL = "docker://redis"
LOCAL_APP_YAML = "./app.yaml"

PASSWORD_LENGTH = 256


class ServerStatus(enum.Enum):
    """
    Different states in which the server can be in.
    """

    RUNNING = 0
    NOT_INITALIZED = 1
    MISSING_CONTAINER = 2
    NOT_RUNNING = 3
    ERROR = 4


def generate_password(length, pass_command: str = None) -> str:
    """
    Function for generating passwords for redis container. If a specified command is given
    then a password would be generated with the given command. If not a password will be
    created by combining a string a characters based on the given length.

    :return:: string value with given length
    """
    if pass_command:
        process = subprocess.run(pass_command.split(), shell=True, stdout=subprocess.PIPE)
        return process.stdout

    characters = list(string.ascii_letters + string.digits + "!@#$%^&*()")

    random.shuffle(characters)

    password = []
    for _ in range(length):
        password.append(random.choice(characters))

    random.shuffle(password)
    return "".join(password)


def parse_redis_output(redis_stdout: BufferedReader) -> Tuple[bool, str]:
    """
    Parse the redis output for a the redis container. It will get all the necessary information
    from the output and returns a dictionary of those values.

    :return:: two values is_successful, dictionary of values from redis output
    """
    if redis_stdout is None:
        return False, "None passed as redis output"
    server_init = False
    redis_config = {}
    line = redis_stdout.readline()
    while line != "" or line is not None:
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


def create_server_config() -> bool:
    """
    Create main configuration file for merlin server in the
    merlin configuration directory. If a configuration already
    exists it will not replace the current configuration and exit.

    :return:: True if success and False if fail
    """
    if not os.path.exists(MERLIN_CONFIG_DIR):
        LOG.error(f"Unable to find main merlin configuration directory at {MERLIN_CONFIG_DIR}")
        return False

    config_dir = os.path.join(MERLIN_CONFIG_DIR, MERLIN_SERVER_SUBDIR)
    if not os.path.exists(config_dir):
        LOG.info("Unable to find exisiting server configuration.")
        LOG.info(f"Creating default configuration in {config_dir}")
        try:
            os.mkdir(config_dir)
        except OSError as err:
            LOG.error(err)
            return False

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

    # Load Merlin Server Configuration and apply it to app.yaml
    with resources.path("merlin.server", MERLIN_SERVER_CONFIG) as merlin_server_config:
        with open(merlin_server_config) as f:  # pylint: disable=C0103
            main_server_config = yaml.load(f, yaml.Loader)
            filename = LOCAL_APP_YAML if os.path.exists(LOCAL_APP_YAML) else AppYaml.default_filename
            merlin_app_yaml = AppYaml(filename)
            merlin_app_yaml.update_data(main_server_config)
            merlin_app_yaml.write(filename)
    LOG.info("Applying merlin server configuration to app.yaml")

    server_config = pull_server_config()
    if not server_config:
        LOG.error('Try to run "merlin server init" again to reinitialize values.')
        return False

    if not os.path.exists(server_config.container.get_config_dir()):
        LOG.info("Creating merlin server directory.")
        os.mkdir(server_config.container.get_config_dir())

    return True


def config_merlin_server():
    """
    Configurate the merlin server with configurations such as username password and etc.
    """

    server_config = pull_server_config()
    if not server_config:
        LOG.error('Try to run "merlin server init" again to reinitialize values.')
        return False

    pass_file = server_config.container.get_pass_file_path()
    if os.path.exists(pass_file):
        LOG.info("Password file already exists. Skipping password generation step.")
    else:
        # if "pass_command" in server_config["container"]:
        #     password = generate_password(PASSWORD_LENGTH, server_config["container"]["pass_command"])
        # else:
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


def pull_server_config() -> ServerConfig:
    """
    Pull the main configuration file and corresponding format configuration file
    as well. Returns the values as a dictionary.

    :return: A instance of ServerConfig containing all the necessary configuration values.
    """
    return_data = {}
    format_needed_keys = ["command", "run_command", "stop_command", "pull_command"]
    process_needed_keys = ["status", "kill"]

    merlin_app_yaml = AppYaml(LOCAL_APP_YAML)
    server_config = merlin_app_yaml.get_data()
    return_data.update(server_config)

    config_dir = os.path.join(MERLIN_CONFIG_DIR, MERLIN_SERVER_SUBDIR)

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
            LOG.error(f'Unable to find "format" in {merlin_app_yaml.default_filename}')
            return None
    else:
        LOG.error(f'Unable to find "container" object in {merlin_app_yaml.default_filename}')
        return None

    # Checking for process values that are needed for main functions and defaults
    if "process" not in server_config:
        LOG.error(f"Process config not found in {merlin_app_yaml.default_filename}")
        return None

    for key in process_needed_keys:
        if key not in server_config["process"]:
            LOG.error(f'Process necessary "{key}" command configuration not found in {merlin_app_yaml.default_filename}')
            return None

    return ServerConfig(return_data)


def pull_server_image() -> bool:
    """
    Fetch the server image using singularity.

    :return:: True if success and False if fail
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
        except OSError:
            LOG.error(f"Destination location {config_dir} is not writable.")
            return False
    else:
        LOG.info("Redis configuration file already exist.")

    return True


def get_server_status():
    """
    Determine the status of the current server.
    This function can be used to check if the servers
    have been initalized, started, or stopped.

    :param `server_dir`: location of all server related files.
    :param `image_name`: name of the image when fetched.
    :return:: A enum value of ServerStatus describing its current state.
    """
    server_config = pull_server_config()
    if not server_config:
        return ServerStatus.NOT_INITALIZED

    if not os.path.exists(server_config.container.get_config_dir()):
        return ServerStatus.NOT_INITALIZED

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


def check_process_file_format(data: dict) -> bool:
    """
    Check to see if the process file has the correct format and contains the expected key values.
    :return:: True if success and False if fail
    """
    required_keys = ["parent_pid", "image_pid", "port", "hostname"]
    for key in required_keys:
        if key not in data:
            return False
    return True


def pull_process_file(file_path: str) -> dict:
    """
    Pull the data from the process file. If one is found returns the data in a dictionary
    if not returns None
    :return:: Data containing in process file.
    """
    with open(file_path, "r") as f:  # pylint: disable=C0103
        data = yaml.load(f, yaml.Loader)
        if check_process_file_format(data):
            return data
    return None


def dump_process_file(data: dict, file_path: str):
    """
    Dump the process data from the dictionary to the specified file path.
    :return:: True if success and False if fail
    """
    if not check_process_file_format(data):
        return False
    with open(file_path, "w+") as f:  # pylint: disable=C0103
        yaml.dump(data, f, yaml.Dumper)
    return True
