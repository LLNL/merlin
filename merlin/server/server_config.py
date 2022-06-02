import enum
import logging
import os
import random
import shutil
import string
import subprocess

import yaml

from merlin.server.server_util import (
    CONTAINER_TYPES,
    MERLIN_CONFIG_DIR,
    MERLIN_SERVER_CONFIG,
    MERLIN_SERVER_SUBDIR,
    RedisUsers,
    ServerConfig,
)


LOG = logging.getLogger("merlin")

# Default values for configuration
CONFIG_DIR = "./merlin_server/"
IMAGE_NAME = "redis_latest.sif"
PROCESS_FILE = "merlin_server.pf"
CONFIG_FILE = "redis.conf"
REDIS_URL = "docker://redis"

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


def generate_password(length, pass_command: str = None):
    if pass_command:
        process = subprocess.run(pass_command.split(), shell=True, stdout=subprocess.PIPE)
        return process.stdout

    characters = list(string.ascii_letters + string.digits + "!@#$%^&*()")

    random.shuffle(characters)

    password = []
    for i in range(length):
        password.append(random.choice(characters))

    random.shuffle(password)
    return "".join(password)


def parse_redis_output(redis_stdout):
    """
    Parse the redis output for a the redis container. It will get all the necessary information
    from the output and returns a dictionary of those values.

    :return:: two values is_successful, dictionary of values from redis output
    """
    if redis_stdout is None:
        return False, "None passed as redis output"
    server_init = False
    redis_config = {}
    for line in redis_stdout:
        if not server_init:
            values = [ln for ln in line.split() if b"=" in ln]
            for val in values:
                key, value = val.split(b"=")
                redis_config[key.decode("utf-8")] = value.strip(b",").strip(b".").decode("utf-8")
            if b"Server initialized" in line:
                server_init = True
        if b"Ready to accept connections" in line:
            return True, redis_config
        if b"aborting" in line:
            return False, line.decode("utf-8")


def create_server_config():
    """
    Create main configuration file for merlin server in the
    merlin configuration directory. If a configuration already
    exists it will not replace the current configuration and exit.
    """
    if not os.path.exists(MERLIN_CONFIG_DIR):
        LOG.error("Unable to find main merlin configuration directory at " + MERLIN_CONFIG_DIR)
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
    files.append(MERLIN_SERVER_CONFIG)
    for file in files:
        file_path = os.path.join(config_dir, file)
        if os.path.exists(file_path):
            LOG.info(f"{file} already exists.")
            continue
        LOG.info(f"Copying file {file} to configuration directory.")
        try:
            shutil.copy(os.path.join(os.path.dirname(os.path.abspath(__file__)), file), config_dir)
        except OSError:
            LOG.error(f"Destination location {config_dir} is not writable.")
            return False

    return True


def config_merlin_server():
    """
    Configurate the merlin server with configurations such as username password and etc.
    """

    server_config = pull_server_config()

    if not os.path.exists(server_config.container.get_config_dir()):
        LOG.info("Creating merlin server directory.")
        os.mkdir(server_config.container.get_config_dir())

    pass_file = server_config.container.get_pass_file_path()
    if os.path.exists(pass_file):
        LOG.info("Password file already exists. Skipping password generation step.")
    else:
        # if "pass_command" in server_config["container"]:
        #     password = generate_password(PASSWORD_LENGTH, server_config["container"]["pass_command"])
        # else:
        password = generate_password(PASSWORD_LENGTH)

        with open(pass_file, "w+") as f:
            f.write(password)

        LOG.info("Creating password file for merlin server container.")

    user_file = server_config.container.get_user_file_path()
    if os.path.exists(user_file):
        LOG.info("User file already exists.")
    else:
        redis_users = RedisUsers(user_file)
        redis_users.add_user("default")
        redis_users.add_user(os.environ.get("USER"))
        redis_users.write()

        LOG.info("User {} created in user file for merlin server container".format(os.environ.get("USER")))


def pull_server_config() -> ServerConfig:
    """
    Pull the main configuration file and corresponding format configuration file
    as well. Returns the values as a dictionary.

    :return: A dictionary containing the main and corresponding format configuration file
    """
    return_data = {}
    format_needed_keys = ["command", "run_command", "stop_command", "pull_command"]
    process_needed_keys = ["status", "kill"]

    config_dir = os.path.join(MERLIN_CONFIG_DIR, MERLIN_SERVER_SUBDIR)
    config_path = os.path.join(config_dir, MERLIN_SERVER_CONFIG)
    if not os.path.exists(config_path):
        LOG.error(f"Unable to pull merlin server configuration from {config_path}")
        return None

    with open(config_path, "r") as cf:
        server_config = yaml.load(cf, yaml.Loader)
        return_data.update(server_config)

    if "container" in server_config:
        if "format" in server_config["container"]:
            format_file = os.path.join(config_dir, server_config["container"]["format"] + ".yaml")
            with open(format_file, "r") as ff:
                format_data = yaml.load(ff, yaml.Loader)
                for key in format_needed_keys:
                    if key not in format_data[server_config["container"]["format"]]:
                        LOG.error(f'Unable to find necessary "{key}" value in format config file {format_file}')
                        return None
                return_data.update(format_data)
        else:
            LOG.error(f'Unable to find "format" in {MERLIN_SERVER_CONFIG}')
            return None
    else:
        LOG.error(f'Unable to find "container" object in {MERLIN_SERVER_CONFIG}')
        return None

    # Checking for process values that are needed for main functions and defaults
    if "process" not in server_config:
        LOG.error("Process config not found in " + MERLIN_SERVER_CONFIG)
        return None

    for key in process_needed_keys:
        if key not in server_config["process"]:
            LOG.error(f'Process necessary "{key}" command configuration not found in {MERLIN_SERVER_CONFIG}')
            return None

    return ServerConfig(return_data)


def pull_server_image():
    """
    Fetch the server image using singularity.
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
            file_dir = os.path.dirname(os.path.abspath(__file__))
            shutil.copy(os.path.join(file_dir, config_file), config_dir)
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


def check_process_file_format(data):
    required_keys = ["parent_pid", "image_pid", "port", "hostname"]
    for key in required_keys:
        if key not in data:
            return False
    return True


def pull_process_file(file_path):
    with open(file_path, "r") as f:
        data = yaml.load(f, yaml.Loader)
        if check_process_file_format(data):
            return data
    return None


def dump_process_file(data, file_path):
    if not check_process_file_format(data):
        return False
    with open(file_path, "w+") as f:
        yaml.dump(data, f, yaml.Dumper)
    return True
