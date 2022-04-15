"""Main functions for instantiating and running Merlin server containers."""

import enum
import logging
import os
import shutil
import subprocess
import time

import yaml


SERVER_DIR = "./merlin_server/"
IMAGE_NAME = "redis_latest.sif"
PID_FILE = "merlin_server.pid"
CONFIG_FILE = "redis.conf"
MERLIN_CONFIG_DIR = os.path.expanduser("~") + "/.merlin/"
MERLIN_SERVER_SUBDIR = "server/"
MERLIN_SERVER_CONFIG = "redis_server.yaml"
CONTAINER_TYPES = ["singularity", "docker", "podman"]


LOG = logging.getLogger("merlin")


class ServerStatus(enum.Enum):
    """
    Different states in which the server can be in.
    """

    NOT_INITALIZED = 0
    MISSING_CONTAINER = 1
    NOT_RUNNING = 2
    RUNNING = 3
    ERROR = 4


def pull_server_config(
    merlin_config_dir: str = MERLIN_CONFIG_DIR,
    merlin_server_subdir: str = MERLIN_SERVER_SUBDIR,
    merlin_server_config: str = MERLIN_SERVER_CONFIG,
) -> dict:
    """
    Pull the main configuration file and corresponding format configuration file
    as well. Returns the values as a dictionary.

    :param `merlin_config_dir`: location of main merlin configuration.
    :param `merlin_server_subdir`: subdirectory for storing the server configuration.
    :param `merlin_server_config`: server config file name
    :return: A dictionary containing the main and corresponding format configuration file
    """
    return_data = {}
    config_dir = os.path.join(merlin_config_dir, merlin_server_subdir)
    config_path = os.path.join(config_dir, merlin_server_config)
    if not os.path.exists(config_path):
        LOG.error("Unable to pull merlin server configuration from " + config_path)
        return None

    with open(config_path, "r") as cf:
        container_data = yaml.load(cf, yaml.Loader)
        return_data.update(container_data)

    if "container" in container_data:
        if "format" in container_data["container"]:
            format_file = os.path.join(config_dir, container_data["container"]["format"] + ".yaml")
            with open(format_file, "r") as ff:
                format_data = yaml.load(ff, yaml.Loader)
                return_data.update(format_data)

    return return_data


def create_server_config(
    merlin_config_dir: str = MERLIN_CONFIG_DIR,
    merlin_server_subdir: str = MERLIN_SERVER_SUBDIR,
    merlin_server_config: str = MERLIN_SERVER_CONFIG,
):
    """
    Create main configuration file for merlin server in the
    merlin configuration directory. If a configuration already
    exists it will not replace the current configuration and exit.

    :param `merlin_config_dir`: location of main merlin configuration.
    :param `merlin_server_subdir`: subdirectory for storing the server configuration.
    :param `merlin_server_config`: server config file name
    """
    if not os.path.exists(merlin_config_dir):
        LOG.error("Unable to find main merlin configuration directory at " + merlin_config_dir)
        return False

    config_dir = os.path.join(merlin_config_dir, merlin_server_subdir)
    if not os.path.exists(config_dir):
        LOG.info("Unable to find exisiting server configuration.")
        LOG.info(f"Creating default configuration in {config_dir}")
        try:
            os.mkdir(merlin_config_dir + merlin_server_subdir)
        except OSError as err:
            LOG.error(err)
            return False

    files = [i + ".yaml" for i in CONTAINER_TYPES]
    files.append(merlin_server_config)
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

    print(pull_server_config())
    return True


def pull_server_image(server_dir: str = SERVER_DIR, image_name: str = IMAGE_NAME):
    """
    Fetch the server image using singularity.

    :param `server_dir`: location of all server related files.
    :param `image_name`: name of the image when fetched.
    """
    if not os.path.exists(server_dir):
        LOG.info("Creating merlin server directory.")
        os.mkdir(server_dir)

    image_loc = server_dir + image_name

    if os.path.exists(image_loc):
        LOG.info(image_loc + " already exists.")
        return False

    LOG.info("Fetching redis image from docker://redis.")
    subprocess.run(["singularity", "pull", image_loc, "docker://redis"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    LOG.info("Copying default redis configuration file.")
    try:
        shutil.copy(os.path.dirname(os.path.abspath(__file__)) + "/" + CONFIG_FILE, server_dir)
    except OSError:
        LOG.error("Destination location " + server_dir + " is not writable.")
        return False
    return True


def start_server(server_dir: str = SERVER_DIR, image_name: str = IMAGE_NAME):
    """
    Start a merlin server container using singularity.

    :param `server_dir`: location of all server related files.
    :param `image_name`: name of the image when fetched.
    """
    current_status = get_server_status(server_dir=server_dir, image_name=image_name)

    if current_status == ServerStatus.NOT_INITALIZED or current_status == ServerStatus.MISSING_CONTAINER:
        LOG.info("Merlin server has not been initialized. Please run 'merlin server init' first.")
        return False

    if current_status == ServerStatus.RUNNING:
        LOG.info("Merlin server already running.")
        LOG.info("Stop current server with 'merlin server stop' before attempting to start a new server.")
        return False

    process = subprocess.Popen(
        ["singularity", "run", server_dir + image_name, server_dir + CONFIG_FILE],
        start_new_session=True,
        close_fds=True,
        stdout=subprocess.DEVNULL,
    )

    with open(server_dir + PID_FILE, "w+") as f:
        f.write(str(process.pid))

    time.sleep(1)

    if get_server_status(server_dir=server_dir, image_name=image_name) != ServerStatus.RUNNING:
        LOG.error("Unable to start merlin server.")
        return False

    LOG.info("Server started with PID " + str(process.pid))
    return True


def stop_server(server_dir: str = SERVER_DIR, image_name: str = IMAGE_NAME):
    """
    Stop running merlin server containers.

    :param `server_dir`: location of all server related files.
    :param `image_name`: name of the image when fetched.
    """
    if get_server_status(server_dir=server_dir, image_name=image_name) != ServerStatus.RUNNING:
        LOG.info("There is no instance of merlin server running.")
        LOG.info("Start a merlin server first with 'merlin server start'")
        return False

    with open(server_dir + PID_FILE, "r") as f:
        read_pid = f.read()
        process = subprocess.run(["pgrep", "-P", str(read_pid)], stdout=subprocess.PIPE)
        if process.stdout == b"":
            LOG.error("Unable to get the PID for the current merlin server.")
            return False

        LOG.info("Attempting to close merlin server PID " + str(read_pid))
        subprocess.run(["kill", str(read_pid)], stdout=subprocess.PIPE)
        time.sleep(1)
        if get_server_status(server_dir=server_dir, image_name=image_name) == ServerStatus.RUNNING:
            LOG.error("Unable to kill process.")
            return False

        LOG.info("Merlin server terminated.")
        return True


def get_server_status(server_dir: str = SERVER_DIR, image_name: str = IMAGE_NAME):
    """
    Determine the status of the current server.
    This function can be used to check if the servers
    have been initalized, started, or stopped.

    :param `server_dir`: location of all server related files.
    :param `image_name`: name of the image when fetched.
    """
    if not os.path.exists(server_dir):
        return ServerStatus.NOT_INITALIZED

    if not os.path.exists(server_dir + image_name):
        return ServerStatus.MISSING_CONTAINER

    if not os.path.exists(server_dir + PID_FILE):
        return ServerStatus.NOT_RUNNING

    with open(server_dir + PID_FILE, "r") as f:
        server_pid = f.read()
        check_process = subprocess.run(["pgrep", "-P", str(server_pid)], stdout=subprocess.PIPE)

        if check_process.stdout == b"":
            return ServerStatus.NOT_RUNNING

    return ServerStatus.RUNNING
