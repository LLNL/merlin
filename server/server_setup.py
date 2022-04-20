"""Main functions for instantiating and running Merlin server containers."""

import enum
import logging
import os
import shutil
import subprocess
import time

from server.server_config import (
    MERLIN_CONFIG_DIR,
    MERLIN_SERVER_CONFIG,
    MERLIN_SERVER_SUBDIR,
    parse_redis_output,
    pull_server_config,
)


# Default values for configuration
CONFIG_DIR = "./merlin_server/"
IMAGE_NAME = "redis_latest.sif"
PID_FILE = "merlin_server.pid"
CONFIG_FILE = "redis.conf"
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


def pull_server_image():
    """
    Fetch the server image using singularity.
    """
    server_config = pull_server_config()
    if not server_config:
        LOG.error('Try to run "merlin server init" again to reinitialize values.')
        return False

    container_config = server_config["container"]
    config_dir = container_config["config_dir"] if "config_dir" in container_config else CONFIG_DIR
    image_name = container_config["image"] if "image" in container_config else IMAGE_NAME
    config_file = container_config["config"] if "config" in container_config else CONFIG_FILE

    if not os.path.exists(config_dir):
        LOG.info("Creating merlin server directory.")
        os.mkdir(config_dir)

    image_path = os.path.join(config_dir, image_name)

    if os.path.exists(image_path):
        LOG.info(f"{image_path} already exists.")
        return False

    LOG.info("Fetching redis image from docker://redis.")
    subprocess.run(["singularity", "pull", image_path, "docker://redis"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    LOG.info("Copying default redis configuration file.")
    try:
        file_dir = os.path.dirname(os.path.abspath(__file__))
        shutil.copy(os.path.join(file_dir, config_file), config_dir)
    except OSError:
        LOG.error(f"Destination location {config_dir} is not writable.")
        return False
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

    container_config = server_config["container"]
    config_dir = container_config["config_dir"] if "config_dir" in container_config else CONFIG_DIR
    image_name = container_config["image"] if "image" in container_config else IMAGE_NAME
    pid_file = container_config["pfile"] if "pfile" in container_config else PID_FILE

    if not os.path.exists(config_dir):
        return ServerStatus.NOT_INITALIZED

    if not os.path.exists(os.path.join(config_dir, image_name)):
        return ServerStatus.MISSING_CONTAINER

    if not os.path.exists(os.path.join(config_dir, pid_file)):
        return ServerStatus.NOT_RUNNING

    with open(os.path.join(config_dir, pid_file), "r") as f:
        server_pid = f.read()
        check_process = subprocess.run(["pgrep", "-P", str(server_pid)], stdout=subprocess.PIPE)

        if check_process.stdout == b"":
            return ServerStatus.NOT_RUNNING

    return ServerStatus.RUNNING


def start_server():
    """
    Start a merlin server container using singularity.

    :param `server_dir`: location of all server related files.
    :param `image_name`: name of the image when fetched.
    """
    current_status = get_server_status()

    if current_status == ServerStatus.NOT_INITALIZED or current_status == ServerStatus.MISSING_CONTAINER:
        LOG.info("Merlin server has not been initialized. Please run 'merlin server init' first.")
        return False

    if current_status == ServerStatus.RUNNING:
        LOG.info("Merlin server already running.")
        LOG.info("Stop current server with 'merlin server stop' before attempting to start a new server.")
        return False

    server_config = pull_server_config()
    if not server_config:
        LOG.error('Try to run "merlin server init" again to reinitialize values.')
        return False
    container_config = server_config["container"]

    config_dir = container_config["config_dir"] if "config_dir" in container_config else CONFIG_DIR
    config_file = container_config["config"] if "config_dir" in container_config else CONFIG_FILE
    image_name = container_config["image"] if "image" in container_config else IMAGE_NAME
    pid_file = container_config["pfile"] if "pfile" in container_config else PID_FILE

    image_path = os.path.join(config_dir, image_name)
    if not os.path.exists(image_path):
        LOG.error("Unable to find image at " + image_path)
        return False

    config_path = os.path.join(config_dir, config_file)
    if not os.path.exists(config_path):
        LOG.error("Unable to find config file at " + config_path)
        return False

    print(image_path, config_path)
    process = subprocess.Popen(
        ["singularity", "run", image_path, config_path],
        start_new_session=True,
        close_fds=True,
        stdout=subprocess.PIPE,
    )

    time.sleep(1)

    redis_start, redis_out = parse_redis_output(process.stdout)

    if not redis_start:
        LOG.error("Redis is unable to start")
        LOG.error('Check to see if there is an unresponsive instance of redis with "ps -e"')
        LOG.error(redis_out.strip("\n"))
        return False

    with open(os.path.join(config_dir, pid_file), "w+") as f:
        f.write(str(process.pid))
        # f.write(redis_config["pid"])

    if get_server_status() != ServerStatus.RUNNING:
        LOG.error("Unable to start merlin server.")
        return False

    LOG.info(f"Server started with PID {str(process.pid)}")

    return True


def stop_server():
    """
    Stop running merlin server containers.

    """
    if get_server_status() != ServerStatus.RUNNING:
        LOG.info("There is no instance of merlin server running.")
        LOG.info("Start a merlin server first with 'merlin server start'")
        return False

    server_config = pull_server_config()
    if not server_config:
        LOG.error('Try to run "merlin server init" again to reinitialize values.')
        return False
    container_config = server_config["container"]

    config_dir = container_config["config_dir"] if "config_dir" in container_config else CONFIG_DIR
    pid_file = container_config["pfile"] if "pfile" in container_config else PID_FILE

    with open(os.path.join(config_dir, pid_file), "r") as f:
        read_pid = f.read()
        process = subprocess.run(["pgrep", "-P", str(read_pid)], stdout=subprocess.PIPE)
        if process.stdout == b"":
            LOG.error("Unable to get the PID for the current merlin server.")
            return False

        LOG.info(f"Attempting to close merlin server PID {str(read_pid)}")
        subprocess.run(["kill", str(read_pid)], stdout=subprocess.PIPE)
        time.sleep(1)
        if get_server_status() == ServerStatus.RUNNING:
            LOG.error("Unable to kill process.")
            return False

        LOG.info("Merlin server terminated.")
        return True
