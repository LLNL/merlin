"""Main functions for instantiating and running Merlin server containers."""

import enum
import logging
import os
import shutil
import socket
import subprocess
import time

from merlin.server.server_config import (
    MERLIN_CONFIG_DIR,
    MERLIN_SERVER_CONFIG,
    MERLIN_SERVER_SUBDIR,
    dump_process_file,
    parse_redis_output,
    pull_process_file,
    pull_server_config,
)


# Default values for configuration
CONFIG_DIR = "./merlin_server/"
IMAGE_NAME = "redis_latest.sif"
PROCESS_FILE = "merlin_server.pf"
CONFIG_FILE = "redis.conf"
REDIS_URL = "docker://redis"
CONTAINER_TYPES = ["singularity", "docker", "podman"]

LOG = logging.getLogger("merlin")


class ServerStatus(enum.Enum):
    """
    Different states in which the server can be in.
    """

    RUNNING = 0
    NOT_INITALIZED = 1
    MISSING_CONTAINER = 2
    NOT_RUNNING = 3
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
    image_url = container_config["url"] if "url" in container_config else REDIS_URL

    if not os.path.exists(config_dir):
        LOG.info("Creating merlin server directory.")
        os.mkdir(config_dir)

    image_path = os.path.join(config_dir, image_name)

    if os.path.exists(image_path):
        LOG.info(f"{image_path} already exists.")
        return False

    LOG.info(f"Fetching redis image from {image_url}")
    format_config = server_config[container_config["format"]]
    subprocess.run(
        format_config["pull_command"]
        .strip("\\")
        .format(command=format_config["command"], image=image_path, url=image_url)
        .split(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

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
    pfile = container_config["pfile"] if "pfile" in container_config else PROCESS_FILE

    if not os.path.exists(config_dir):
        return ServerStatus.NOT_INITALIZED

    if not os.path.exists(os.path.join(config_dir, image_name)):
        return ServerStatus.MISSING_CONTAINER

    if not os.path.exists(os.path.join(config_dir, pfile)):
        return ServerStatus.NOT_RUNNING

    pf_data = pull_process_file(os.path.join(config_dir, pfile))
    parent_pid = pf_data["parent_pid"]

    check_process = subprocess.run(
        server_config["process"]["status"].strip("\\").format(pid=parent_pid).split(),
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )

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
    pfile = container_config["pfile"] if "pfile" in container_config else PROCESS_FILE

    image_path = os.path.join(config_dir, image_name)
    if not os.path.exists(image_path):
        LOG.error("Unable to find image at " + image_path)
        return False

    config_path = os.path.join(config_dir, config_file)
    if not os.path.exists(config_path):
        LOG.error("Unable to find config file at " + config_path)
        return False

    format_config = server_config[container_config["format"]]
    process = subprocess.Popen(
        format_config["run_command"]
        .strip("\\")
        .format(command=container_config["format"], image=image_path, config=config_path)
        .split(),
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

    redis_out["image_pid"] = redis_out.pop("pid")
    redis_out["parent_pid"] = process.pid
    redis_out["hostname"] = socket.gethostname()
    if not dump_process_file(redis_out, os.path.join(config_dir, pfile)):
        LOG.error("Unable to create process file for container.")
        return False

    if get_server_status() != ServerStatus.RUNNING:
        LOG.error("Unable to start merlin server.")
        return False

    LOG.info(f"Server started with PID {str(process.pid)}.")
    LOG.info(f'Merlin server operating on "{redis_out["hostname"]}" and port "{redis_out["port"]}".')

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
    pfile = container_config["pfile"] if "pfile" in container_config else PROCESS_FILE
    image_name = container_config["name"] if "name" in container_config else IMAGE_NAME

    pf_data = pull_process_file(os.path.join(config_dir, pfile))
    read_pid = pf_data["parent_pid"]

    process = subprocess.run(
        server_config["process"]["status"].strip("\\").format(pid=read_pid).split(), stdout=subprocess.PIPE
    )
    if process.stdout == b"":
        LOG.error("Unable to get the PID for the current merlin server.")
        return False

    format_config = server_config[container_config["format"]]
    command = server_config["process"]["kill"].strip("\\").format(pid=read_pid).split()
    if format_config["stop_command"] != "kill":
        command = format_config["stop_command"].strip("\\").format(name=image_name).split()

    LOG.info(f"Attempting to close merlin server PID {str(read_pid)}")

    subprocess.run(command, stdout=subprocess.PIPE)
    time.sleep(1)
    if get_server_status() == ServerStatus.RUNNING:
        LOG.error("Unable to kill process.")
        return False

    LOG.info("Merlin server terminated.")
    return True
