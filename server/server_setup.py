"""Main functions for instantiating and running Merlin server containers."""

import enum
import os
import subprocess
import time


SERVER_DIR = "./merlin_server/"
IMAGE_NAME = "redis_latest.sif"
PID_FILE = "merlin_server.pid"
CONFIG_FILE = "redis.conf"

class ServerStatus(enum.Enum):
    """
    Different states in which the server can be in.
    """
    NOT_INITALIZED = 0
    MISSING_CONTAINER = 1
    NOT_RUNNING = 2
    RUNNING = 3
    ERROR = 4


def fetch_server_image(server_dir: str = SERVER_DIR, image_name: str = IMAGE_NAME):
    """
    Fetch the server image using singularity.

    :param `server_dir`: location of all server related files.
    :param `image_name`: name of the image when fetched.
    """
    if not os.path.exists(server_dir):
        print("Creating merlin server directory.")
        os.mkdir(server_dir)

    image_loc = server_dir + image_name

    if os.path.exists(image_loc):
        print(image_loc + " already exists.")
        return

    print("Fetching redis image from docker://redis.")
    subprocess.run(["singularity", "pull", image_loc, "docker://redis"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def start_server(server_dir: str = SERVER_DIR, image_name: str = IMAGE_NAME):
    """
    Start a merlin server container using singularity.

    :param `server_dir`: location of all server related files.
    :param `image_name`: name of the image when fetched.
    """
    current_status = get_server_status(server_dir=server_dir, image_name=image_name)

    if current_status == ServerStatus.NOT_INITALIZED or current_status == ServerStatus.MISSING_CONTAINER:
        print("Merlin server has not been initialized. Please run 'merlin server init' first.")
        return False

    if current_status == ServerStatus.RUNNING:
        print("Merlin server already running.")
        print("Stop current server with 'merlin server stop' before attempting to start a new server.")
        return False

    file_dir = os.path.dirname(os.path.abspath(__file__)) + "/"
    process = subprocess.Popen(
        ["singularity", "run", server_dir + image_name, file_dir + CONFIG_FILE],
        start_new_session=True,
        close_fds=True,
        stdout=subprocess.DEVNULL,
    )

    with open(server_dir + PID_FILE, "w+") as f:
        f.write(str(process.pid))

    time.sleep(1)

    if get_server_status(server_dir=server_dir, image_name=image_name) != ServerStatus.RUNNING:
        print("Unable to start merlin server.")
        return False

    print("Server started with PID", str(process.pid))
    return True


def stop_server(server_dir: str = SERVER_DIR, image_name: str = IMAGE_NAME):
    """
    Stop running merlin server containers.

    :param `server_dir`: location of all server related files.
    :param `image_name`: name of the image when fetched.
    """
    if get_server_status(server_dir=server_dir, image_name=image_name) != ServerStatus.RUNNING:
        print("There is no instance of merlin server running.")
        print("Start a merlin server first with 'merlin server start'")
        return False

    with open(server_dir + PID_FILE, "r") as f:
        read_pid = f.read()
        process = subprocess.run(["pgrep", "-P", str(read_pid)], stdout=subprocess.PIPE)
        if process.stdout == b"":
            print("Unable to get the PID for the current merlin server.")
            return False

        print("Attempting to close merlin server PID", str(read_pid))
        subprocess.run(["kill", str(read_pid)], stdout=subprocess.PIPE)
        time.sleep(1)
        if get_server_status(server_dir=server_dir, image_name=image_name) == ServerStatus.RUNNING:
            print("Unable to kill process.")
            return False

        print("Merlin server terminated.")
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
