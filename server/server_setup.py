import enum
import subprocess
import os
import sys
import time

SERVER_DIR = "./merlin_server/"
IMAGE_NAME = "redis_latest.sif"
PID_FILE = "merlin_server.pid"

class SERVER_STATUS(enum.Enum):
    NOT_INITALIZED = 0
    MISSING_CONTAINER = 1
    NOT_RUNNING = 2
    RUNNING = 3
    ERROR = 4

def fetch_server_image(server_dir:str = SERVER_DIR, image_name:str = IMAGE_NAME):
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
    process = subprocess.run(
        ["singularity", "pull", image_loc, "docker://redis"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    


def start_server(server_dir:str = SERVER_DIR, image_name:str = IMAGE_NAME):
    """
    Start a merlin server container using singularity.

    :param `server_dir`: location of all server related files.
    :param `image_name`: name of the image when fetched.
    """
    current_status = get_server_status(server_dir=server_dir, image_name=image_name)
    if current_status == SERVER_STATUS.RUNNING:
        print("M server already running.")
        print("Stop current server with 'merlin server stop' before attempting to start a new server.")
        return False
    
    if (current_status == SERVER_STATUS.NOT_INITALIZED or
        current_status == SERVER_STATUS.MISSING_CONTAINER):
        fetch_server_image(server_dir=server_dir, image_name=image_name)


    process = subprocess.Popen(
        ["singularity", "run", server_dir + image_name, ],
        start_new_session=True,
        close_fds=True,
        stdout=subprocess.DEVNULL
    )
    with open(server_dir + PID_FILE, "w+") as f:
        f.write(str(process.pid))
    
    print("Server started with PID " + str(process.pid))
    return True

def stop_server(server_dir:str = SERVER_DIR, image_name:str = IMAGE_NAME):
    """
    Stop running merlin server containers.

    :param `server_dir`: location of all server related files.
    :param `image_name`: name of the image when fetched.
    """
    if get_server_status(server_dir=server_dir, image_name=image_name) != SERVER_STATUS.RUNNING:
        print("There is no instance of merlin server running.")
        print("Start a merlin server first with 'merlin server start'")
        return False

    with open(server_dir + PID_FILE, "r") as f:
        read_pid = f.read()
        process = subprocess.run(
            ["pgrep", "-P", str(read_pid)],
            stdout=subprocess.PIPE
        )
        if process.stdout == b'':
            print("Unable to get the PID for the current merlin server.")
            return False
        
        print("Attempting to close merlin server PID ", str(read_pid))
        subprocess.run(
            ["kill", str(read_pid)],
            stdout=subprocess.PIPE
        )
        time.sleep(1)
        if get_server_status(server_dir=server_dir, image_name=image_name) == SERVER_STATUS.RUNNING:
            print("Unable to kill process.")
            return False
        
        print("Merlin server terminated.")
        return True

def get_server_status(server_dir:str = SERVER_DIR, image_name:str = IMAGE_NAME):
    """
    Determine the status of the current server.
    This function can be used to check if the servers
    have been initalized, started, or stopped.
    
    :param `server_dir`: location of all server related files.
    :param `image_name`: name of the image when fetched.    
    """
    if not os.path.exists(server_dir):
        return SERVER_STATUS.NOT_INITALIZED
    
    if not os.path.exists(server_dir + image_name):
        return SERVER_STATUS.MISSING_CONTAINER
    
    if not os.path.exists(server_dir + PID_FILE):
        return SERVER_STATUS.NOT_RUNNING
    
    with open(server_dir + PID_FILE, "r") as f:
        server_pid = f.read()
        check_process = subprocess.run(
            ["pgrep", "-P", str(server_pid)],
            stdout=subprocess.PIPE
        )

        if check_process.stdout == b'':
            return SERVER_STATUS.NOT_RUNNING
        
    return SERVER_STATUS.RUNNING

if __name__ == "__main__":
    for i in sys.argv[1:]:
        if i == 'start':
            start_server()
        elif i == 'stop':
            stop_server()
        elif i =='status':
            print(get_server_status())