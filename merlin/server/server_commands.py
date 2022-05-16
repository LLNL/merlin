"""Main functions for instantiating and running Merlin server containers."""

import logging
import os
import socket
import subprocess
import time
from argparse import Namespace

from merlin.server.server_config import (
    CONFIG_DIR,
    CONFIG_FILE,
    IMAGE_NAME,
    PROCESS_FILE,
    RedisConfig,
    ServerStatus,
    config_merlin_server,
    create_server_config,
    dump_process_file,
    get_server_status,
    parse_redis_output,
    pull_process_file,
    pull_server_config,
    pull_server_image,
)
from merlin.server.server_util import valid_ipv4, valid_port


LOG = logging.getLogger("merlin")


def init_server():
    """
    Initialize merlin server by checking and initializing main configuration directory
    and local server configuration.
    """

    if not create_server_config():
        LOG.info("Merlin server initialization failed.")
        return

    config_merlin_server()

    pull_server_image()
    LOG.info("Merlin server initialization successful.")


def config_server(args: Namespace):
    redis_config = RedisConfig(os.path.join(CONFIG_DIR, CONFIG_FILE))
    if args.ipaddress is not None:
        # Check if ipaddress is valid
        if valid_ipv4(args.ipaddress):
            # Set ip address in redis config
            if not redis_config.set_config_value("bind", args.ipaddress):
                LOG.error("Unable to set ip address for redis config")
        else:
            LOG.error("Invalid IPv4 address given.")
        LOG.info(f"Ipaddress is set to {args.ipaddress}")

    if args.port is not None:
        # Check if port is valid
        if valid_port(args.port):
            # Set port in redis config
            if not redis_config.set_config_value("port", args.port):
                LOG.error("Unable to set port for redis config")
        else:
            LOG.error("Invalid port given.")
        LOG.info(f"Port is set to {args.port}")

    if args.master_user is not None:
        # Set the main user for the container
        if not redis_config.set_config_value("masteruser", args.master_user):
            LOG.error("Unable to set masteruser.")
        LOG.info(f"Master user is set to {args.master_user}")

    if args.password is not None:
        if os.path.exists(args.password):
            # Save the location of the password file in merlin_server_config
            if not redis_config.set_config_value("requirepass", args.password):
                LOG.error("Unable to set password file for redis config")
        else:
            LOG.error(f"Password file {args.password} doesn't exist.")
        LOG.info(f"Password file set to {args.password}")

    if args.add_user is not None:
        # Create a new user in container
        # Log the user in a file
        # Generated an associated password file for user
        # Return the password file for the user
        print("add_user", args.add_user)

    if args.remove_user is not None:
        # Read the user from the list of avaliable users
        # Remove user from container
        # Remove user from file
        print("remove_user", args.remove_user)

    if args.directory is not None:
        # Validate the directory input
        if os.path.exists(args.directory):
            # Set the save directory to the redis config
            if not redis_config.set_config_value("dir", args.directory):
                LOG.error("Unable to set directory for redis config")
        else:
            LOG.error("Directory given does not exist.")
        LOG.info(f"Directory is set to {args.directory}")

    if args.snapshot_seconds is not None:
        # Set the snapshot second in the redis config
        value = redis_config.get_config_value("save")
        if value is None:
            LOG.error("Unable to get exisiting parameter values for snapshot")
        else:
            value = value.split()
            value[0] = args.snapshot_seconds
            value = " ".join(value)
            if not redis_config.set_config_value("save", value):
                LOG.error("Unable to set snapshot value seconds")
        LOG.info(f"Snapshot wait time is set to {args.snapshot_seconds} seconds")

    if args.snapshot_changes is not None:
        # Set the snapshot changes into the redis config
        value = redis_config.get_config_value("save")
        if value is None:
            LOG.error("Unable to get exisiting parameter values for snapshot")
        else:
            value = value.split()
            value[1] = args.snapshot_changes
            value = " ".join(value)
            if not redis_config.set_config_value("save", value):
                LOG.error("Unable to set snapshot value seconds")
        LOG.info("Snapshot threshold is set to {args.snapshot_changes} changes")

    if args.snapshot_file is not None:
        # Set the snapshot file in the redis config
        if not redis_config.set_config_value("dbfilename", args.snapshot_file):
            LOG.error("Unable to set snapshot_file name")

        LOG.info(f"Snapshot file is set to {args.snapshot_file}")

    if args.append_mode is not None:
        valid_modes = ["always", "everysec", "no"]

        # Validate the append mode (always, everysec, no)
        if args.append_mode not in valid_modes:
            # Set the append mode in the redis config
            if not redis_config.set_config_value("appendfsync", args.append_mode):
                LOG.error("Unable to set append_mode in redis config")
        else:
            LOG.error("Not a valid append_mode(Only valid modes are always, everysec, no)")

        LOG.info(f"Append mode is set to {args.append_mode}")

    if args.append_file is not None:
        # Set the append file in the redis config
        if not redis_config.set_config_value("appendfilename", args.append_file):
            LOG.error("Unable to set append filename.")
        LOG.info(f"Append file is set to {args.append_file}")

    if redis_config.changes_made():
        redis_config.write()
        LOG.info("Merlin server config has changed. Restart merlin server to apply new configuration.")
        LOG.info("Run 'merlin server restart' to restart running merlin server")
        LOG.info("Run 'merlin server start' to start merlin server instance.")
    else:
        LOG.info("Add changes to config file using flags. Check changable configs with 'merlin server config --help'")


def status_server():
    """
    Get the server status of the any current running containers for merlin server
    """
    current_status = get_server_status()
    if current_status == ServerStatus.NOT_INITALIZED:
        LOG.info("Merlin server has not been initialized.")
        LOG.info("Please initalize server by running 'merlin server init'")
    elif current_status == ServerStatus.MISSING_CONTAINER:
        LOG.info("Unable to find server image.")
        LOG.info("Ensure there is a .sif file in merlin server directory.")
    elif current_status == ServerStatus.NOT_RUNNING:
        LOG.info("Merlin server is not running.")
    elif current_status == ServerStatus.RUNNING:
        LOG.info("Merlin server is running.")


def start_server():
    """
    Start a merlin server container using singularity.
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


def restart_server():
    if get_server_status() != ServerStatus.RUNNING:
        LOG.info("Merlin server is not currently running.")
        LOG.info("Please start a merlin server instance first with 'merlin server start'")
        return False
    stop_server()
    time.sleep(1)
    start_server()
    return True
