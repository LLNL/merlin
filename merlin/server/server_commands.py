##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""Main functions for instantiating and running Merlin server containers."""

import logging
import os
import socket
import subprocess
import time
from argparse import Namespace

from merlin.server.server_config import (
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
from merlin.server.server_util import AppYaml, RedisConfig, RedisUsers, ServerConfig


LOG = logging.getLogger("merlin")


def init_server():
    """
    Initializes the Merlin server by setting up the main configuration directory
    and local server configuration.
    """

    if not create_server_config():
        LOG.info("Merlin server initialization failed.")
        return
    pull_server_image()

    config_merlin_server()

    LOG.info("Merlin server initialization successful.")


def apply_config_changes(server_config: ServerConfig, args: Namespace):
    """
    Applies configuration changes to the Merlin server based on user-provided arguments.

    This function modifies the Redis configuration and user settings as specified by
    the user through the provided arguments. It updates various Redis settings, such as
    IP address, port, password, directory, snapshot settings, append mode, and file paths.
    If any changes are made, the updated configuration is written to the appropriate files.

    Args:
        server_config (server.server_util.ServerConfig): An instance of `ServerConfig` containing
            all the necessary configuration values for the server.
        args (Namespace): An argparse `Namespace` object containing user-provided arguments
            from the argument parser.
    """
    redis_config = RedisConfig(server_config.container.get_config_path())

    redis_config.set_ip_address(args.ipaddress)

    redis_config.set_port(args.port)

    redis_config.set_password(args.password)
    if args.password is not None:
        redis_users = RedisUsers(server_config.container.get_user_file_path())
        redis_users.set_password("default", args.password)
        redis_users.write()
        pass_file = server_config.container.get_pass_file_path()
        with open(pass_file, "w") as pfile:
            pfile.write(args.password)

    redis_config.set_directory(args.directory)

    redis_config.set_snapshot(seconds=args.snapshot_seconds, changes=args.snapshot_changes)

    redis_config.set_snapshot_file(args.snapshot_file)

    redis_config.set_append_mode(args.append_mode)

    redis_config.set_append_file(args.append_file)

    if redis_config.changes_made():
        redis_config.write()
        LOG.info("Merlin server config has changed. Restart merlin server to apply new configuration.")
        LOG.info("Run 'merlin server restart' to restart running merlin server")
        LOG.info("Run 'merlin server start' to start merlin server instance.")
    else:
        LOG.info("Add changes to config file and exisiting containers.")


# Pylint complains that there's too many branches in this function but
# it looks clean to me so we'll ignore it
def config_server(args: Namespace):  # pylint: disable=R0912
    """
    Processes the Merlin server configuration flags to make changes and edits
    to appropriate configurations based on user input.

    Args:
        args (Namespace): An argparse `Namespace` object containing user-provided arguments
            from the argument parser.
    """
    server_config_before_changes = pull_server_config()
    if not server_config_before_changes:
        LOG.error('Try to run "merlin server init" again to reinitialize values.')
        return False

    apply_config_changes(server_config_before_changes, args)

    server_config_after_changes = pull_server_config()
    if not server_config_after_changes:
        LOG.error('Try to run "merlin server init" again to reinitialize values.')
        return False

    # Read the user from the list of avaliable users
    redis_users = RedisUsers(server_config_after_changes.container.get_user_file_path())
    redis_config = RedisConfig(server_config_after_changes.container.get_config_path())

    if args.add_user is not None:
        # Log the user in a file
        if redis_users.add_user(user=args.add_user[0], password=args.add_user[1]):
            redis_users.write()
            LOG.info(f"Added user {args.add_user[0]} to merlin server")
            # Create a new user in container
            if get_server_status() == ServerStatus.RUNNING:
                LOG.info("Adding user to current merlin server instance")
                redis_users.apply_to_redis(redis_config.get_ip_address(), redis_config.get_port(), redis_config.get_password())
        else:
            LOG.error(f"User '{args.add_user[0]}' already exisits within current users")

    if args.remove_user is not None:
        # Remove user from file
        if redis_users.remove_user(args.remove_user):
            redis_users.write()
            LOG.info(f"Removed user {args.remove_user} to merlin server")
            # Remove user from container
            if get_server_status() == ServerStatus.RUNNING:
                LOG.info("Removing user to current merlin server instance")
                redis_users.apply_to_redis(redis_config.get_ip_address(), redis_config.get_port(), redis_config.get_password())
        else:
            LOG.error(f"User '{args.remove_user}' doesn't exist within current users.")

    return None


def status_server():
    """
    Retrieves and displays the current status of the Merlin server.

    This function checks the status of any running containers for the Merlin server
    and logs appropriate messages based on the server's state.
    """
    current_status = get_server_status()
    if current_status == ServerStatus.NOT_INITIALIZED:
        LOG.info("Merlin server has not been initialized.")
        LOG.info("Please initalize server by running 'merlin server init'")
    elif current_status == ServerStatus.MISSING_CONTAINER:
        LOG.info("Unable to find server image.")
        LOG.info("Ensure there is a .sif file in merlin server directory.")
    elif current_status == ServerStatus.NOT_RUNNING:
        LOG.info("Merlin server is not running.")
    elif current_status == ServerStatus.RUNNING:
        LOG.info("Merlin server is running.")


def check_for_not_running_server() -> bool:
    """
    Checks if the Merlin server status is `NOT_RUNNING` before starting a new server.

    If the server status is anything other than `NOT_RUNNING`, logs an appropriate
    error message to inform the user.

    Returns:
        True if the server status is `NOT_RUNNING`, False otherwise.
    """
    current_status = get_server_status()
    uninitialized_err = "Merlin server has not been intitialized. Please run 'merlin server init' first."
    status_errors = {
        ServerStatus.NOT_INITIALIZED: uninitialized_err,
        ServerStatus.MISSING_CONTAINER: uninitialized_err,
        ServerStatus.RUNNING: """Merlin server already running.
                              Stop current server with 'merlin server stop' before attempting to start a new server.""",
    }

    if current_status in status_errors:
        LOG.info(status_errors[current_status])
        return False

    return True


def start_container(server_config: ServerConfig) -> subprocess.Popen:
    """
    Starts a container based on the provided server configuration.

    This function uses the server configuration to locate the necessary image and
    configuration files, validates their existence, and starts the container using
    a subprocess.

    Args:
        server_config (server.server_util.ServerConfig): An instance of `ServerConfig`
            containing information about the server to start, including paths to the image
            and configuration files.

    Returns:
        A subprocess object representing the running container process, or `None` if
            required files are missing.
    """
    image_path = server_config.container.get_image_path()
    config_path = server_config.container.get_config_path()
    path_errors = {
        image_path: "image",
        config_path: "config file",
    }

    for path in (image_path, config_path):
        if not os.path.exists(path):
            LOG.error(f"Unable to find {path_errors[path]} at {path}")
            return None

    # Pylint wants us to use with here but we don't need that
    process = subprocess.Popen(  # pylint: disable=R1732
        server_config.container_format.get_run_command()
        .strip("\\")
        .format(
            command=server_config.container_format.get_command(),
            home_dir=server_config.container.get_config_dir(),
            image=image_path,
            config=config_path,
        )
        .split(),
        start_new_session=True,
        close_fds=True,
        stdout=subprocess.PIPE,
    )

    time.sleep(1)

    return process


def server_started(process: subprocess.Popen, server_config: ServerConfig) -> bool:
    """
    Verifies that the server started by [`start_container`][server.server_commands.start_container]
    is running properly.

    This function checks the Redis output to ensure the server started successfully,
    creates a process file for the container, and validates that the server status
    is `RUNNING`.

    Args:
        process (subprocess.Popen): The subprocess object representing the container
            process started by `start_container`.
        server_config (server.server_util.ServerConfig): An instance of `ServerConfig`
            containing information about the Redis server configuration.

    Returns:
        True if the server started successfully, False otherwise.
    """
    redis_start, redis_out = parse_redis_output(process.stdout)

    if not redis_start:
        LOG.error("Redis is unable to start")
        LOG.error('Check to see if there is an unresponsive instance of redis with "ps -e"')
        LOG.error(redis_out.strip("\n"))
        return False

    redis_out["image_pid"] = redis_out.pop("pid")
    redis_out["parent_pid"] = process.pid
    redis_out["hostname"] = socket.gethostname()
    if not dump_process_file(redis_out, server_config.container.get_pfile_path()):
        LOG.error("Unable to create process file for container.")
        return False

    if get_server_status() != ServerStatus.RUNNING:
        LOG.error("Unable to start merlin server.")
        return False

    LOG.info(f"Server started with PID {str(process.pid)}.")
    LOG.info(f'Merlin server operating on "{redis_out["hostname"]}" and port "{redis_out["port"]}".')
    return True


def start_server() -> bool:  # pylint: disable=R0911
    """
    Starts a Merlin server container.

    This function performs several steps to start the server, including checking
    for an existing non-running server, pulling the server configuration, starting
    the container, verifying the server startup, and applying Redis user and
    configuration settings. It also generates a new `app.yaml` file for the server
    configuration.

    Returns:
        True if the server was successfully started, False otherwise.
    """
    if not check_for_not_running_server():
        return False

    server_config = pull_server_config()
    if not server_config:
        LOG.error('Try to run "merlin server init" again to reinitialize values.')
        return False

    process = start_container(server_config)
    if process is None:
        return False

    if not server_started(process, server_config):
        return False

    redis_users = RedisUsers(server_config.container.get_user_file_path())
    redis_config = RedisConfig(server_config.container.get_config_path())
    redis_users.apply_to_redis(redis_config.get_ip_address(), redis_config.get_port(), redis_config.get_password())

    new_app_yaml = os.path.join(server_config.container.get_config_dir(), "app.yaml")
    app_yaml = AppYaml()
    app_yaml.apply_server_config(server_config=server_config)
    app_yaml.write(new_app_yaml)
    LOG.info(f"New app.yaml written to {new_app_yaml}.")
    LOG.info("Replace app.yaml in ~/.merlin/app.yaml to use merlin server as main configuration.")
    LOG.info("To use for local runs, move app.yaml into the running directory.")

    return True


def stop_server() -> bool:
    """
    Stops a running Merlin server container.

    This function checks the current server status, retrieves the server configuration,
    and attempts to terminate the running server process. If successful, the server
    process is stopped, and the function returns True. Otherwise, it logs errors
    and returns False.

    Returns:
        True if the server was successfully stopped, False otherwise.
    """
    if get_server_status() != ServerStatus.RUNNING:
        LOG.info("There is no instance of merlin server running.")
        LOG.info("Start a merlin server first with 'merlin server start'")
        return False

    server_config = pull_server_config()
    if not server_config:
        LOG.error('Try to run "merlin server init" again to reinitialize values.')
        return False

    pf_data = pull_process_file(server_config.container.get_pfile_path())
    read_pid = pf_data["parent_pid"]

    process = subprocess.run(
        server_config.process.get_status_command().strip("\\").format(pid=read_pid).split(), stdout=subprocess.PIPE
    )
    if process.stdout == b"":
        LOG.error("Unable to get the PID for the current merlin server.")
        return False

    command = server_config.process.get_kill_command().strip("\\").format(pid=read_pid).split()
    if server_config.container_format.get_stop_command() != "kill":
        command = (
            server_config.container_format.get_stop_command()
            .strip("\\")
            .format(name=server_config.container.get_image_name)
            .split()
        )

    LOG.info(f"Attempting to close merlin server PID {str(read_pid)}")

    subprocess.run(command, stdout=subprocess.PIPE)
    time.sleep(1)
    if get_server_status() == ServerStatus.RUNNING:
        LOG.error("Unable to kill process.")
        return False

    LOG.info("Merlin server terminated.")
    return True


def restart_server() -> bool:
    """
    Restarts a running Merlin server instance.

    This function stops the currently running Merlin server and then starts it again.
    If the server is not running, it logs a message and returns False.

    Returns:
        True if the server was successfully restarted, False otherwise.
    """
    if get_server_status() != ServerStatus.RUNNING:
        LOG.info("Merlin server is not currently running.")
        LOG.info("Please start a merlin server instance first with 'merlin server start'")
        return False
    stop_server()
    time.sleep(1)
    start_server()
    return True
