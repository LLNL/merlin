##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `server_commands.py` module.
"""

import logging
import os
import subprocess
from argparse import Namespace
from typing import Dict, List

import pytest

from merlin.server.server_commands import (
    check_for_not_running_server,
    config_server,
    init_server,
    restart_server,
    server_started,
    start_container,
    start_server,
    status_server,
    stop_server,
)
from merlin.server.server_config import ServerStatus
from merlin.server.server_util import ServerConfig


def test_init_server_create_server_fail(mocker: "Fixture", caplog: "Fixture"):  # noqa: F821
    """
    Test the `init_server` function with `create_server_config` returning False.
    This should log a failure message.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    """
    caplog.set_level(logging.INFO)
    create_server_mock = mocker.patch("merlin.server.server_commands.create_server_config", return_value=False)
    init_server()
    create_server_mock.assert_called_once()
    assert "Merlin server initialization failed." in caplog.text


def test_init_server_create_server_success(mocker: "Fixture", caplog: "Fixture"):  # noqa: F821
    """
    Test the `init_server` function with `create_server_config` returning True.
    This should log a sucess message.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    """
    caplog.set_level(logging.INFO)
    create_server_mock = mocker.patch("merlin.server.server_commands.create_server_config", return_value=True)
    pull_server_mock = mocker.patch("merlin.server.server_commands.pull_server_image", return_value=True)
    config_merlin_mock = mocker.patch("merlin.server.server_commands.config_merlin_server", return_value=True)
    init_server()
    create_server_mock.assert_called_once()
    pull_server_mock.assert_called_once()
    config_merlin_mock.assert_called_once()
    assert "Merlin server initialization successful." in caplog.text


def test_config_server_no_server_config(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_config_server_args: Namespace,
):
    """
    Test the `config_server` function with no server config. This should log an error
    and return False.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_config_server_args: An argparse Namespace with args needed by `config_server`
    """
    mocker.patch("merlin.server.server_commands.pull_server_config", return_value=None)
    assert not config_server(server_config_server_args)
    assert 'Try to run "merlin server init" again to reinitialize values.' in caplog.text


@pytest.mark.parametrize(
    "server_status, status_name",
    [
        (ServerStatus.RUNNING, "running"),
        (ServerStatus.NOT_RUNNING, "not_running"),
    ],
)
def test_config_server_add_user_remove_user_success(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_testing_dir: str,
    server_config_server_args: Namespace,
    server_server_config: Dict[str, Dict[str, str]],
    server_status: ServerStatus,
    status_name: str,
):
    """
    Test the `config_server` function by adding and removing a user. This will be ran with and without
    the server status being set to RUNNING. For each scenario we should expect:
    - RUNNING -> RedisUsers.write and RedisUsers.apply_to_redis are both called twice
    - NOT_RUNNING -> RedisUsers.write is called twice and RedisUsers.apply_to_redis is not called at all

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_testing_dir: The path to the the temp output directory for server tests
    :param server_config_server_args: An argparse Namespace with args needed by `config_server`
    :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
    :param server_status: The server status for this test (either RUNNING or NOT_RUNNING)
    :param status_name: The name of the status in string form so we can have unique users for each test
    """
    caplog.set_level(logging.INFO)

    # Set up the add_user and remove_user calls to test
    user_to_add_and_remove = f"test_config_server_modification_user_{status_name}"
    server_config_server_args.add_user = [user_to_add_and_remove, "test_config_server_modification_password"]
    server_config_server_args.remove_user = user_to_add_and_remove

    # Create mocks of the necessary calls for this function
    mocker.patch("merlin.server.server_commands.pull_server_config", return_value=ServerConfig(server_server_config))
    mocker.patch("merlin.server.server_commands.apply_config_changes")
    mocker.patch("merlin.server.server_commands.get_server_status", return_value=server_status)
    write_mock = mocker.patch("merlin.server.server_util.RedisUsers.write")
    apply_to_redis_mock = mocker.patch("merlin.server.server_util.RedisUsers.apply_to_redis")

    # Run the test
    expected_apply_calls = 2 if server_status == ServerStatus.RUNNING else 0
    assert config_server(server_config_server_args) is None
    assert write_mock.call_count == 2
    assert apply_to_redis_mock.call_count == expected_apply_calls
    assert f"Added user {user_to_add_and_remove} to merlin server" in caplog.text
    assert f"Removed user {user_to_add_and_remove} to merlin server" in caplog.text


def test_config_server_add_user_remove_user_failure(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_config_server_args: Namespace,
    server_server_config: Dict[str, Dict[str, str]],
):
    """
    Test the `config_server` function by attempting to add a user that already exists (we do this through mock)
    and removing a user that doesn't exist. This should run to completion but never call RedisUsers.write. It
    should also log two error messages.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_config_server_args: An argparse Namespace with args needed by `config_server`
    :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
    """
    # Set up the add_user and remove_user calls to test (these users should never actually be added/removed)
    user_to_add_and_remove = "test_config_user_not_ever_added"
    server_config_server_args.add_user = [user_to_add_and_remove, "test_config_server_modification_password"]
    server_config_server_args.remove_user = user_to_add_and_remove

    # Create mocks of the necessary calls for this function
    mocker.patch("merlin.server.server_commands.pull_server_config", return_value=ServerConfig(server_server_config))
    mocker.patch("merlin.server.server_commands.apply_config_changes")
    mocker.patch("merlin.server.server_util.RedisUsers.add_user", return_value=False)
    write_mock = mocker.patch("merlin.server.server_util.RedisUsers.write")

    # Run the test
    assert config_server(server_config_server_args) is None
    assert write_mock.call_count == 0
    assert f"User '{user_to_add_and_remove}' already exisits within current users" in caplog.text
    assert f"User '{user_to_add_and_remove}' doesn't exist within current users." in caplog.text


@pytest.mark.parametrize(
    "server_status, expected_log_msgs",
    [
        (
            ServerStatus.NOT_INITIALIZED,
            ["Merlin server has not been initialized.", "Please initalize server by running 'merlin server init'"],
        ),
        (
            ServerStatus.MISSING_CONTAINER,
            ["Unable to find server image.", "Ensure there is a .sif file in merlin server directory."],
        ),
        (ServerStatus.NOT_RUNNING, ["Merlin server is not running."]),
        (ServerStatus.RUNNING, ["Merlin server is running."]),
    ],
)
def test_status_server(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_status: ServerStatus,
    expected_log_msgs: List[str],
):
    """
    Test the `status_server` function to make sure it produces the correct logs for each status.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_status: The server status for this test
    :param expected_log_msgs: The logs we're expecting from this test
    """
    caplog.set_level(logging.INFO)
    mocker.patch("merlin.server.server_commands.get_server_status", return_value=server_status)
    status_server()
    for expected_log_msg in expected_log_msgs:
        assert expected_log_msg in caplog.text


@pytest.mark.parametrize(
    "server_status, expected_result, expected_log_msg",
    [
        (
            ServerStatus.NOT_INITIALIZED,
            False,
            "Merlin server has not been intitialized. Please run 'merlin server init' first.",
        ),
        (
            ServerStatus.MISSING_CONTAINER,
            False,
            "Merlin server has not been intitialized. Please run 'merlin server init' first.",
        ),
        (ServerStatus.NOT_RUNNING, True, None),
        (
            ServerStatus.RUNNING,
            False,
            """Merlin server already running.
                              Stop current server with 'merlin server stop' before attempting to start a new server.""",
        ),
    ],
)
def test_check_for_not_running_server(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_status: ServerStatus,
    expected_result: bool,
    expected_log_msg: str,
):
    """
    Test the `check_for_not_running_server` function with different server statuses.
    There should be a logged message for each status and the results we should expect are as
    follows:
    - NOT_RUNNING status should return True
    - any other status should return False

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_status: The server status for this test
    :param expected_result: The expected result (T/F) for this test
    :param expected_log_msg: The log we're expecting from this test
    """
    caplog.set_level(logging.INFO)
    mocker.patch("merlin.server.server_commands.get_server_status", return_value=server_status)
    assert check_for_not_running_server() == expected_result
    if expected_log_msg is not None:
        assert expected_log_msg in caplog.text


def test_start_container_invalid_image_path(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_testing_dir: str,
    server_server_config: Dict[str, Dict[str, str]],
):
    """
    Test the `start_container` function with a nonexistent image path.
    This should log an error and return None.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_testing_dir: The path to the the temp output directory for server tests
    :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
    """
    image_file = "nonexistent.image"
    server_server_config["container"]["image"] = image_file
    server_server_config["container"]["config"] = "start_container.config"

    # Create the config path so we ensure it exists
    config_path = f"{server_testing_dir}/{server_server_config['container']['config']}"
    with open(config_path, "w"):
        pass

    assert start_container(ServerConfig(server_server_config)) is None
    assert f"Unable to find image at {os.path.join(server_testing_dir, image_file)}" in caplog.text


def test_start_container_invalid_config_path(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_testing_dir: str,
    server_server_config: Dict[str, Dict[str, str]],
):
    """
    Test the `start_container` function with a nonexistent config path.
    This should log an error and return None.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_testing_dir: The path to the the temp output directory for server tests
    :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
    """
    config_file = "nonexistent.config"
    server_server_config["container"]["image"] = "start_container.image"
    server_server_config["container"]["config"] = config_file

    # Create the config path so we ensure it exists
    image_path = f"{server_testing_dir}/{server_server_config['container']['image']}"
    with open(image_path, "w"):
        pass

    assert start_container(ServerConfig(server_server_config)) is None
    assert f"Unable to find config file at {os.path.join(server_testing_dir, config_file)}" in caplog.text


def test_start_container_valid_paths(mocker: "Fixture", server_server_config: Dict[str, Dict[str, str]]):  # noqa: F821
    """
    Test the `start_container` function with valid image and config paths.
    This should return a subprocess.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
    """
    expected_return = "fake subprocess"
    mocker.patch("subprocess.Popen", return_value=expected_return)
    mocker.patch("os.path.exists", return_value=True)
    assert start_container(ServerConfig(server_server_config)) == expected_return


def test_server_started_no_redis_start(mocker: "Fixture", caplog: "Fixture"):  # noqa: F821
    """
    Test the `server_started` function with redis not starting. This should log errors and
    return False.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    """
    mock_process = mocker.Mock()
    mock_process.stdout = mocker.Mock()

    expected_redis_out_msg = "Reached end of redis output without seeing 'Ready to accept connections'"
    mocker.patch("merlin.server.server_commands.parse_redis_output", return_value=(False, expected_redis_out_msg))

    assert not server_started(mock_process, "unecessary_config")
    assert "Redis is unable to start" in caplog.text
    assert 'Check to see if there is an unresponsive instance of redis with "ps -e"' in caplog.text
    assert expected_redis_out_msg in caplog.text


def test_server_started_process_file_dump_fail(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_server_config: Dict[str, Dict[str, str]],
):
    """
    Test the `server_started` function with the dump to the process file failing.
    This should log an error and return False.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
    """
    mock_process = mocker.Mock()
    mock_process.pid = 1234
    mock_process.stdout = mocker.Mock()

    image_pid = 5678
    mocker.patch("merlin.server.server_commands.parse_redis_output", return_value=(True, {"pid": image_pid}))
    mocker.patch("merlin.server.server_commands.dump_process_file", return_value=False)

    assert not server_started(mock_process, ServerConfig(server_server_config))
    assert "Unable to create process file for container." in caplog.text


@pytest.mark.parametrize(
    "server_status",
    [
        ServerStatus.NOT_RUNNING,
        ServerStatus.MISSING_CONTAINER,
        ServerStatus.NOT_INITIALIZED,
    ],
)
def test_server_started_server_not_running(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_server_config: Dict[str, Dict[str, str]],
    server_status: ServerStatus,
):
    """
    Test the `server_started` function with the server status returning a non-running status.
    This should log an error and return False.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
    :param server_status: The server status for this test
    """
    mock_process = mocker.Mock()
    mock_process.pid = 1234
    mock_process.stdout = mocker.Mock()

    image_pid = 5678
    mocker.patch("merlin.server.server_commands.parse_redis_output", return_value=(True, {"pid": image_pid}))
    mocker.patch("merlin.server.server_commands.dump_process_file", return_value=True)
    mocker.patch("merlin.server.server_commands.get_server_status", return_value=server_status)

    assert not server_started(mock_process, ServerConfig(server_server_config))
    assert "Unable to start merlin server." in caplog.text


def test_server_started_no_issues(mocker: "Fixture", server_server_config: Dict[str, Dict[str, str]]):  # noqa: F821
    """
    Test the `server_started` function with no issues starting the server.
    This should return True.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
    """
    mock_process = mocker.Mock()
    mock_process.pid = 1234
    mock_process.stdout = mocker.Mock()

    image_pid = 5678
    mocker.patch("merlin.server.server_commands.parse_redis_output", return_value=(True, {"pid": image_pid, "port": 6379}))
    mocker.patch("merlin.server.server_commands.dump_process_file", return_value=True)
    mocker.patch("merlin.server.server_commands.get_server_status", return_value=ServerStatus.RUNNING)

    assert server_started(mock_process, ServerConfig(server_server_config))


def test_start_server_no_running_server(mocker: "Fixture"):  # noqa: F821
    """
    Test the `start_server` function with no running server. This should return False.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    """
    mocker.patch("merlin.server.server_commands.check_for_not_running_server", return_value=False)
    assert not start_server()


def test_start_server_no_server_config(mocker: "Fixture", caplog: "Fixture"):  # noqa: F821
    """
    Test the `start_server` function with no running server. This should return False
    and log an error.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    """
    mocker.patch("merlin.server.server_commands.check_for_not_running_server", return_value=True)
    mocker.patch("merlin.server.server_commands.pull_server_config", return_value=None)
    assert not start_server()
    assert 'Try to run "merlin server init" again to reinitialize values.' in caplog.text


def test_start_server_redis_container_startup_fail(mocker: "Fixture"):  # noqa: F821
    """
    Test the `start_server` function with the redis container startup failing. This should return False.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    """
    mocker.patch("merlin.server.server_commands.check_for_not_running_server", return_value=True)
    mocker.patch("merlin.server.server_commands.pull_server_config", return_value=True)
    mocker.patch("merlin.server.server_commands.start_container", return_value=None)
    assert not start_server()


def test_start_server_server_did_not_start(mocker: "Fixture"):  # noqa: F821
    """
    Test the `start_server` function with the server startup failing. This should return False.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    """
    mocker.patch("merlin.server.server_commands.check_for_not_running_server", return_value=True)
    mocker.patch("merlin.server.server_commands.pull_server_config", return_value=True)
    mocker.patch("merlin.server.server_commands.start_container", return_value=True)
    mocker.patch("merlin.server.server_commands.server_started", return_value=False)
    assert not start_server()


def test_start_server_successful_start(
    mocker: "Fixture",  # noqa: F821
    server_testing_dir: str,
    server_server_config: Dict[str, Dict[str, str]],
    server_redis_conf_file: str,
):
    """
    Test the `start_server` function with a successful start. This should return True.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param server_testing_dir: The path to the the temp output directory for server tests
    :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
    :param server_redis_conf_file: The path to a dummy redis configuration file
    """
    mocker.patch("merlin.server.server_commands.check_for_not_running_server", return_value=True)
    mocker.patch("merlin.server.server_commands.pull_server_config", return_value=ServerConfig(server_server_config))
    mocker.patch("merlin.server.server_commands.start_container", return_value=True)
    mocker.patch("merlin.server.server_commands.server_started", return_value=True)
    mocker.patch("merlin.server.server_commands.RedisUsers")
    mocker.patch("merlin.server.server_commands.RedisConfig")
    mocker.patch("merlin.server.server_commands.AppYaml")
    mocker.patch("merlin.server.server_util.ContainerConfig.get_config_path", return_value=server_redis_conf_file)
    mocker.patch("os.path.join", return_value=f"{server_testing_dir}/start_server_app.yaml")

    assert start_server()


@pytest.mark.parametrize(
    "server_status",
    [
        ServerStatus.NOT_RUNNING,
        ServerStatus.MISSING_CONTAINER,
        ServerStatus.NOT_INITIALIZED,
    ],
)
def test_stop_server_server_not_running(
    mocker: "Fixture", caplog: "Fixture", server_status: ServerStatus  # noqa: F821  # noqa: F821
):
    """
    Test the `stop_server` function with a server that's not running. This should log two messages
    and return False.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_status: The server status for this test
    """
    caplog.set_level(logging.INFO)
    mocker.patch("merlin.server.server_commands.get_server_status", return_value=server_status)
    assert not stop_server()
    assert "There is no instance of merlin server running." in caplog.text
    assert "Start a merlin server first with 'merlin server start'" in caplog.text


def test_stop_server_no_server_config(mocker: "Fixture", caplog: "Fixture"):  # noqa: F821
    """
    Test the `stop_server` function with no server config being pulled. This should log a message
    and return False.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    """
    mocker.patch("merlin.server.server_commands.get_server_status", return_value=ServerStatus.RUNNING)
    mocker.patch("merlin.server.server_commands.pull_server_config", return_value=None)
    assert not stop_server()
    assert 'Try to run "merlin server init" again to reinitialize values.' in caplog.text


def test_stop_server_empty_stdout(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_server_config: ServerConfig,
):
    """
    Test the `stop_server` function with no server config being pulled. This should log a message
    and return False.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
    """
    mocker.patch("merlin.server.server_commands.get_server_status", return_value=ServerStatus.RUNNING)
    mocker.patch("merlin.server.server_commands.pull_server_config", return_value=ServerConfig(server_server_config))
    mocker.patch("merlin.server.server_commands.pull_process_file", return_value={"parent_pid": 123})
    mock_run = mocker.patch("subprocess.run")
    mock_run.return_value.stdout = b""
    assert not stop_server()
    assert "Unable to get the PID for the current merlin server." in caplog.text


def test_stop_server_unable_to_stop_server(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_server_config: ServerConfig,
):
    """
    Test the `stop_server` function with the server status still RUNNING after trying
    to kill the server. This should log an error and return False.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
    """
    mocker.patch("merlin.server.server_commands.get_server_status", return_value=ServerStatus.RUNNING)
    mocker.patch("merlin.server.server_commands.pull_server_config", return_value=ServerConfig(server_server_config))
    mocker.patch("merlin.server.server_commands.pull_process_file", return_value={"parent_pid": 123})
    mock_run = mocker.patch("subprocess.run")
    mock_run.return_value.stdout = b"some output from status check"
    assert not stop_server()
    assert "Unable to kill process." in caplog.text


def test_stop_server_stop_command_is_not_kill(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_server_config: ServerConfig,
):
    """
    Test the `stop_server` function with a stop command that's not 'kill'.
    This should run through the command successfully and return True. The subprocess
    should run the command we provide in this test.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
    """
    mocker.patch(
        "merlin.server.server_commands.get_server_status", side_effect=[ServerStatus.RUNNING, ServerStatus.NOT_RUNNING]
    )
    mocker.patch("merlin.server.server_commands.pull_server_config", return_value=ServerConfig(server_server_config))
    mocker.patch("merlin.server.server_commands.pull_process_file", return_value={"parent_pid": 123})
    custom_stop_command = "not a kill command"
    mocker.patch("merlin.server.server_util.ContainerFormatConfig.get_stop_command", return_value=custom_stop_command)
    mock_run = mocker.patch("subprocess.run")
    mock_run.return_value.stdout = b"some output from status check"
    assert stop_server()
    mock_run.assert_called_with(custom_stop_command.split(), stdout=subprocess.PIPE)


@pytest.mark.parametrize(
    "server_status",
    [
        ServerStatus.NOT_RUNNING,
        ServerStatus.MISSING_CONTAINER,
        ServerStatus.NOT_INITIALIZED,
    ],
)
def test_restart_server_server_not_running(mocker: "Fixture", caplog: "Fixture", server_status: ServerStatus):  # noqa: F821
    """
    Test the `restart_server` function with a server that's not running.
    This should log two messages and return False.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_status: The server status for this test
    """
    caplog.set_level(logging.INFO)
    mocker.patch("merlin.server.server_commands.get_server_status", return_value=server_status)
    assert not restart_server()
    assert "Merlin server is not currently running." in caplog.text
    assert "Please start a merlin server instance first with 'merlin server start'" in caplog.text


def test_restart_server_successful_restart(mocker: "Fixture"):  # noqa: F821
    """
    Test the `restart_server` function with a successful restart. This should call
    `stop_server` and `start_server`, and return True.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    """
    mocker.patch("merlin.server.server_commands.get_server_status", return_value=ServerStatus.RUNNING)
    stop_server_mock = mocker.patch("merlin.server.server_commands.stop_server")
    start_server_mock = mocker.patch("merlin.server.server_commands.start_server")
    assert restart_server()
    stop_server_mock.assert_called_once()
    start_server_mock.assert_called_once()
