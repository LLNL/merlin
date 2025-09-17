##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `server_config.py` module.
"""

import io
import logging
import os
import string
from importlib import resources
from typing import Dict, Tuple, Union

import pytest
import yaml

from merlin.server.server_config import (
    PASSWORD_LENGTH,
    ServerStatus,
    check_process_file_format,
    config_merlin_server,
    copy_container_command_files,
    create_server_config,
    dump_process_file,
    generate_password,
    get_server_status,
    parse_redis_output,
    pull_process_file,
    pull_server_config,
    pull_server_image,
)
from merlin.server.server_util import CONTAINER_TYPES, MERLIN_SERVER_SUBDIR, ServerConfig


def test_generate_password_no_pass_command():
    """
    Test the `generate_password` function with no password command.
    This should generate a password of 256 (PASSWORD_LENGTH) random ASCII characters.
    """
    generated_password = generate_password(PASSWORD_LENGTH)
    assert len(generated_password) == PASSWORD_LENGTH
    valid_ascii_chars = string.ascii_letters + string.digits + "!@#$%^&*()"
    for ch in generated_password:
        assert ch in valid_ascii_chars


def test_generate_password_with_pass_command():
    """
    Test the `generate_password` function with no password command.
    This should generate a password of 256 (PASSWORD_LENGTH) random ASCII characters.
    """
    test_pass = "test-password"
    generated_password = generate_password(0, pass_command=f"echo {test_pass}")
    assert generated_password == test_pass


@pytest.mark.parametrize(
    "line, expected_return",
    [
        (None, (False, "None passed as redis output")),
        (b"", (False, "Reached end of redis output without seeing 'Ready to accept connections'")),
        (b"Ready to accept connections", (True, {})),
        (b"aborting", (False, "aborting")),
        (b"Fatal error", (False, "Fatal error")),
    ],
)
def test_parse_redis_output_with_basic_input(line: Union[None, bytes], expected_return: Tuple[bool, Union[str, Dict]]):
    """
    Test the `parse_redis_output` function with basic input.
    Here "basic input" means single line input or None as input.

    :param line: The value to pass in as input to `parse_redis_output`
    :param expected_return: The expected return value based on what was passed in for `line`
    """
    if line is None:
        reader_input = None
    else:
        buffer = io.BytesIO(line)
        reader_input = io.BufferedReader(buffer)
    actual_return = parse_redis_output(reader_input)
    assert expected_return == actual_return


@pytest.mark.parametrize(
    "lines, expected_config",
    [
        (  # Testing setting vars before initialized message
            b"port=6379 blah blah server=127.0.0.1\nServer initialized\nReady to accept connections",
            {"port": "6379", "server": "127.0.0.1"},
        ),
        (  # Testing setting vars after initialized message
            b"Server initialized\nport=6379 blah blah server=127.0.0.1\nReady to accept connections",
            {},
        ),
        (  # Testing setting vars before + after initialized message
            b"blah blah max_connections=100 blah\n"
            b"Server initialized\n"
            b"port=6379 blah blah server=127.0.0.1\n"
            b"Ready to accept connections",
            {"max_connections": "100"},
        ),
    ],
)
def test_parse_redis_output_with_vars(lines: bytes, expected_config: Tuple[bool, Union[str, Dict]]):
    """
    Test the `parse_redis_output` function with input that has variables in lines.
    This should set any variable given before the "Server initialized" message is provided.

    We'll test setting vars before the initialized message, after, and both before and after.

    :param lines: The lines to pass in as input to `parse_redis_output`
    :param expected_config: The expected config dict based on what was passed in for `lines`
    """
    buffer = io.BytesIO(lines)
    reader_input = io.BufferedReader(buffer)
    _, actual_vars = parse_redis_output(reader_input)
    assert expected_config == actual_vars


def test_copy_container_command_files_with_existing_files(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_testing_dir: str,
):
    """
    Test the `copy_container_command_files` function with files that already exist.
    This should skip trying to create the files, log 3 "file already exists" messages,
    and return True.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_testing_dir: The path to the the temp output directory for server tests
    """
    caplog.set_level(logging.INFO)
    mocker.patch("os.path.exists", return_value=True)
    assert copy_container_command_files(server_testing_dir)
    file_names = [f"{container}.yaml" for container in CONTAINER_TYPES]
    for file in file_names:
        assert f"{file} already exists." in caplog.text


def test_copy_container_command_files_with_nonexisting_files(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_testing_dir: str,
):
    """
    Test the `copy_container_command_files` function with files that don't already exist.
    This should create the files, log messages for each file, and return True

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_testing_dir: The path to the the temp output directory for server tests
    """
    caplog.set_level(logging.INFO)

    # Mock the os.path.exists function so it returns False
    mocker.patch("os.path.exists", return_value=False)

    # Mock the resources.path context manager
    mock_path = mocker.patch("merlin.server.server_config.resources.path")
    mock_path.return_value.__enter__.return_value = "mocked_file_path"

    # Mock the open builtin
    mock_data = mocker.mock_open(read_data="mocked data")
    mocker.patch("builtins.open", mock_data)

    assert copy_container_command_files(server_testing_dir)
    file_names = [f"{container}.yaml" for container in CONTAINER_TYPES]
    for file in file_names:
        assert f"Copying file {file} to configuration directory." in caplog.text


def test_copy_container_command_files_with_oserror(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_testing_dir: str,
):
    """
    Test the `copy_container_command_files` function with an OSError being raised.
    This should log an error message and return False.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_testing_dir: The path to the the temp output directory for server tests
    """
    # Mock the open function to raise an OSError
    mocker.patch("builtins.open", side_effect=OSError("File not writeable"))

    assert not copy_container_command_files(server_testing_dir)
    assert f"Destination location {server_testing_dir} is not writable." in caplog.text


def test_create_server_config_merlin_config_dir_nonexistent(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_testing_dir: str,
):
    """
    Tests the `create_server_config` function with MERLIN_HOME not existing.
    This should log an error and return False.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_testing_dir: The path to the the temp output directory for server tests
    """
    nonexistent_dir = f"{server_testing_dir}/merlin_config_dir"
    mocker.patch("merlin.server.server_config.MERLIN_HOME", nonexistent_dir)
    assert not create_server_config()
    assert f"Unable to find main merlin configuration directory at {nonexistent_dir}" in caplog.text


def test_create_server_config_server_subdir_nonexistent_oserror(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_testing_dir: str,
):
    """
    Tests the `create_server_config` function with MERLIN_HOME/MERLIN_SERVER_SUBDIR
    not existing and an OSError being raised. This should log an error and return False.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_testing_dir: The path to the the temp output directory for server tests
    """

    # Mock MERLIN_HOME and MERLIN_SERVER_SUBDIR
    nonexistent_server_subdir = "test_create_server_config_server_subdir_nonexistent"
    mocker.patch("merlin.server.server_config.MERLIN_HOME", server_testing_dir)
    mocker.patch("merlin.server.server_config.MERLIN_SERVER_SUBDIR", nonexistent_server_subdir)

    # Mock os.mkdir so it raises an OSError
    err_msg = "File not writeable"
    mocker.patch("os.mkdir", side_effect=OSError(err_msg))
    assert not create_server_config()
    assert err_msg in caplog.text


def test_create_server_config_no_server_config(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_testing_dir: str,
):
    """
    Tests the `create_server_config` function with the call to `pull_server_config()`
    returning None. This should log an error and return False.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_testing_dir: The path to the the temp output directory for server tests
    """

    # Mock the necessary variables/functions to get us to the pull_server_config call
    mocker.patch("merlin.server.server_config.MERLIN_HOME", server_testing_dir)
    mocker.patch("merlin.server.server_config.copy_container_command_files", return_value=True)
    mock_open_func = mocker.mock_open(read_data="key: value")
    mocker.patch("builtins.open", mock_open_func)

    # Mock the pull_server_config call (what we're actually testing) and run the test
    mocker.patch("merlin.server.server_config.pull_server_config", return_value=None)
    assert not create_server_config()
    assert 'Try to run "merlin server init" again to reinitialize values.' in caplog.text


def test_create_server_config_no_server_dir(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_testing_dir: str,
    server_server_config: Dict[str, str],
):
    """
    Tests the `create_server_config` function with the call to
    `server_config.container.get_config_dir()` returning a non-existent path. This should
    log a message and create the directory, then return True.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_testing_dir: The path to the the temp output directory for server tests
    :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
    """
    caplog.set_level(logging.INFO)

    # Mock the necessary variables/functions to get us to the get_config_dir call
    mocker.patch("merlin.server.server_config.MERLIN_HOME", server_testing_dir)
    mocker.patch("merlin.server.server_config.copy_container_command_files", return_value=True)
    mock_open_func = mocker.mock_open(read_data="key: value")
    mocker.patch("builtins.open", mock_open_func)
    mocker.patch("merlin.server.server_config.load_server_config", return_value=ServerConfig(server_server_config))

    # Mock the get_config_dir call to return a directory that doesn't exist yet
    nonexistent_dir = os.path.join(server_testing_dir, "merlin_server")
    mocker.patch("merlin.server.server_util.ContainerConfig.get_config_dir", return_value=nonexistent_dir)

    assert create_server_config()
    assert os.path.exists(nonexistent_dir)
    assert "Creating merlin server directory." in caplog.text


def test_config_merlin_server_no_server_config(mocker: "Fixture", caplog: "Fixture"):  # noqa: F821
    """
    Test the `config_merlin_server` function with the call to `pull_server_config()`
    returning None. This should log an error and return False.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    """
    mocker.patch("merlin.server.server_config.pull_server_config", return_value=None)
    assert not config_merlin_server()
    assert 'Try to run "merlin server init" again to reinitialize values.' in caplog.text


def test_config_merlin_server_pass_user_exist(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_testing_dir: str,
    server_server_config: Dict[str, str],
):
    """
    Tests the `config_merlin_server` function with a password file and user file already
    existing. This should log 2 messages and return None.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_testing_dir: The path to the the temp output directory for server tests
    :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
    """
    caplog.set_level(logging.INFO)

    # Create the password file and user file
    pass_file = f"{server_testing_dir}/existent_pass_file.txt"
    user_file = f"{server_testing_dir}/existent_user_file.txt"
    with open(pass_file, "w"), open(user_file, "w"):
        pass

    # Mock necessary calls
    mocker.patch("merlin.server.server_config.pull_server_config", return_value=ServerConfig(server_server_config))
    mocker.patch("merlin.server.server_util.ContainerConfig.get_pass_file_path", return_value=pass_file)
    mocker.patch("merlin.server.server_util.ContainerConfig.get_user_file_path", return_value=user_file)

    assert config_merlin_server() is None
    assert "Password file already exists. Skipping password generation step." in caplog.text
    assert "User file already exists." in caplog.text


def test_config_merlin_server_pass_user_dont_exist(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_testing_dir: str,
    server_server_config: Dict[str, str],
):
    """
    Tests the `config_merlin_server` function with a password file and user file that don't
    already exist. This should log 2 messages, create the files, and return None.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_testing_dir: The path to the the temp output directory for server tests
    :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
    """
    caplog.set_level(logging.INFO)

    # Create vars for the nonexistent password file and user file
    pass_file = f"{server_testing_dir}/nonexistent_pass_file.txt"
    user_file = f"{server_testing_dir}/nonexistent_user_file.txt"

    # Mock necessary calls
    mocker.patch("merlin.server.server_config.pull_server_config", return_value=ServerConfig(server_server_config))
    mocker.patch("merlin.server.server_util.ContainerConfig.get_pass_file_path", return_value=pass_file)
    mocker.patch("merlin.server.server_util.ContainerConfig.get_user_file_path", return_value=user_file)

    assert config_merlin_server() is None
    assert os.path.exists(pass_file)
    assert os.path.exists(user_file)
    assert "Creating password file for merlin server container." in caplog.text
    assert f"User {os.environ.get('USER')} created in user file for merlin server container" in caplog.text


def setup_pull_server_config_mock(
    mocker: "Fixture",  # noqa: F821
    server_testing_dir: str,
    server_app_yaml_contents: Dict[str, Union[str, int]],
    server_server_config: Dict[str, Dict[str, str]],
):
    """
    Setup the necessary mocker calls for the `pull_server_config` function.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param server_testing_dir: The path to the the temp output directory for server tests
    :param server_app_yaml_contents: A dict of app.yaml configurations
    :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
    """
    mocker.patch("merlin.server.server_util.AppYaml.get_data", return_value=server_app_yaml_contents)
    mocker.patch("merlin.server.server_config.MERLIN_HOME", server_testing_dir)
    mock_data = mocker.mock_open(read_data=str(server_server_config))
    mocker.patch("builtins.open", mock_data)


@pytest.mark.parametrize(
    "key_to_delete, expected_log_message",
    [
        ("container", 'Unable to find "container"'),
        ("container.format", 'Unable to find "format"'),
        ("process", 'Unable to find "process"'),
    ],
)
def test_pull_server_config_missing_config_keys(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_testing_dir: str,
    server_app_yaml_contents: Dict[str, Union[str, int]],
    server_server_config: Dict[str, Dict[str, str]],
    key_to_delete: str,
    expected_log_message: str,
):
    """
    Test the `pull_server_config` function with missing container-related keys in the
    app.yaml file contents. This should log an error message and return None.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_testing_dir: The path to the the temp output directory for server tests
    :param server_app_yaml_contents: A dict of app.yaml configurations
    :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
    :param key_to_delete: The key to delete from the app.yaml contents
    :param expected_log_message: The expected log message when the key is missing
    """
    # Handle nested key deletion
    keys = key_to_delete.split(".")
    temp_app_yaml = server_app_yaml_contents
    for key in keys[:-1]:
        temp_app_yaml = temp_app_yaml[key]
    del temp_app_yaml[keys[-1]]

    setup_pull_server_config_mock(mocker, server_testing_dir, server_app_yaml_contents, server_server_config)

    assert pull_server_config() is None
    assert expected_log_message in caplog.text


@pytest.mark.parametrize("key_to_delete", ["command", "run_command", "stop_command", "pull_command"])
def test_pull_server_config_missing_format_needed_keys(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_testing_dir: str,
    server_app_yaml_contents: Dict[str, Union[str, int]],
    server_container_format_config_data: Dict[str, str],
    server_server_config: Dict[str, Dict[str, str]],
    key_to_delete: str,
):
    """
    Test the `pull_server_config` function with necessary format keys missing in the
    singularity.yaml file contents. This should log an error message and return None.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_testing_dir: The path to the the temp output directory for server tests
    :param server_app_yaml_contents: A dict of app.yaml configurations
    :param server_container_format_config_data: A pytest fixture of test data to pass to the ContainerFormatConfig class
    :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
    :param key_to_delete: The key to delete from the singularity.yaml contents
    """
    del server_container_format_config_data[key_to_delete]
    setup_pull_server_config_mock(mocker, server_testing_dir, server_app_yaml_contents, server_server_config)

    assert pull_server_config() is None
    format_file_basename = server_app_yaml_contents["container"]["format"] + ".yaml"
    format_file = os.path.join(server_testing_dir, MERLIN_SERVER_SUBDIR)
    format_file = os.path.join(format_file, format_file_basename)
    assert f'Unable to find necessary "{key_to_delete}" value in format config file {format_file}' in caplog.text


@pytest.mark.parametrize("key_to_delete", ["status", "kill"])
def test_pull_server_config_missing_process_needed_key(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_testing_dir: str,
    server_app_yaml_contents: Dict[str, Union[str, int]],
    server_process_config_data: Dict[str, str],
    server_server_config: Dict[str, Dict[str, str]],
    key_to_delete: str,
):
    """
    Test the `pull_server_config` function with necessary process keys missing.
    This should log an error message and return None.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_testing_dir: The path to the the temp output directory for server tests
    :param server_app_yaml_contents: A dict of app.yaml configurations
    :param server_process_config_data: A pytest fixture of test data to pass to the ProcessConfig class
    :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
    :param key_to_delete: The key to delete from the process config entry
    """
    del server_process_config_data[key_to_delete]
    setup_pull_server_config_mock(mocker, server_testing_dir, server_app_yaml_contents, server_server_config)

    assert pull_server_config() is None
    assert f'Process necessary "{key_to_delete}" command configuration not found in' in caplog.text


def test_pull_server_config_no_issues(
    mocker: "Fixture",  # noqa: F821
    server_testing_dir: str,
    server_app_yaml_contents: Dict[str, Union[str, int]],
    server_server_config: Dict[str, Dict[str, str]],
):
    """
    Test the `pull_server_config` function without any problems. This should
    return a ServerConfig object.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param server_testing_dir: The path to the the temp output directory for server tests
    :param server_app_yaml_contents: A dict of app.yaml configurations
    :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
    """
    setup_pull_server_config_mock(mocker, server_testing_dir, server_app_yaml_contents, server_server_config)
    assert isinstance(pull_server_config(), ServerConfig)


def test_pull_server_image_no_server_config(mocker: "Fixture", caplog: "Fixture"):  # noqa: F821
    """
    Test the `pull_server_image` function with no server config being found.
    This should return False and log an error message.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    """
    mocker.patch("merlin.server.server_config.pull_server_config", return_value=None)
    assert not pull_server_image()
    assert 'Try to run "merlin server init" again to reinitialize values.' in caplog.text


def setup_pull_server_image_mock(
    mocker: "Fixture",  # noqa: F821
    server_testing_dir: str,
    server_server_config: Dict[str, Dict[str, str]],
    config_dir: str,
    config_file: str,
    image_file: str,
    create_config_file: bool = False,
    create_image_file: bool = False,
):
    """
    Set up the necessary mock calls for the `pull_server_image` function.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param server_testing_dir: The path to the the temp output directory for server tests
    :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
    """
    image_url = "docker://redis"
    image_path = f"{server_testing_dir}/{image_file}"

    os.makedirs(config_dir, exist_ok=True)

    mocker.patch("merlin.server.server_config.pull_server_config", return_value=ServerConfig(server_server_config))
    mocker.patch("merlin.server.server_util.ContainerConfig.get_config_dir", return_value=config_dir)
    mocker.patch("merlin.server.server_util.ContainerConfig.get_config_name", return_value=config_file)
    mocker.patch("merlin.server.server_util.ContainerConfig.get_image_url", return_value=image_url)
    mocker.patch("merlin.server.server_util.ContainerConfig.get_image_path", return_value=image_path)

    if create_config_file:
        with open(os.path.join(config_dir, config_file), "w"):
            pass

    if create_image_file:
        with open(image_path, "w"):
            pass


def test_pull_server_image_no_image_path_no_config_path(
    mocker: "Fixture",  # noqa: F821
    server_testing_dir: str,
    server_server_config: Dict[str, Dict[str, str]],
):
    """
    Test the `pull_server_image` function with no image path and no configuration
    path. This should run a subprocess for the image path and create the configuration
    file. It should also return True.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param server_testing_dir: The path to the the temp output directory for server tests
    :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
    """
    # Set up mock calls to simulate the setup of this function
    config_dir = f"{server_testing_dir}/config_dir"
    config_file = "redis.conf"
    image_file = "pull_server_image_no_image_path_no_config_path_image_nonexistent.sif"
    setup_pull_server_image_mock(mocker, server_testing_dir, server_server_config, config_dir, config_file, image_file)
    mocked_subprocess = mocker.patch("subprocess.run")

    # Mock the open function
    read_data = "Mocked file content"
    mocked_open = mocker.mock_open(read_data=read_data)
    mocked_open.write = mocker.Mock()
    mocker.patch("builtins.open", mocked_open)

    # Call the function
    assert pull_server_image()

    # Assert that the subprocess call to pull the image was called
    mocked_subprocess.assert_called_once()

    # Assert that open was called with the correct arguments
    mocked_open.assert_any_call(os.path.join(config_dir, config_file), "w")
    with resources.path("merlin.server", config_file) as file:
        mocked_open.assert_any_call(file, "r")
    assert mocked_open.call_count == 2

    # Assert that the write method was called with the expected content
    mocked_open().write.assert_called_once_with(read_data)


def test_pull_server_image_both_paths_exist(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_testing_dir: str,
    server_server_config: Dict[str, Dict[str, str]],
):
    """
    Test the `pull_server_image` function with both an image path and a configuration
    path that both exist. This should log two messages and return True.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_testing_dir: The path to the the temp output directory for server tests
    :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
    """
    caplog.set_level(logging.INFO)

    # Set up mock calls to simulate the setup of this function
    config_dir = f"{server_testing_dir}/config_dir"
    config_file = "pull_server_image_both_paths_exist_config.yaml"
    image_file = "pull_server_image_both_paths_exist_image.sif"
    setup_pull_server_image_mock(
        mocker,
        server_testing_dir,
        server_server_config,
        config_dir,
        config_file,
        image_file,
        create_config_file=True,
        create_image_file=True,
    )

    assert pull_server_image()
    assert f"{image_file} already exists." in caplog.text
    assert "Redis configuration file already exist." in caplog.text


def test_pull_server_image_os_error(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_testing_dir: str,
    server_server_config: Dict[str, Dict[str, str]],
):
    """
    Test the `pull_server_image` function with an image path but no configuration
    path. We'll force this to raise an OSError when writing to the configuration file
    to ensure it's handled properly. This should log an error and return False.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_testing_dir: The path to the the temp output directory for server tests
    :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
    """
    # Set up mock calls to simulate the setup of this function
    config_dir = f"{server_testing_dir}/config_dir"
    config_file = "pull_server_image_os_error_config.yaml"
    image_file = "pull_server_image_os_error_config_nonexistent.sif"
    setup_pull_server_image_mock(
        mocker,
        server_testing_dir,
        server_server_config,
        config_dir,
        config_file,
        image_file,
        create_image_file=True,
    )

    # Mock the open function
    mocker.patch("builtins.open", side_effect=OSError)

    # Run the test
    assert not pull_server_image()
    assert f"Destination location {config_dir} is not writable." in caplog.text


@pytest.mark.parametrize(
    "server_config_exists, config_exists, image_exists, pfile_exists, expected_status",
    [
        (False, True, True, True, ServerStatus.NOT_INITIALIZED),  # No server config
        (True, False, True, True, ServerStatus.NOT_INITIALIZED),  # Config dir does not exist
        (True, True, False, True, ServerStatus.MISSING_CONTAINER),  # Image path does not exist
        (True, True, True, False, ServerStatus.NOT_RUNNING),  # Pfile path does not exist
    ],
)
def test_get_server_status_initial_checks(
    mocker: "Fixture",  # noqa: F821
    server_server_config: Dict[str, Dict[str, str]],
    server_config_exists: bool,
    config_exists: bool,
    image_exists: bool,
    pfile_exists: bool,
    expected_status: ServerStatus,
):
    """
    Test the `get_server_status` function for the initial conditional checks that it looks for.
    These checks include:
    - no server configuration -> should return NOT_INITIALIZED
    - no config directory path -> should return NOT_INITIALIZED
    - no image path -> should return MISSING_CONTAINER
    - no password file -> should return NOT_RUNNING

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
    :param server_config_exists: A boolean to denote whether the server config exists in this test or not
    :param config_exists: A boolean to denote whether the config dir exists in this test or not
    :param image_exists: A boolean to denote whether the image path exists in this test or not
    :param pfile_exists: A boolean to denote whether the password file exists in this test or not
    :param expected_status: The status we're expecting `get_server_status` to return for this test
    """
    # Mock the necessary calls
    if server_config_exists:
        mocker.patch("merlin.server.server_config.pull_server_config", return_value=ServerConfig(server_server_config))
        mocker.patch("merlin.server.server_util.ContainerConfig.get_config_dir", return_value="config_dir")
        mocker.patch("merlin.server.server_util.ContainerConfig.get_image_path", return_value="image_path")
        mocker.patch("merlin.server.server_util.ContainerConfig.get_pfile_path", return_value="pfile_path")

        # Mock os.path.exists to return the desired values
        mocker.patch(
            "os.path.exists",
            side_effect=lambda path: {"config_dir": config_exists, "image_path": image_exists, "pfile_path": pfile_exists}.get(
                path, False
            ),
        )
    else:
        mocker.patch("merlin.server.server_config.pull_server_config", return_value=None)

    # Call the function and assert the expected status
    assert get_server_status() == expected_status


@pytest.mark.parametrize(
    "stdout_val, expected_status",
    [
        (b"", ServerStatus.NOT_RUNNING),  # No stdout from subprocess
        (b"Successfully started", ServerStatus.RUNNING),  # Stdout from subprocess exists
    ],
)
def test_get_server_status_subprocess_check(
    mocker: "Fixture",  # noqa: F821
    server_server_config: Dict[str, Dict[str, str]],
    stdout_val: bytes,
    expected_status: ServerStatus,
):
    """
    Test the `get_server_status` function with empty stdout return from the subprocess run.
    This should return a NOT_RUNNING status.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
    """
    mocker.patch("merlin.server.server_config.pull_server_config", return_value=ServerConfig(server_server_config))
    mocker.patch("os.path.exists", return_value=True)
    mocker.patch("merlin.server.server_config.pull_process_file", return_value={"parent_pid": 123})
    mock_run = mocker.patch("subprocess.run")
    mock_run.return_value.stdout = stdout_val

    assert get_server_status() == expected_status


@pytest.mark.parametrize(
    "data_to_test, expected_result",
    [
        ({"image_pid": 123, "port": 6379, "hostname": "dummy_server"}, False),  # No parent_pid entry
        ({"parent_pid": 123, "port": 6379, "hostname": "dummy_server"}, False),  # No image_pid entry
        ({"parent_pid": 123, "image_pid": 456, "hostname": "dummy_server"}, False),  # No port entry
        ({"parent_pid": 123, "image_pid": 123, "port": 6379}, False),  # No hostname entry
        ({"parent_pid": 123, "image_pid": 123, "port": 6379, "hostname": "dummy_server"}, True),  # All required entries exist
    ],
)
def test_check_process_file_format(data_to_test: Dict[str, Union[int, str]], expected_result: bool):
    """
    Test the `check_process_file_format` function. The first 4 parametrized tests above should all
    return False as they're all missing a required key. The final parametrized test above should return
    True since it has every required key.

    :param data_to_test: The data dict that we'll pass in to the `check_process_file_format` function
    :param expected_result: The return value we expect based on `data_to_test`
    """
    assert check_process_file_format(data_to_test) == expected_result


def test_pull_process_file_valid_file(server_testing_dir: str, server_process_file_contents: Dict[str, Union[int, str]]):
    """
    Test the `pull_process_file` function with a valid process file. This test will create a test
    process file with valid contents that `pull_process_file` will read in and return.

    :param server_testing_dir: The path to the the temp output directory for server tests
    :param server_process_file_contents: A fixture representing process file contents
    """
    # Create the valid process file in our temp testing directory
    process_filepath = f"{server_testing_dir}/valid_process_file.yaml"
    with open(process_filepath, "w") as process_file:
        yaml.dump(server_process_file_contents, process_file)

    # Run the test
    assert pull_process_file(process_filepath) == server_process_file_contents


def test_pull_process_file_invalid_file(server_testing_dir: str, server_process_file_contents: Dict[str, Union[int, str]]):
    """
    Test the `pull_process_file` function with an invalid process file. This test will create a test
    process file with invalid contents that `pull_process_file` will try to read in. Once it sees
    that the file is invalid it will return None.

    :param server_testing_dir: The path to the the temp output directory for server tests
    :param server_process_file_contents: A fixture representing process file contents
    """
    # Remove a key from the process file contents so that it's no longer valid
    del server_process_file_contents["hostname"]

    # Create the invalid process file in our temp testing directory
    process_filepath = f"{server_testing_dir}/invalid_process_file.yaml"
    with open(process_filepath, "w") as process_file:
        yaml.dump(server_process_file_contents, process_file)

    # Run the test
    assert pull_process_file(process_filepath) is None


def test_dump_process_file_invalid_file(server_process_file_contents: Dict[str, Union[int, str]]):
    """
    Test the `dump_process_file` function with invalid process file data. This should return False.

    :param server_process_file_contents: A fixture representing process file contents
    """
    # Remove a key from the process file contents so that it's no longer valid and run the test
    del server_process_file_contents["parent_pid"]
    assert not dump_process_file(server_process_file_contents, "some_filepath.yaml")


def test_dump_process_file_valid_file(server_testing_dir: str, server_process_file_contents: Dict[str, Union[int, str]]):
    """
    Test the `dump_process_file` function with invalid process file data. This should return False.

    :param server_testing_dir: The path to the the temp output directory for server tests
    :param server_process_file_contents: A fixture representing process file contents
    """
    process_filepath = f"{server_testing_dir}/dumped_process_file.yaml"
    assert dump_process_file(server_process_file_contents, process_filepath)
    assert os.path.exists(process_filepath)
