"""
Tests for the `server_config.py` module.
"""

import io
import logging
import os
import string
from typing import Dict, Tuple, Union

import pytest

from merlin.server.server_util import CONTAINER_TYPES, ServerConfig
from merlin.server.server_config import (
    PASSWORD_LENGTH,
    check_process_file_format,
    config_merlin_server,
    create_server_config,
    dump_process_file,
    generate_password,
    get_server_status,
    parse_redis_output,
    pull_process_file,
    pull_server_config,
    pull_server_image,
    write_container_command_files,
)


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


def test_write_container_command_files_with_existing_files(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_testing_dir: str,
):
    """
    Test the `write_container_command_files` function with files that already exist.
    This should skip trying to create the files, log 3 "file already exists" messages,
    and return True.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_testing_dir: The path to the the temp output directory for server tests
    """
    caplog.set_level(logging.INFO)
    mocker.patch('os.path.exists', return_value=True)
    assert write_container_command_files(server_testing_dir)
    file_names = [f"{container}.yaml" for container in CONTAINER_TYPES]
    for file in file_names:
        assert f"{file} already exists." in caplog.text


def test_write_container_command_files_with_nonexisting_files(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_testing_dir: str,
):
    """
    Test the `write_container_command_files` function with files that don't already exist.
    This should create the files, log messages for each file, and return True

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_testing_dir: The path to the the temp output directory for server tests
    """
    caplog.set_level(logging.INFO)

    # Mock the os.path.exists function so it returns False
    mocker.patch('os.path.exists', return_value=False)

    # Mock the resources.path context manager
    mock_path = mocker.patch("merlin.server.server_config.resources.path")
    mock_path.return_value.__enter__.return_value = "mocked_file_path"

    # Mock the open builtin
    mock_data = mocker.mock_open(read_data="mocked data")
    mocker.patch("builtins.open", mock_data)

    assert write_container_command_files(server_testing_dir)
    file_names = [f"{container}.yaml" for container in CONTAINER_TYPES]
    for file in file_names:
        assert f"Copying file {file} to configuration directory." in caplog.text


def test_write_container_command_files_with_oserror(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_testing_dir: str,
):
    """
    Test the `write_container_command_files` function with an OSError being raised.
    This should log an error message and return False.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_testing_dir: The path to the the temp output directory for server tests
    """
    # Mock the open function to raise an OSError
    mocker.patch("builtins.open", side_effect=OSError("File not writeable"))

    assert not write_container_command_files(server_testing_dir)
    assert f"Destination location {server_testing_dir} is not writable." in caplog.text


def test_create_server_config_merlin_config_dir_nonexistent(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_testing_dir: str,
):
    """
    Tests the `create_server_config` function with MERLIN_CONFIG_DIR not existing.
    This should log an error and return False.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_testing_dir: The path to the the temp output directory for server tests
    """
    nonexistent_dir = f"{server_testing_dir}/merlin_config_dir"
    mocker.patch('merlin.server.server_config.MERLIN_CONFIG_DIR', nonexistent_dir)
    assert not create_server_config()
    assert f"Unable to find main merlin configuration directory at {nonexistent_dir}" in caplog.text


def test_create_server_config_server_subdir_nonexistent_oserror(
    mocker: "Fixture",  # noqa: F821
    caplog: "Fixture",  # noqa: F821
    server_testing_dir: str,
):
    """
    Tests the `create_server_config` function with MERLIN_CONFIG_DIR/MERLIN_SERVER_SUBDIR
    not existing and an OSError being raised. This should log an error and return False.

    :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
    :param caplog: A built-in fixture from the pytest library to capture logs
    :param server_testing_dir: The path to the the temp output directory for server tests
    """

    # Mock MERLIN_CONFIG_DIR and MERLIN_SERVER_SUBDIR
    nonexistent_server_subdir = "test_create_server_config_server_subdir_nonexistent"
    mocker.patch('merlin.server.server_config.MERLIN_CONFIG_DIR', server_testing_dir)
    mocker.patch('merlin.server.server_config.MERLIN_SERVER_SUBDIR', nonexistent_server_subdir)

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
    mocker.patch("merlin.server.server_config.MERLIN_CONFIG_DIR", server_testing_dir)
    mocker.patch("merlin.server.server_config.write_container_command_files", return_value=True)
    mock_open_func = mocker.mock_open(read_data='key: value')
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
    mocker.patch("merlin.server.server_config.MERLIN_CONFIG_DIR", server_testing_dir)
    mocker.patch("merlin.server.server_config.write_container_command_files", return_value=True)
    mock_open_func = mocker.mock_open(read_data='key: value')
    mocker.patch("builtins.open", mock_open_func)
    mocker.patch("merlin.server.server_config.pull_server_config", return_value=ServerConfig(server_server_config))

    # Mock the get_config_dir call to return a directory that doesn't exist yet
    nonexistent_dir = f"{server_testing_dir}/merlin_server"
    mocker.patch("merlin.server.server_util.ContainerConfig.get_config_dir", return_value=nonexistent_dir)

    assert create_server_config()
    assert os.path.exists(nonexistent_dir)
    assert "Creating merlin server directory." in caplog.text
