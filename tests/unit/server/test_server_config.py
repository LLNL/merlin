"""
Tests for the `server_config.py` module.
"""

import io
import string
from typing import Dict, Tuple, Union

import pytest

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
