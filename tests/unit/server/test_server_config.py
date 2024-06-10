"""
Tests for the `server_config.py` module.
"""

import string

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


