##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the merlin/config/utils.py module.
"""

import os

import pytest

from merlin.config.configfile import CONFIG
from merlin.config.utils import (
    Priority,
    determine_priority_map,
    get_priority,
    is_rabbit_broker,
    is_redis_broker,
    resolve_password,
)
from tests.constants import SERVER_PASS
from tests.fixture_types import FixtureCallable, FixtureStr
from tests.utils import create_pass_file


@pytest.fixture
def server_configuration_testing_dir(create_testing_dir: FixtureCallable, temp_output_dir: FixtureStr) -> FixtureStr:
    """
    Fixture to create a temporary output directory for tests related to the results backend functionality.

    Args:
        create_testing_dir: A fixture which returns a function that creates the testing directory.
        temp_output_dir: The path to the temporary output directory we'll be using for this test run.

    Returns:
        The path to the temporary testing directory for server configuration related tests.
    """
    return create_testing_dir(temp_output_dir, "results_backend_testing")


def test_is_rabbit_broker():
    """Test the `is_rabbit_broker` by passing in rabbit as the broker"""
    assert is_rabbit_broker("rabbitmq") is True
    assert is_rabbit_broker("amqp") is True
    assert is_rabbit_broker("amqps") is True


def test_is_rabbit_broker_invalid():
    """Test the `is_rabbit_broker` by passing in an invalid broker"""
    assert is_rabbit_broker("redis") is False
    assert is_rabbit_broker("") is False


def test_is_redis_broker():
    """Test the `is_redis_broker` by passing in redis as the broker"""
    assert is_redis_broker("redis") is True
    assert is_redis_broker("rediss") is True
    assert is_redis_broker("redis+socket") is True


def test_is_redis_broker_invalid():
    """Test the `is_redis_broker` by passing in an invalid broker"""
    assert is_redis_broker("rabbitmq") is False
    assert is_redis_broker("") is False


def test_get_priority_rabbit_broker(rabbit_broker_config: FixtureCallable):
    """
    Test the `get_priority` function with rabbit as the broker.
    Low priority for rabbit is 1 and high is 9.

    Args:
        rabbit_broker_config: A fixture to set the CONFIG object to a test configuration that we'll use here
    """
    assert get_priority(Priority.LOW) == 1
    assert get_priority(Priority.MID) == 5
    assert get_priority(Priority.HIGH) == 9
    assert get_priority(Priority.RETRY) == 10


def test_get_priority_redis_broker(redis_broker_config_function: FixtureCallable):
    """
    Test the `get_priority` function with redis as the broker.
    Low priority for redis is 10 and high is 2.

    Args:
        redis_broker_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
    """
    assert get_priority(Priority.LOW) == 10
    assert get_priority(Priority.MID) == 5
    assert get_priority(Priority.HIGH) == 2
    assert get_priority(Priority.RETRY) == 1


def test_get_priority_invalid_broker(redis_broker_config_function: FixtureCallable):
    """
    Test the `get_priority` function with an invalid broker.
    This should raise a ValueError.

    Args:
        redis_broker_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
    """
    CONFIG.broker.name = "invalid"
    with pytest.raises(ValueError) as excinfo:
        get_priority(Priority.LOW)
    assert "Unsupported broker name: invalid" in str(excinfo.value)


def test_get_priority_invalid_priority(redis_broker_config_function: FixtureCallable):
    """
    Test the `get_priority` function with an invalid priority.
    This should raise a TypeError.

    Args:
        redis_broker_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
    """
    with pytest.raises(ValueError) as excinfo:
        get_priority("invalid_priority")
    assert "Invalid priority: invalid_priority" in str(excinfo.value)


def test_determine_priority_map_rabbit():
    """
    Test the `determine_priority_map` function with rabbit as the broker.
    This should return the following map:
    {Priority.LOW: 1, Priority.MID: 5, Priority.HIGH: 9, Priority.RETRY: 10}
    """
    expected = {Priority.LOW: 1, Priority.MID: 5, Priority.HIGH: 9, Priority.RETRY: 10}
    actual = determine_priority_map("rabbitmq")
    assert actual == expected


def test_determine_priority_map_redis():
    """
    Test the `determine_priority_map` function with redis as the broker.
    This should return the following map:
    {Priority.LOW: 10, Priority.MID: 5, Priority.HIGH: 2, Priority.RETRY: 1}
    """
    expected = {Priority.LOW: 10, Priority.MID: 5, Priority.HIGH: 2, Priority.RETRY: 1}
    actual = determine_priority_map("redis")
    assert actual == expected


def test_determine_priority_map_invalid():
    """
    Test the `determine_priority_map` function with an invalid broker.
    This should raise a ValueError.
    """
    with pytest.raises(ValueError) as excinfo:
        determine_priority_map("invalid_broker")
    assert "Unsupported broker name: invalid_broker" in str(excinfo.value)


def test_resolve_password_pass_file_in_merlin():
    """
    Test the `resolve_password` function with the password file in the ~/.merlin/
    directory. We'll create a dummy file in this directory and delete it once the test
    is done.
    """

    # Check if the .merlin directory exists and create it if it doesn't
    remove_merlin_dir_after_test = False
    path_to_merlin_dir = os.path.expanduser("~/.merlin")
    if not os.path.exists(path_to_merlin_dir):
        remove_merlin_dir_after_test = True
        os.mkdir(path_to_merlin_dir)

    # Create the test password file
    pass_filename = "test.pass"
    full_pass_filepath = f"{path_to_merlin_dir}/{pass_filename}"
    create_pass_file(full_pass_filepath)

    try:
        # Run the test
        assert resolve_password(pass_filename, "test server type") == SERVER_PASS
        # Cleanup
        os.remove(full_pass_filepath)
        if remove_merlin_dir_after_test:
            os.rmdir(path_to_merlin_dir)
    except AssertionError as exc:
        # If the test fails, make sure we clean up the files/dirs created
        os.remove(full_pass_filepath)
        if remove_merlin_dir_after_test:
            os.rmdir(path_to_merlin_dir)
        raise AssertionError from exc


def test_resolve_password_pass_file_not_in_merlin(server_configuration_testing_dir: str):
    """
    Test the `resolve_password` function with the password file not in the ~/.merlin/
    directory. By using the `server_configuration_testing_dir` fixture, our cwd will be the temporary directory.
    We'll create a password file in the this directory for this test and have `resolve_password`
    read from that.

    Args:
        server_configuration_testing_dir: The path to the temporary testing directory for server config related tests.
    """
    pass_file = "test.pass"
    create_pass_file(pass_file)

    assert resolve_password(pass_file, "test server type") == SERVER_PASS


def test_resolve_password_directly_pass_password():
    """
    Test the `resolve_password` function by passing the password directly to this
    function instead of a password file.
    """
    assert resolve_password(SERVER_PASS, "test server type") == SERVER_PASS


def test_resolve_password_using_certs_path(server_configuration_testing_dir: str):
    """
    Test the `resolve_password` function with certs_path set to our temporary testing path.
    We'll create a password file in the temporary directory for this test and have `resolve_password`
    read from that.

    Args:
        server_configuration_testing_dir: The path to the temporary testing directory for server config related tests.
    """
    pass_filename = "test_certs.pass"
    full_pass_filepath = os.path.join(server_configuration_testing_dir, pass_filename)
    create_pass_file(full_pass_filepath)

    assert resolve_password(pass_filename, "test server type", certs_path=server_configuration_testing_dir) == SERVER_PASS
