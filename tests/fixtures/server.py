"""
Fixtures specifically for help testing the modules in the server/ directory.
"""
import os
import pytest
import yaml
from typing import Dict

@pytest.fixture(scope="class")
def server_container_config_data(temp_output_dir: str) -> Dict[str, str]:
    """
    Fixture to provide sample data for ContainerConfig tests

    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
    :returns: A dict containing the necessary key/values for the ContainerConfig object
    """
    return {
        "format": "docker",
        "image_type": "postgres",
        "image": "postgres:latest",
        "url": "postgres://localhost",
        "config": "postgres.conf",
        "config_dir": "/path/to/config",
        "pfile": "merlin_server_postgres.pf",
        "pass_file": f"{temp_output_dir}/postgres.pass",
        "user_file": "postgres.users",
    }

@pytest.fixture(scope="class")
def server_container_format_config_data() -> Dict[str, str]:
    """
    Fixture to provide sample data for ContainerFormatConfig tests

    :returns: A dict containing the necessary key/values for the ContainerFormatConfig object
    """
    return {
        "command": "docker",
        "run_command": "{command} run --name {name} -d {image}",
        "stop_command": "{command} stop {name}",
        "pull_command": "{command} pull {url}",
    }

@pytest.fixture(scope="class")
def server_process_config_data() -> Dict[str, str]:
    """
    Fixture to provide sample data for ProcessConfig tests

    :returns: A dict containing the necessary key/values for the ProcessConfig object
    """
    return {
        "status": "status {pid}",
        "kill": "terminate {pid}",
    }

@pytest.fixture(scope="class")
def server_server_config(
    server_container_config_data: Dict[str, str],
    server_process_config_data: Dict[str, str],
    server_container_format_config_data: Dict[str, str],
) -> Dict[str, Dict[str, str]]:
    """
    Fixture to provide sample data for ServerConfig tests

    :param server_container_config_data: A pytest fixture of test data to pass to the ContainerConfig class
    :param server_process_config_data: A pytest fixture of test data to pass to the ProcessConfig class
    :param server_container_format_config_data: A pytest fixture of test data to pass to the ContainerFormatConfig class
    :returns: A dictionary containing each of the configuration dicts we'll need
    """
    return {
        "container": server_container_config_data,
        "process": server_process_config_data,
        "docker": server_container_format_config_data,
    }


@pytest.fixture(scope="session")
def server_testing_dir(temp_output_dir: str) -> str:
    """
    Fixture to create a temporary output directory for tests related to the server functionality.

    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
    :returns: The path to the temporary testing directory for server tests
    """
    testing_dir = f"{temp_output_dir}/server_testing/"
    if not os.path.exists(testing_dir):
        os.mkdir(testing_dir)

    return testing_dir


@pytest.fixture(scope="session")
def server_redis_conf_file(server_testing_dir: str) -> str:
    """
    Fixture to write a redis.conf file to the temporary output directory.

    If a test will modify this file with a file write, you should make a copy of
    this file to modify instead.

    :param server_testing_dir: A pytest fixture that defines a path to the the output directory we'll write to
    :returns: The path to the redis configuration file we'll use for testing
    """
    redis_conf_file = f"{server_testing_dir}/redis.conf"
    file_contents = """
    # ip address
    bind 127.0.0.1

    # port
    port 6379

    # password
    requirepass merlin_password

    # directory
    dir ./

    # snapshot
    save 300 100

    # db file
    dbfilename dump.rdb

    # append mode
    appendfsync everysec

    # append file
    appendfilename appendonly.aof

    # dummy trailing comment
    """.strip().replace("    ", "")

    with open(redis_conf_file, "w") as rcf:
        rcf.write(file_contents)

    return redis_conf_file

@pytest.fixture(scope="session")
def server_users() -> dict:
    """
    Create a dictionary of two test users with identical configuration settings.

    :returns: A dict containing the two test users and their settings
    """
    users = {
        "default": {
            "channels": '*',
            "commands": '@all',
            "hash_password": '1ba9249af0c73dacb0f9a70567126624076b5bee40de811e65f57eabcdaf490a',
            "keys": '*',
            "status": 'on',
        },
        "test_user": {
            "channels": '*',
            "commands": '@all',
            "hash_password": '1ba9249af0c73dacb0f9a70567126624076b5bee40de811e65f57eabcdaf490a',
            "keys": '*',
            "status": 'on',
        }
    }
    return users

@pytest.fixture(scope="session")
def server_redis_users_file(server_testing_dir: str, server_users: dict) -> str:
    """
    Fixture to write a redis.users file to the temporary output directory.

    If a test will modify this file with a file write, you should make a copy of
    this file to modify instead.

    :param server_testing_dir: A pytest fixture that defines a path to the the output directory we'll write to
    :param server_users: A dict of test user configurations
    :returns: The path to the redis user configuration file we'll use for testing
    """
    redis_users_file = f"{server_testing_dir}/redis.users"

    with open(redis_users_file, "w") as ruf:
        yaml.dump(server_users, ruf)

    return redis_users_file