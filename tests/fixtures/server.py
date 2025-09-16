##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Fixtures specifically for help testing the modules in the server/ directory.
"""

import os
from argparse import Namespace
from typing import Dict, Union

import pytest
import yaml

from tests.fixture_types import FixtureCallable, FixtureDict, FixtureNamespace, FixtureStr


# pylint: disable=redefined-outer-name


@pytest.fixture(scope="session")
def server_testing_dir(create_testing_dir: FixtureCallable, temp_output_dir: FixtureStr) -> FixtureStr:
    """
    Fixture to create a temporary output directory for tests related to the server functionality.

    Args:
        create_testing_dir: A fixture which returns a function that creates the testing directory.
        temp_output_dir: The path to the temporary ouptut directory we'll be using for this test run.

    Returns:
        The path to the temporary testing directory for server tests.
    """
    return create_testing_dir(temp_output_dir, "server_testing")


@pytest.fixture(scope="session")
def server_redis_conf_file(server_testing_dir: FixtureStr) -> FixtureStr:
    """
    Fixture to write a redis.conf file to the temporary output directory.

    If a test will modify this file with a file write, you should make a copy of
    this file to modify instead.

    :param server_testing_dir: A pytest fixture that defines a path to the output directory we'll write to
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
    """.strip().replace(
        "    ", ""
    )

    with open(redis_conf_file, "w") as rcf:
        rcf.write(file_contents)

    return redis_conf_file


@pytest.fixture(scope="session")
def server_redis_pass_file(server_testing_dir: FixtureStr) -> FixtureStr:
    """
    Fixture to create a redis password file in the temporary output directory.

    If a test will modify this file with a file write, you should make a copy of
    this file to modify instead.

    :param server_testing_dir: A pytest fixture that defines a path to the output directory we'll write to
    :returns: The path to the redis password file
    """
    redis_pass_file = f"{server_testing_dir}/redis.pass"

    with open(redis_pass_file, "w") as rpf:
        rpf.write("server-tests-password")

    return redis_pass_file


@pytest.fixture(scope="session")
def server_users() -> FixtureDict[str, Dict[str, str]]:
    """
    Create a dictionary of two test users with identical configuration settings.

    :returns: A dict containing the two test users and their settings
    """
    users = {
        "default": {
            "channels": "*",
            "commands": "@all",
            "hash_password": "1ba9249af0c73dacb0f9a70567126624076b5bee40de811e65f57eabcdaf490a",
            "keys": "*",
            "status": "on",
        },
        "test_user": {
            "channels": "*",
            "commands": "@all",
            "hash_password": "1ba9249af0c73dacb0f9a70567126624076b5bee40de811e65f57eabcdaf490a",
            "keys": "*",
            "status": "on",
        },
    }
    return users


@pytest.fixture(scope="session")
def server_redis_users_file(server_testing_dir: FixtureStr, server_users: FixtureDict[str, Dict[str, str]]) -> FixtureStr:
    """
    Fixture to write a redis.users file to the temporary output directory.

    If a test will modify this file with a file write, you should make a copy of
    this file to modify instead.

    :param server_testing_dir: A pytest fixture that defines a path to the output directory we'll write to
    :param server_users: A dict of test user configurations
    :returns: The path to the redis user configuration file we'll use for testing
    """
    redis_users_file = f"{server_testing_dir}/redis.users"

    with open(redis_users_file, "w") as ruf:
        yaml.dump(server_users, ruf)

    return redis_users_file


@pytest.fixture(scope="class")
def server_container_config_data(
    server_testing_dir: FixtureStr,
    server_redis_conf_file: FixtureStr,
    server_redis_pass_file: FixtureStr,
    server_redis_users_file: FixtureStr,
) -> FixtureDict[str, str]:
    """
    Fixture to provide sample data for ContainerConfig tests.

    :param server_testing_dir: A pytest fixture that defines a path to the output directory we'll write to
    :param server_redis_conf_file: A pytest fixture that defines a path to a redis configuration file
    :param server_redis_pass_file: A pytest fixture that defines a path to a redis password file
    :param server_redis_users_file: A pytest fixture that defines a path to a redis users file
    :returns: A dict containing the necessary key/values for the ContainerConfig object
    """

    return {
        "format": "singularity",
        "image_type": "redis",
        "image": "redis_latest.sif",
        "url": "docker://redis",
        "config": server_redis_conf_file.split("/")[-1],
        "config_dir": server_testing_dir,
        "pfile": "merlin_server.pf",
        "pass_file": server_redis_pass_file.split("/")[-1],
        "user_file": server_redis_users_file.split("/")[-1],
    }


@pytest.fixture(scope="class")
def server_container_format_config_data() -> FixtureDict[str, str]:
    """
    Fixture to provide sample data for ContainerFormatConfig tests

    :returns: A dict containing the necessary key/values for the ContainerFormatConfig object
    """
    return {
        "command": "singularity",
        "run_command": "{command} run -H {home_dir} {image} {config}",
        "stop_command": "kill",
        "pull_command": "{command} pull {image} {url}",
    }


@pytest.fixture(scope="class")
def server_process_config_data() -> FixtureDict[str, str]:
    """
    Fixture to provide sample data for ProcessConfig tests

    :returns: A dict containing the necessary key/values for the ProcessConfig object
    """
    return {
        "status": "pgrep -P {pid}",
        "kill": "kill {pid}",
    }


@pytest.fixture(scope="class")
def server_server_config(
    server_container_config_data: FixtureDict[str, str],
    server_process_config_data: FixtureDict[str, str],
    server_container_format_config_data: FixtureDict[str, str],
) -> FixtureDict[str, FixtureDict[str, str]]:
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
        "singularity": server_container_format_config_data,
    }


@pytest.fixture(scope="function")
def server_app_yaml_contents(
    server_redis_pass_file: FixtureStr,
    server_container_config_data: FixtureDict[str, str],
    server_process_config_data: FixtureDict[str, str],
) -> FixtureDict[str, Union[str, int]]:
    """
    Fixture to create the contents of an app.yaml file.

    :param server_redis_pass_file: A pytest fixture that defines a path to a redis password file
    :param server_container_config_data: A pytest fixture of test data to pass to the ContainerConfig class
    :param server_process_config_data: A pytest fixture of test data to pass to the ProcessConfig class
    :returns: A dict with typical app.yaml contents
    """
    contents = {
        "broker": {
            "cert_reqs": "none",
            "name": "redis",
            "password": server_redis_pass_file,
            "port": 6379,
            "server": "127.0.0.1",
            "username": "default",
            "vhost": "testhost",
        },
        "container": server_container_config_data,
        "process": server_process_config_data,
        "results_backend": {
            "cert_reqs": "none",
            "db_num": 0,
            "name": "redis",
            "password": server_redis_pass_file,
            "port": 6379,
            "server": "127.0.0.1",
            "username": "default",
        },
    }
    return contents


@pytest.fixture(scope="function")
def server_app_yaml(server_testing_dir: FixtureStr, server_app_yaml_contents: FixtureDict[str, Union[str, int]]) -> FixtureStr:
    """
    Fixture to create an app.yaml file in the temporary output directory.

    NOTE this must be function scoped since server_app_yaml_contents is function scoped.

    :param server_testing_dir: A pytest fixture that defines a path to the output directory we'll write to
    :param server_app_yaml_contents: A pytest fixture that creates a dict of contents for an app.yaml file
    :returns: The path to the app.yaml file
    """
    app_yaml_file = f"{server_testing_dir}/app.yaml"

    if not os.path.exists(app_yaml_file):
        with open(app_yaml_file, "w") as ayf:
            yaml.dump(server_app_yaml_contents, ayf)

    return app_yaml_file


@pytest.fixture(scope="function")
def server_process_file_contents() -> FixtureDict[str, Union[str, int]]:
    """Fixture to represent process file contents."""
    return {"parent_pid": 123, "image_pid": 456, "port": 6379, "hostname": "dummy_server"}


@pytest.fixture(scope="function")
def server_config_server_args() -> FixtureNamespace:
    """
    Setup an argparse Namespace with all args that the `config_server`
    function will need. These can be modified on a test-by-test basis.

    :returns: An argparse Namespace with args needed by `config_server`
    """
    return Namespace(
        ipaddress=None,
        port=None,
        password=None,
        directory=None,
        snapshot_seconds=None,
        snapshot_changes=None,
        snapshot_file=None,
        append_mode=None,
        append_file=None,
        add_user=None,
        remove_user=None,
    )
