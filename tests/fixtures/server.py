"""
Fixtures specifically for help testing the modules in the server/ directory.
"""
import pytest
import shutil
from typing import Dict

@pytest.fixture(scope="class")
def server_container_config_data(temp_output_dir: str):
    """
    Fixture to provide sample data for ContainerConfig tests

    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
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
def server_container_format_config_data():
    """
    Fixture to provide sample data for ContainerFormatConfig tests
    """
    return {
        "command": "docker",
        "run_command": "{command} run --name {name} -d {image}",
        "stop_command": "{command} stop {name}",
        "pull_command": "{command} pull {url}",
    }

@pytest.fixture(scope="class")
def server_process_config_data():
    """
    Fixture to provide sample data for ProcessConfig tests
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
):
    """
    Fixture to provide sample data for ServerConfig tests

    :param server_container_config_data: A pytest fixture of test data to pass to the ContainerConfig class
    :param server_process_config_data: A pytest fixture of test data to pass to the ProcessConfig class
    :param server_container_format_config_data: A pytest fixture of test data to pass to the ContainerFormatConfig class
    """
    return {
        "container": server_container_config_data,
        "process": server_process_config_data,
        "docker": server_container_format_config_data,
    }


@pytest.fixture(scope="class")
def server_redis_conf_file(temp_output_dir: str):
    """
    Fixture to copy the redis.conf file from the merlin/server/ directory to the
    temporary output directory and provide the path to the copied file

    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
    """
    # TODO
    # - will probably have to do more than just copying over the conf file
    # - likely want to create our own test conf file with the settings that
    #   can be modified by RedisConf instead
    path_to_redis_conf = f"{os.path.dirname(os.path.abspath(__file__))}/../../merlin/server/redis.conf"
    path_to_copied_redis = f"{temp_output_dir}/redis.conf"
    shutil.copy(path_to_redis_conf, path_to_copied_redis)
    return path_to_copied_redis