"""
Tests for the `server_util.py` module.
"""
import os
import pytest
from typing import Callable, Dict, Union

from merlin.server.server_util import (
    AppYaml,
    ContainerConfig,
    ContainerFormatConfig,
    ProcessConfig,
    RedisConfig,
    RedisUsers,
    ServerConfig,
    valid_ipv4,
    valid_port
)

@pytest.mark.parametrize("valid_ip", [
    "0.0.0.0",
    "127.0.0.1",
    "14.105.200.58",
    "255.255.255.255",
])
def test_valid_ipv4_valid_ip(valid_ip: str):
    """
    Test the `valid_ipv4` function with valid IPs.
    This should return True.

    :param valid_ip: A valid port to test.
                        These are pulled from the parametrized list defined above this test.
    """
    assert valid_ipv4(valid_ip)

@pytest.mark.parametrize("invalid_ip", [
    "256.0.0.1",
    "-1.0.0.1",
    None,
    "127.0.01",
])
def test_valid_ipv4_invalid_ip(invalid_ip: Union[str, None]):
    """
    Test the `valid_ipv4` function with invalid IPs.
    An IP is valid if every integer separated by the '.' delimiter are between 0 and 255.
    This should return False for both IPs tested here.

    :param invalid_ip: An invalid port to test.
                          These are pulled from the parametrized list defined above this test.
    """
    assert not valid_ipv4(invalid_ip)

@pytest.mark.parametrize("valid_input", [
    1,
    433,
    65535,
])
def test_valid_port_valid_input(valid_input: int):
    """
    Test the `valid_port` function with valid port numbers.
    Valid ports are ports between 1 and 65535.
    This should return True.

    :param valid_input: A valid input value to test.
                        These are pulled from the parametrized list defined above this test.
    """
    assert valid_port(valid_input)

@pytest.mark.parametrize("invalid_input", [
    -1,
    0,
    65536,
])
def test_valid_port_invalid_input(invalid_input: int):
    """
    Test the `valid_port` function with invalid inputs.
    Valid ports are ports between 1 and 65535.
    This should return False for each invalid input tested.

    :param invalid_input: An invalid input value to test.
                          These are pulled from the parametrized list defined above this test.
    """
    assert not valid_port(invalid_input)


class TestContainerConfig:
    """Tests for the ContainerConfig class."""

    def test_init_with_complete_data(self, server_container_config_data: Dict[str, str]):
        """
        Tests that __init__ populates attributes correctly with complete data

        :param server_container_config_data: A pytest fixture of test data to pass to the ContainerConfig class
        """
        config = ContainerConfig(server_container_config_data)
        assert config.format == server_container_config_data["format"]
        assert config.image_type == server_container_config_data["image_type"]
        assert config.image == server_container_config_data["image"]
        assert config.url == server_container_config_data["url"]
        assert config.config == server_container_config_data["config"]
        assert config.config_dir == server_container_config_data["config_dir"]
        assert config.pfile == server_container_config_data["pfile"]
        assert config.pass_file == server_container_config_data["pass_file"]
        assert config.user_file == server_container_config_data["user_file"]

    def test_init_with_missing_data(self):
        """
        Tests that __init__ uses defaults for missing data
        """
        incomplete_data = {"format": "docker"}
        config = ContainerConfig(incomplete_data)
        assert config.format == incomplete_data["format"]
        assert config.image_type == ContainerConfig.IMAGE_TYPE
        assert config.image == ContainerConfig.IMAGE_NAME
        assert config.url == ContainerConfig.REDIS_URL
        assert config.config == ContainerConfig.CONFIG_FILE
        assert config.config_dir == ContainerConfig.CONFIG_DIR
        assert config.pfile == ContainerConfig.PROCESS_FILE
        assert config.pass_file == ContainerConfig.PASSWORD_FILE
        assert config.user_file == ContainerConfig.USERS_FILE

    @pytest.mark.parametrize("attr_name", [
        "image",
        "config",
        "pfile",
        "pass_file",
        "user_file",
    ])
    def test_get_path_methods(self, server_container_config_data: Dict[str, str], attr_name: str):
        """
        Tests that get_*_path methods construct the correct path

        :param server_container_config_data: A pytest fixture of test data to pass to the ContainerConfig class
        :param attr_name: Name of the attribute to be tested. These are pulled from the parametrized list defined above this test.
        """
        config = ContainerConfig(server_container_config_data)
        get_path_method = getattr(config, f"get_{attr_name}_path")  # Dynamically get the method based on attr_name
        expected_path = os.path.join(server_container_config_data["config_dir"], server_container_config_data[attr_name])
        assert get_path_method() == expected_path

    @pytest.mark.parametrize("getter_name, expected_attr", [
        ("get_format", "format"),
        ("get_image_type", "image_type"),
        ("get_image_name", "image"),
        ("get_image_url", "url"),
        ("get_config_name", "config"),
        ("get_config_dir", "config_dir"),
        ("get_pfile_name", "pfile"),
        ("get_pass_file_name", "pass_file"),
        ("get_user_file_name", "user_file"),
    ])
    def test_getter_methods(self, server_container_config_data: Dict[str, str], getter_name: str, expected_attr: str):
        """
        Tests that all getter methods return the correct attribute values

        :param server_container_config_data: A pytest fixture of test data to pass to the ContainerConfig class
        :param getter_name: Name of the getter method to test. This is pulled from the parametrized list defined above this test.
        :param expected_attr: Name of the corresponding attribute. This is pulled from the parametrized list defined above this test.
        """
        config = ContainerConfig(server_container_config_data)
        getter = getattr(config, getter_name)
        assert getter() == server_container_config_data[expected_attr]

    def test_get_container_password(self, server_container_config_data: Dict[str, str]):
        """
        Test that the get_container_password is reading the password file properly

        :param server_container_config_data: A pytest fixture of test data to pass to the ContainerConfig class
        """
        # Write a fake password to the password file
        test_password = "super-secret-password"
        with open(server_container_config_data["pass_file"], "w") as pass_file:
            pass_file.write(test_password)

        # Run the test
        config = ContainerConfig(server_container_config_data)
        assert config.get_container_password() == test_password


class TestContainerFormatConfig:
    """Tests for the ContainerFormatConfig class."""

    def test_init_with_complete_data(self, server_container_format_config_data: Dict[str, str]):
        """
        Tests that __init__ populates attributes correctly with complete data

        :param server_container_format_config_data: A pytest fixture of test data to pass to the ContainerFormatConfig class
        """
        config = ContainerFormatConfig(server_container_format_config_data)
        assert config.command == server_container_format_config_data["command"]
        assert config.run_command == server_container_format_config_data["run_command"]
        assert config.stop_command == server_container_format_config_data["stop_command"]
        assert config.pull_command == server_container_format_config_data["pull_command"]

    def test_init_with_missing_data(self):
        """
        Tests that __init__ uses defaults for missing data
        """
        incomplete_data = {"command": "docker"}
        config = ContainerFormatConfig(incomplete_data)
        assert config.command == incomplete_data["command"]
        assert config.run_command == config.RUN_COMMAND
        assert config.stop_command == config.STOP_COMMAND
        assert config.pull_command == config.PULL_COMMAND

    @pytest.mark.parametrize("getter_name, expected_attr", [
        ("get_command", "command"),
        ("get_run_command", "run_command"),
        ("get_stop_command", "stop_command"),
        ("get_pull_command", "pull_command"),
    ])
    def test_getter_methods(self, server_container_format_config_data: Dict[str, str], getter_name: str, expected_attr: str):
        """
        Tests that all getter methods return the correct attribute values

        :param server_container_format_config_data: A pytest fixture of test data to pass to the ContainerFormatConfig class
        :param getter_name: Name of the getter method to test. This is pulled from the parametrized list defined above this test.
        :param expected_attr: Name of the corresponding attribute. This is pulled from the parametrized list defined above this test.
        """
        config = ContainerFormatConfig(server_container_format_config_data)
        getter = getattr(config, getter_name)
        assert getter() == server_container_format_config_data[expected_attr]


class TestProcessConfig:
    """Tests for the ProcessConfig class."""

    def test_init_with_complete_data(self, server_process_config_data: Dict[str, str]):
        """
        Tests that __init__ populates attributes correctly with complete data

        :param server_process_config_data: A pytest fixture of test data to pass to the ProcessConfig class
        """
        config = ProcessConfig(server_process_config_data)
        assert config.status == server_process_config_data["status"]
        assert config.kill == server_process_config_data["kill"]

    def test_init_with_missing_data(self):
        """
        Tests that __init__ uses defaults for missing data
        """
        incomplete_data = {"status": "status {pid}"}
        config = ProcessConfig(incomplete_data)
        assert config.status == incomplete_data["status"]
        assert config.kill == config.KILL_COMMAND

    @pytest.mark.parametrize("getter_name, expected_attr", [
        ("get_status_command", "status"),
        ("get_kill_command", "kill"),
    ])
    def test_getter_methods(self, server_process_config_data: Dict[str, str], getter_name: str, expected_attr: str):
        """
        Tests that all getter methods return the correct attribute values

        :param server_process_config_data: A pytest fixture of test data to pass to the ProcessConfig class
        :param getter_name: Name of the getter method to test. This is pulled from the parametrized list defined above this test.
        :param expected_attr: Name of the corresponding attribute. This is pulled from the parametrized list defined above this test.
        """
        config = ProcessConfig(server_process_config_data)
        getter = getattr(config, getter_name)
        assert getter() == server_process_config_data[expected_attr]


class TestServerConfig:
    """Tests for the ServerConfig class."""

    def test_init_with_complete_data(self, server_server_config: Dict[str, str]):
        """
        Tests that __init__ populates attributes correctly with complete data

        :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
        """
        config = ServerConfig(server_server_config)
        assert config.container == ContainerConfig(server_server_config["container"])
        assert config.process == ProcessConfig(server_server_config["process"])
        assert config.container_format == ContainerFormatConfig(server_server_config["docker"])

    def test_init_with_missing_data(self, server_process_config_data: Dict[str, str]):
        """
        Tests that __init__ uses None for missing data

        :param server_process_config_data: A pytest fixture of test data to pass to the ContainerConfig class
        """
        incomplete_data = {"process": server_process_config_data}
        config = ServerConfig(incomplete_data)
        assert config.process == ProcessConfig(server_process_config_data)
        assert config.container is None
        assert config.container_format is None


# class TestRedisConfig:
#     """Tests for the RedisConfig class."""

#     def test_parse(self, server_redis_conf_file):
#         raise ValueError
