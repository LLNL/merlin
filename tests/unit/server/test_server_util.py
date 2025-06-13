##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `server_util.py` module.
"""

import filecmp
import hashlib
import os
from typing import Dict, Union

import pytest

from merlin.server.server_util import (
    CONFIG_DIR,
    AppYaml,
    ContainerConfig,
    ContainerFormatConfig,
    ProcessConfig,
    RedisConfig,
    RedisUsers,
    ServerConfig,
    valid_ipv4,
    valid_port,
)


@pytest.mark.parametrize(
    "valid_ip",
    [
        "0.0.0.0",
        "127.0.0.1",
        "14.105.200.58",
        "255.255.255.255",
    ],
)
def test_valid_ipv4_valid_ip(valid_ip: str):
    """
    Test the `valid_ipv4` function with valid IPs.
    This should return True.

    :param valid_ip: A valid port to test. These are pulled from the parametrized
                     list defined above this test.
    """
    assert valid_ipv4(valid_ip)


@pytest.mark.parametrize(
    "invalid_ip",
    [
        "256.0.0.1",
        "-1.0.0.1",
        None,
        "127.0.01",
    ],
)
def test_valid_ipv4_invalid_ip(invalid_ip: Union[str, None]):
    """
    Test the `valid_ipv4` function with invalid IPs.
    An IP is valid if every integer separated by the '.' delimiter are between 0 and 255.
    This should return False for both IPs tested here.

    :param invalid_ip: An invalid port to test. These are pulled from the parametrized
                       list defined above this test.
    """
    assert not valid_ipv4(invalid_ip)


@pytest.mark.parametrize(
    "valid_input",
    [
        1,
        433,
        65535,
    ],
)
def test_valid_port_valid_input(valid_input: int):
    """
    Test the `valid_port` function with valid port numbers.
    Valid ports are ports between 1 and 65535.
    This should return True.

    :param valid_input: A valid input value to test. These are pulled from the parametrized
                        list defined above this test.
    """
    assert valid_port(valid_input)


@pytest.mark.parametrize(
    "invalid_input",
    [
        -1,
        0,
        65536,
    ],
)
def test_valid_port_invalid_input(invalid_input: int):
    """
    Test the `valid_port` function with invalid inputs.
    Valid ports are ports between 1 and 65535.
    This should return False for each invalid input tested.

    :param invalid_input: An invalid input value to test. These are pulled from the parametrized
                          list defined above this test.
    """
    assert not valid_port(invalid_input)


class TestContainerConfig:
    """Tests for the ContainerConfig class."""

    def test_init_with_complete_data(self, server_container_config_data: Dict[str, str]):
        """
        Tests that __init__ populates attributes correctly with complete data.

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
        Tests that __init__ uses defaults for missing data.
        """
        incomplete_data = {"format": "docker"}
        config = ContainerConfig(incomplete_data)
        assert config.format == incomplete_data["format"]
        assert config.image_type == ContainerConfig.IMAGE_TYPE
        assert config.image == ContainerConfig.IMAGE_NAME
        assert config.url == ContainerConfig.REDIS_URL
        assert config.config == ContainerConfig.CONFIG_FILE
        assert config.config_dir == CONFIG_DIR
        assert config.pfile == ContainerConfig.PROCESS_FILE
        assert config.pass_file == ContainerConfig.PASSWORD_FILE
        assert config.user_file == ContainerConfig.USERS_FILE

    @pytest.mark.parametrize(
        "attr_name",
        [
            "image",
            "config",
            "pfile",
            "pass_file",
            "user_file",
        ],
    )
    def test_get_path_methods(self, server_container_config_data: Dict[str, str], attr_name: str):
        """
        Tests that get_*_path methods construct the correct path.

        :param server_container_config_data: A pytest fixture of test data to pass to the ContainerConfig class
        :param attr_name: Name of the attribute to be tested. These are pulled from the parametrized list defined above this test.
        """
        config = ContainerConfig(server_container_config_data)
        get_path_method = getattr(config, f"get_{attr_name}_path")  # Dynamically get the method based on attr_name
        expected_path = os.path.join(server_container_config_data["config_dir"], server_container_config_data[attr_name])
        assert get_path_method() == expected_path

    @pytest.mark.parametrize(
        "getter_name, expected_attr",
        [
            ("get_format", "format"),
            ("get_image_type", "image_type"),
            ("get_image_name", "image"),
            ("get_image_url", "url"),
            ("get_config_name", "config"),
            ("get_config_dir", "config_dir"),
            ("get_pfile_name", "pfile"),
            ("get_pass_file_name", "pass_file"),
            ("get_user_file_name", "user_file"),
        ],
    )
    def test_getter_methods(self, server_container_config_data: Dict[str, str], getter_name: str, expected_attr: str):
        """
        Tests that all getter methods return the correct attribute values.

        :param server_container_config_data: A pytest fixture of test data to pass to the ContainerConfig class
        :param getter_name: Name of the getter method to test. This is pulled from the parametrized list defined above this test.
        :param expected_attr: Name of the corresponding attribute. This is pulled from the parametrized list defined above this test.
        """
        config = ContainerConfig(server_container_config_data)
        getter = getattr(config, getter_name)
        assert getter() == server_container_config_data[expected_attr]

    def test_get_container_password(self, server_testing_dir: str, server_container_config_data: Dict[str, str]):
        """
        Test that the `get_container_password` method is reading the password file properly.

        :param server_testing_dir: The path to the the temp output directory for server tests
        :param server_container_config_data: A pytest fixture of test data to pass to the ContainerConfig class
        """
        # Write a fake password to the password file
        test_password = "super-secret-password"
        temp_pass_file = f"{server_testing_dir}/temp.pass"
        with open(temp_pass_file, "w") as pass_file:
            pass_file.write(test_password)

        # Use temp pass file
        orig_pass_file = server_container_config_data["pass_file"]
        server_container_config_data["pass_file"] = temp_pass_file

        try:
            # Run the test
            config = ContainerConfig(server_container_config_data)
            assert config.get_container_password() == test_password
        except Exception as exc:
            # If there was a problem, reset to the original password file
            server_container_config_data["pass_file"] = orig_pass_file
            raise exc


class TestContainerFormatConfig:
    """Tests for the ContainerFormatConfig class."""

    def test_init_with_complete_data(self, server_container_format_config_data: Dict[str, str]):
        """
        Tests that __init__ populates attributes correctly with complete data.

        :param server_container_format_config_data: A pytest fixture of test data to pass to the ContainerFormatConfig class
        """
        config = ContainerFormatConfig(server_container_format_config_data)
        assert config.command == server_container_format_config_data["command"]
        assert config.run_command == server_container_format_config_data["run_command"]
        assert config.stop_command == server_container_format_config_data["stop_command"]
        assert config.pull_command == server_container_format_config_data["pull_command"]

    def test_init_with_missing_data(self):
        """
        Tests that __init__ uses defaults for missing data.
        """
        incomplete_data = {"command": "docker"}
        config = ContainerFormatConfig(incomplete_data)
        assert config.command == incomplete_data["command"]
        assert config.run_command == config.RUN_COMMAND
        assert config.stop_command == config.STOP_COMMAND
        assert config.pull_command == config.PULL_COMMAND

    @pytest.mark.parametrize(
        "getter_name, expected_attr",
        [
            ("get_command", "command"),
            ("get_run_command", "run_command"),
            ("get_stop_command", "stop_command"),
            ("get_pull_command", "pull_command"),
        ],
    )
    def test_getter_methods(self, server_container_format_config_data: Dict[str, str], getter_name: str, expected_attr: str):
        """
        Tests that all getter methods return the correct attribute values.

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
        Tests that __init__ populates attributes correctly with complete data.

        :param server_process_config_data: A pytest fixture of test data to pass to the ProcessConfig class
        """
        config = ProcessConfig(server_process_config_data)
        assert config.status == server_process_config_data["status"]
        assert config.kill == server_process_config_data["kill"]

    def test_init_with_missing_data(self):
        """
        Tests that __init__ uses defaults for missing data.
        """
        incomplete_data = {"status": "status {pid}"}
        config = ProcessConfig(incomplete_data)
        assert config.status == incomplete_data["status"]
        assert config.kill == config.KILL_COMMAND

    @pytest.mark.parametrize(
        "getter_name, expected_attr",
        [
            ("get_status_command", "status"),
            ("get_kill_command", "kill"),
        ],
    )
    def test_getter_methods(self, server_process_config_data: Dict[str, str], getter_name: str, expected_attr: str):
        """
        Tests that all getter methods return the correct attribute values.

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
        Tests that __init__ populates attributes correctly with complete data.

        :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
        """
        config = ServerConfig(server_server_config)
        assert config.container == ContainerConfig(server_server_config["container"])
        assert config.process == ProcessConfig(server_server_config["process"])
        assert config.container_format == ContainerFormatConfig(server_server_config["singularity"])

    def test_init_with_missing_data(self, server_process_config_data: Dict[str, str]):
        """
        Tests that __init__ uses None for missing data.

        :param server_process_config_data: A pytest fixture of test data to pass to the ContainerConfig class
        """
        incomplete_data = {"process": server_process_config_data}
        config = ServerConfig(incomplete_data)
        assert config.process == ProcessConfig(server_process_config_data)
        assert config.container is None
        assert config.container_format is None


class TestRedisUsers:
    """
    Tests for the RedisUsers class.

    TODO add integration test(s) for `apply_to_redis` method of this class.
    """

    class TestUser:
        """Tests for the RedisUsers.User class."""

        def test_initializaiton(self):
            """Test the initialization process of the User class."""
            user = RedisUsers.User()
            assert user.status == "on"
            assert user.hash_password == hashlib.sha256(b"password").hexdigest()
            assert user.keys == "*"
            assert user.channels == "*"
            assert user.commands == "@all"

        def test_parse_dict(self):
            """Test the `parse_dict` method of the User class."""
            test_dict = {
                "status": "test_status",
                "hash_password": "test_password",
                "keys": "test_keys",
                "channels": "test_channels",
                "commands": "test_commands",
            }
            user = RedisUsers.User()
            user.parse_dict(test_dict)
            assert user.status == test_dict["status"]
            assert user.hash_password == test_dict["hash_password"]
            assert user.keys == test_dict["keys"]
            assert user.channels == test_dict["channels"]
            assert user.commands == test_dict["commands"]

        def test_get_user_dict(self):
            """Test the `get_user_dict` method of the User class."""
            test_dict = {
                "status": "test_status",
                "hash_password": "test_password",
                "keys": "test_keys",
                "channels": "test_channels",
                "commands": "test_commands",
                "invalid_key": "invalid_val",
            }
            user = RedisUsers.User()
            user.parse_dict(test_dict)  # Set the test values
            actual_dict = user.get_user_dict()
            assert "invalid_key" not in actual_dict  # Check that the invalid key isn't parsed

            # Check that the values are as expected
            for key, val in actual_dict.items():
                if key == "status":
                    assert val == "on"
                else:
                    assert val == test_dict[key]

        def test_set_password(self):
            """Test the `set_password` method of the User class."""
            user = RedisUsers.User()
            pass_to_set = "dummy_password"
            user.set_password(pass_to_set)
            assert user.hash_password == hashlib.sha256(bytes(pass_to_set, "utf-8")).hexdigest()

    def test_initialization(self, server_redis_users_file: str, server_users: dict):
        """
        Test the initialization process of the RedisUsers class.

        :param server_redis_users_file: The path to a dummy redis users file
        :param server_users: A dict of test user configurations
        """
        redis_users = RedisUsers(server_redis_users_file)
        assert redis_users.filename == server_redis_users_file
        assert len(redis_users.users) == len(server_users)

    def test_write(self, server_redis_users_file: str, server_testing_dir: str):
        """
        Test that the write functionality works by writing the contents of a dummy
        users file to a blank users file.

        :param server_redis_users_file: The path to a dummy redis users file
        :param server_testing_dir: The path to the the temp output directory for server tests
        """
        copy_redis_users_file = f"{server_testing_dir}/redis_copy.users"

        # Create a RedisUsers object with the basic redis users file
        redis_users = RedisUsers(server_redis_users_file)

        # Change the filepath of the redis users file to be the copy that we'll write to
        redis_users.filename = copy_redis_users_file

        # Run the test
        redis_users.write()

        # Check that the contents of the copied file match the contents of the basic file
        assert filecmp.cmp(server_redis_users_file, copy_redis_users_file)

    def test_add_user_nonexistent(self, server_redis_users_file: str):
        """
        Test the `add_user` method with a user that doesn't exists.
        This should return True and add the user to the list of users.

        :param server_redis_users_file: The path to a dummy redis users file
        """
        redis_users = RedisUsers(server_redis_users_file)
        num_users_before = len(redis_users.users)
        assert redis_users.add_user("new_user")
        assert len(redis_users.users) == num_users_before + 1

    def test_add_user_exists(self, server_redis_users_file: str):
        """
        Test the `add_user` method with a user that already exists.
        This should return False.

        :param server_redis_users_file: The path to a dummy redis users file
        """
        redis_users = RedisUsers(server_redis_users_file)
        assert not redis_users.add_user("test_user")

    def test_set_password_valid(self, server_redis_users_file: str):
        """
        Test the `set_password` method with a user that exists.
        This should return True and change the password for the user.

        :param server_redis_users_file: The path to a dummy redis users file
        """
        redis_users = RedisUsers(server_redis_users_file)
        pass_to_set = "new_password"
        assert redis_users.set_password("test_user", pass_to_set)
        expected_hash_pass = hashlib.sha256(bytes(pass_to_set, "utf-8")).hexdigest()
        assert redis_users.users["test_user"].hash_password == expected_hash_pass

    def test_set_password_invalid(self, server_redis_users_file: str):
        """
        Test the `set_password` method with a user that doesn't exist.
        This should return False.

        :param server_redis_users_file: The path to a dummy redis users file
        """
        redis_users = RedisUsers(server_redis_users_file)
        assert not redis_users.set_password("nonexistent_user", "new_password")

    def test_remove_user_valid(self, server_redis_users_file: str):
        """
        Test the `remove_user` method with a user that exists.
        This should return True and remove the user from the list of users.

        :param server_redis_users_file: The path to a dummy redis users file
        """
        redis_users = RedisUsers(server_redis_users_file)
        num_users_before = len(redis_users.users)
        assert redis_users.remove_user("test_user")
        assert len(redis_users.users) == num_users_before - 1

    def test_remove_user_invalid(self, server_redis_users_file: str):
        """
        Test the `remove_user` method with a user that doesn't exist.
        This should return False and not modify the user list.

        :param server_redis_users_file: The path to a dummy redis users file
        """
        redis_users = RedisUsers(server_redis_users_file)
        assert not redis_users.remove_user("nonexistent_user")


class TestAppYaml:
    """Tests for the AppYaml class."""

    def test_initialization(self, server_app_yaml: str, server_app_yaml_contents: dict):
        """
        Test the initialization process of the AppYaml class.

        :param server_app_yaml: The path to an app.yaml file
        :param server_app_yaml_contents: A dict of app.yaml configurations
        """
        app_yaml = AppYaml(server_app_yaml)
        assert app_yaml.get_data() == server_app_yaml_contents

    def test_apply_server_config(self, server_app_yaml: str, server_server_config: Dict[str, str]):
        """
        Test the `apply_server_config` method. This should update the data attribute.

        :param server_app_yaml: The path to an app.yaml file
        :param server_server_config: A pytest fixture of test data to pass to the ServerConfig class
        """
        app_yaml = AppYaml(server_app_yaml)
        server_config = ServerConfig(server_server_config)
        redis_config = RedisConfig(server_config.container.get_config_path())
        app_yaml.apply_server_config(server_config)

        assert app_yaml.data[app_yaml.broker_name]["name"] == server_config.container.get_image_type()
        assert app_yaml.data[app_yaml.broker_name]["username"] == "default"
        assert app_yaml.data[app_yaml.broker_name]["password"] == server_config.container.get_pass_file_path()
        assert app_yaml.data[app_yaml.broker_name]["server"] == redis_config.get_ip_address()
        assert app_yaml.data[app_yaml.broker_name]["port"] == redis_config.get_port()

        assert app_yaml.data[app_yaml.results_name]["name"] == server_config.container.get_image_type()
        assert app_yaml.data[app_yaml.results_name]["username"] == "default"
        assert app_yaml.data[app_yaml.results_name]["password"] == server_config.container.get_pass_file_path()
        assert app_yaml.data[app_yaml.results_name]["server"] == redis_config.get_ip_address()
        assert app_yaml.data[app_yaml.results_name]["port"] == redis_config.get_port()

    def test_update_data(self, server_app_yaml: str):
        """
        Test the `update_data` method. This should update the data attribute.

        :param server_app_yaml: The path to an app.yaml file
        """
        app_yaml = AppYaml(server_app_yaml)
        new_data = {app_yaml.broker_name: {"username": "new_user"}}
        app_yaml.update_data(new_data)

        assert app_yaml.data[app_yaml.broker_name]["username"] == "new_user"

    def test_write(self, server_app_yaml: str, server_testing_dir: str):
        """
        Test the `write` method. This should write data to a file.

        :param server_app_yaml: The path to an app.yaml file
        :param server_testing_dir: The path to the the temp output directory for server tests
        """
        copy_app_yaml = f"{server_testing_dir}/app_copy.yaml"

        # Create a AppYaml object with the basic app.yaml file
        app_yaml = AppYaml(server_app_yaml)

        # Run the test
        app_yaml.write(copy_app_yaml)

        # Check that the contents of the copied file match the contents of the basic file
        assert filecmp.cmp(server_app_yaml, copy_app_yaml)
