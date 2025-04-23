"""
Tests for the `config_commands.py` module.
"""

import logging
import os
from argparse import Namespace
from unittest.mock import MagicMock

import yaml
from pytest import LogCaptureFixture

from merlin.config.config_commands import (
    create_template_config,
    update_backend,
    update_broker,
    update_config,
    update_rabbitmq_config,
    update_redis_config,
)
from tests.fixture_types import FixtureCallable, FixtureStr


class TestCreateTemplateConfig:
    """
    Tests for the `create_template_config` function.
    """

    app_yaml_filename = "app.yaml"

    def run_valid_broker_test(
        self, mocker: MagicMock, config_testing_dir: FixtureStr, broker_type: str, expected_data_config_file: str
    ):
        """
        Helper function to test `create_template_config` for different valid broker types.

        Args:
            mocker: The mocking object for patching.
            config_testing_dir: The path to the temporary testing directory for tests of files in the `config` directory.
            broker_type: The broker type to test (e.g., "redis" or "rabbitmq").
            expected_data_config_file: The expected path to the data configuration file.
        """
        # Create a mocked context manager for the `resources.path` call
        mock_context_manager = mocker.MagicMock()
        mock_context_manager.__enter__.return_value = expected_data_config_file
        mock_context_manager.__exit__.return_value = None
        mocked_resources_path = mocker.patch("merlin.config.config_commands.resources.path", return_value=mock_context_manager)

        # Mock the `create_celery_config` call so that we don't actually create an app.yaml file
        mocked_create_celery_config = mocker.patch("merlin.config.config_commands.create_celery_config")

        # Run the test
        create_template_config("celery", config_testing_dir, broker_type)

        # Ensure the correct calls were made
        mocked_resources_path.assert_called_once_with("merlin.data.celery", os.path.basename(expected_data_config_file))
        mocked_create_celery_config.assert_called_once_with(
            config_testing_dir,
            self.app_yaml_filename,
            expected_data_config_file,
        )

    def test_create_template_config_celery_with_redis_broker(self, mocker: MagicMock, config_testing_dir: FixtureStr):
        """
        Test that the `create_template_config` command will create an app.yaml file that points to a Redis
        broker.

        Args:
            mocker: A built-in fixture from the pytest-mock library to create a Mock object.
            config_testing_dir: The path to the temporary testing directory for tests of files in the `config` directory.
        """
        data_config_file = os.path.join("merlin", "data", "celery", "app_redis.yaml")
        self.run_valid_broker_test(mocker, config_testing_dir, "redis", data_config_file)

    def test_create_template_config_celery_with_rabbitmq_broker(self, mocker: MagicMock, config_testing_dir: FixtureStr):
        """
        Test that the `create_template_config` command will create an app.yaml file that points to a RabbitMQ
        broker.

        Args:
            mocker: A built-in fixture from the pytest-mock library to create a Mock object.
            config_testing_dir: The path to the temporary testing directory for tests of files in the `config` directory.
        """
        data_config_file = os.path.join("merlin", "data", "celery", "app.yaml")
        self.run_valid_broker_test(mocker, config_testing_dir, "rabbitmq", data_config_file)

    def test_create_template_config_invalid_task_server(self, caplog: LogCaptureFixture, config_testing_dir: FixtureStr):
        """
        Test that when given an invalid server, `create_template_config` logs an error message and does not
        create the app.yaml file.

        Args:
            caplog: A built-in fixture from the pytest library to capture logs.
            config_testing_dir: The path to the temporary testing directory for tests of files in the `config` directory.
        """
        caplog.set_level(logging.INFO)
        create_template_config("invalid_server", config_testing_dir, "redis")
        assert "Only celery can be configured currently." in caplog.text, "Missing expected log message"
        assert not os.path.exists(
            os.path.join(config_testing_dir, self.app_yaml_filename)
        ), "An app.yaml file was created when it shouldn't have been"

    def test_create_template_config_creates_directory_if_not_exists(self, config_testing_dir: FixtureStr):
        """
        Test that when given a directory that does not yet exist, the `create_template_config` command creates
        the directory.

        Args:
            config_testing_dir: The path to the temporary testing directory for tests of files in the `config` directory.
        """
        nonexistent_dir = os.path.join(config_testing_dir, "nonexistent_dir")
        create_template_config("celery", nonexistent_dir, "rabbitmq")

        assert os.path.exists(nonexistent_dir), f"The directory {nonexistent_dir} was not created when it should have been"
        assert os.path.exists(
            os.path.join(nonexistent_dir, "app.yaml")
        ), "The app.yaml file was not created when it should have been"


class TestUpdateConfig:
    """
    Tests for the `update_config` function.
    """

    def test_update_config_updates_fields(self):
        """
        Verify that fields in `required_fields` are correctly updated in the `config`.
        """
        test_server = "test_server"
        test_port = 6379
        args = Namespace(server=test_server, port=test_port)
        config = {"server": "", "port": 0}
        required_fields = ["server", "port"]

        update_config(args, config, required_fields)

        assert (
            config["server"] == test_server
        ), f"Expected 'server' entry to be '{test_server}' but it was '{config['server']}'"
        assert config["port"] == test_port, f"Expected 'port' entry to be '{test_port}' but it was '{config['port']}'"

    def test_update_config_ignores_missing_fields(self):
        """
        Ensure that fields not present in `args` are ignored and not updated in `config`.
        """
        test_server = "test_server"
        args = Namespace(server=test_server)
        config = {"server": "", "port": 0}
        required_fields = ["server", "port"]

        update_config(args, config, required_fields)

        # Port should not be changed but server should be
        assert (
            config["server"] == test_server
        ), f"Expected 'server' entry to be '{test_server}' but it was '{config['server']}'"
        assert config["port"] == 0, f"Expected 'port' entry to be '0' but it was '{config['port']}'"

    def test_update_config_handles_password_file(self):
        """
        Check that `password_file` is mapped to `password` in the `config`.
        """
        test_pass_file = "secret_password"
        args = Namespace(password_file=test_pass_file)
        config = {"password": ""}
        required_fields = ["password_file"]

        update_config(args, config, required_fields)

        assert (
            config["password"] == test_pass_file
        ), f"Expected 'password' entry to be '{test_pass_file}' but it was '{config['password']}'"

    def test_update_config_no_changes_for_none_values(self):
        """
        Ensure that `None` values in `args` do not update the `config`.
        """
        args = Namespace(username=None, db_num=None)
        existing_user = "existing_user"
        existing_db = 5
        config = {"username": existing_user, "db_num": existing_db}
        required_fields = ["username", "db_num"]

        update_config(args, config, required_fields)

        # Should see no change to either entry here
        assert (
            config["username"] == existing_user
        ), f"Expected 'username' entry to be '{existing_user}' but it was '{config['username']}'"
        assert (
            config["db_num"] == existing_db
        ), f"Expected 'db_num' entry to be '{existing_db}' but it was '{config['db_num']}'"

    def test_update_config_does_not_modify_other_fields(self):
        """
        Verify that fields not in `required_fields` remain unchanged in `config`.
        """
        new_vhost = "new_vhost"
        args = Namespace(vhost=new_vhost)
        existing_port = 6379
        existing_db = 5
        config = {"vhost": "", "port": existing_port, "db_num": existing_db}
        required_fields = ["vhost", "port", "db_num"]

        update_config(args, config, required_fields)

        # Only the vhost should be updated
        assert config["vhost"] == new_vhost, f"Expected 'vhost' entry to be '{new_vhost}' but it was '{config['vhost']}'"
        assert config["port"] == existing_port, f"Expected 'port' entry to be '{existing_port}' but it was '{config['port']}'"
        assert (
            config["db_num"] == existing_db
        ), f"Expected 'db_num' entry to be '{existing_db}' but it was '{config['db_num']}'"


class TestUpdateRedisConfig:
    """
    Tests for the `update_redis_config` function.
    """

    def test_update_redis_config_updates_required_fields(self):
        """
        Verify that fields in `required_fields` are correctly updated in the `config`.
        """
        new_pass = ("secret_password",)
        new_server = ("localhost",)
        new_port = 6379
        new_db = 0
        new_certs = "required"
        args = Namespace(password_file=new_pass, server=new_server, port=new_port, db_num=new_db, cert_reqs=new_certs)
        config = {"name": "", "username": "", "password": "", "server": "", "port": 0, "db_num": -1, "cert_reqs": ""}
        config_type = "broker"

        update_redis_config(args, config, config_type)

        assert (
            config["password"] == new_pass
        ), f"Expected 'password' entry to be '{new_pass}' but it was '{config['password']}'"
        assert config["server"] == new_server, f"Expected 'server' entry to be '{new_server}' but it was '{config['server']}'"
        assert config["port"] == new_port, f"Expected 'port' entry to be '{new_port}' but it was '{config['port']}'"
        assert config["db_num"] == new_db, f"Expected 'db_num' entry to be '{new_db}' but it was '{config['db_num']}'"
        assert (
            config["cert_reqs"] == new_certs
        ), f"Expected 'cert_res' entry to be '{new_certs}' but it was '{config['cert_reqs']}'"

    def test_update_redis_config_sets_name_to_rediss(self):
        """
        Ensure the `name` field is always set to "rediss".
        """
        args = Namespace()
        config = {"name": ""}
        config_type = "backend"

        update_redis_config(args, config, config_type)

        assert config["name"] == "rediss", f"Expected 'name' entry to be 'rediss' but it was '{config['name']}'"

    def test_update_redis_config_ignores_username_and_vhost(self):
        """
        Confirm that `username` is set to an empty string and `vhost` is not set, regardless of input.
        """
        args = Namespace(username="test_user", vhost="test_vhost")
        config = {"username": "existing_user"}
        config_type = "broker"

        update_redis_config(args, config, config_type)

        # Redis doesn't use username
        assert config["username"] == "", f"Expected 'username' entry to be '' but it was '{config['username']}'"
        assert "vhost" not in config, "Expected to not find a 'vhost' entry in config but there was one."

    def test_update_redis_config_no_changes_for_missing_fields(self):
        """
        Ensure that fields not present in `args` are ignored and not updated in `config`.
        """
        args = Namespace(password_file=None, server=None, port=None, db_num=None, cert_reqs=None)
        existing_pass = "existing_password"
        existing_server = "existing_server"
        existing_port = 6379
        existing_db = 0
        existing_certs = "existing_cert_reqs"
        config = {
            "password": existing_pass,
            "server": existing_server,
            "port": existing_port,
            "db_num": existing_db,
            "cert_reqs": existing_certs,
        }
        config_type = "broker"

        update_redis_config(args, config, config_type)

        assert (
            config["password"] == existing_pass
        ), f"Expected 'password' entry to be '{existing_pass}' but it was '{config['password']}'"
        assert (
            config["server"] == existing_server
        ), f"Expected 'server' entry to be '{existing_server}' but it was '{config['server']}'"
        assert config["port"] == existing_port, f"Expected 'port' entry to be '{existing_port}' but it was '{config['port']}'"
        assert (
            config["db_num"] == existing_db
        ), f"Expected 'db_num' entry to be '{existing_db}' but it was '{config['db_num']}'"
        assert (
            config["cert_reqs"] == existing_certs
        ), f"Expected 'cert_res' entry to be '{existing_certs}' but it was '{config['cert_reqs']}'"


class TestUpdateRabbitMQConfig:
    """
    Tests for the `update_rabbitmq_config` function.
    """

    def test_update_rabbitmq_config_updates_required_fields(self):
        """
        Verify that fields in `required_fields` are correctly updated in the `config`.
        """
        new_username = "test_user"
        new_pass = ("secret_password",)
        new_server = ("localhost",)
        new_port = 5672
        new_vhost = "test_vhost"
        new_certs = "required"
        args = Namespace(
            username=new_username,
            password_file=new_pass,
            server=new_server,
            port=new_port,
            vhost=new_vhost,
            cert_reqs=new_certs,
        )
        config = {"name": "", "username": "", "password": "", "server": "", "port": 0, "vhost": "", "cert_reqs": ""}

        update_rabbitmq_config(args, config)

        assert (
            config["username"] == new_username
        ), f"Expected 'username' entry to be '{new_username}' but it was '{config['username']}'"
        assert (
            config["password"] == new_pass
        ), f"Expected 'password' entry to be '{new_pass}' but it was '{config['password']}'"
        assert config["server"] == new_server, f"Expected 'server' entry to be '{new_server}' but it was '{config['server']}'"
        assert config["port"] == new_port, f"Expected 'port' entry to be '{new_port}' but it was '{config['port']}'"
        assert config["vhost"] == new_vhost, f"Expected 'vhost' entry to be '{new_vhost}' but it was '{config['vhost']}'"
        assert (
            config["cert_reqs"] == new_certs
        ), f"Expected 'cert_res' entry to be '{new_certs}' but it was '{config['cert_reqs']}'"

    def test_update_rabbitmq_config_sets_name_to_rabbitmq(self):
        """
        Ensure the `name` field is always set to "rabbitmq".
        """
        args = Namespace()
        config = {"name": ""}

        update_rabbitmq_config(args, config)

        assert config["name"] == "rabbitmq", f"Expected 'name' entry to be 'rabbitmq' but it was '{config['name']}'"

    def test_update_rabbitmq_config_no_changes_for_missing_fields(self):
        """
        Ensure that fields not present in `args` are ignored and not updated in `config`.
        """
        args = Namespace(password_file=None, server=None, port=None, db_num=None, cert_reqs=None)
        existing_username = "existing_user"
        existing_pass = "existing_password"
        existing_server = "existing_server"
        existing_port = 5672
        existing_vhost = "existing_vhost"
        existing_certs = "existing_cert_reqs"
        config = {
            "username": existing_username,
            "password": existing_pass,
            "server": existing_server,
            "port": existing_port,
            "vhost": existing_vhost,
            "cert_reqs": existing_certs,
        }

        update_rabbitmq_config(args, config)

        assert (
            config["username"] == existing_username
        ), f"Expected 'username' entry to be '{existing_username}' but it was '{config['username']}'"
        assert (
            config["password"] == existing_pass
        ), f"Expected 'password' entry to be '{existing_pass}' but it was '{config['password']}'"
        assert (
            config["server"] == existing_server
        ), f"Expected 'server' entry to be '{existing_server}' but it was '{config['server']}'"
        assert config["port"] == existing_port, f"Expected 'port' entry to be '{existing_port}' but it was '{config['port']}'"
        assert (
            config["vhost"] == existing_vhost
        ), f"Expected 'vhost' entry to be '{existing_vhost}' but it was '{config['vhost']}'"
        assert (
            config["cert_reqs"] == existing_certs
        ), f"Expected 'cert_res' entry to be '{existing_certs}' but it was '{config['cert_reqs']}'"


class TestUpdateBroker:
    """
    Tests for the `update_broker` function.
    """

    def test_update_broker_redis(self, config_create_app_yaml: FixtureCallable, config_testing_dir: FixtureStr):
        """
        Verify that the `update_broker` function can successfully update redis brokers.

        Args:
            config_create_app_yaml: A function that creates an app.yaml file.
            config_testing_dir: The path to the temporary testing directory for tests of files in the `config` directory.
        """
        # Expected Redis broker configuration
        expected_pass = "secret_password"
        expected_server = "server_connection_string"
        expected_port = 6379
        expected_broker_config = {
            "name": "rediss",
            "username": "",
            "password": expected_pass,
            "server": expected_server,
            "port": expected_port,
            "db_num": 0,
            "cert_reqs": "none",
        }

        # Create the app.yaml file in the temporary directory
        filename = "test_update_broker_redis_app.yaml"
        config_create_app_yaml(filename, broker="redis")

        # Ensure the created app.yaml file exists
        app_yaml_filepath = os.path.join(config_testing_dir, filename)
        assert os.path.exists(app_yaml_filepath)

        # Update the broker configuration
        args = Namespace(type="redis", password_file=expected_pass, server=expected_server, port=expected_port)
        update_broker(args, app_yaml_filepath)

        # Verify the updated app.yaml file
        with open(app_yaml_filepath, "r") as f:
            updated_config = yaml.safe_load(f)

        assert "broker" in updated_config
        assert updated_config["broker"] == expected_broker_config

    def test_update_broker_rabbitmq(self, config_create_app_yaml: FixtureCallable, config_testing_dir: FixtureStr):
        """
        Verify that the `update_broker` function can successfully update rabbitmq brokers.

        Args:
            config_create_app_yaml: A function that creates an app.yaml file.
            config_testing_dir: The path to the temporary testing directory for tests of files in the `config` directory.
        """
        # Expected Redis broker configuration
        expected_username = "new_user"
        expected_pass = "secret_password"
        expected_server = "server_connection_string"
        expected_port = 5672
        expected_vhost = "new_vhost"
        expected_broker_config = {
            "name": "rabbitmq",
            "username": expected_username,
            "password": expected_pass,
            "server": expected_server,
            "port": expected_port,
            "vhost": expected_vhost,
            "cert_reqs": "none",
        }

        # Create the app.yaml file in the temporary directory
        filename = "test_update_broker_rabbitmq_app.yaml"
        config_create_app_yaml(filename, broker="rabbitmq")

        # Ensure the created app.yaml file exists
        app_yaml_filepath = os.path.join(config_testing_dir, filename)
        assert os.path.exists(app_yaml_filepath)

        # Update the broker configuration
        args = Namespace(
            type="rabbitmq",
            username=expected_username,
            password_file=expected_pass,
            server=expected_server,
            port=expected_port,
            vhost=expected_vhost,
        )
        update_broker(args, app_yaml_filepath)

        # Verify the updated app.yaml file
        with open(app_yaml_filepath, "r") as f:
            updated_config = yaml.safe_load(f)

        assert "broker" in updated_config
        assert updated_config["broker"] == expected_broker_config

    def test_update_broker_invalid_type(
        self, caplog: LogCaptureFixture, config_create_app_yaml: FixtureCallable, config_testing_dir: FixtureStr
    ):
        """
        Test the `update_broker` function with an invalid broker type.

        Args:
            caplog: A built-in fixture from the pytest library to capture logs.
            config_create_app_yaml: A function that creates an app.yaml file.
            config_testing_dir: The path to the temporary testing directory for tests of files in the `config` directory.
        """
        # Create the app.yaml file in the temporary directory
        filename = "test_update_broker_invalid_type_app.yaml"
        config_create_app_yaml(filename, broker="rabbitmq")

        # Path to the created app.yaml file
        app_yaml_filepath = os.path.join(config_testing_dir, filename)

        # Ensure the file exists
        assert os.path.exists(app_yaml_filepath)

        # Update the broker configuration with an invalid type
        new_username = "new_username"
        args = Namespace(type="invalid_type", username=new_username)

        update_broker(args, app_yaml_filepath)

        # Verify that the file content remains unchanged
        with open(app_yaml_filepath, "r") as f:
            unchanged_config = yaml.safe_load(f)
            print(f"uncahnged_config: {unchanged_config}")

        assert "Invalid broker type. Use 'redis' or 'rabbitmq'." in caplog.text, "Missing expected log message"
        assert "broker" in unchanged_config
        assert unchanged_config["broker"]["username"] != new_username  # Ensure the config wasn't updated


class TestUpdateBackend:
    """
    Tests for the `update_backend` function.
    """

    def test_update_backend_redis(self, config_create_app_yaml: FixtureCallable, config_testing_dir: FixtureStr):
        """
        Verify that the `update_backend` function can successfully update redis backends.

        Args:
            config_create_app_yaml: A function that creates an app.yaml file.
            config_testing_dir: The path to the temporary testing directory for tests of files in the `config` directory.
        """
        # Expected Redis backend configuration
        expected_pass = "secret_password"
        expected_server = "server_connection_string"
        expected_port = 6379
        expected_db = 3
        expected_backend_config = {
            "name": "rediss",
            "username": "",
            "password": expected_pass,
            "server": expected_server,
            "encryption_key": "~/.merlin/encrypt_data_key",
            "port": expected_port,
            "db_num": expected_db,
            "cert_reqs": "none",
        }

        # Create the app.yaml file in the temporary directory
        filename = "test_update_backend_redis_app.yaml"
        config_create_app_yaml(filename)

        # Ensure the created app.yaml file exists
        app_yaml_filepath = os.path.join(config_testing_dir, filename)
        assert os.path.exists(app_yaml_filepath)

        # Update the backend configuration
        args = Namespace(
            type="redis",
            password_file=expected_pass,
            server=expected_server,
            port=expected_port,
            db_num=expected_db,
        )
        update_backend(args, app_yaml_filepath)

        # Verify the updated app.yaml file
        with open(app_yaml_filepath, "r") as f:
            updated_config = yaml.safe_load(f)

        assert "results_backend" in updated_config
        assert updated_config["results_backend"] == expected_backend_config

    def test_update_backend_invalid_type(
        self, caplog: LogCaptureFixture, config_create_app_yaml: FixtureCallable, config_testing_dir: FixtureStr
    ):
        """
        Test the `update_backend` function with an invalid backend type.

        Args:
            caplog: A built-in fixture from the pytest library to capture logs.
            config_create_app_yaml: A function that creates an app.yaml file.
            config_testing_dir: The path to the temporary testing directory for tests of files in the `config` directory.
        """
        # Create the app.yaml file in the temporary directory
        filename = "test_update_backend_invalid_type_app.yaml"
        config_create_app_yaml(filename)

        # Path to the created app.yaml file
        app_yaml_filepath = os.path.join(config_testing_dir, filename)

        # Ensure the file exists
        assert os.path.exists(app_yaml_filepath)

        # Update the backend configuration with an invalid type
        new_db = 3
        args = Namespace(type="invalid_type", db_num=new_db)

        update_backend(args, app_yaml_filepath)

        # Verify that the file content remains unchanged
        with open(app_yaml_filepath, "r") as f:
            unchanged_config = yaml.safe_load(f)

        assert "Invalid backend type. Use 'redis'." in caplog.text, "Missing expected log message"
        assert "results_backend" in unchanged_config
        assert unchanged_config["results_backend"]["db_num"] != 3  # Ensure the config wasn't updated
