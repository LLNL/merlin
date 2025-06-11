##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `merlin_config_manager.py` module.
"""

import os
from argparse import Namespace

import pytest
import yaml
from pytest_mock import MockerFixture

from merlin.config.merlin_config_manager import MerlinConfigManager
from tests.fixture_types import FixtureCallable, FixtureStr


@pytest.fixture(scope="session")
def merlin_config_manager_testing_dir(create_testing_dir: FixtureCallable, config_testing_dir: FixtureStr) -> FixtureStr:
    """
    Fixture to create a temporary output directory for tests related to testing the
    `config` directory.

    Args:
        create_testing_dir: A fixture which returns a function that creates the testing directory.
        config_testing_dir: The path to the temporary ouptut directory for config tests

    Returns:
        The path to the temporary testing directory for tests of the `merlin_config_manager.py` module
    """
    return create_testing_dir(config_testing_dir, "merlin_config_manager_tests")


@pytest.fixture
def args() -> Namespace:
    """
    Fixture for creating a mock `Namespace` object with default arguments.

    Returns:
        A mock object containing default configuration arguments for testing.
    """
    return Namespace(
        config_file=None,
        task_server="celery",
        broker="redis",
        type="redis",
        password_file="fake_pass.txt",
        server="localhost",
        port=6379,
        db_num=0,
        cert_reqs="none",
        username="",
        password="",
        vhost="/",
        test=True,
    )


def test_create_template_config_creates_file(
    mocker: MockerFixture, args: Namespace, merlin_config_manager_testing_dir: FixtureStr
):
    """
    Test that `create_template_config` creates the expected configuration file.

    Args:
        mocker (MockerFixture): The mocker fixture for mocking dependencies.
        args (Namespace): The mock configuration arguments fixture.
        merlin_config_manager_testing_dir (str): The directory used for testing configurations.
    """
    output_dir = os.path.join(merlin_config_manager_testing_dir, "test_create_template_config_creates_file")
    mock_encrypt = mocker.patch("merlin.common.security.encrypt.init_key")
    args.config_file = os.path.join(output_dir, "app.yaml")

    config_manager = MerlinConfigManager(args)
    config_manager.create_template_config()

    mock_encrypt.assert_called_once()
    assert os.path.exists(args.config_file)


def test_save_config_path(mocker: MockerFixture, args: Namespace, merlin_config_manager_testing_dir: FixtureStr):
    """
    Test that `save_config_path` writes the correct config file path.

    Args:
        mocker (pytest_mock.MockerFixture): The mocker fixture for mocking dependencies.
        args (Namespace): The mock configuration arguments fixture.
        merlin_config_manager_testing_dir (str): The directory used for testing configurations.
    """
    # Mock CONFIG_PATH_FILE and set a config file path
    output_dir = os.path.join(merlin_config_manager_testing_dir, "test_save_config_path")
    mocked_config_path_file = os.path.join(output_dir, "config_path.txt")
    mocker.patch("merlin.config.merlin_config_manager.CONFIG_PATH_FILE", mocked_config_path_file)
    args.config_file = os.path.join(output_dir, "app.yaml")

    # Run the test
    config_manager = MerlinConfigManager(args)
    config_manager.create_template_config()
    config_manager.save_config_path()

    # Check that the output is correct
    assert os.path.exists(mocked_config_path_file)
    with open(mocked_config_path_file) as f:
        path = f.read().strip()
    assert path == args.config_file


def test_update_redis_broker_config(args: Namespace, merlin_config_manager_testing_dir: FixtureStr):
    """
    Test that `update_backend` correctly updates the Redis broker configuration.

    Args:
        args (Namespace): The mock configuration arguments fixture.
        merlin_config_manager_testing_dir (str): The directory used for testing configurations.
    """
    output_dir = os.path.join(merlin_config_manager_testing_dir, "test_update_redis_broker_config")
    args.config_file = os.path.join(output_dir, "app.yaml")
    args.port = 1234
    args.db_num = 1
    config_manager = MerlinConfigManager(args)
    config_manager.create_template_config()

    config = {"broker": {}}
    with open(args.config_file, "w") as f:
        yaml.dump(config, f)

    config_manager.update_broker()

    with open(args.config_file) as f:
        updated_config = yaml.safe_load(f)

    assert updated_config["broker"]["name"] == "rediss"  # from `args` fixture
    assert updated_config["broker"]["server"] == "localhost"  # from `args` fixture
    assert updated_config["broker"]["port"] == 1234
    assert updated_config["broker"]["db_num"] == 1


def test_update_redis_backend_config(args: Namespace, merlin_config_manager_testing_dir: FixtureStr):
    """
    Test that `update_backend` correctly updates the Redis backend configuration.

    Args:
        args (Namespace): The mock configuration arguments fixture.
        merlin_config_manager_testing_dir (str): The directory used for testing configurations.
    """
    output_dir = os.path.join(merlin_config_manager_testing_dir, "test_update_redis_broker_config")
    args.config_file = os.path.join(output_dir, "app.yaml")
    args.port = 1234
    args.db_num = 1
    config_manager = MerlinConfigManager(args)
    config_manager.create_template_config()

    config = {"results_backend": {}}
    with open(args.config_file, "w") as f:
        yaml.dump(config, f)

    config_manager.update_backend()

    with open(args.config_file) as f:
        updated_config = yaml.safe_load(f)

    assert updated_config["results_backend"]["name"] == "rediss"  # from `args` fixture
    assert updated_config["results_backend"]["server"] == "localhost"  # from `args` fixture
    assert updated_config["results_backend"]["port"] == 1234
    assert updated_config["results_backend"]["db_num"] == 1


def test_update_rabbitmq_config(args: Namespace):
    """
    Test that `update_rabbitmq_config` correctly updates the RabbitMQ configuration.

    Args:
        args (Namespace): The mock configuration arguments fixture.
    """
    args.type = "rabbitmq"
    args.password_file = "pw.txt"
    args.username = "user"
    args.vhost = "vhost"
    args.server = "server"
    args.port = 5672
    args.cert_reqs = "CERT_REQUIRED"
    args.config_file = "dummy.yaml"

    config_manager = MerlinConfigManager(args)
    config = {}
    config_manager.update_rabbitmq_config(config)
    assert config["name"] == "rabbitmq"
    assert config["username"] == "user"
    assert config["vhost"] == "vhost"


def test_update_config_generic(args: Namespace):
    """
    Test that `update_config` correctly updates the configuration with generic fields.

    Args:
        args (Namespace): The mock configuration arguments fixture.
    """
    args.test_field = "test_value"
    args.config_file = "dummy.yaml"
    config_manager = MerlinConfigManager(args)
    config = {}
    config_manager.update_config(config, ["test_field"])
    assert config["test_field"] == "test_value"
