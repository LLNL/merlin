##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the configfile.py module.
"""

import getpass
import logging
import os
import shutil
import ssl
from copy import copy, deepcopy
from unittest.mock import MagicMock, patch

import pytest
import yaml
from _pytest.capture import CaptureFixture
from pytest_mock import MockerFixture

from merlin.config.configfile import (
    CONFIG,
    default_config_info,
    find_config_file,
    get_cert_file,
    get_config,
    get_default_config,
    get_ssl_entries,
    initialize_config,
    is_debug,
    is_local_mode,
    load_config,
    load_default_celery,
    load_defaults,
    merge_sslmap,
    process_ssl_map,
    set_local_mode,
    set_username_and_vhost,
)
from tests.constants import CERT_FILES
from tests.fixture_types import FixtureCallable, FixtureStr
from tests.utils import create_dir


COPIED_APP_FILENAME = "app_copy.yaml"
DUMMY_APP_FILEPATH = os.path.join(os.path.dirname(__file__), "dummy_app.yaml")


@pytest.fixture(scope="session")
def configfile_testing_dir(create_testing_dir: FixtureCallable, config_testing_dir: FixtureStr) -> FixtureStr:
    """
    Fixture to create a temporary output directory for tests related to testing the
    `config` directory.

    Args:
        create_testing_dir: A fixture which returns a function that creates the testing directory.
        config_testing_dir: The path to the temporary ouptut directory for config tests.

    Returns:
        The path to the temporary testing directory for tests of the `configfile.py` module
    """
    return create_testing_dir(config_testing_dir, "configfile_tests")


@pytest.fixture(scope="session")
def demo_app_yaml(configfile_testing_dir: FixtureStr) -> FixtureStr:
    """
    Fixture that creates an empty `app.yaml` file in the specified testing directory.

    Args:
        configfile_testing_dir (FixtureStr): The directory used for testing configurations.

    Returns:
        The path to the newly created `app.yaml` file.
    """
    app_yaml_path = os.path.join(configfile_testing_dir, "app.yaml")
    with open(app_yaml_path, "w"):
        pass
    return app_yaml_path


@pytest.fixture(scope="session")
def config_path(configfile_testing_dir: FixtureStr, demo_app_yaml: FixtureStr) -> FixtureStr:
    """
    Fixture that creates a `config_path.txt` file containing the path to `app.yaml`.

    Args:
        configfile_testing_dir (FixtureStr): The directory used for testing configurations.
        demo_app_yaml (FixtureStr): The path to the `app.yaml` file created by the `demo_app_yaml` fixture.

    Returns:
        The path to the newly created `config_path.txt` file.
    """
    config_path_file = os.path.join(configfile_testing_dir, "config_path.txt")
    with open(config_path_file, "w") as cpf:
        cpf.write(demo_app_yaml)
    return config_path_file


@pytest.fixture(autouse=True)
def reset_local_mode():
    """
    Reset IS_LOCAL_MODE before each test.

    This is done automatically without having to manually use this fixture in each test
    with the use of `autouse=True`.
    """
    set_local_mode(False)
    yield
    set_local_mode(False)


def create_app_yaml(app_yaml_filepath: str):
    """
    Create a dummy app.yaml file at `app_yaml_filepath`.

    :param app_yaml_filepath: The location to create an app.yaml file at
    """
    full_app_yaml_filepath = f"{app_yaml_filepath}/app.yaml"
    if not os.path.exists(full_app_yaml_filepath):
        shutil.copy(DUMMY_APP_FILEPATH, full_app_yaml_filepath)


def test_local_mode_toggle_and_logging(caplog: CaptureFixture):
    """
    Test enabling/disabling local mode and logging behavior.

    Args:
        caplog: A built-in fixture from the pytest library to capture logs.
    """
    caplog.set_level(logging.INFO)
    # Test default state
    assert is_local_mode() is False

    # Test enabling (default parameter and explicit True)
    set_local_mode()  # Default True
    assert is_local_mode() is True
    assert "Running Merlin in local mode (no configuration file required)" in caplog.text

    # Test disabling
    set_local_mode(False)
    assert is_local_mode() is False


def test_default_config_structure_and_values():
    """
    Test default config structure and values.
    """
    config = get_default_config()

    # Verify structure and key values
    expected_config = {
        "broker": {
            "username": "user",
            "vhost": "vhost",
            "server": "localhost",
            "name": "rabbitmq",
            "port": 5672,
            "protocol": "amqp",
        },
        "celery": {"omit_queue_tag": False, "queue_tag": "[merlin]_", "override": None},
        "results_backend": {"server": "localhost", "name": "redis", "port": 6379, "protocol": "redis"},
    }

    assert config == expected_config
    assert isinstance(config, dict)


def test_load_config(configfile_testing_dir: FixtureStr):
    """
    Test the `load_config` function.

    Args:
        configfile_testing_dir (FixtureStr): The directory used for testing configurations.
    """
    configfile_dir = os.path.join(configfile_testing_dir, "test_load_config")
    create_dir(configfile_dir)
    create_app_yaml(configfile_dir)

    with open(DUMMY_APP_FILEPATH, "r") as dummy_app_file:
        expected = yaml.load(dummy_app_file, yaml.Loader)

    actual = load_config(os.path.join(configfile_dir, "app.yaml"))
    assert actual == expected


def test_load_config_invalid_file():
    """
    Test the `load_config` function with an invalid filepath.
    """
    assert load_config("invalid/filepath") is None


def test_find_config_file_config_path_file_exists_and_is_valid(
    mocker: MockerFixture, demo_app_yaml: FixtureStr, config_path: FixtureStr
):
    """
    Test that `find_config_file` correctly returns the path to the configuration file
    when `CONFIG_PATH_FILE` exists and points to a valid `app.yaml` file.

    Args:
        mocker (MockerFixture): Pytest mocker fixture for mocking functionality.
        demo_app_yaml (FixtureStr): Path to the valid `app.yaml` file.
        config_path (FixtureStr): Path to the `CONFIG_PATH_FILE` containing the valid configuration path.
    """
    mocker.patch("merlin.config.configfile.CONFIG_PATH_FILE", config_path)
    result = find_config_file()
    assert result == demo_app_yaml


def test_find_config_file_config_path_file_exists_but_invalid(mocker: MockerFixture, configfile_testing_dir: FixtureStr):
    """
    Test that `find_config_file` returns `None` when `CONFIG_PATH_FILE` exists but points to an invalid path.

    Args:
        mocker (MockerFixture): Pytest mocker fixture for mocking functionality.
        configfile_testing_dir (FixtureStr): Directory used for testing invalid configuration paths.
    """
    config_file_path = os.path.join(configfile_testing_dir, "config_path_app_yaml_doesnt_exist.txt")
    with open(config_file_path, "w") as cfp:
        cfp.write("invalid_app.yaml")
    mocker.patch("merlin.config.configfile.CONFIG_PATH_FILE", config_file_path)
    mocker.patch("merlin.config.configfile.MERLIN_HOME", os.path.join(configfile_testing_dir, ".merlin"))
    result = find_config_file()
    assert result is None


def test_find_config_file_local_app_yaml_exists(mocker: MockerFixture, configfile_testing_dir: FixtureStr):
    """
    Test that `find_config_file` correctly returns the path to `app.yaml` when it exists in the current working directory.

    Args:
        mocker (MockerFixture): Pytest mocker fixture for mocking functionality.
        configfile_testing_dir (FixtureStr): Directory used for testing.
    """
    mocker.patch("merlin.config.configfile.CONFIG_PATH_FILE", os.path.join(configfile_testing_dir, "invalid_config_path.txt"))
    mocker.patch("os.getcwd", return_value=configfile_testing_dir)
    local_app_yaml = os.path.join(configfile_testing_dir, "app.yaml")
    mocker.patch("os.path.isfile", side_effect=lambda x: x == local_app_yaml)

    result = find_config_file()
    assert result == local_app_yaml


def test_find_config_file_merlin_home_app_yaml_exists(mocker: MockerFixture, configfile_testing_dir: FixtureStr):
    """
    Test that `find_config_file` correctly returns the path to `app.yaml` when it exists in the `MERLIN_HOME` directory.

    Args:
        mocker (MockerFixture): Pytest mocker fixture for mocking functionality.
        configfile_testing_dir (FixtureStr): Directory used for testing.
    """
    mocker.patch("merlin.config.configfile.CONFIG_PATH_FILE", os.path.join(configfile_testing_dir, "invalid_config_path.txt"))
    mocker.patch("merlin.config.configfile.MERLIN_HOME", configfile_testing_dir)
    merlin_home_app_yaml = os.path.join(configfile_testing_dir, "app.yaml")
    mocker.patch("os.path.isfile", side_effect=lambda x: x == merlin_home_app_yaml)

    result = find_config_file()
    assert result == merlin_home_app_yaml


def test_find_config_file_no_app_yaml_found(mocker: MockerFixture):
    """
    Test that `find_config_file` returns `None` when no `app.yaml` file is found in any location.

    Args:
        mocker (MockerFixture): Pytest mocker fixture for mocking functionality.
    """
    mocker.patch("os.path.isfile", return_value=False)
    result = find_config_file()
    assert result is None


def test_find_config_file_path_provided_app_yaml_exists(mocker: MockerFixture, configfile_testing_dir: FixtureStr):
    """
    Test that `find_config_file` correctly returns the path to `app.yaml` when a valid directory path is provided.

    Args:
        mocker (MockerFixture): Pytest mocker fixture for mocking functionality.
        configfile_testing_dir (FixtureStr): Directory used for testing.
    """
    mocker.patch("merlin.config.configfile.CONFIG_PATH_FILE", os.path.join(configfile_testing_dir, "invalid_config_path.txt"))
    mocker.patch("os.path.isfile", side_effect=lambda x: x == "/mock/provided/path/app.yaml")
    mocker.patch("os.path.exists", return_value=True)
    result = find_config_file("/mock/provided/path")
    assert result == "/mock/provided/path/app.yaml"


def test_find_config_file_path_provided_app_yaml_does_not_exist(mocker: MockerFixture):
    """
    Test that `find_config_file` returns `None` when a directory path is provided but `app.yaml` does not exist in that directory.

    Args:
        mocker (MockerFixture): Pytest mocker fixture for mocking functionality.
    """
    mocker.patch("os.path.isfile", return_value=False)
    mocker.patch("os.path.exists", return_value=False)
    result = find_config_file("/mock/provided/path")
    assert result is None


def test_set_username_and_vhost_nothing_to_load():
    """
    Test the `set_username_and_vhost` function with nothing to load. In other words, in this
    test the config dict will have a username and vhost already set for the broker. We'll
    create the dict then make a copy of it to test against after calling the function.
    """
    actual_config = {"broker": {"username": "default", "vhost": "host4testing"}}
    expected_config = deepcopy(actual_config)
    assert actual_config is not expected_config

    set_username_and_vhost(actual_config)

    # Ensure that nothing was modified after our call to set_username_and_vhost
    assert actual_config == expected_config


def test_set_username_and_vhost_no_username():
    """
    Test the `set_username_and_vhost` function with no username. In other words, in this
    test the config dict will have vhost already set for the broker but not a username.
    """
    expected_config = {"broker": {"username": getpass.getuser(), "vhost": "host4testing"}}
    actual_config = {"broker": {"vhost": "host4testing"}}
    set_username_and_vhost(actual_config)

    # Ensure that the username was set in the call to set_username_and_vhost
    assert actual_config == expected_config


def test_set_username_and_vhost_no_vhost():
    """
    Test the `set_username_and_vhost` function with no vhost. In other words, in this
    test the config dict will have username already set for the broker but not a vhost.
    """
    expected_config = {"broker": {"username": "default", "vhost": getpass.getuser()}}
    actual_config = {"broker": {"username": "default"}}
    set_username_and_vhost(actual_config)

    # Ensure that the vhost was set in the call to set_username_and_vhost
    assert actual_config == expected_config


def test_load_default_celery_nothing_to_load():
    """
    Test the `load_default_celery` function with nothing to load. In other words, in this
    test the config dict will have a celery entry containing omit_queue_tag, queue_tag, and
    override. We'll create the dict then make a copy of it to test against after calling
    the function.
    """
    actual_config = {"celery": {"omit_queue_tag": False, "queue_tag": "[merlin]_", "override": None}}
    expected_config = deepcopy(actual_config)
    assert actual_config is not expected_config

    load_default_celery(actual_config)

    # Ensure that nothing was modified after our call to load_default_celery
    assert actual_config == expected_config


def test_load_default_celery_no_omit_queue_tag():
    """
    Test the `load_default_celery` function with no omit_queue_tag. The function should
    create a default entry of False for this.
    """
    actual_config = {"celery": {"queue_tag": "[merlin]_", "override": None}}
    expected_config = {"celery": {"omit_queue_tag": False, "queue_tag": "[merlin]_", "override": None}}
    load_default_celery(actual_config)

    # Ensure that the omit_queue_tag was set in the call to load_default_celery
    assert actual_config == expected_config


def test_load_default_celery_no_queue_tag():
    """
    Test the `load_default_celery` function with no queue_tag. The function should
    create a default entry of '[merlin]_' for this.
    """
    actual_config = {"celery": {"omit_queue_tag": False, "override": None}}
    expected_config = {"celery": {"omit_queue_tag": False, "queue_tag": "[merlin]_", "override": None}}
    load_default_celery(actual_config)

    # Ensure that the queue_tag was set in the call to load_default_celery
    assert actual_config == expected_config


def test_load_default_celery_no_override():
    """
    Test the `load_default_celery` function with no override. The function should
    create a default entry of None for this.
    """
    actual_config = {"celery": {"omit_queue_tag": False, "queue_tag": "[merlin]_"}}
    expected_config = {"celery": {"omit_queue_tag": False, "queue_tag": "[merlin]_", "override": None}}
    load_default_celery(actual_config)

    # Ensure that the override was set in the call to load_default_celery
    assert actual_config == expected_config


def test_load_default_celery_no_celery_block():
    """
    Test the `load_default_celery` function with no celery block. The function should
    create a default entry of
    {"celery": {"omit_queue_tag": False, "queue_tag": "[merlin]_", "override": None}} for this.
    """
    actual_config = {}
    expected_config = {"celery": {"omit_queue_tag": False, "queue_tag": "[merlin]_", "override": None}}
    load_default_celery(actual_config)

    # Ensure that the celery block was set in the call to load_default_celery
    assert actual_config == expected_config


def test_load_defaults():
    """
    Test that the `load_defaults` function loads the user names and the celery block properly.
    """
    actual_config = {"broker": {}}
    expected_config = {
        "broker": {"username": getpass.getuser(), "vhost": getpass.getuser()},
        "celery": {"omit_queue_tag": False, "queue_tag": "[merlin]_", "override": None},
    }
    load_defaults(actual_config)

    assert actual_config == expected_config


def test_get_config(configfile_testing_dir: FixtureStr):
    """
    Test the `get_config` function.

    Args:
        configfile_testing_dir (FixtureStr): The directory used for testing configurations.
    """

    # Create the configfile directory and put an app.yaml file there
    configfile_dir = os.path.join(configfile_testing_dir, "test_get_config")
    create_dir(configfile_dir)
    create_app_yaml(configfile_dir)

    # Load up the contents of the dummy app.yaml file that we copied
    with open(DUMMY_APP_FILEPATH, "r") as dummy_app_file:
        expected = yaml.load(dummy_app_file, yaml.Loader)

    # Add in default settings that should be added
    expected["celery"]["omit_queue_tag"] = False
    expected["celery"]["queue_tag"] = "[merlin]_"

    actual = get_config(configfile_dir)

    assert actual == expected


def test_get_config_invalid_path():
    """
    Test the `get_config` function with an invalid path. This should raise a ValueError.
    """
    with pytest.raises(ValueError) as excinfo:
        get_config("invalid/path")

    assert "Cannot find a merlin config file!" in str(excinfo.value)


def test_is_debug_no_merlin_debug():
    """
    Test the `is_debug` function without having MERLIN_DEBUG in the environment.
    This should return False.
    """

    # Delete the current val of MERLIN_DEBUG and store it (if there is one)
    reset_merlin_debug = False
    debug_val = None
    if "MERLIN_DEBUG" in os.environ:
        debug_val = copy(os.environ["MERLIN_DEBUG"])
        del os.environ["MERLIN_DEBUG"]
        reset_merlin_debug = True

    # Run the test
    try:
        assert is_debug() is False
    except AssertionError as exc:
        # Make sure to reset the value of MERLIN_DEBUG even if the test fails
        if reset_merlin_debug:
            os.environ["MERLIN_DEBUG"] = debug_val
        raise AssertionError from exc

    # Reset the value of MERLIN_DEBUG
    if reset_merlin_debug:
        os.environ["MERLIN_DEBUG"] = debug_val


def test_is_debug_with_merlin_debug():
    """
    Test the `is_debug` function with having MERLIN_DEBUG in the environment.
    This should return True.
    """

    # Grab the current value of MERLIN_DEBUG if there is one
    reset_merlin_debug = False
    debug_val = None
    if "MERLIN_DEBUG" in os.environ and int(os.environ["MERLIN_DEBUG"]) != 1:
        debug_val = copy(os.environ["MERLIN_DEBUG"])
        reset_merlin_debug = True

    # Set the MERLIN_DEBUG value to be 1
    os.environ["MERLIN_DEBUG"] = "1"

    try:
        assert is_debug() is True
    except AssertionError as exc:
        # Make sure to reset the value of MERLIN_DEBUG even if the test fails
        if reset_merlin_debug:
            os.environ["MERLIN_DEBUG"] = debug_val
        raise AssertionError from exc

    # Reset the value of MERLIN_DEBUG
    if reset_merlin_debug:
        os.environ["MERLIN_DEBUG"] = debug_val


def test_default_config_info(mocker: MockerFixture):
    """
    Test the `default_config_info` function.

    Args:
        mocker (MockerFixture): Pytest mocker fixture for mocking functionality.
    """
    # Mock the necessary functions/variables
    config_file_path = "/mock/config/app.yaml"
    debug_mode = False
    merlin_home = "/mock/merlin_home/"
    find_config_file_mock = mocker.patch("merlin.config.configfile.find_config_file", return_value=config_file_path)
    is_debug_mock = mocker.patch("merlin.config.configfile.is_debug", return_value=debug_mode)
    mocker.patch("merlin.config.configfile.MERLIN_HOME", merlin_home)

    # Run the test and verify the output
    expected = {
        "config_file": config_file_path,
        "is_debug": debug_mode,
        "merlin_home": merlin_home,
        "merlin_home_exists": False,
    }
    actual = default_config_info()
    find_config_file_mock.assert_called_once()
    is_debug_mock.assert_called_once()
    assert actual == expected


def test_get_cert_file_all_valid_args(mysql_results_backend_config: "fixture", merlin_server_dir: str):  # noqa: F821
    """
    Test the `get_cert_file` function with all valid arguments.

    :param mysql_results_backend_config: A fixture to set the CONFIG object to a test configuration that we'll use here
    :param merlin_server_dir: The path to the temporary merlin server directory that's housing our cert files
    """
    expected = f"{merlin_server_dir}/{CERT_FILES['ssl_key']}"
    actual = get_cert_file(
        server_type="Results Backend", config=CONFIG.results_backend, cert_name="keyfile", cert_path=merlin_server_dir
    )
    assert actual == expected


def test_get_cert_file_invalid_cert_name(mysql_results_backend_config: "fixture", merlin_server_dir: str):  # noqa: F821
    """
    Test the `get_cert_file` function with an invalid cert_name argument. This should just return None.

    :param mysql_results_backend_config: A fixture to set the CONFIG object to a test configuration that we'll use here
    :param merlin_server_dir: The path to the temporary merlin server directory that's housing our cert files
    """
    actual = get_cert_file(
        server_type="Results Backend", config=CONFIG.results_backend, cert_name="invalid", cert_path=merlin_server_dir
    )
    assert actual is None


def test_get_cert_file_nonexistent_cert_path(
    mysql_results_backend_config: "fixture", temp_output_dir: str, merlin_server_dir: str  # noqa: F821
):
    """
    Test the `get_cert_file` function with cert_path argument that doesn't exist.
    This should still return the nonexistent path at the root of our temporary directory for testing.

    :param mysql_results_backend_config: A fixture to set the CONFIG object to a test configuration that we'll use here
    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
    :param merlin_server_dir: The path to the temporary merlin server directory that's housing our cert files
    """
    CONFIG.results_backend.certfile = "new_certfile.pem"
    expected = f"{temp_output_dir}/new_certfile.pem"
    actual = get_cert_file(
        server_type="Results Backend", config=CONFIG.results_backend, cert_name="certfile", cert_path=merlin_server_dir
    )
    assert actual == expected


def test_get_ssl_entries_required_certs(mysql_results_backend_config: "fixture", temp_output_dir: str):  # noqa: F821
    """
    Test the `get_ssl_entries` function with mysql as the results_backend. For this test we'll make
    cert reqs be required.

    :param mysql_results_backend_config: A fixture to set the CONFIG object to a test configuration that we'll use here
    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
    """
    CONFIG.results_backend.cert_reqs = "required"

    expected = {
        "ssl_key": f"{temp_output_dir}/{CERT_FILES['ssl_key']}",
        "ssl_cert": f"{temp_output_dir}/{CERT_FILES['ssl_cert']}",
        "ssl_ca": f"{temp_output_dir}/{CERT_FILES['ssl_ca']}",
        "cert_reqs": ssl.CERT_REQUIRED,
    }
    actual = get_ssl_entries(
        server_type="Results Backend", server_name="mysql", server_config=CONFIG.results_backend, cert_path=temp_output_dir
    )
    assert expected == actual


def test_get_ssl_entries_optional_certs(mysql_results_backend_config: "fixture", temp_output_dir: str):  # noqa: F821
    """
    Test the `get_ssl_entries` function with mysql as the results_backend. For this test we'll make
    cert reqs be optional.

    :param mysql_results_backend_config: A fixture to set the CONFIG object to a test configuration that we'll use here
    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
    """
    CONFIG.results_backend.cert_reqs = "optional"

    expected = {
        "ssl_key": f"{temp_output_dir}/{CERT_FILES['ssl_key']}",
        "ssl_cert": f"{temp_output_dir}/{CERT_FILES['ssl_cert']}",
        "ssl_ca": f"{temp_output_dir}/{CERT_FILES['ssl_ca']}",
        "cert_reqs": ssl.CERT_OPTIONAL,
    }
    actual = get_ssl_entries(
        server_type="Results Backend", server_name="mysql", server_config=CONFIG.results_backend, cert_path=temp_output_dir
    )
    assert expected == actual


def test_get_ssl_entries_none_certs(mysql_results_backend_config: "fixture", temp_output_dir: str):  # noqa: F821
    """
    Test the `get_ssl_entries` function with mysql as the results_backend. For this test we won't require
    any cert reqs.

    :param mysql_results_backend_config: A fixture to set the CONFIG object to a test configuration that we'll use here
    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
    """
    CONFIG.results_backend.cert_reqs = "none"

    expected = {
        "ssl_key": f"{temp_output_dir}/{CERT_FILES['ssl_key']}",
        "ssl_cert": f"{temp_output_dir}/{CERT_FILES['ssl_cert']}",
        "ssl_ca": f"{temp_output_dir}/{CERT_FILES['ssl_ca']}",
        "cert_reqs": ssl.CERT_NONE,
    }
    actual = get_ssl_entries(
        server_type="Results Backend", server_name="mysql", server_config=CONFIG.results_backend, cert_path=temp_output_dir
    )
    assert expected == actual


def test_get_ssl_entries_omit_certs(mysql_results_backend_config: "fixture", temp_output_dir: str):  # noqa: F821
    """
    Test the `get_ssl_entries` function with mysql as the results_backend. For this test we'll completely
    omit the cert_reqs option

    :param mysql_results_backend_config: A fixture to set the CONFIG object to a test configuration that we'll use here
    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
    """
    del CONFIG.results_backend.cert_reqs

    expected = {
        "ssl_key": f"{temp_output_dir}/{CERT_FILES['ssl_key']}",
        "ssl_cert": f"{temp_output_dir}/{CERT_FILES['ssl_cert']}",
        "ssl_ca": f"{temp_output_dir}/{CERT_FILES['ssl_ca']}",
        "cert_reqs": ssl.CERT_REQUIRED,
    }
    actual = get_ssl_entries(
        server_type="Results Backend", server_name="mysql", server_config=CONFIG.results_backend, cert_path=temp_output_dir
    )
    assert expected == actual


def test_get_ssl_entries_with_ssl_protocol(mysql_results_backend_config: "fixture", temp_output_dir: str):  # noqa: F821
    """
    Test the `get_ssl_entries` function with mysql as the results_backend. For this test we'll add in a
    dummy ssl_protocol value that should get added to the dict that's output.

    :param mysql_results_backend_config: A fixture to set the CONFIG object to a test configuration that we'll use here
    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
    """
    protocol = "test_protocol"
    CONFIG.results_backend.ssl_protocol = protocol

    expected = {
        "ssl_key": f"{temp_output_dir}/{CERT_FILES['ssl_key']}",
        "ssl_cert": f"{temp_output_dir}/{CERT_FILES['ssl_cert']}",
        "ssl_ca": f"{temp_output_dir}/{CERT_FILES['ssl_ca']}",
        "cert_reqs": ssl.CERT_NONE,
        "ssl_protocol": protocol,
    }
    actual = get_ssl_entries(
        server_type="Results Backend", server_name="mysql", server_config=CONFIG.results_backend, cert_path=temp_output_dir
    )
    assert expected == actual


def test_process_ssl_map_mysql():
    """Test the `process_ssl_map` function with mysql as the server name."""
    expected = {"keyfile": "ssl_key", "certfile": "ssl_cert", "ca_certs": "ssl_ca"}
    actual = process_ssl_map("mysql")
    assert actual == expected


def test_process_ssl_map_rediss():
    """Test the `process_ssl_map` function with rediss as the server name."""
    expected = {
        "keyfile": "ssl_keyfile",
        "certfile": "ssl_certfile",
        "ca_certs": "ssl_ca_certs",
        "cert_reqs": "ssl_cert_reqs",
    }
    actual = process_ssl_map("rediss")
    assert actual == expected


def test_merge_sslmap_all_keys_present():
    """
    Test the `merge_sslmap` function with all keys from server_ssl in ssl_map.
    We'll assume we're using a rediss server for this.
    """
    expected = {
        "ssl_keyfile": "/path/to/keyfile",
        "ssl_certfile": "/path/to/certfile",
        "ssl_ca_certs": "/path/to/ca_file",
        "ssl_cert_reqs": ssl.CERT_NONE,
    }
    test_server_ssl = {
        "keyfile": "/path/to/keyfile",
        "certfile": "/path/to/certfile",
        "ca_certs": "/path/to/ca_file",
        "cert_reqs": ssl.CERT_NONE,
    }
    test_ssl_map = {
        "keyfile": "ssl_keyfile",
        "certfile": "ssl_certfile",
        "ca_certs": "ssl_ca_certs",
        "cert_reqs": "ssl_cert_reqs",
    }
    actual = merge_sslmap(test_server_ssl, test_ssl_map)
    assert actual == expected


def test_merge_sslmap_some_keys_present():
    """
    Test the `merge_sslmap` function with some keys from server_ssl in ssl_map and others not.
    We'll assume we're using a rediss server for this.
    """
    expected = {
        "ssl_keyfile": "/path/to/keyfile",
        "ssl_certfile": "/path/to/certfile",
        "ssl_ca_certs": "/path/to/ca_file",
        "ssl_cert_reqs": ssl.CERT_NONE,
        "new_key": "new_val",
        "second_new_key": "second_new_val",
    }
    test_server_ssl = {
        "keyfile": "/path/to/keyfile",
        "certfile": "/path/to/certfile",
        "ca_certs": "/path/to/ca_file",
        "cert_reqs": ssl.CERT_NONE,
        "new_key": "new_val",
        "second_new_key": "second_new_val",
    }
    test_ssl_map = {
        "keyfile": "ssl_keyfile",
        "certfile": "ssl_certfile",
        "ca_certs": "ssl_ca_certs",
        "cert_reqs": "ssl_cert_reqs",
    }
    actual = merge_sslmap(test_server_ssl, test_ssl_map)
    assert actual == expected


@patch("merlin.config.configfile.Config")
@patch("merlin.config.configfile.get_config")
def test_initialize_config_default_parameters(mock_get_config: MagicMock, mock_config_class: MagicMock):
    """
    Test initialize_config with default parameters.

    Args:
        mock_get_config: A mocked `get_config` function.
        mock_config_class: A mocked `Config` object.
    """
    mock_app_config = {"test": "config"}
    mock_get_config.return_value = mock_app_config
    mock_config_instance = MagicMock()
    mock_config_class.return_value = mock_config_instance

    result = initialize_config()

    mock_get_config.assert_called_once_with(None)
    mock_config_class.assert_called_once_with(mock_app_config)
    assert result == mock_config_instance
    assert is_local_mode() is False


@patch("merlin.config.configfile.Config")
@patch("merlin.config.configfile.get_config")
def test_initialize_config_with_path(mock_get_config: MagicMock, mock_config_class: MagicMock):
    """
    Test initialize_config with custom path.

    Args:
        mock_get_config: A mocked `get_config` function.
        mock_config_class: A mocked `Config` object.
    """
    mock_app_config = {"test": "config"}
    mock_get_config.return_value = mock_app_config
    mock_config_instance = MagicMock()
    mock_config_class.return_value = mock_config_instance

    test_path = "/path/to/config"
    result = initialize_config(path=test_path)

    mock_get_config.assert_called_once_with(test_path)
    mock_config_class.assert_called_once_with(mock_app_config)
    assert result == mock_config_instance
    assert is_local_mode() is False


@patch("merlin.config.configfile.Config")
@patch("merlin.config.configfile.get_config")
def test_initialize_config_with_local_mode_true(mock_get_config: MagicMock, mock_config_class: MagicMock):
    """
    Test initialize_config with local_mode=True.

    Args:
        mock_get_config: A mocked `get_config` function.
        mock_config_class: A mocked `Config` object.
    """
    mock_app_config = {"test": "config"}
    mock_get_config.return_value = mock_app_config
    mock_config_instance = MagicMock()
    mock_config_class.return_value = mock_config_instance

    with patch("merlin.config.configfile.LOG"):
        result = initialize_config(local_mode=True)

    mock_get_config.assert_called_once_with(None)
    mock_config_class.assert_called_once_with(mock_app_config)
    assert result == mock_config_instance
    assert is_local_mode() is True


@patch("merlin.config.configfile.Config")
@patch("merlin.config.configfile.get_config")
def test_initialize_config_with_path_and_local_mode(mock_get_config: MagicMock, mock_config_class: MagicMock):
    """
    Test initialize_config with both path and local_mode.

    Args:
        mock_get_config: A mocked `get_config` function.
        mock_config_class: A mocked `Config` object.
    """
    mock_app_config = {"test": "config"}
    mock_get_config.return_value = mock_app_config
    mock_config_instance = MagicMock()
    mock_config_class.return_value = mock_config_instance

    test_path = "/custom/path"
    with patch("merlin.config.configfile.LOG"):
        result = initialize_config(path=test_path, local_mode=True)

    mock_get_config.assert_called_once_with(test_path)
    mock_config_class.assert_called_once_with(mock_app_config)
    assert result == mock_config_instance
    assert is_local_mode() is True


@patch("merlin.config.configfile.Config")
@patch("merlin.config.configfile.get_config")
def test_initialize_config_with_local_mode_false(mock_get_config: MagicMock, mock_config_class: MagicMock):
    """
    Test initialize_config with explicit local_mode=False.

    Args:
        mock_get_config: A mocked `get_config` function.
        mock_config_class: A mocked `Config` object.
    """
    mock_app_config = {"test": "config"}
    mock_get_config.return_value = mock_app_config
    mock_config_instance = MagicMock()
    mock_config_class.return_value = mock_config_instance

    result = initialize_config(local_mode=False)

    mock_get_config.assert_called_once_with(None)
    mock_config_class.assert_called_once_with(mock_app_config)
    assert result == mock_config_instance
    assert is_local_mode() is False
