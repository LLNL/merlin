"""
Tests for the configfile.py module.
"""
import getpass
import os
import shutil
import ssl
from copy import copy, deepcopy

import pytest
import yaml

from merlin.config.configfile import (
    CONFIG,
    default_config_info,
    find_config_file,
    get_cert_file,
    get_config,
    get_ssl_entries,
    is_debug,
    load_config,
    load_default_celery,
    load_default_user_names,
    load_defaults,
    merge_sslmap,
    process_ssl_map,
)
from tests.constants import CERT_FILES
from tests.utils import create_dir


CONFIGFILE_DIR = "{temp_output_dir}/test_configfile"
COPIED_APP_FILENAME = "app_copy.yaml"
DUMMY_APP_FILEPATH = f"{os.path.dirname(__file__)}/dummy_app.yaml"


def create_app_yaml(app_yaml_filepath: str):
    """
    Create a dummy app.yaml file at `app_yaml_filepath`.

    :param app_yaml_filepath: The location to create an app.yaml file at
    """
    full_app_yaml_filepath = f"{app_yaml_filepath}/app.yaml"
    if not os.path.exists(full_app_yaml_filepath):
        shutil.copy(DUMMY_APP_FILEPATH, full_app_yaml_filepath)


def test_load_config(temp_output_dir: str):
    """
    Test the `load_config` function.

    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
    """
    configfile_dir = CONFIGFILE_DIR.format(temp_output_dir=temp_output_dir)
    create_dir(configfile_dir)
    create_app_yaml(configfile_dir)

    with open(DUMMY_APP_FILEPATH, "r") as dummy_app_file:
        expected = yaml.load(dummy_app_file, yaml.Loader)

    actual = load_config(f"{configfile_dir}/app.yaml")
    assert actual == expected


def test_load_config_invalid_file():
    """
    Test the `load_config` function with an invalid filepath.
    """
    assert load_config("invalid/filepath") is None


def test_find_config_file_valid_path(temp_output_dir: str):
    """
    Test the `find_config_file` function with passing a valid path in.

    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
    """
    configfile_dir = CONFIGFILE_DIR.format(temp_output_dir=temp_output_dir)
    create_dir(configfile_dir)
    create_app_yaml(configfile_dir)

    assert find_config_file(configfile_dir) == f"{configfile_dir}/app.yaml"


def test_find_config_file_invalid_path():
    """
    Test the `find_config_file` function with passing an invalid path in.
    """
    assert find_config_file("invalid/path") is None


def test_find_config_file_local_path(temp_output_dir: str):
    """
    Test the `find_config_file` function by having it find a local (in our cwd) app.yaml file.
    We'll use the `temp_output_dir` fixture so that our current working directory is in a temp
    location.

    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
    """

    # Create the configfile directory and put an app.yaml file there
    configfile_dir = CONFIGFILE_DIR.format(temp_output_dir=temp_output_dir)
    create_dir(configfile_dir)
    create_app_yaml(configfile_dir)

    # Move into the configfile directory and run the test
    os.chdir(configfile_dir)
    try:
        assert find_config_file() == f"{os.getcwd()}/app.yaml"
    except AssertionError as exc:
        # Move back to the temp output directory even if the test fails
        os.chdir(temp_output_dir)
        raise AssertionError from exc

    # Move back to the temp output directory
    os.chdir(temp_output_dir)


def test_find_config_file_merlin_home_path(temp_output_dir: str):
    """
    Test the `find_config_file` function by having it find an app.yaml file in our merlin directory.
    We'll use the `temp_output_dir` fixture so that our current working directory is in a temp
    location.

    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
    """
    merlin_home = os.path.expanduser("~/.merlin")
    if not os.path.exists(merlin_home):
        os.mkdir(merlin_home)
    create_app_yaml(merlin_home)
    assert find_config_file() == f"{merlin_home}/app.yaml"


def check_for_and_move_app_yaml(dir_to_check: str) -> bool:
    """
    Check for any app.yaml files in `dir_to_check`. If one is found, rename it.
    Return True if an app.yaml was found, false otherwise.

    :param dir_to_check: The directory to search for an app.yaml in
    :returns: True if an app.yaml was found. False otherwise.
    """
    for filename in os.listdir(dir_to_check):
        full_path = os.path.join(dir_to_check, filename)
        if os.path.isfile(full_path) and filename == "app.yaml":
            os.rename(full_path, f"{dir_to_check}/{COPIED_APP_FILENAME}")
            return True
    return False


def test_find_config_file_no_path(temp_output_dir: str):
    """
    Test the `find_config_file` function by making it unable to find any app.yaml path.
    We'll use the `temp_output_dir` fixture so that our current working directory is in a temp
    location.

    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
    """

    # Rename any app.yaml in the cwd
    cwd_path = os.getcwd()
    cwd_had_app_yaml = check_for_and_move_app_yaml(cwd_path)

    # Rename any app.yaml in the merlin home directory
    merlin_home_dir = os.path.expanduser("~/.merlin")
    merlin_home_had_app_yaml = check_for_and_move_app_yaml(merlin_home_dir)

    try:
        assert find_config_file() is None
    except AssertionError as exc:
        # Reset the cwd app.yaml even if the test fails
        if cwd_had_app_yaml:
            os.rename(f"{cwd_path}/{COPIED_APP_FILENAME}", f"{cwd_path}/app.yaml")

        # Reset the merlin home app.yaml even if the test fails
        if merlin_home_had_app_yaml:
            os.rename(f"{merlin_home_dir}/{COPIED_APP_FILENAME}", f"{merlin_home_dir}/app.yaml")

        raise AssertionError from exc

    # Reset the cwd app.yaml
    if cwd_had_app_yaml:
        os.rename(f"{cwd_path}/{COPIED_APP_FILENAME}", f"{cwd_path}/app.yaml")

    # Reset the merlin home app.yaml
    if merlin_home_had_app_yaml:
        os.rename(f"{merlin_home_dir}/{COPIED_APP_FILENAME}", f"{merlin_home_dir}/app.yaml")


def test_load_default_user_names_nothing_to_load():
    """
    Test the `load_default_user_names` function with nothing to load. In other words, in this
    test the config dict will have a username and vhost already set for the broker. We'll
    create the dict then make a copy of it to test against after calling the function.
    """
    actual_config = {"broker": {"username": "default", "vhost": "host4testing"}}
    expected_config = deepcopy(actual_config)
    assert actual_config is not expected_config

    load_default_user_names(actual_config)

    # Ensure that nothing was modified after our call to load_default_user_names
    assert actual_config == expected_config


def test_load_default_user_names_no_username():
    """
    Test the `load_default_user_names` function with no username. In other words, in this
    test the config dict will have vhost already set for the broker but not a username.
    """
    expected_config = {"broker": {"username": getpass.getuser(), "vhost": "host4testing"}}
    actual_config = {"broker": {"vhost": "host4testing"}}
    load_default_user_names(actual_config)

    # Ensure that the username was set in the call to load_default_user_names
    assert actual_config == expected_config


def test_load_default_user_names_no_vhost():
    """
    Test the `load_default_user_names` function with no vhost. In other words, in this
    test the config dict will have username already set for the broker but not a vhost.
    """
    expected_config = {"broker": {"username": "default", "vhost": getpass.getuser()}}
    actual_config = {"broker": {"username": "default"}}
    load_default_user_names(actual_config)

    # Ensure that the vhost was set in the call to load_default_user_names
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


def test_get_config(temp_output_dir: str):
    """
    Test the `get_config` function.

    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
    """

    # Create the configfile directory and put an app.yaml file there
    configfile_dir = CONFIGFILE_DIR.format(temp_output_dir=temp_output_dir)
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


def test_default_config_info(temp_output_dir: str):
    """
    Test the `default_config_info` function.

    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
    """

    # Create the configfile directory and put an app.yaml file there
    configfile_dir = CONFIGFILE_DIR.format(temp_output_dir=temp_output_dir)
    create_dir(configfile_dir)
    create_app_yaml(configfile_dir)
    cwd = os.getcwd()
    os.chdir(configfile_dir)

    # Delete the current val of MERLIN_DEBUG and store it (if there is one)
    reset_merlin_debug = False
    debug_val = None
    if "MERLIN_DEBUG" in os.environ:
        debug_val = copy(os.environ["MERLIN_DEBUG"])
        del os.environ["MERLIN_DEBUG"]
        reset_merlin_debug = True

    # Create the merlin home directory if it doesn't already exist
    merlin_home = f"{os.path.expanduser('~')}/.merlin"
    remove_merlin_home = False
    if not os.path.exists(merlin_home):
        os.mkdir(merlin_home)
        remove_merlin_home = True

    # Run the test
    try:
        expected = {
            "config_file": f"{configfile_dir}/app.yaml",
            "is_debug": False,
            "merlin_home": merlin_home,
            "merlin_home_exists": True,
        }
        actual = default_config_info()
        assert actual == expected
    except AssertionError as exc:
        # Make sure to reset values even if the test fails
        if reset_merlin_debug:
            os.environ["MERLIN_DEBUG"] = debug_val
        if remove_merlin_home:
            os.rmdir(merlin_home)
        raise AssertionError from exc

    # Reset values if necessary
    if reset_merlin_debug:
        os.environ["MERLIN_DEBUG"] = debug_val
    if remove_merlin_home:
        os.rmdir(merlin_home)

    os.chdir(cwd)


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
