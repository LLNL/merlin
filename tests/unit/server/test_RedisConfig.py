##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the RedisConfig class of the `server_util.py` module.

This class is especially large so that's why these tests have been
moved to their own file.
"""

import filecmp
import logging
from typing import Any

import pytest

from merlin.server.server_util import RedisConfig


class TestRedisConfig:
    """Tests for the RedisConfig class."""

    def test_initialization(self, server_redis_conf_file: str):
        """
        Using a dummy redis configuration file, test that the initialization
        of the RedisConfig class behaves as expected.

        :param server_redis_conf_file: The path to a dummy redis configuration file
        """
        expected_entries = {
            "bind": "127.0.0.1",
            "port": "6379",
            "requirepass": "merlin_password",
            "dir": "./",
            "save": "300 100",
            "dbfilename": "dump.rdb",
            "appendfsync": "everysec",
            "appendfilename": "appendonly.aof",
        }
        expected_comments = {
            "bind": "# ip address\n",
            "port": "\n# port\n",
            "requirepass": "\n# password\n",
            "dir": "\n# directory\n",
            "save": "\n# snapshot\n",
            "dbfilename": "\n# db file\n",
            "appendfsync": "\n# append mode\n",
            "appendfilename": "\n# append file\n",
        }
        expected_trailing_comment = "\n# dummy trailing comment"
        expected_entry_order = list(expected_entries.keys())
        redis_config = RedisConfig(server_redis_conf_file)
        assert redis_config.filename == server_redis_conf_file
        assert not redis_config.changed
        assert redis_config.entries == expected_entries
        assert redis_config.entry_order == expected_entry_order
        assert redis_config.comments == expected_comments
        assert redis_config.trailing_comments == expected_trailing_comment

    def test_write(self, server_redis_conf_file: str, server_testing_dir: str):
        """
        Test that the write functionality works by writing the contents of a dummy
        configuration file to a blank configuration file.

        :param server_redis_conf_file: The path to a dummy redis configuration file
        :param server_testing_dir: The path to the the temp output directory for server tests
        """
        copy_redis_conf_file = f"{server_testing_dir}/redis_copy.conf"

        # Create a RedisConf object with the basic redis conf file
        redis_config = RedisConfig(server_redis_conf_file)

        # Change the filepath of the redis config file to be the copy that we'll write to
        redis_config.set_filename(copy_redis_conf_file)

        # Run the test
        redis_config.write()

        # Check that the contents of the copied file match the contents of the basic file
        assert filecmp.cmp(server_redis_conf_file, copy_redis_conf_file)

    @pytest.mark.parametrize("key, val, expected_return", [("port", 1234, True), ("invalid_key", "dummy_val", False)])
    def test_set_config_value(self, server_redis_conf_file: str, key: str, val: Any, expected_return: bool):
        """
        Test the `set_config_value` method with valid and invalid keys.

        :param server_redis_conf_file: The path to a dummy redis configuration file
        :param key: The key value to modify with `set_config_value`
        :param val: The value to set `key` to
        :param expected_return: The expected return from `set_config_value`
        """
        redis_config = RedisConfig(server_redis_conf_file)
        actual_return = redis_config.set_config_value(key, val)
        assert actual_return == expected_return
        if expected_return:
            assert redis_config.entries[key] == val
            assert redis_config.changes_made()
        else:
            assert not redis_config.changes_made()

    @pytest.mark.parametrize(
        "key, expected_val",
        [
            ("bind", "127.0.0.1"),
            ("port", "6379"),
            ("requirepass", "merlin_password"),
            ("dir", "./"),
            ("save", "300 100"),
            ("dbfilename", "dump.rdb"),
            ("appendfsync", "everysec"),
            ("appendfilename", "appendonly.aof"),
            ("invalid_key", None),
        ],
    )
    def test_get_config_value(self, server_redis_conf_file: str, key: str, expected_val: str):
        """
        Test the `get_config_value` method with valid and invalid keys.

        :param server_redis_conf_file: The path to a dummy redis configuration file
        :param key: The key value to modify with `set_config_value`
        :param expected_val: The value we're expecting to get by querying `key`
        """
        redis_conf = RedisConfig(server_redis_conf_file)
        assert redis_conf.get_config_value(key) == expected_val

    @pytest.mark.parametrize(
        "ip_to_set",
        [
            "127.0.0.1",  # Most common IP
            "0.0.0.0",  # Edge case (low)
            "255.255.255.255",  # Edge case (high)
            "123.222.199.20",  # Random valid IP
        ],
    )
    def test_set_ip_address_valid(self, caplog: "Fixture", server_redis_conf_file: str, ip_to_set: str):  # noqa: F821
        """
        Test the `set_ip_address` method with valid ips. These should all return True
        and set the 'bind' value to whatever `ip_to_set` is.

        :param caplog: A built-in fixture from the pytest library to capture logs
        :param server_redis_conf_file: The path to a dummy redis configuration file
        :param ip_to_set: The ip address to set
        """
        caplog.set_level(logging.INFO)
        redis_config = RedisConfig(server_redis_conf_file)
        assert redis_config.set_ip_address(ip_to_set)
        assert f"Ipaddress is set to {ip_to_set}" in caplog.text, "Missing expected log message"
        assert redis_config.get_ip_address() == ip_to_set

    @pytest.mark.parametrize(
        "ip_to_set, expected_log",
        [
            (None, None),  # No IP
            ("0.0.0", "Invalid IPv4 address given."),  # Invalid IPv4
            ("bind-unset", "Unable to set ip address for redis config"),  # Special invalid case where bind doesn't exist
        ],
    )
    def test_set_ip_address_invalid(
        self,
        caplog: "Fixture",  # noqa: F821
        server_redis_conf_file: str,
        ip_to_set: str,
        expected_log: str,
    ):
        """
        Test the `set_ip_address` method with invalid ips. These should all return False.
        and not modify the 'bind' setting.

        :param caplog: A built-in fixture from the pytest library to capture logs
        :param server_redis_conf_file: The path to a dummy redis configuration file
        :param ip_to_set: The ip address to set
        :param expected_log: The string we're expecting the logger to log
        """
        redis_config = RedisConfig(server_redis_conf_file)
        # For the test where bind is unset, delete bind from dict and set new ip val to a valid value
        if ip_to_set == "bind-unset":
            del redis_config.entries["bind"]
            ip_to_set = "127.0.0.1"
        assert not redis_config.set_ip_address(ip_to_set)
        assert redis_config.get_ip_address() != ip_to_set
        if expected_log is not None:
            assert expected_log in caplog.text, "Missing expected log message"

    @pytest.mark.parametrize(
        "port_to_set",
        [
            6379,  # Most common port
            1,  # Edge case (low)
            65535,  # Edge case (high)
            12345,  # Random valid port
        ],
    )
    def test_set_port_valid(
        self,
        caplog: "Fixture",  # noqa: F821
        server_redis_conf_file: str,
        port_to_set: str,
    ):
        """
        Test the `set_port` method with valid ports. These should all return True
        and set the 'port' value to whatever `port_to_set` is.

        :param caplog: A built-in fixture from the pytest library to capture logs
        :param server_redis_conf_file: The path to a dummy redis configuration file
        :param port_to_set: The port to set
        """
        caplog.set_level(logging.INFO)
        redis_config = RedisConfig(server_redis_conf_file)
        assert redis_config.set_port(port_to_set)
        assert redis_config.get_port() == port_to_set
        assert f"Port is set to {port_to_set}" in caplog.text, "Missing expected log message"

    @pytest.mark.parametrize(
        "port_to_set, expected_log",
        [
            (None, None),  # No port
            (0, "Invalid port given."),  # Edge case (low)
            (65536, "Invalid port given."),  # Edge case (high)
            ("port-unset", "Unable to set port for redis config"),  # Special invalid case where port doesn't exist
        ],
    )
    def test_set_port_invalid(
        self,
        caplog: "Fixture",  # noqa: F821
        server_redis_conf_file: str,
        port_to_set: str,
        expected_log: str,
    ):
        """
        Test the `set_port` method with invalid inputs. These should all return False
        and not modify the 'port' setting.

        :param caplog: A built-in fixture from the pytest library to capture logs
        :param server_redis_conf_file: The path to a dummy redis configuration file
        :param port_to_set: The port to set
        :param expected_log: The string we're expecting the logger to log
        """
        redis_config = RedisConfig(server_redis_conf_file)
        # For the test where port is unset, delete port from dict and set port val to a valid value
        if port_to_set == "port-unset":
            del redis_config.entries["port"]
            port_to_set = 5
        assert not redis_config.set_port(port_to_set)
        assert redis_config.get_port() != port_to_set
        if expected_log is not None:
            assert expected_log in caplog.text, "Missing expected log message"

    @pytest.mark.parametrize(
        "pass_to_set, expected_return",
        [
            ("valid_password", True),  # Valid password
            (None, False),  # Invalid password
        ],
    )
    def test_set_password(
        self,
        caplog: "Fixture",  # noqa: F821
        server_redis_conf_file: str,
        pass_to_set: str,
        expected_return: bool,
    ):
        """
        Test the `set_password` method with both valid and invalid input.

        :param caplog: A built-in fixture from the pytest library to capture logs
        :param server_redis_conf_file: The path to a dummy redis configuration file
        :param pass_to_set: The password to set
        :param expected_return: The expected return value
        """
        caplog.set_level(logging.INFO)
        redis_conf = RedisConfig(server_redis_conf_file)
        assert redis_conf.set_password(pass_to_set) == expected_return
        if expected_return:
            assert redis_conf.get_password() == pass_to_set
            assert "New password set" in caplog.text, "Missing expected log message"

    def test_set_directory_valid(
        self,
        caplog: "Fixture",  # noqa: F821
        server_redis_conf_file: str,
        server_testing_dir: str,
    ):
        """
        Test the `set_directory` method with valid input. This should return True, modify the
        'dir' value, and log some messages about creating/setting the directory.

        :param caplog: A built-in fixture from the pytest library to capture logs
        :param server_redis_conf_file: The path to a dummy redis configuration file
        :param server_testing_dir: The path to the the temp output directory for server tests
        """
        caplog.set_level(logging.INFO)
        redis_config = RedisConfig(server_redis_conf_file)
        dir_to_set = f"{server_testing_dir}/dummy_dir"
        assert redis_config.set_directory(dir_to_set)
        assert redis_config.get_config_value("dir") == dir_to_set
        assert f"Created directory {dir_to_set}" in caplog.text, "Missing created log message"
        assert f"Directory is set to {dir_to_set}" in caplog.text, "Missing set log message"

    def test_set_directory_none(self, server_redis_conf_file: str):
        """
        Test the `set_directory` method with None as the input. This should return False
        and not modify the 'dir' setting.

        :param server_redis_conf_file: The path to a dummy redis configuration file
        """
        redis_config = RedisConfig(server_redis_conf_file)
        assert not redis_config.set_directory(None)
        assert redis_config.get_config_value("dir") is not None

    def test_set_directory_dir_unset(
        self,
        caplog: "Fixture",  # noqa: F821
        server_redis_conf_file: str,
        server_testing_dir: str,
    ):
        """
        Test the `set_directory` method with the 'dir' setting not existing. This should
        return False and log an error message.

        :param caplog: A built-in fixture from the pytest library to capture logs
        :param server_redis_conf_file: The path to a dummy redis configuration file
        :param server_testing_dir: The path to the the temp output directory for server tests
        """
        redis_config = RedisConfig(server_redis_conf_file)
        del redis_config.entries["dir"]
        dir_to_set = f"{server_testing_dir}/dummy_dir"
        assert not redis_config.set_directory(dir_to_set)
        assert "Unable to set directory for redis config" in caplog.text, "Missing expected log message"

    def test_set_snapshot_valid(self, caplog: "Fixture", server_redis_conf_file: str):  # noqa: F821
        """
        Test the `set_snapshot` method with a valid input for 'seconds' and 'changes'.
        This should return True and modify both values of 'save'.

        :param caplog: A built-in fixture from the pytest library to capture logs
        :param server_redis_conf_file: The path to a dummy redis configuration file
        """
        caplog.set_level(logging.INFO)
        redis_conf = RedisConfig(server_redis_conf_file)
        snap_sec_to_set = 20
        snap_changes_to_set = 30
        assert redis_conf.set_snapshot(seconds=snap_sec_to_set, changes=snap_changes_to_set)
        save_val = redis_conf.get_config_value("save").split()
        assert save_val[0] == str(snap_sec_to_set)
        assert save_val[1] == str(snap_changes_to_set)
        expected_log = (
            f"Snapshot wait time is set to {snap_sec_to_set} seconds. "
            f"Snapshot threshold is set to {snap_changes_to_set} changes"
        )
        assert expected_log in caplog.text, "Missing expected log message"

    def test_set_snapshot_just_seconds(self, caplog: "Fixture", server_redis_conf_file: str):  # noqa: F821
        """
        Test the `set_snapshot` method with a valid input for 'seconds'. This should
        return True and modify the first value of 'save'.

        :param caplog: A built-in fixture from the pytest library to capture logs
        :param server_redis_conf_file: The path to a dummy redis configuration file
        """
        caplog.set_level(logging.INFO)
        redis_conf = RedisConfig(server_redis_conf_file)
        orig_save = redis_conf.get_config_value("save").split()
        snap_sec_to_set = 20
        assert redis_conf.set_snapshot(seconds=snap_sec_to_set)
        save_val = redis_conf.get_config_value("save").split()
        assert save_val[0] == str(snap_sec_to_set)
        assert save_val[1] == orig_save[1]
        expected_log = f"Snapshot wait time is set to {snap_sec_to_set} seconds. "
        assert expected_log in caplog.text, "Missing expected log message"

    def test_set_snapshot_just_changes(self, caplog: "Fixture", server_redis_conf_file: str):  # noqa: F821
        """
        Test the `set_snapshot` method with a valid input for 'changes'. This should
        return True and modify the second value of 'save'.

        :param caplog: A built-in fixture from the pytest library to capture logs
        :param server_redis_conf_file: The path to a dummy redis configuration file
        """
        caplog.set_level(logging.INFO)
        redis_conf = RedisConfig(server_redis_conf_file)
        orig_save = redis_conf.get_config_value("save").split()
        snap_changes_to_set = 30
        assert redis_conf.set_snapshot(changes=snap_changes_to_set)
        save_val = redis_conf.get_config_value("save").split()
        assert save_val[0] == orig_save[0]
        assert save_val[1] == str(snap_changes_to_set)
        expected_log = f"Snapshot threshold is set to {snap_changes_to_set} changes"
        assert expected_log in caplog.text, "Missing expected log message"

    def test_set_snapshot_none(self, server_redis_conf_file: str):
        """
        Test the `set_snapshot` method with None as the input for both seconds
        and changes. This should return False.

        :param server_redis_conf_file: The path to a dummy redis configuration file
        """
        redis_conf = RedisConfig(server_redis_conf_file)
        assert not redis_conf.set_snapshot(seconds=None, changes=None)

    def test_set_snapshot_save_unset(self, caplog: "Fixture", server_redis_conf_file: str):  # noqa: F821
        """
        Test the `set_snapshot` method with the 'save' setting not existing. This should
        return False and log an error message.

        :param caplog: A built-in fixture from the pytest library to capture logs
        :param server_redis_conf_file: The path to a dummy redis configuration file
        """
        redis_conf = RedisConfig(server_redis_conf_file)
        del redis_conf.entries["save"]
        assert not redis_conf.set_snapshot(seconds=20)
        assert "Unable to get exisiting parameter values for snapshot" in caplog.text, "Missing expected log message"

    def test_set_snapshot_file_valid(self, caplog: "Fixture", server_redis_conf_file: str):  # noqa: F821
        """
        Test the `set_snapshot_file` method with a valid input. This should
        return True and modify the value of 'dbfilename'.

        :param caplog: A built-in fixture from the pytest library to capture logs
        :param server_redis_conf_file: The path to a dummy redis configuration file
        """
        caplog.set_level(logging.INFO)
        redis_conf = RedisConfig(server_redis_conf_file)
        filename = "dummy_file.rdb"
        assert redis_conf.set_snapshot_file(filename)
        assert redis_conf.get_config_value("dbfilename") == filename
        assert f"Snapshot file is set to {filename}" in caplog.text, "Missing expected log message"

    def test_set_snapshot_file_none(self, server_redis_conf_file: str):
        """
        Test the `set_snapshot_file` method with None as the input.
        This should return False.

        :param server_redis_conf_file: The path to a dummy redis configuration file
        """
        redis_conf = RedisConfig(server_redis_conf_file)
        assert not redis_conf.set_snapshot_file(None)

    def test_set_snapshot_file_dbfilename_unset(self, caplog: "Fixture", server_redis_conf_file: str):  # noqa: F821
        """
        Test the `set_snapshot` method with the 'dbfilename' setting not existing. This should
        return False and log an error message.

        :param caplog: A built-in fixture from the pytest library to capture logs
        :param server_redis_conf_file: The path to a dummy redis configuration file
        """
        redis_conf = RedisConfig(server_redis_conf_file)
        del redis_conf.entries["dbfilename"]
        filename = "dummy_file.rdb"
        assert not redis_conf.set_snapshot_file(filename)
        assert redis_conf.get_config_value("dbfilename") != filename
        assert "Unable to set snapshot_file name" in caplog.text, "Missing expected log message"

    @pytest.mark.parametrize(
        "mode_to_set",
        [
            "always",
            "everysec",
            "no",
        ],
    )
    def test_set_append_mode_valid(
        self,
        caplog: "Fixture",  # noqa: F821
        server_redis_conf_file: str,
        mode_to_set: str,
    ):
        """
        Test the `set_append_mode` method with valid modes. These should all return True
        and modify the value of 'appendfsync'.

        :param caplog: A built-in fixture from the pytest library to capture logs
        :param server_redis_conf_file: The path to a dummy redis configuration file
        :param mode_to_set: The mode to set
        """
        caplog.set_level(logging.INFO)
        redis_conf = RedisConfig(server_redis_conf_file)
        assert redis_conf.set_append_mode(mode_to_set)
        assert redis_conf.get_config_value("appendfsync") == mode_to_set
        assert f"Append mode is set to {mode_to_set}" in caplog.text, "Missing expected log message"

    def test_set_append_mode_invalid(self, caplog: "Fixture", server_redis_conf_file: str):  # noqa: F821
        """
        Test the `set_append_mode` method with an invalid mode. This should return False
        and log an error message.

        :param caplog: A built-in fixture from the pytest library to capture logs
        :param server_redis_conf_file: The path to a dummy redis configuration file
        """
        redis_conf = RedisConfig(server_redis_conf_file)
        invalid_mode = "invalid"
        assert not redis_conf.set_append_mode(invalid_mode)
        assert redis_conf.get_config_value("appendfsync") != invalid_mode
        expected_log = "Not a valid append_mode (Only valid modes are always, everysec, no)"
        assert expected_log in caplog.text, "Missing expected log message"

    def test_set_append_mode_none(self, server_redis_conf_file: str):
        """
        Test the `set_append_mode` method with None as the input.
        This should return False.

        :param server_redis_conf_file: The path to a dummy redis configuration file
        """
        redis_conf = RedisConfig(server_redis_conf_file)
        assert not redis_conf.set_append_mode(None)

    def test_set_append_mode_appendfsync_unset(self, caplog: "Fixture", server_redis_conf_file: str):  # noqa: F821
        """
        Test the `set_append_mode` method with the 'appendfsync' setting not existing. This should
        return False and log an error message.

        :param caplog: A built-in fixture from the pytest library to capture logs
        :param server_redis_conf_file: The path to a dummy redis configuration file
        """
        redis_conf = RedisConfig(server_redis_conf_file)
        del redis_conf.entries["appendfsync"]
        mode = "no"
        assert not redis_conf.set_append_mode(mode)
        assert redis_conf.get_config_value("appendfsync") != mode
        assert "Unable to set append_mode in redis config" in caplog.text, "Missing expected log message"

    def test_set_append_file_valid(self, caplog: "Fixture", server_redis_conf_file: str):  # noqa: F821
        """
        Test the `set_append_file` method with a valid file. This should return True
        and modify the value of 'appendfilename'.

        :param caplog: A built-in fixture from the pytest library to capture logs
        :param server_redis_conf_file: The path to a dummy redis configuration file
        """
        caplog.set_level(logging.INFO)
        redis_conf = RedisConfig(server_redis_conf_file)
        valid_file = "valid"
        assert redis_conf.set_append_file(valid_file)
        assert redis_conf.get_config_value("appendfilename") == f'"{valid_file}"'
        assert f"Append file is set to {valid_file}" in caplog.text, "Missing expected log message"

    def test_set_append_file_none(self, server_redis_conf_file: str):
        """
        Test the `set_append_file` method with None as the input.
        This should return False.

        :param server_redis_conf_file: The path to a dummy redis configuration file
        """
        redis_conf = RedisConfig(server_redis_conf_file)
        assert not redis_conf.set_append_file(None)

    def test_set_append_file_appendfilename_unset(self, caplog: "Fixture", server_redis_conf_file: str):  # noqa: F821
        """
        Test the `set_append_file` method with the 'appendfilename' setting not existing. This should
        return False and log an error message.

        :param caplog: A built-in fixture from the pytest library to capture logs
        :param server_redis_conf_file: The path to a dummy redis configuration file
        """
        redis_conf = RedisConfig(server_redis_conf_file)
        del redis_conf.entries["appendfilename"]
        filename = "valid_filename"
        assert not redis_conf.set_append_file(filename)
        assert redis_conf.get_config_value("appendfilename") != filename
        assert "Unable to set append filename." in caplog.text, "Missing expected log message"
