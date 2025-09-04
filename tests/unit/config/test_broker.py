##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `broker.py` file.
"""

import os
from ssl import CERT_NONE
from typing import Any, Dict

import pytest

from merlin.config.broker import (
    RABBITMQ_CONNECTION,
    REDISSOCK_CONNECTION,
    get_connection_string,
    get_rabbit_connection,
    get_redis_connection,
    get_redissock_connection,
    get_ssl_config,
)
from merlin.config.configfile import CONFIG
from tests.constants import SERVER_PASS


def test_get_connection_string_invalid_broker(redis_broker_config_function: "fixture"):  # noqa: F821
    """
    Test the `get_connection_string` function with an invalid broker (a broker that isn't one of:
    ["rabbitmq", "redis", "rediss", "redis+socket", "amqps", "amqp"]).

    :param redis_broker_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
    """
    CONFIG.broker.name = "invalid_broker"
    with pytest.raises(ValueError):
        get_connection_string()


def test_get_connection_string_no_broker(redis_broker_config_function: "fixture"):  # noqa: F821
    """
    Test the `get_connection_string` function without a broker name value in the CONFIG object. This
    should raise a ValueError just like the `test_get_connection_string_invalid_broker` does.

    :param redis_broker_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
    """
    del CONFIG.broker.name
    with pytest.raises(ValueError):
        get_connection_string()


def test_get_connection_string_simple(redis_broker_config_function: "fixture"):  # noqa: F821
    """
    Test the `get_connection_string` function in the simplest way that we can. This function
    will automatically check for a broker url and if it finds one in the CONFIG object it will just
    return the value it finds.

    :param redis_broker_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
    """
    test_url = "test_url"
    CONFIG.broker.url = test_url
    actual = get_connection_string()
    assert actual == test_url


def test_get_ssl_config_no_broker(redis_broker_config_function: "fixture"):  # noqa: F821
    """
    Test the `get_ssl_config` function without a broker. This should return False.

    :param redis_broker_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
    """
    del CONFIG.broker.name
    assert not get_ssl_config()


class TestRabbitBroker:
    """
    This class will house all tests necessary for our broker module when using a
    rabbit broker.
    """

    def run_get_rabbit_connection(self, expected_vals: Dict[str, Any], include_password: bool, conn: str):
        """
        Helper method to run the tests for the `get_rabbit_connection`.

        :param expected_vals: A dict of expected values for this test. Format:
            {"conn": "<connection>",
            "vhost": "host4testing",
            "username": "default",
            "password": "<password filepath>",
            "server": "127.0.0.1",
            "port": <port number>}
        :param include_password: If True, include the password in the output. Otherwise don't.
        :param conn: The connection type to pass in (either amqp or amqps)
        """
        expected = RABBITMQ_CONNECTION.format(**expected_vals)
        actual = get_rabbit_connection(include_password=include_password, conn=conn)
        assert actual == expected

    def test_get_rabbit_connection(self, rabbit_broker_config: "fixture"):  # noqa: F821
        """
        Test the `get_rabbit_connection` function.

        :param rabbit_broker_config: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        conn = "amqps"
        expected_vals = {
            "conn": conn,
            "vhost": "host4testing",
            "username": "default",
            "password": SERVER_PASS,
            "server": "127.0.0.1",
            "port": 5671,
        }
        self.run_get_rabbit_connection(expected_vals=expected_vals, include_password=True, conn=conn)

    def test_get_rabbit_connection_dont_include_password(self, rabbit_broker_config: "fixture"):  # noqa: F821
        """
        Test the `get_rabbit_connection` function but set include_password to False. This should * out the
        password

        :param rabbit_broker_config: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        conn = "amqps"
        expected_vals = {
            "conn": conn,
            "vhost": "host4testing",
            "username": "default",
            "password": "******",
            "server": "127.0.0.1",
            "port": 5671,
        }
        self.run_get_rabbit_connection(expected_vals=expected_vals, include_password=False, conn=conn)

    def test_get_rabbit_connection_no_port_amqp(self, rabbit_broker_config: "fixture"):  # noqa: F821
        """
        Test the `get_rabbit_connection` function with no port in the CONFIG object. This should use
        5672 as the port since we're using amqp as the connection.

        :param rabbit_broker_config: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        del CONFIG.broker.port
        CONFIG.broker.name = "amqp"
        conn = "amqp"
        expected_vals = {
            "conn": conn,
            "vhost": "host4testing",
            "username": "default",
            "password": SERVER_PASS,
            "server": "127.0.0.1",
            "port": 5672,
        }
        self.run_get_rabbit_connection(expected_vals=expected_vals, include_password=True, conn=conn)

    def test_get_rabbit_connection_no_port_amqps(self, rabbit_broker_config: "fixture"):  # noqa: F821
        """
        Test the `get_rabbit_connection` function with no port in the CONFIG object. This should use
        5671 as the port since we're using amqps as the connection.

        :param rabbit_broker_config: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        del CONFIG.broker.port
        conn = "amqps"
        expected_vals = {
            "conn": conn,
            "vhost": "host4testing",
            "username": "default",
            "password": SERVER_PASS,
            "server": "127.0.0.1",
            "port": 5671,
        }
        self.run_get_rabbit_connection(expected_vals=expected_vals, include_password=True, conn=conn)

    def test_get_rabbit_connection_no_password(self, rabbit_broker_config: "fixture"):  # noqa: F821
        """
        Test the `get_rabbit_connection` function with no password file set. This should raise a ValueError.

        :param rabbit_broker_config: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        del CONFIG.broker.password
        with pytest.raises(ValueError) as excinfo:
            get_rabbit_connection(True)
        assert "Broker: No password provided for RabbitMQ" in str(excinfo.value)

    def test_get_rabbit_connection_invalid_pass_filepath(
        self,
        rabbit_broker_config: "fixture",  # noqa: F821
    ):
        """
        Test the `get_rabbit_connection` function when the password filepath does not exist.

        The resulting connection string should include the literal password value.

        Args:
            rabbit_broker_config: Fixture that sets the CONFIG object to a test broker configuration.
            caplog: Pytest fixture to capture log output.
        """
        CONFIG.broker.password = "invalid_filepath"

        conn_str = get_rabbit_connection(True)

        # Verify that the password is used literally in the connection string
        assert "invalid_filepath" in conn_str
        # Sanity check for other expected parts of the connection string
        assert CONFIG.broker.username in conn_str
        assert CONFIG.broker.server in conn_str

    def run_get_connection_string(self, expected_vals: Dict[str, Any]):
        """
        Helper method to run the tests for the `get_connection_string`.

        :param expected_vals: A dict of expected values for this test. Format:
            {"conn": "<connection>",
            "vhost": "host4testing",
            "username": "default",
            "password": "<password filepath>",
            "server": "127.0.0.1",
            "port": <port number>}
        """
        expected = RABBITMQ_CONNECTION.format(**expected_vals)
        actual = get_connection_string()
        assert actual == expected

    def test_get_connection_string_rabbitmq(self, rabbit_broker_config: "fixture"):  # noqa: F821
        """
        Test the `get_connection_string` function with rabbitmq as the broker.

        :param rabbit_broker_config: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        expected_vals = {
            "conn": "amqps",
            "vhost": "host4testing",
            "username": "default",
            "password": SERVER_PASS,
            "server": "127.0.0.1",
            "port": 5671,
        }
        self.run_get_connection_string(expected_vals)

    def test_get_connection_string_amqp(self, rabbit_broker_config: "fixture"):  # noqa: F821
        """
        Test the `get_connection_string` function with amqp as the broker.

        :param rabbit_broker_config: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        del CONFIG.broker.port
        CONFIG.broker.name = "amqp"
        expected_vals = {
            "conn": "amqp",
            "vhost": "host4testing",
            "username": "default",
            "password": SERVER_PASS,
            "server": "127.0.0.1",
            "port": 5672,
        }
        self.run_get_connection_string(expected_vals)


class TestRedisBroker:
    """
    This class will house all tests necessary for our broker module when using a
    redis broker.
    """

    def run_get_redissock_connection(self, expected_vals: Dict[str, str]):
        """
        Helper method to run the tests for the `get_redissock_connection`.

        :param expected_vals: A dict of expected values for this test. Format:
            {"db_num": "<number>", "path": "<path>"}
        """
        expected = REDISSOCK_CONNECTION.format(**expected_vals)
        actual = get_redissock_connection()
        assert actual == expected

    def test_get_redissock_connection(self, redis_broker_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_redissock_connection` function with both a db_num and a broker path set.

        :param redis_broker_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        # Create and store a fake path and db_num for testing
        test_path = "/fake/path/to/broker"
        test_db_num = "45"
        CONFIG.broker.path = test_path
        CONFIG.broker.db_num = test_db_num

        # Set up our expected vals and compare against the actual result
        expected_vals = {"db_num": test_db_num, "path": test_path}
        self.run_get_redissock_connection(expected_vals)

    def test_get_redissock_connection_no_db(self, redis_broker_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_redissock_connection` function with a broker path set but no db num.
        This should default the db_num to 0.

        :param redis_broker_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        # Create and store a fake path for testing
        test_path = "/fake/path/to/broker"
        CONFIG.broker.path = test_path

        # Set up our expected vals and compare against the actual result
        expected_vals = {"db_num": 0, "path": test_path}
        self.run_get_redissock_connection(expected_vals)

    def test_get_redissock_connection_no_path(self, redis_broker_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_redissock_connection` function with a db num set but no broker path.
        This should raise an AttributeError since there will be no path value to read from
        in `CONFIG.broker`.

        :param redis_broker_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        CONFIG.broker.db_num = "45"
        with pytest.raises(AttributeError):
            get_redissock_connection()

    def test_get_redissock_connection_no_path_nor_db(self, redis_broker_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_redissock_connection` function with neither a broker path nor a db num set.
        This should raise an AttributeError since there will be no path value to read from
        in `CONFIG.broker`.

        :param redis_broker_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        with pytest.raises(AttributeError):
            get_redissock_connection()

    def run_get_redis_connection(self, expected_vals: Dict[str, Any], include_password: bool, use_ssl: bool):
        """
        Helper method to run the tests for the `get_redis_connection`.

        :param expected_vals: A dict of expected values for this test. Format:
             {"urlbase": "<redis or rediss>", "spass": "<spass val>", "server": "127.0.0.1", "port": <port num>, "db_num": <db num>}
        :param include_password: If True, include the password in the output. Otherwise don't.
        :param use_ssl: If True, use ssl for the connection. Otherwise don't.
        """
        expected = "{urlbase}://{spass}{server}:{port}/{db_num}".format(**expected_vals)
        actual = get_redis_connection(include_password=include_password, use_ssl=use_ssl)
        assert expected == actual

    def test_get_redis_connection(self, redis_broker_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_redis_connection` function with default functionality (including password and not using ssl).

        :param redis_broker_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        expected_vals = {
            "urlbase": "redis",
            "spass": "default:merlin-test-server@",
            "server": "127.0.0.1",
            "port": 6379,
            "db_num": 0,
        }
        self.run_get_redis_connection(expected_vals=expected_vals, include_password=True, use_ssl=False)

    def test_get_redis_connection_no_port(self, redis_broker_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_redis_connection` function with default functionality (including password and not using ssl).
        We'll run this after deleting the port setting from the CONFIG object. This should still run and give us
        port = 6379.

        :param redis_broker_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        del CONFIG.broker.port
        expected_vals = {
            "urlbase": "redis",
            "spass": "default:merlin-test-server@",
            "server": "127.0.0.1",
            "port": 6379,
            "db_num": 0,
        }
        self.run_get_redis_connection(expected_vals=expected_vals, include_password=True, use_ssl=False)

    def test_get_redis_connection_with_db(self, redis_broker_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_redis_connection` function with default functionality (including password and not using ssl).
        We'll run this after adding the db_num setting to the CONFIG object.

        :param redis_broker_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        test_db_num = "45"
        CONFIG.broker.db_num = test_db_num
        expected_vals = {
            "urlbase": "redis",
            "spass": "default:merlin-test-server@",
            "server": "127.0.0.1",
            "port": 6379,
            "db_num": test_db_num,
        }
        self.run_get_redis_connection(expected_vals=expected_vals, include_password=True, use_ssl=False)

    def test_get_redis_connection_no_username(self, redis_broker_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_redis_connection` function with default functionality (including password and not using ssl).
        We'll run this after deleting the username setting from the CONFIG object. This should still run and give us
        username = ''.

        :param redis_broker_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        del CONFIG.broker.username
        expected_vals = {"urlbase": "redis", "spass": ":merlin-test-server@", "server": "127.0.0.1", "port": 6379, "db_num": 0}
        self.run_get_redis_connection(expected_vals=expected_vals, include_password=True, use_ssl=False)

    def test_get_redis_connection_invalid_pass_file(self, redis_broker_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_redis_connection` function with default functionality (including password and not using ssl).
        We'll run this after changing the permissions of the password file so it can't be opened. This should raise
        a ValueError.

        Args:
            redis_broker_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        # Capture the initial permissions of the password file so we can reset them
        orig_file_permissions = os.stat(CONFIG.broker.password).st_mode

        # Change the permissions of the password file so it can't be read
        os.chmod(CONFIG.broker.password, 0o222)

        try:
            with pytest.raises(ValueError, match="could not be read"):
                get_redis_connection(include_password=True, use_ssl=True)
        finally:
            os.chmod(CONFIG.broker.password, orig_file_permissions)

    def test_get_redis_connection_dont_include_password(self, redis_broker_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_redis_connection` function without including the password. This should place 6 *s
        where the password would normally be placed in spass.

        :param redis_broker_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        expected_vals = {"urlbase": "redis", "spass": "default:******@", "server": "127.0.0.1", "port": 6379, "db_num": 0}
        self.run_get_redis_connection(expected_vals=expected_vals, include_password=False, use_ssl=False)

    def test_get_redis_connection_use_ssl(self, redis_broker_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_redis_connection` function with using ssl. This should change the urlbase to rediss (with two 's').

        :param redis_broker_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        expected_vals = {
            "urlbase": "rediss",
            "spass": "default:merlin-test-server@",
            "server": "127.0.0.1",
            "port": 6379,
            "db_num": 0,
        }
        self.run_get_redis_connection(expected_vals=expected_vals, include_password=True, use_ssl=True)

    def test_get_redis_connection_no_password(self, redis_broker_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_redis_connection` function with default functionality (including password and not using ssl).
        We'll run this after deleting the password setting from the CONFIG object. This should still run and give us
        spass = ''.

        :param redis_broker_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        del CONFIG.broker.password
        expected_vals = {"urlbase": "redis", "spass": "", "server": "127.0.0.1", "port": 6379, "db_num": 0}
        self.run_get_redis_connection(expected_vals=expected_vals, include_password=True, use_ssl=False)

    def test_get_connection_string_redis(self, redis_broker_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_connection_string` function with redis as the broker (this is what our CONFIG
        is set to by default with the redis_broker_config_function fixture).

        :param redis_broker_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        expected_vals = {
            "urlbase": "redis",
            "spass": "default:merlin-test-server@",
            "server": "127.0.0.1",
            "port": 6379,
            "db_num": 0,
        }
        expected = "{urlbase}://{spass}{server}:{port}/{db_num}".format(**expected_vals)
        actual = get_connection_string()
        assert expected == actual

    def test_get_connection_string_rediss(self, redis_broker_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_connection_string` function with rediss (with two 's') as the broker.

        :param redis_broker_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        CONFIG.broker.name = "rediss"
        expected_vals = {
            "urlbase": "rediss",
            "spass": "default:merlin-test-server@",
            "server": "127.0.0.1",
            "port": 6379,
            "db_num": 0,
        }
        expected = "{urlbase}://{spass}{server}:{port}/{db_num}".format(**expected_vals)
        actual = get_connection_string()
        assert expected == actual

    def test_get_connection_string_redis_socket(self, redis_broker_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_connection_string` function with redis+socket as the broker.

        :param redis_broker_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        # Change our broker
        CONFIG.broker.name = "redis+socket"

        # Create and store a fake path and db_num for testing
        test_path = "/fake/path/to/broker"
        test_db_num = "45"
        CONFIG.broker.path = test_path
        CONFIG.broker.db_num = test_db_num

        # Set up our expected vals and compare against the actual result
        expected_vals = {"db_num": test_db_num, "path": test_path}
        expected = REDISSOCK_CONNECTION.format(**expected_vals)
        actual = get_connection_string()
        assert actual == expected

    def test_get_ssl_config_redis(self, redis_broker_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_ssl_config` function with redis as the broker (this is the default in our tests).
        This should return False.

        :param redis_broker_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        assert not get_ssl_config()

    def test_get_ssl_config_rediss(self, redis_broker_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_ssl_config` function with rediss (with two 's') as the broker.
        This should return a dict of cert reqs with ssl.CERT_NONE as the value.

        :param redis_broker_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        CONFIG.broker.name = "rediss"
        expected = {"ssl_cert_reqs": CERT_NONE}
        actual = get_ssl_config()
        assert actual == expected
