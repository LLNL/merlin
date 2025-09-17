##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `results_backend.py` file.
"""

import os
from ssl import CERT_NONE
from typing import Any, Dict

import pytest

from merlin.config.configfile import CONFIG
from merlin.config.results_backend import (
    MYSQL_CONFIG_FILENAMES,
    MYSQL_CONNECTION_STRING,
    SQLITE_CONNECTION_STRING,
    get_connection_string,
    get_mysql,
    get_mysql_config,
    get_redis,
    get_ssl_config,
)
from tests.constants import CERT_FILES, SERVER_PASS
from tests.utils import create_cert_files


def test_get_ssl_config_no_results_backend(config_function: "fixture"):  # noqa: F821
    """
    Test the `get_ssl_config` function with no results_backend set. This should return False.
    NOTE: we're using the config fixture here to make sure values are reset after this test finishes.
    We won't actually use anything from the config fixture.

    :param config: A fixture to set up the CONFIG object for us
    """
    del CONFIG.results_backend.name
    assert get_ssl_config() is False


def test_get_connection_string_no_results_backend(config_function: "fixture"):  # noqa: F821
    """
    Test the `get_connection_string` function with no results_backend set.
    This should raise a ValueError.
    NOTE: we're using the config fixture here to make sure values are reset after this test finishes.
    We won't actually use anything from the config fixture.

    :param config: A fixture to set up the CONFIG object for us
    """
    del CONFIG.results_backend.name
    with pytest.raises(ValueError) as excinfo:
        get_connection_string()

    assert "'' is not a supported results backend" in str(excinfo.value)


class TestRedisResultsBackend:
    """
    This class will house all tests necessary for our results_backend module when using a
    redis results_backend.
    """

    def run_get_redis(
        self,
        expected_vals: Dict[str, Any],
        certs_path: str = None,
        include_password: bool = True,
        ssl: bool = False,
    ):
        """
        Helper method for running tests for the `get_redis` function.

        :param expected_vals: A dict of expected values for this test. Format:
            {"urlbase": "redis",
            "spass": "<spass string>",
            "server": "127.0.0.1",
            "port": 6379,
            "db_num": 0}
        :param certs_path: A string denoting the path to the certification files
        :param include_password: If True, include the password in the output. Otherwise don't.
        :param ssl: If True, use ssl. Otherwise, don't.
        """
        expected = "{urlbase}://{spass}{server}:{port}/{db_num}".format(**expected_vals)
        actual = get_redis(certs_path=certs_path, include_password=include_password, ssl=ssl)
        assert actual == expected

    def test_get_redis(self, redis_results_backend_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_redis` function with default functionality.

        :param redis_results_backend_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        expected_vals = {
            "urlbase": "redis",
            "spass": f"default:{SERVER_PASS}@",
            "server": "127.0.0.1",
            "port": 6379,
            "db_num": 0,
        }
        self.run_get_redis(expected_vals=expected_vals, certs_path=None, include_password=True, ssl=False)

    def test_get_redis_dont_include_password(self, redis_results_backend_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_redis` function with the password hidden. This should * out the password.

        :param redis_results_backend_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        expected_vals = {
            "urlbase": "redis",
            "spass": "default:******@",
            "server": "127.0.0.1",
            "port": 6379,
            "db_num": 0,
        }
        self.run_get_redis(expected_vals=expected_vals, certs_path=None, include_password=False, ssl=False)

    def test_get_redis_using_ssl(self, redis_results_backend_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_redis` function with ssl enabled.

        :param redis_results_backend_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        expected_vals = {
            "urlbase": "rediss",
            "spass": f"default:{SERVER_PASS}@",
            "server": "127.0.0.1",
            "port": 6379,
            "db_num": 0,
        }
        self.run_get_redis(expected_vals=expected_vals, certs_path=None, include_password=True, ssl=True)

    def test_get_redis_no_port(self, redis_results_backend_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_redis` function with no port in our CONFIG object. This should default to port=6379.

        :param redis_results_backend_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        del CONFIG.results_backend.port
        expected_vals = {
            "urlbase": "redis",
            "spass": f"default:{SERVER_PASS}@",
            "server": "127.0.0.1",
            "port": 6379,
            "db_num": 0,
        }
        self.run_get_redis(expected_vals=expected_vals, certs_path=None, include_password=True, ssl=False)

    def test_get_redis_no_db_num(self, redis_results_backend_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_redis` function with no db_num in our CONFIG object. This should default to db_num=0.

        :param redis_results_backend_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        del CONFIG.results_backend.db_num
        expected_vals = {
            "urlbase": "redis",
            "spass": f"default:{SERVER_PASS}@",
            "server": "127.0.0.1",
            "port": 6379,
            "db_num": 0,
        }
        self.run_get_redis(expected_vals=expected_vals, certs_path=None, include_password=True, ssl=False)

    def test_get_redis_no_username(self, redis_results_backend_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_redis` function with no username in our CONFIG object. This should default to username=''.

        :param redis_results_backend_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        del CONFIG.results_backend.username
        expected_vals = {
            "urlbase": "redis",
            "spass": f":{SERVER_PASS}@",
            "server": "127.0.0.1",
            "port": 6379,
            "db_num": 0,
        }
        self.run_get_redis(expected_vals=expected_vals, certs_path=None, include_password=True, ssl=False)

    def test_get_redis_no_password_file(self, redis_results_backend_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_redis` function with no password filepath in our CONFIG object. This should default to spass=''.

        :param redis_results_backend_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        del CONFIG.results_backend.password
        expected_vals = {
            "urlbase": "redis",
            "spass": "",
            "server": "127.0.0.1",
            "port": 6379,
            "db_num": 0,
        }
        self.run_get_redis(expected_vals=expected_vals, certs_path=None, include_password=True, ssl=False)

    def test_get_redis_invalid_pass_file(self, redis_results_backend_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_redis` function. We'll run this after changing the permissions of the password file so it
        can't be opened. This should raise a ValueError

        Args:
            redis_results_backend_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """

        # Capture the initial permissions of the password file so we can reset them
        orig_file_permissions = os.stat(CONFIG.results_backend.password).st_mode

        # Change the permissions of the password file so it can't be read
        os.chmod(CONFIG.results_backend.password, 0o222)

        try:
            with pytest.raises(ValueError, match="could not be read"):
                get_redis(certs_path=None, include_password=True, ssl=False)
        finally:
            os.chmod(CONFIG.results_backend.password, orig_file_permissions)

    def test_get_ssl_config_redis(self, redis_results_backend_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_ssl_config` function with redis as the results_backend. This should return False since
        ssl requires using rediss (with two 's').

        :param redis_results_backend_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        assert get_ssl_config() is False

    def test_get_ssl_config_rediss(self, redis_results_backend_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_ssl_config` function with rediss as the results_backend.
        This should return a dict of cert reqs with ssl.CERT_NONE as the value.

        :param redis_results_backend_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        CONFIG.results_backend.name = "rediss"
        assert get_ssl_config() == {"ssl_cert_reqs": CERT_NONE}

    def test_get_ssl_config_rediss_no_cert_reqs(self, redis_results_backend_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_ssl_config` function with rediss as the results_backend and no cert_reqs set.
        This should return True.

        :param redis_results_backend_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        del CONFIG.results_backend.cert_reqs
        CONFIG.results_backend.name = "rediss"
        assert get_ssl_config() is True

    def test_get_connection_string_redis(self, redis_results_backend_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_connection_string` function with redis as the results_backend.

        :param redis_results_backend_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        expected_vals = {
            "urlbase": "redis",
            "spass": f"default:{SERVER_PASS}@",
            "server": "127.0.0.1",
            "port": 6379,
            "db_num": 0,
        }
        expected = "{urlbase}://{spass}{server}:{port}/{db_num}".format(**expected_vals)
        actual = get_connection_string()
        assert actual == expected

    def test_get_connection_string_rediss(self, redis_results_backend_config_function: "fixture"):  # noqa: F821
        """
        Test the `get_connection_string` function with rediss as the results_backend.

        :param redis_results_backend_config_function: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        CONFIG.results_backend.name = "rediss"
        expected_vals = {
            "urlbase": "rediss",
            "spass": f"default:{SERVER_PASS}@",
            "server": "127.0.0.1",
            "port": 6379,
            "db_num": 0,
        }
        expected = "{urlbase}://{spass}{server}:{port}/{db_num}".format(**expected_vals)
        actual = get_connection_string()
        assert actual == expected


class TestMySQLResultsBackend:
    """
    This class will house all tests necessary for our results_backend module when using a
    MySQL results_backend.
    NOTE: You'll notice a lot of these tests are setting CONFIG.results_backend.name to be
    "invalid". This is so that we can get by the first if statement in the `get_mysql_config`
    function.
    """

    def test_get_mysql_config_certs_set(self, mysql_results_backend_config: "fixture", merlin_server_dir: str):  # noqa: F821
        """
        Test the `get_mysql_config` function with the certs dict getting set and returned.

        :param mysql_results_backend_config: A fixture to set the CONFIG object to a test configuration that we'll use here
        :param merlin_server_dir: The directory that has the test certification files
        """
        CONFIG.results_backend.name = "invalid"
        expected = {}
        for key, cert_file in CERT_FILES.items():
            expected[key] = f"{merlin_server_dir}/{cert_file}"
        actual = get_mysql_config(merlin_server_dir, CERT_FILES)
        assert actual == expected

    def test_get_mysql_config_ssl_exists(self, mysql_results_backend_config: "fixture", temp_output_dir: str):  # noqa: F821
        """
        Test the `get_mysql_config` function with mysql_ssl being found. This should just return the ssl value that's found.

        :param mysql_results_backend_config: A fixture to set the CONFIG object to a test configuration that we'll use here
        :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
        """
        expected = {key: f"{temp_output_dir}/{cert_file}" for key, cert_file in CERT_FILES.items()}
        expected["cert_reqs"] = CERT_NONE
        assert get_mysql_config(None, None) == expected

    def test_get_mysql_config_no_mysql_certs(
        self, mysql_results_backend_config: "fixture", merlin_server_dir: str  # noqa: F821
    ):
        """
        Test the `get_mysql_config` function with no mysql certs dict.

        :param mysql_results_backend_config: A fixture to set the CONFIG object to a test configuration that we'll use here
        :param merlin_server_dir: The directory that has the test certification files
        """
        CONFIG.results_backend.name = "invalid"
        assert get_mysql_config(merlin_server_dir, {}) == {}

    def test_get_mysql_config_invalid_certs_path(self, mysql_results_backend_config: "fixture"):  # noqa: F821
        """
        Test the `get_mysql_config` function with an invalid certs path. This should return False.

        :param mysql_results_backend_config: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        CONFIG.results_backend.name = "invalid"
        assert get_mysql_config("invalid/path", CERT_FILES) is False

    def run_get_mysql(
        self, expected_vals: Dict[str, Any], certs_path: str, mysql_certs: Dict[str, str], include_password: bool
    ):
        """
        Helper method for running tests for the `get_mysql` function.

        :param expected_vals: A dict of expected values for this test. Format:
            {"cert_reqs": cert reqs dict,
            "user": "default",
            "password": "<password filepath>",
            "server": "127.0.0.1",
            "ssl_cert": "test-rabbit-client-cert.pem",
            "ssl_ca": "test-mysql-ca-cert.pem",
            "ssl_key": "test-rabbit-client-key.pem"}
        :param certs_path: A string denoting the path to the certification files
        :param mysql_certs: A dict of cert files
        :param include_password: If True, include the password in the output. Otherwise don't.
        """
        expected = MYSQL_CONNECTION_STRING.format(**expected_vals)
        actual = get_mysql(certs_path=certs_path, mysql_certs=mysql_certs, include_password=include_password)
        assert actual == expected

    def test_get_mysql(self, mysql_results_backend_config: "fixture", merlin_server_dir: str):  # noqa: F821
        """
        Test the `get_mysql` function with default behavior.

        :param mysql_results_backend_config: A fixture to set the CONFIG object to a test configuration that we'll use here
        :param merlin_server_dir: The directory that has the test certification files
        """
        CONFIG.results_backend.name = "invalid"
        expected_vals = {
            "cert_reqs": CERT_NONE,
            "user": "default",
            "password": SERVER_PASS,
            "server": "127.0.0.1",
        }
        for key, cert_file in CERT_FILES.items():
            expected_vals[key] = f"{merlin_server_dir}/{cert_file}"
        self.run_get_mysql(
            expected_vals=expected_vals, certs_path=merlin_server_dir, mysql_certs=CERT_FILES, include_password=True
        )

    def test_get_mysql_dont_include_password(
        self, mysql_results_backend_config: "fixture", merlin_server_dir: str  # noqa: F821
    ):
        """
        Test the `get_mysql` function but set include_password to False. This should * out the password.

        :param mysql_results_backend_config: A fixture to set the CONFIG object to a test configuration that we'll use here
        :param merlin_server_dir: The directory that has the test certification files
        """
        CONFIG.results_backend.name = "invalid"
        expected_vals = {
            "cert_reqs": CERT_NONE,
            "user": "default",
            "password": "******",
            "server": "127.0.0.1",
        }
        for key, cert_file in CERT_FILES.items():
            expected_vals[key] = f"{merlin_server_dir}/{cert_file}"
        self.run_get_mysql(
            expected_vals=expected_vals, certs_path=merlin_server_dir, mysql_certs=CERT_FILES, include_password=False
        )

    def test_get_mysql_no_mysql_certs(self, mysql_results_backend_config: "fixture", merlin_server_dir: str):  # noqa: F821
        """
        Test the `get_mysql` function with no mysql_certs passed in. This should use default config filenames so we'll
        have to create these default files.

        :param mysql_results_backend_config: A fixture to set the CONFIG object to a test configuration that we'll use here
        :param merlin_server_dir: The directory that has the test certification files
        """
        CONFIG.results_backend.name = "invalid"
        expected_vals = {
            "cert_reqs": CERT_NONE,
            "user": "default",
            "password": SERVER_PASS,
            "server": "127.0.0.1",
        }

        create_cert_files(merlin_server_dir, MYSQL_CONFIG_FILENAMES)

        for key, cert_file in MYSQL_CONFIG_FILENAMES.items():
            # Password file is already is already set in expected_vals dict
            if key == "password":
                continue
            expected_vals[key] = f"{merlin_server_dir}/{cert_file}"

        self.run_get_mysql(expected_vals=expected_vals, certs_path=merlin_server_dir, mysql_certs=None, include_password=True)

    def test_get_mysql_no_server(self, mysql_results_backend_config: "fixture"):  # noqa: F821
        """
        Test the `get_mysql` function with no server set. This should raise a TypeError.

        :param mysql_results_backend_config: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        CONFIG.results_backend.server = False
        with pytest.raises(TypeError) as excinfo:
            get_mysql()
        assert "Results backend: server False does not have a configuration" in str(excinfo.value)

    def test_get_mysql_invalid_certs_path(self, mysql_results_backend_config: "fixture"):  # noqa: F821
        """
        Test the `get_mysql` function with an invalid certs_path. This should raise a TypeError.

        :param mysql_results_backend_config: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        CONFIG.results_backend.name = "invalid"
        with pytest.raises(TypeError) as excinfo:
            get_mysql(certs_path="invalid_path", mysql_certs=CERT_FILES)
        err_msg = f"""The connection information for MySQL could not be set, cannot find:\n
        {CERT_FILES}\ncheck the celery/certs path or set the ssl information in the app.yaml file."""
        assert err_msg in str(excinfo.value)

    def test_get_ssl_config_mysql(self, mysql_results_backend_config: "fixture", temp_output_dir: str):  # noqa: F821
        """
        Test the `get_ssl_config` function with mysql as the results_backend.
        This should return a dict of cert reqs with ssl.CERT_NONE as the value.

        :param mysql_results_backend_config: A fixture to set the CONFIG object to a test configuration that we'll use here
        :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
        """
        expected = {key: f"{temp_output_dir}/{cert_file}" for key, cert_file in CERT_FILES.items()}
        expected["cert_reqs"] = CERT_NONE
        assert get_ssl_config() == expected

    def test_get_ssl_config_mysql_celery_check(self, mysql_results_backend_config: "fixture"):  # noqa: F821
        """
        Test the `get_ssl_config` function with mysql as the results_backend and celery_check set.
        This should return False.

        :param mysql_results_backend_config: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        assert get_ssl_config(celery_check=True) is False

    def test_get_connection_string_mysql(self, mysql_results_backend_config: "fixture", merlin_server_dir: str):  # noqa: F821
        """
        Test the `get_connection_string` function with MySQL as the results_backend.

        :param mysql_results_backend_config: A fixture to set the CONFIG object to a test configuration that we'll use here
        :param merlin_server_dir: The directory that has the test certification files
        """
        CONFIG.celery.certs = merlin_server_dir

        create_cert_files(merlin_server_dir, MYSQL_CONFIG_FILENAMES)
        CONFIG.results_backend.keyfile = MYSQL_CONFIG_FILENAMES["ssl_key"]
        CONFIG.results_backend.certfile = MYSQL_CONFIG_FILENAMES["ssl_cert"]
        CONFIG.results_backend.ca_certs = MYSQL_CONFIG_FILENAMES["ssl_ca"]

        expected_vals = {
            "cert_reqs": CERT_NONE,
            "user": "default",
            "password": SERVER_PASS,
            "server": "127.0.0.1",
        }
        for key, cert_file in MYSQL_CONFIG_FILENAMES.items():
            # Password file is already is already set in expected_vals dict
            if key == "password":
                continue
            expected_vals[key] = f"{merlin_server_dir}/{cert_file}"

        assert MYSQL_CONNECTION_STRING.format(**expected_vals) == get_connection_string(include_password=True)

    def test_get_connection_string_sqlite(self, mysql_results_backend_config: "fixture"):  # noqa: F821
        """
        Test the `get_connection_string` function with sqlite as the results_backend.

        :param mysql_results_backend_config: A fixture to set the CONFIG object to a test configuration that we'll use here
        """
        CONFIG.results_backend.name = "sqlite"
        assert get_connection_string() == SQLITE_CONNECTION_STRING
