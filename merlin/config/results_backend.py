##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module provides functionality for managing and configuring connection strings
and SSL settings for various results backends, including MySQL, Redis, Rediss, and SQLite.
The module relies on the application's configuration file (`app.yaml`) to determine backend
settings and certificate paths.
"""
from __future__ import print_function

import logging
import os
from typing import Dict
from urllib.parse import quote

from merlin.config.config_filepaths import MERLIN_HOME
from merlin.config.configfile import CONFIG, get_ssl_entries


LOG = logging.getLogger(__name__)

BACKENDS = ["sqlite", "mysql", "redis", "rediss", "none"]


# Default files needed for the package to connect to the Rabbit instance.
MYSQL_CONFIG_FILENAMES = {
    "ssl_cert": "rabbit-client-cert.pem",
    "ssl_ca": "mysql-ca-cert.pem",
    "ssl_key": "rabbit-client-key.pem",
    "password": "mysql.password",
}


# fmt: off
MYSQL_CONNECTION_STRING = (
    "db+mysql+mysqldb://{user}:{password}@{server}/mlsi"
    "?ssl_ca={ssl_ca}"
    "&ssl_cert={ssl_cert}"
    "&ssl_key={ssl_key}"
)
# fmt: on

SQLITE_CONNECTION_STRING = os.path.join(MERLIN_HOME, "merlin.db")


def get_backend_password(password_file: str, certs_path: str = None) -> str:
    """
    Retrieves the backend password from a specified file or returns the provided password value.

    This function attempts to locate the password file in several locations:

    1. The default Merlin directory (`~/.merlin`).
    2. The path specified by `password_file`.
    3. A directory specified by `certs_path` (if provided).

    If the password file is found, the password is read from the file. If the file cannot be
    found, the value of `password_file` is treated as the password itself and returned.

    Args:
        password_file (str): The file path or value for the password. If this is not a valid
            file path, it is treated as the password itself.
        certs_path (str, optional): An optional directory path where SSL certificates and
            password files may be located.

    Returns:
        The backend password, either retrieved from the file or the provided value.
    """
    password = None

    mer_pass = os.path.join(os.path.expanduser("~/.merlin"), password_file)
    password_file = os.path.expanduser(password_file)

    password_filepath = ""
    if os.path.exists(mer_pass):
        password_filepath = mer_pass
    elif os.path.exists(password_file):
        password_filepath = password_file
    elif certs_path:
        password_filepath = os.path.join(certs_path, password_file)

    if not os.path.exists(password_filepath):
        # The password was given instead of the filepath.
        password = password_file.strip()
    else:
        with open(password_filepath, "r") as f:  # pylint: disable=C0103
            line = f.readline().strip()
            password = quote(line, safe="")

    LOG.debug(
        "Results backend: certs_path was provided and used in password resolution."
        if certs_path
        else "Results backend: certs_path was not provided."
    )
    LOG.debug("Password resolution: using file." if password_filepath else "Password resolution: using direct value.")

    return password


# flake8 complains about cyclomatic complexity because of all the try-excepts,
# this isn't so complicated it can't be followed and tucking things in functions
# would make it less readable, so complexity evaluation is off
def get_redis(certs_path: str = None, include_password: bool = True, ssl: bool = False) -> str:  # noqa C901
    """
    Constructs and returns a Redis or Rediss connection URL based on the provided parameters and configuration.

    Args:
        certs_path (str, optional): The path to SSL certificates and password files.
        include_password (bool, optional): Whether to include the password in the connection URL.
            If True, the password will be included; otherwise, it will be masked.
        ssl (bool, optional): Flag indicating whether to use SSL for the connection (Rediss).
            If True, the connection URL will use the "rediss" protocol; otherwise, it will use "redis".

    Returns:
        A Redis or Rediss connection URL formatted based on the provided parameters and configuration.
    """
    server = CONFIG.results_backend.server
    password_file = ""

    urlbase = "rediss" if ssl else "redis"

    try:
        port = CONFIG.results_backend.port
    except (KeyError, AttributeError):
        port = 6379
        LOG.debug("Results backend: using default Redis port.")

    try:
        db_num = CONFIG.results_backend.db_num
    except (KeyError, AttributeError):
        db_num = 0
        LOG.debug("Results backend: using default Redis database number.")

    try:
        username = CONFIG.results_backend.username
    except (KeyError, AttributeError):
        username = ""

    try:
        password_file = CONFIG.results_backend.password
        try:
            password = get_backend_password(password_file, certs_path=certs_path)
        except IOError:
            password = CONFIG.results_backend.password

        if include_password:
            spass = f"{username}:{password}@"
        else:
            spass = f"{username}:******@"
    except (KeyError, AttributeError):
        spass = ""
        LOG.debug("Results backend: no Redis password configured in backend config.")

    LOG.debug(
        f"Results backend: {'password file specified in config' if password_file else 'no password file specified; using direct value'}."
    )
    LOG.debug(f"Results backend: certs_path was {'provided' if certs_path else 'not provided'}.")
    LOG.debug(f"Results backend: Redis server address {'configured' if server else 'not found in config'}.")

    return f"{urlbase}://{spass}{server}:{port}/{db_num}"


def get_mysql_config(certs_path: str, mysql_certs: Dict) -> Dict:
    """
    Determines whether all required information for connecting to MySQL as the Celery
    results backend is available, and returns the MySQL SSL configuration or certificate paths.

    Args:
        certs_path (str): The path to the directory containing SSL certificates and password files.
        mysql_certs (Dict): A dictionary mapping certificate keys (e.g., 'cert', 'key', 'ca')
            to their expected filenames.

    Returns:
        A dictionary containing the paths to the required MySQL certificates if they exist.
    """
    mysql_ssl = get_ssl_config(celery_check=False)
    if mysql_ssl:
        return mysql_ssl

    if not os.path.exists(certs_path):
        return False
    files = os.listdir(certs_path)

    certs = {}
    for key, filename in mysql_certs.items():
        for f in files:  # pylint: disable=C0103
            if not f == filename:
                continue

            certs[key] = os.path.join(certs_path, f)

    return certs


def get_mysql(certs_path: str = None, mysql_certs: Dict = None, include_password: bool = True) -> str:
    """
    Constructs and returns a formatted MySQL connection string based on the provided parameters
    and application configuration.

    Args:
        certs_path (str, optional): The path to the directory containing SSL certificates and password files.
        mysql_certs (dict, optional): A dictionary mapping MySQL certificate keys (e.g., 'ssl_key', 'ssl_cert', 'ssl_ca')
            to their expected filenames. If this is None, it uses the default `MYSQL_CONFIG_FILENAMES`.
        include_password (bool, optional): Whether to include the password in the connection string.
            If True, the password will be included; otherwise, it will be masked.

    Returns:
        A formatted MySQL connection string.

    Raises:
        TypeError: \n
            - If the `server` configuration is missing or invalid.
            - If the MySQL connection information cannot be set due to missing certificates or configuration.
    """
    dbname = CONFIG.results_backend.dbname
    password_file = CONFIG.results_backend.password
    server = CONFIG.results_backend.server

    # Adding an initial start for printing configurations. This should
    # eventually be configured to use a logger. This logic should also
    # eventually be decoupled so we can print debug messages similar to our
    # Python debugging messages.
    LOG.debug(f"Results backend: database name is {'configured' if dbname else 'missing'}.")
    LOG.debug(
        f"Results backend: password file {'specified in configuration' if password_file else 'not specified in configuration; using direct value'}."
    )
    LOG.debug(f"Results backend: server address is {'configured' if server else 'missing'}.")
    LOG.debug(f"Results backend: certs_path was {'provided' if certs_path else 'not provided'}.")

    if not server:
        msg = f"Results backend: server {server} does not have a configuration"
        raise TypeError(msg)  # TypeError since server is None and not str

    password = get_backend_password(password_file, certs_path=certs_path)

    if mysql_certs is None:
        mysql_certs = MYSQL_CONFIG_FILENAMES

    mysql_config = get_mysql_config(certs_path, mysql_certs)

    if not mysql_config:
        msg = f"""The connection information for MySQL could not be set, cannot find:\n
        {mysql_certs}\ncheck the celery/certs path or set the ssl information in the app.yaml file."""
        raise TypeError(msg)  # TypeError since mysql_config is None when it shouldn't be

    mysql_config["user"] = CONFIG.results_backend.username
    if include_password:
        mysql_config["password"] = password
    else:
        mysql_config["password"] = "******"
    mysql_config["server"] = server

    # Ensure the ssl_key, ssl_ca, and ssl_cert keys are all set
    if mysql_certs == MYSQL_CONFIG_FILENAMES:
        for key, cert_file in mysql_certs.items():
            if key not in mysql_config:
                mysql_config[key] = os.path.join(certs_path, cert_file)

    return MYSQL_CONNECTION_STRING.format(**mysql_config)


def get_connection_string(include_password: bool = True) -> str:
    """
    Determines the appropriate results backend to use based on the package configuration
    and returns the corresponding connection string.

    If a URL is explicitly defined in the configuration (`CONFIG.results_backend.url`),
    it is returned as the connection string.

    Args:
        include_password (bool, optional): Whether to include the password in the connection string.
            If True, the password will be included; otherwise, it will be masked.

    Returns:
        The connection string for the configured results backend.

    Raises:
        ValueError: If the specified results backend in the configuration is not supported.
    """
    try:
        return CONFIG.results_backend.url
    except AttributeError:
        pass

    try:
        backend = CONFIG.results_backend.name.lower()
    except AttributeError:
        backend = ""

    if backend not in BACKENDS:
        msg = f"'{backend}' is not a supported results backend"
        raise ValueError(msg)

    try:
        certs_path = CONFIG.celery.certs
        certs_path = os.path.abspath(os.path.expanduser(certs_path))
    except AttributeError:
        certs_path = None

    return _resolve_backend_string(backend, certs_path, include_password)


def _resolve_backend_string(backend: str, certs_path: str, include_password: bool) -> str:
    """
    Resolves and returns the connection string for the specified results backend.

    Based on the backend type provided, this function delegates the connection string
    generation to the appropriate helper function or returns a predefined connection string.

    Args:
        backend (str): The name of the results backend (e.g., "mysql", "sqlite", "redis", "rediss").
        certs_path (str): The path to SSL certificates and password files, used for certain backends
            (e.g., MySQL and Redis).
        include_password (bool): Whether to include the password in the connection string.
            If True, the password will be included; otherwise, it will be masked.

    Returns:
        The connection string for the specified backend, or `None` if the backend is unsupported.
    """
    if "mysql" in backend:
        return get_mysql(certs_path=certs_path, include_password=include_password)

    if "sqlite" in backend:
        return SQLITE_CONNECTION_STRING

    if backend == "redis":
        return get_redis(certs_path=certs_path, include_password=include_password)

    if backend == "rediss":
        return get_redis(certs_path=certs_path, include_password=include_password, ssl=True)

    return None


def get_ssl_config(celery_check: bool = False) -> bool:
    """
    Retrieves the SSL configuration for the results backend based on the settings
    specified in the `app.yaml` configuration file.

    This function determines whether SSL should be enabled for the results backend
    and returns the appropriate configuration. It supports various backend types
    such as MySQL, Redis, and Rediss.

    Args:
        celery_check (bool, optional): If True, the function returns the SSL settings
            specifically for configuring Celery.

    Returns:
        The SSL configuration for the results backend. Returns `True` if SSL is enabled,
            `False` otherwise.
    """
    results_backend = ""
    try:
        results_backend = CONFIG.results_backend.url.split(":")[0]
    except AttributeError:
        # The results_backend may not have a url
        pass

    try:
        results_backend = CONFIG.results_backend.name.lower()
    except AttributeError:
        # The results_backend may not have a name
        pass

    if results_backend not in BACKENDS:
        return False

    try:
        certs_path = CONFIG.celery.certs
    except AttributeError:
        certs_path = None

    results_backend_ssl = get_ssl_entries("Results Backend", results_backend, CONFIG.results_backend, certs_path)

    if results_backend == "rediss":
        if not results_backend_ssl:
            results_backend_ssl = True
        return results_backend_ssl

    if results_backend and "mysql" in results_backend:
        if not celery_check:
            return results_backend_ssl

    return False
