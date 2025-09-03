##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module provides utility functions and constants to manage broker configurations and connection strings
for various messaging systems, including RabbitMQ and Redis. It supports multiple connection protocols
and configurations, such as SSL, Unix sockets, and password inclusion.

The module defines constants for supported brokers and connection string templates, along with functions
to construct and retrieve connection strings and SSL configurations based on settings defined in the
`app.yaml` configuration file.
"""
from __future__ import print_function

import getpass
import logging
import os
import ssl
from os.path import expanduser
from typing import Dict, List, Optional, Union
from urllib.parse import quote

from merlin.config.configfile import CONFIG, get_ssl_entries


LOG: logging.Logger = logging.getLogger(__name__)

BROKERS: List[str] = ["rabbitmq", "redis", "rediss", "redis+socket", "amqps", "amqp"]

RABBITMQ_CONNECTION: str = "{conn}://{username}:{password}@{server}:{port}/{vhost}"
REDISSOCK_CONNECTION: str = "redis+socket://{path}?virtual_host={db_num}"
USER = getpass.getuser()


def read_file(filepath: str) -> str:
    """
    Safely reads the first line from a file and returns it with special characters URL-encoded.

    Args:
        filepath (str): The path to the file to be read.

    Returns:
        The first line of the file, stripped of leading and trailing whitespace,
            with special characters URL-encoded.
    """
    with open(filepath, "r") as f:  # pylint: disable=C0103
        line = f.readline().strip()
        return quote(line, safe="")


def get_rabbit_connection(include_password: bool, conn: str = "amqps") -> str:
    """
    Constructs and returns a RabbitMQ connection string based on broker configurations.

    This function reads broker configurations (such as server, port, username, password, and vhost)
    and formats them into a RabbitMQ connection string. Optionally, the password can be included
    in the connection string if `include_password` is set to `True`.

    Args:
        include_password (bool): Whether to include the password in the connection string.
        conn (str, optional): The connection protocol to use. Defaults to "amqps".
            Supported values are "amqp" and "amqps".

    Returns:
        A formatted RabbitMQ connection string.

    Raises:
        ValueError: If the password file path is not provided in the broker configuration, or if
            the password file does not exist or cannot be read.
    """
    LOG.debug(f"Broker: connection = {conn}")

    vhost = CONFIG.broker.vhost
    LOG.debug(f"Broker: vhost = {vhost}")

    username = CONFIG.broker.username
    LOG.debug(f"Broker: username = {username}")

    server = CONFIG.broker.server
    LOG.debug(f"Broker: server = {server}")

    try:
        password_filepath = CONFIG.broker.password
        LOG.debug("Broker: password file path has been configured.")
        password_filepath = os.path.abspath(expanduser(password_filepath))
    except (AttributeError, KeyError) as exc:
        raise ValueError("Broker: No password provided for RabbitMQ") from exc

    try:
        password = read_file(password_filepath)
    except IOError as exc:
        raise ValueError(f"Broker: RabbitMQ password file {password_filepath} does not exist") from exc

    try:
        port = CONFIG.broker.port
        LOG.debug(f"Broker: RabbitMQ port = {port}")
    except (AttributeError, KeyError):
        if conn == "amqp":
            port = 5672
        else:
            port = 5671
        LOG.debug(f"Broker: RabbitMQ using default port = {port}")

    # Test configurations.
    rabbitmq_config = {
        "conn": conn,
        "vhost": vhost,
        "username": username,
        "password": "******",
        "server": server,
        "port": port,
    }

    if include_password:
        rabbitmq_config["password"] = password

    return RABBITMQ_CONNECTION.format(**rabbitmq_config)


def get_redissock_connection() -> str:
    """
    Constructs and returns a Redis connection string using a Unix socket.

    This function retrieves broker configurations, such as the database number (`db_num`) and
    the Unix socket file path (`path`), and formats them into a Redis connection string.

    If the database number is not specified in the configuration, it defaults to `0`.

    Returns:
        A formatted Redis connection string using a Unix socket.
    """
    try:
        db_num = CONFIG.broker.db_num
    except (AttributeError, KeyError):
        db_num = 0
        LOG.debug(f"Broker: redis+socket using default db_num = {db_num}")

    redis_config = {"db_num": db_num, "path": CONFIG.broker.path}

    return REDISSOCK_CONNECTION.format(**redis_config)


# flake8 complains this function is too complex, we don't gain much nesting any of this as a separate function,
# however, cyclomatic complexity examination is off to get around this
def get_redis_connection(include_password: bool, use_ssl: bool = False) -> str:  # noqa C901
    """
    Constructs and returns a Redis connection string, optionally using SSL and including a password.

    This function retrieves broker configurations (such as server, port, username, password, and database number)
    and formats them into a Redis connection string. The connection can be configured to use SSL (`rediss` protocol)
    and optionally include the password in the connection string.

    Args:
        include_password (bool): Whether to include the password in the connection string.
        use_ssl (bool, optional): Whether to use the `rediss` protocol (SSL).

    Returns:
        A formatted Redis connection string.
    """
    server = CONFIG.broker.server
    LOG.debug(f"Broker: server = {server}")

    urlbase = "rediss" if use_ssl else "redis"

    try:
        port = CONFIG.broker.port
        LOG.debug(f"Broker: redis port = {port}")
    except (AttributeError, KeyError):
        port = 6379
        LOG.debug(f"Broker: redis using default port = {port}")

    try:
        db_num = CONFIG.broker.db_num
    except (AttributeError, KeyError):
        db_num = 0
        LOG.debug(f"Broker: redis using default db_num = {db_num}")

    try:
        username = CONFIG.broker.username
    except (AttributeError, KeyError):
        username = ""

    try:
        password_filepath = CONFIG.broker.password
        try:
            password = read_file(password_filepath)
        except IOError:
            password = CONFIG.broker.password
        if include_password:
            spass = f"{username}:{password}@"
        else:
            spass = f"{username}:******@"
    except (AttributeError, KeyError):
        spass = ""
        LOG.debug(f"Broker: redis using default password = {spass}")

    return f"{urlbase}://{spass}{server}:{port}/{db_num}"


def get_connection_string(include_password: bool = True) -> str:
    """
    Constructs and returns a connection string based on the broker configuration.

    This function retrieves the connection string from the `CONFIG.broker.url` if available.
    Otherwise, it determines the connection string based on the broker name specified in the
    configuration file (`app.yaml`). If the broker name is not supported, a `ValueError` is raised.

    Args:
        include_password (bool): Whether to include the password in the connection string.

    Returns:
        A formatted connection string based on the broker configuration.

    Raises:
        ValueError: If the broker name is not supported.
    """
    try:
        return CONFIG.broker.url
    except AttributeError:
        # The broker may not have a url
        pass

    try:
        broker = CONFIG.broker.name.lower()
    except AttributeError:
        broker = ""

    if broker not in BROKERS:
        raise ValueError(f"Error: {broker} is not a supported broker.")
    return _sort_valid_broker(broker, include_password)


def _sort_valid_broker(broker: str, include_password: bool) -> str:
    """
    Determines and returns the appropriate connection string for a given broker.

    This function selects the connection string generation method based on the broker type
    provided as input. Supported brokers include RabbitMQ (`amqp` or `amqps`), Redis (`redis`),
    Redis over SSL (`rediss`), and Redis over a socket (`redis+socket`).

    Args:
        broker (str): The name of the broker. Must be one of the supported broker types:
            `rabbitmq`, `amqps`, `amqp`, `redis+socket`, `redis`, or `rediss`.
        include_password (bool): Whether to include the password in the connection string.

    Returns:
        A formatted connection string for the specified broker.
    """
    if broker in ("rabbitmq", "amqps"):
        return get_rabbit_connection(include_password, conn="amqps")

    if broker == "amqp":
        return get_rabbit_connection(include_password, conn="amqp")

    if broker == "redis+socket":
        return get_redissock_connection()

    if broker == "redis":
        return get_redis_connection(include_password)

    # broker must be rediss
    return get_redis_connection(include_password, use_ssl=True)


def get_ssl_config() -> Union[bool, Dict[str, Union[str, ssl.VerifyMode]]]:
    """
    Retrieves the SSL configuration for the broker based on the settings in the `app.yaml` configuration file.

    This function determines whether SSL should be used for the broker connection and, if applicable,
    returns the SSL configuration details. If the broker does not require SSL or is unsupported,
    the function returns `False`.

    Returns:
        This returns either:\n
            - `False` if SSL is not required or the broker is unsupported.
            - A dictionary containing SSL configuration details if SSL is required.
              The dictionary may include keys such as certificate paths and verification modes.
    """
    broker: Union[bool, str] = ""
    try:
        broker = CONFIG.broker.url.split(":")[0]
    except AttributeError:
        # The broker may not have a url
        pass

    try:
        broker = CONFIG.broker.name.lower()
    except AttributeError:
        # The broker may not have a name
        pass

    if broker not in BROKERS:
        return False

    certs_path: Optional[str]
    try:
        certs_path = CONFIG.celery.certs
    except AttributeError:
        certs_path = None

    broker_ssl: Union[bool, Dict[str, Union[str, ssl.VerifyMode]]] = get_ssl_entries(
        "Broker", broker, CONFIG.broker, certs_path
    )

    if not broker_ssl:
        broker_ssl = True

    if broker in ("rabbitmq", "rediss", "amqps"):
        return broker_ssl

    return False
