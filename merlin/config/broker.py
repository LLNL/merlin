###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.11.0.
#
# For details, see https://github.com/LLNL/merlin.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################

"""Logic for configuring the celery broker."""
from __future__ import print_function

import getpass
import logging
import os
import ssl
from os.path import expanduser
from typing import Dict, List, Optional, Union

from merlin.config.configfile import CONFIG, get_ssl_entries


try:
    from urllib import quote
except ImportError:
    from urllib.parse import quote


LOG: logging.Logger = logging.getLogger(__name__)

BROKERS: List[str] = ["rabbitmq", "redis", "rediss", "redis+socket", "amqps", "amqp"]

RABBITMQ_CONNECTION: str = "{conn}://{username}:{password}@{server}:{port}/{vhost}"
REDISSOCK_CONNECTION: str = "redis+socket://{path}?virtual_host={db_num}"
USER = getpass.getuser()


def read_file(filepath):
    "Safe file read from filepath"
    with open(filepath, "r") as f:  # pylint: disable=C0103
        line = f.readline().strip()
        return quote(line, safe="")


def get_rabbit_connection(include_password, conn="amqps"):
    """
    Given the path to the directory where the broker configurations are stored
    setup and return the RabbitMQ connection string.

    :param include_password : Format the connection for ouput by setting this True
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
        LOG.debug(f"Broker: password filepath = {password_filepath}")
        password_filepath = os.path.abspath(expanduser(password_filepath))
    except KeyError as e:  # pylint: disable=C0103
        raise ValueError("Broker: No password provided for RabbitMQ") from e

    try:
        password = read_file(password_filepath)
    except IOError as e:  # pylint: disable=C0103
        raise ValueError(f"Broker: RabbitMQ password file {password_filepath} does not exist") from e

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


def get_redissock_connection():
    """
    Given the path to the directory where the broker configurations are stored
    setup and return the redis+socket connection string.
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
def get_redis_connection(include_password, use_ssl=False):  # noqa C901
    """
    Return the redis or rediss specific connection

    :param include_password : Format the connection for ouput by setting this True
    :param use_ssl : Flag to use rediss output
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


def get_connection_string(include_password=True):
    """
    Return the connection string based on the configuration specified in the
    `app.yaml` config file.

    If the url variable is present, return that as the connection string.

    :param include_password : The connection can be formatted for output by
                              setting this to True
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

    try:
        config_path = CONFIG.celery.certs
        config_path = os.path.abspath(os.path.expanduser(config_path))
    except AttributeError:
        config_path = None

    if broker not in BROKERS:
        raise ValueError(f"Error: {broker} is not a supported broker.")
    return _sort_valid_broker(broker, include_password)


def _sort_valid_broker(broker, include_password):
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
    Return the ssl config based on the configuration specified in the
    `app.yaml` config file.

    :return: Returns either False if no ssl
    :rtype: Union[bool, Dict[str, Union[str, ssl.VerifyMode]]]
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
