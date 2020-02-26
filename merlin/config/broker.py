###############################################################################
# Copyright (c) 2019, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.3.0.
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

from merlin.config.configfile import CONFIG


try:
    from urllib import quote
except ImportError:
    from urllib.parse import quote


LOG = logging.getLogger(__name__)

BROKERS = ["rabbitmq", "redis", "redis+socket"]

RABBITMQ_CONNECTION = "amqps://{username}:{password}@{server}:{port}//{vhost}"
REDISSOCK_CONNECTION = "redis+socket://{path}?virtual_host={db_num}"
USER = getpass.getuser()


def read_file(filepath):
    with open(filepath, "r") as f:
        line = f.readline().strip()
        return quote(line, safe="")


def get_rabbit_connection(config_path, include_password):
    """
    Given the path to the directory where the broker configurations are stored
    setup and return the RabbitMQ connection string.
    """
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
    except KeyError:
        raise ValueError("Broker: No password provided for RabbitMQ")

    try:
        password = read_file(password_filepath)
    except IOError:
        raise ValueError(
            f"Broker: RabbitMQ password file {password_filepath} does not exist"
        )

    try:
        port = CONFIG.broker.port
        LOG.debug(f"Broker: RabbitMQ port = {port}")
    except (AttributeError, KeyError):
        port = 5671
        LOG.debug(f"Broker: RabbitMQ using default port = {port}")

    # Test configurations.
    rabbitmq_config = {
        "vhost": vhost,
        "username": username,
        "password": "******",
        "server": server,
        "port": port,
    }

    if include_password:
        rabbitmq_config["password"] = password

    return RABBITMQ_CONNECTION.format(**rabbitmq_config)


def get_redissock_connection(config_path, include_password):
    """
    Given the path to the directory where the broker configurations are stored
    setup and return the redis+socket connection string.
    """
    try:
        db_num = CONFIG.broker.db_num
    except (AttributeError, KeyError):
        db_num = 0
        LOG.warning(f"Broker: redis+socket using default db_num = {db_num}")

    redis_config = {"db_num": db_num, "path": CONFIG.broker.path}

    return REDISSOCK_CONNECTION.format(**redis_config)


def get_redis_connection(config_path, include_password):
    server = CONFIG.broker.server
    LOG.info(f"Broker: server = {server}")

    try:
        port = CONFIG.broker.port
        LOG.debug(f"Broker: redis port = {port}")
    except (AttributeError, KeyError):
        port = 6379
        LOG.warning(f"Broker: redis using default port = {port}")

    try:
        db_num = CONFIG.broker.db_num
    except (AttributeError, KeyError):
        db_num = 0
        LOG.warning(f"Broker: redis using default db_num = {db_num}")

    try:
        username = CONFIG.broker.username
        password_filepath = CONFIG.broker.password
        try:
            password = read_file(password_filepath)
        except IOError:
            password = CONFIG.broker.password
        if include_password:
            spass = "%s:%s@" % (username, password)
        else:
            spass = "%s:%s@" % (username, "******")
    except (AttributeError, KeyError):
        spass = ""
        LOG.warning(f"Broker: redis using default password = {spass}")

    redis_broker = "redis://%s%s:%d/%d" % (spass, server, port, db_num)

    return redis_broker


def get_connection_string(include_password=True):
    """
    Return the connection string based on the configuration specified in the
    `merlin.yaml` config file.
    """
    broker = CONFIG.broker.name
    try:
        config_path = CONFIG.celery.certs
    except AttributeError:
        config_path = None

    if broker not in BROKERS:
        raise ValueError(f"Error: {broker} is not a supported broker.")

    if broker == "rabbitmq":
        return get_rabbit_connection(config_path, include_password)

    if broker == "redis+socket":
        return get_redissock_connection(config_path, include_password)

    if broker == "redis":
        return get_redis_connection(config_path, include_password)

    return None


def get_ssl_config():
    """
    Return the ssl config based on the configuration specified in the
    `merlin.yaml` config file.
    """
    broker = CONFIG.broker.name
    try:
        config_path = CONFIG.celery.certs
    except AttributeError:
        config_path = None

    if broker not in BROKERS:
        raise ValueError(f"Error: {broker} is not a supported broker.")

    broker_ssl = {}
    try:
        broker_ssl["keyfile"] = CONFIG.broker.keyfile
        LOG.debug(f"Broker: ssl keyfile = {broker_ssl['keyfile']}")
    except (AttributeError, KeyError):
        LOG.debug(f"Broker: ssl keyfile not present")
    try:
        broker_ssl["certfile"] = CONFIG.broker.certfile
        LOG.debug(f"Broker: ssl certfile = {broker_ssl['certfile']}")
    except (AttributeError, KeyError):
        LOG.debug(f"Broker: ssl certfile not present")
    try:
        broker_ssl["ca_certs"] = CONFIG.broker.ca_certs
        LOG.debug(f"Broker ssl ca_certs = {broker_ssl['ca_certs']}")
    except (AttributeError, KeyError):
        LOG.debug(f"Broker: ssl ca_certs not present")

    if broker_ssl:
        broker_ssl["cert_reqs"] = ssl.CERT_REQUIRED
    else:
        broker_ssl = True

    if broker == "rabbitmq":
        return broker_ssl

    if "redis" in broker:
        try:
            redis_broker_ssl = {}
            for k, v in broker_ssl.items():
                redis_broker_ssl["ssl_" + k] = v
        except AttributeError:
            return True

    return None
