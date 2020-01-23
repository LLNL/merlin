###############################################################################
# Copyright (c) 2019, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.2.0.
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

"""
This module contains the logic for configuring the Celery results backend.
"""
from __future__ import print_function

import logging
import os

from merlin.config.configfile import CONFIG


try:
    from urllib import quote
except ImportError:
    from urllib.parse import quote


LOG = logging.getLogger(__name__)

BACKENDS = ["sqlite", "mysql", "redis", "none"]


# Default files needed for the package to connect to the Rabbit instance.
MYSQL_CONFIG_FILENAMES = {
    "ssl_cert": "rabbit-client-cert.pem",
    "ssl_ca": "mysql-ca-cert.pem",
    "ssl_key": "rabbit-client-key.pem",
    "password": "mysql.password",
}


MYSQL_CONNECTION_STRING = (
    "db+mysql+mysqldb://{user}:{password}@{server}/mlsi"
    "?ssl_ca={ssl_ca}"
    "&ssl_cert={ssl_cert}"
    "&ssl_key={ssl_key}"
)


SQLITE_CONNECTION_STRING = "db+sqlite:///results.db"


def get_backend_password(password_file, certs_path=None):
    if certs_path is None:
        certs_path = CONFIG.celery.certs

    password = None

    password_file = os.path.expanduser(password_file)

    if os.path.exists(password_file):
        password_filepath = password_file
    else:
        password_filepath = os.path.join(certs_path, password_file)

    if not os.path.exists(password_filepath):
        # The password was given instead of the filepath.
        password = password_file
    else:
        with open(password_filepath, "r") as f:
            line = f.readline().strip()
            password = quote(line, safe="")

    LOG.debug(f"Results backend certs_path = {certs_path}")
    LOG.debug(f"Results backend password_filepath = {password_filepath}")

    return password


def get_redis(certs_path=None, include_password=True):
    server = CONFIG.results_backend.server
    password_file = ""

    try:
        port = CONFIG.results_backend.port
    except (KeyError, AttributeError):
        port = 6379
        LOG.warning(f"Results backend redis using default port = {port}")

    try:
        db_num = CONFIG.results_backend.db_num
    except (KeyError, AttributeError):
        db_num = 0
        LOG.warning(f"Results backend redis using default db_num = {db_num}")

    try:
        username = CONFIG.results_backend.username
        password_file = CONFIG.results_backend.password
        try:
            password = get_backend_password(password_file, certs_path=certs_path)
        except IOError:
            password = CONFIG.results_backend.password

        if include_password:
            spass = "%s:%s@" % (username, password)
        else:
            spass = "%s:%s@" % (username, "******")
    except (KeyError, AttributeError):
        spass = ""
        LOG.warning(f"Results backend redis using default password = {spass}")

    LOG.debug(f"Results backend password_file = {password_file}")
    LOG.debug(f"Results backend server = {server}")
    LOG.debug(f"Results backend certs_path = {certs_path}")

    return "redis://%s%s:%d/%d" % (spass, server, port, db_num)


def get_mysql_config(certs_path, mysql_certs):
    """
    Determine if all the information for connecting MySQL as the Celery
    results backend exists.
    """
    if not os.path.exists(certs_path):
        return False
    files = os.listdir(certs_path)

    certs = {}
    for key, filename in mysql_certs.items():
        for f in files:
            if not f == filename:
                continue

            certs[key] = os.path.join(certs_path, f)

    return certs


def get_mysql(certs_path=None, mysql_certs=None, include_password=True):
    """Returns the formatted MySQL connection string."""
    if certs_path is None:
        certs_path = CONFIG.celery.certs

    dbname = CONFIG.results_backend.dbname
    password_file = CONFIG.results_backend.password
    server = CONFIG.results_backend.server

    # Adding an initial start for printing configurations. This should
    # eventually be configured to use a logger. This logic should also
    # eventually be decoupled so we can print debug messages similar to our
    # Python debugging messages.
    LOG.debug(f"Results backend dbname = {dbname}")
    LOG.debug(f"Results backend password_file = {password_file}")
    LOG.debug(f"Results backend server = {server}")
    LOG.debug(f"Results backend certs_path = {certs_path}")

    if not server:
        msg = f"Results backend server {server} does not have a configuration"
        raise Exception(msg)

    password = get_backend_password(password_file, certs_path=certs_path)

    if mysql_certs is None:
        mysql_certs = MYSQL_CONFIG_FILENAMES

    mysql_config = get_mysql_config(certs_path, mysql_certs)

    if not mysql_config:
        msg = "The connection information for MySQL could not be set."
        raise Exception(msg)

    mysql_config["user"] = CONFIG.results_backend.username
    if include_password:
        mysql_config["password"] = password
    else:
        mysql_config["password"] = "******"
    mysql_config["server"] = server

    return MYSQL_CONNECTION_STRING.format(**mysql_config)


def get_connection_string(include_password=True):
    """
    Given the package configuration determine what results backend to use and
    return the connection string.
    """
    backend = CONFIG.results_backend.name.lower()

    if backend not in BACKENDS:
        msg = f"'{backend}' is not a supported results backend"
        raise ValueError(msg)

    if backend == "mysql":
        return get_mysql(include_password=include_password)

    if backend == "sqlite":
        return SQLITE_CONNECTION_STRING

    if backend == "redis":
        return get_redis(include_password=include_password)

    return None


def verify_config():
    """Verifies the backend configurations."""
    try:
        results_backend()  # TODO ERROR this function is undefined
        has_backend = True
    except ValueError:
        has_backend = False

    LOG.debug(f"Has Results Backend: {has_backend}")
