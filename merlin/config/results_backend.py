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

"""
This module contains the logic for configuring the Celery results backend.
"""
from __future__ import print_function

import logging
import os

from merlin.config.configfile import CONFIG, get_ssl_entries


try:
    from urllib import quote
except ImportError:
    from urllib.parse import quote


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


SQLITE_CONNECTION_STRING = "db+sqlite:///results.db"


def get_backend_password(password_file, certs_path=None):
    """
    Check for password in file.
    If the password is  not found in the given password_file,
    then the certs_path will be searched for the file,
    if this file cannot be found, the password value will
    be returned.

    :param password_file : The file path for the password
    :param certs_path : The path for ssl certificates and passwords
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

    LOG.debug(f"Results backend: aux password path (certs_path) = {certs_path}")
    LOG.debug(f"Results backend: password_filepath = {password_filepath}")

    return password


# flake8 complains about cyclomatic complexity because of all the try-excepts,
# this isn't so complicated it can't be followed and tucking things in functions
# would make it less readable, so complexity evaluation is off
def get_redis(certs_path=None, include_password=True, ssl=False):  # noqa C901
    """
    Return the redis or rediss specific connection

    :param certs_path : The path for ssl certificates and passwords
    :param include_password : Format the connection for ouput by setting this True
    :param ssl : Flag to use rediss output
    """
    server = CONFIG.results_backend.server
    password_file = ""

    urlbase = "rediss" if ssl else "redis"

    try:
        port = CONFIG.results_backend.port
    except (KeyError, AttributeError):
        port = 6379
        LOG.debug(f"Results backend: redis using default port = {port}")

    try:
        db_num = CONFIG.results_backend.db_num
    except (KeyError, AttributeError):
        db_num = 0
        LOG.debug(f"Results backend: redis using default db_num = {db_num}")

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
        LOG.debug(f"Results backend: redis using default password = {spass}")

    LOG.debug(f"Results backend: password_file = {password_file}")
    LOG.debug(f"Results backend: server = {server}")
    LOG.debug(f"Results backend: certs_path = {certs_path}")

    return f"{urlbase}://{spass}{server}:{port}/{db_num}"


def get_mysql_config(certs_path, mysql_certs):
    """
    Determine if all the information for connecting MySQL as the Celery
    results backend exists.

    :param certs_path : The path for ssl certificates and passwords
    :param mysql_certs : The dict of mysql certificates
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


def get_mysql(certs_path=None, mysql_certs=None, include_password=True):
    """
    Returns the formatted MySQL connection string.

    :param certs_path : The path for ssl certificates and passwords
    :param mysql_certs : The dict of mysql certificates
    :param include_password : Format the connection for ouput by setting this True
    """
    dbname = CONFIG.results_backend.dbname
    password_file = CONFIG.results_backend.password
    server = CONFIG.results_backend.server

    # Adding an initial start for printing configurations. This should
    # eventually be configured to use a logger. This logic should also
    # eventually be decoupled so we can print debug messages similar to our
    # Python debugging messages.
    LOG.debug(f"Results backend: dbname = {dbname}")
    LOG.debug(f"Results backend: password_file = {password_file}")
    LOG.debug(f"Results backend: server = {server}")
    LOG.debug(f"Results backend: certs_path = {certs_path}")

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

    return MYSQL_CONNECTION_STRING.format(**mysql_config)


def get_connection_string(include_password=True):
    """
    Given the package configuration determine what results backend to use and
    return the connection string.

    If the url variable is present, return that as the connection string.

    :param config_path : The path for ssl certificates and passwords
    :param include_password : Format the connection for ouput by setting this True
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


def _resolve_backend_string(backend, certs_path, include_password):
    if "mysql" in backend:
        return get_mysql(certs_path=certs_path, include_password=include_password)

    if "sqlite" in backend:
        return SQLITE_CONNECTION_STRING

    if backend == "redis":
        return get_redis(certs_path=certs_path, include_password=include_password)

    if backend == "rediss":
        return get_redis(certs_path=certs_path, include_password=include_password, ssl=True)

    return None


def get_ssl_config(celery_check=False):
    """
    Return the ssl config based on the configuration specified in the
    `app.yaml` config file.

    :param celery_check : Return the proper results ssl setting when configuring celery
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
