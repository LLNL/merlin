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
This module handles the logic for the Merlin config files for setting up all
configurations.
"""
import getpass
import logging
import os
import ssl
from typing import Dict, Optional, Union

from merlin.config import Config
from merlin.utils import load_yaml


LOG: logging.Logger = logging.getLogger(__name__)

APP_FILENAME: str = "app.yaml"
CONFIG: Optional[Config] = None

USER_HOME: str = os.path.expanduser("~")
MERLIN_HOME: str = os.path.join(USER_HOME, ".merlin")


def load_config(filepath):
    """
    Given the path to the merlin YAML config file, read the file and return
    a dictionary of the contents.

    :param filepath : Read a yaml file given by filepath
    """
    if not os.path.isfile(filepath):
        LOG.info(f"No app config file at {filepath}")
        return None
    LOG.info(f"Reading app config from file {filepath}")
    return load_yaml(filepath)


def find_config_file(path=None):
    """
    Given a dir path, find and return the path to the merlin application
    config file.

    :param path : The path to search for the app.yaml file
    """
    if path is None:
        local_app = os.path.join(os.getcwd(), APP_FILENAME)
        path_app = os.path.join(MERLIN_HOME, APP_FILENAME)

        if os.path.isfile(local_app):
            return local_app
        if os.path.isfile(path_app):
            return path_app
        return None

    app_path = os.path.join(path, APP_FILENAME)
    if os.path.exists(app_path):
        return app_path

    return None


def load_default_user_names(config):
    """
    Load broker.username and broker.vhost defaults if they are not present in
    the current configuration. Doing this here prevents other areas that rely
    on config from needing to know that those fields could not be defined by
    the user.

    :param config : The namespace config object
    """
    try:
        config["broker"]["username"]
    except KeyError:
        username = getpass.getuser()
        config["broker"]["username"] = username
    try:
        config["broker"]["vhost"]
    except KeyError:
        vhost = getpass.getuser()
        config["broker"]["vhost"] = vhost


def get_config(path: Optional[str]) -> Dict:
    """
    Load a merlin configuration file and return a dictionary of the
    configurations.

    :param [Optional[str]] path : The path to search for the config file.
    :return: the config file to coordinate brokers/results backend/task manager."
    :rtype: A Dict with all the config data.
    """
    filepath: Optional[str] = find_config_file(path)

    if filepath is None:
        raise ValueError(
            f"Cannot find a merlin config file! Run 'merlin config' and edit the file '{MERLIN_HOME}/{APP_FILENAME}'"
        )

    config: Dict = load_config(filepath)
    load_defaults(config)
    return config


def load_default_celery(config):
    """Creates the celery default configuration"""
    try:
        config["celery"]
    except KeyError:
        config["celery"] = {}
    try:
        config["celery"]["omit_queue_tag"]
    except KeyError:
        config["celery"]["omit_queue_tag"] = False
    try:
        config["celery"]["queue_tag"]
    except KeyError:
        config["celery"]["queue_tag"] = "[merlin]_"
    try:
        config["celery"]["override"]
    except KeyError:
        config["celery"]["override"] = None


def load_defaults(config):
    """Loads default configuration values"""
    load_default_user_names(config)
    load_default_celery(config)


def is_debug():
    """
    Check for MERLIN_DEBUG in environment to set a debugging flag
    """
    if "MERLIN_DEBUG" in os.environ and int(os.environ["MERLIN_DEBUG"]) == 1:
        return True
    return False


def default_config_info():
    """Return information about Merlin's default configurations."""
    return {
        "config_file": find_config_file(),
        "is_debug": is_debug(),
        "merlin_home": MERLIN_HOME,
        "merlin_home_exists": os.path.exists(MERLIN_HOME),
    }


def get_cert_file(server_type, config, cert_name, cert_path):
    """
    Check if a ssl certificate file is present in the config

    :param server_type : The server type for output (Broker, Results Backend)
    :param config : The server config
    :param cert_name : The argument in cert argument name
    :param cert_path : The optional cert path
    """
    cert_file = None
    try:
        cert_file = getattr(config, cert_name)
        cert_file = os.path.abspath(os.path.expanduser(cert_file))
        if not os.path.exists(cert_file) and cert_path:
            base_cert_file = os.path.basename(cert_file)
            new_cert_file = os.path.join(cert_path, base_cert_file)
            new_cert_file = os.path.abspath(os.path.expanduser(new_cert_file))
            if os.path.exists(new_cert_file):
                cert_file = new_cert_file
            else:
                LOG.error(f"{server_type}: The file for {cert_name} does not exist, searched {cert_file} and {new_cert_file}")
        LOG.debug(f"{server_type}: {cert_name} = {cert_file}")
    except (AttributeError, KeyError):
        LOG.debug(f"{server_type}: {cert_name} not present")

    return cert_file


def get_ssl_entries(
    server_type: str, server_name: str, server_config: Config, cert_path: str
) -> Dict[str, Union[str, ssl.VerifyMode]]:
    """
    Check if a ssl certificate file is present in the config

    :param [str] server_type : The server type
    :param [str] server_name : The server name for output
    :param [Config] server_config : The server config
    :param [str] cert_path : The optional cert path
    :return : The data needed to manage an ssl certification.
    :rtype : A Dict.
    """
    server_ssl: Dict[str, Union[str, ssl.VerifyMode]] = {}

    keyfile: Optional[str] = get_cert_file(server_type, server_config, "keyfile", cert_path)
    if keyfile:
        server_ssl["keyfile"] = keyfile

    certfile: Optional[str] = get_cert_file(server_type, server_config, "certfile", cert_path)
    if certfile:
        server_ssl["certfile"] = certfile

    ca_certsfile: Optional[str] = get_cert_file(server_type, server_config, "ca_certs", cert_path)
    if ca_certsfile:
        server_ssl["ca_certs"] = ca_certsfile

    try:
        if server_config.cert_reqs == "required":
            server_ssl["cert_reqs"] = ssl.CERT_REQUIRED
        elif server_config.cert_reqs == "optional":
            server_ssl["cert_reqs"] = ssl.CERT_OPTIONAL
        elif server_config.cert_reqs == "none":
            server_ssl["cert_reqs"] = ssl.CERT_NONE
        LOG.debug(f"{server_type}: cert_reqs = {server_ssl['cert_reqs']}")
    except (AttributeError, KeyError):
        LOG.debug(f"{server_type}: ssl cert_reqs not present")

    try:
        server_ssl["ssl_protocol"] = server_config.ssl_protocol
        LOG.debug(f"{server_type}: ssl_protocol = {server_ssl['ssl_protocol']}")
    except (AttributeError, KeyError):
        LOG.debug(f"{server_type}: ssl ssl_protocol not present")

    if server_ssl and "cert_reqs" not in server_ssl:
        server_ssl["cert_reqs"] = ssl.CERT_REQUIRED

    ssl_map: Dict[str, str] = process_ssl_map(server_name)

    if server_ssl and ssl_map:
        server_ssl = merge_sslmap(server_ssl, ssl_map)

    return server_ssl


def process_ssl_map(server_name: str) -> Optional[Dict[str, str]]:
    """
    Process a special map for rediss and mysql.

    :param server_name : The server name for output
    """
    ssl_map: Dict[str, str] = {}
    # The redis server requires key names with ssl_
    if server_name == "rediss":
        ssl_map = {
            "keyfile": "ssl_keyfile",
            "certfile": "ssl_certfile",
            "ca_certs": "ssl_ca_certs",
            "cert_reqs": "ssl_cert_reqs",
        }

    # The mysql server requires key names with ssl_ and different var names
    if "mysql" in server_name:
        ssl_map = {"keyfile": "ssl_key", "certfile": "ssl_cert", "ca_certs": "ssl_ca"}

    return ssl_map


def merge_sslmap(server_ssl: Dict[str, Union[str, ssl.VerifyMode]], ssl_map: Dict[str, str]) -> Dict:
    """
    The different servers have different key var expectations, this updates the keys of the ssl_server dict with keys from
    the ssl_map if using rediss or mysql.

    : param server_ssl : the dict constructed in get_ssl_entries, here updated with keys from ssl_map
    : param ssl_map : the dict holding special key:value pairs for rediss and mysql
    """
    new_server_ssl: Dict[str, Union[str, ssl.VerifyMode]] = {}

    for key in server_ssl:
        if key in ssl_map:
            new_server_ssl[ssl_map[key]] = server_ssl[key]
        else:
            new_server_ssl[key] = server_ssl[key]

    return new_server_ssl


app_config: Dict = get_config(None)
CONFIG = Config(app_config)
