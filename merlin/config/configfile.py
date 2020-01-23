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
This module handles the logic for the Merlin config files for setting up all
configurations.
"""
import getpass
import logging
import os

from merlin.config import Config
from merlin.utils import load_yaml


LOG = logging.getLogger(__name__)

APP_FILENAME = "app.yaml"
CONFIG = None

USER_HOME = os.path.expanduser("~")
MERLIN_HOME = os.path.join(USER_HOME, ".merlin")


def load_config(filepath):
    """
    Given the path to the merlin YAML config file, read the file and return
    a dictionary of the contents.
    """
    if not os.path.isfile(filepath):
        LOG.info(f"No app config file at {filepath}")
    else:
        LOG.info(f"Reading app config from file {filepath}")
        return load_yaml(filepath)


def find_config_file(path=None):
    """
    Given a dir path, find and return the path to the merlin application
    config file.
    """
    filename = APP_FILENAME

    if path is None:
        local_app = os.path.join(os.getcwd(), filename)
        path_app = os.path.join(MERLIN_HOME, filename)

        if os.path.isfile(local_app):
            return local_app
        elif os.path.isfile(path_app):
            return path_app
        else:
            LOG.error(
                f'Cannot find a config file, run merlin config and " \
                "edit the file, "{MERLIN_HOME}/{filename}"'
            )

    app_path = os.path.join(path, filename)
    if os.path.exists(app_path):
        return app_path

    LOG.error(
        f'Cannot find a config file, run merlin config and edit the " \
        "file, "{MERLIN_HOME}/{filename}"'
    )

    return None


def load_default_user_names(config):
    """
    Load broker.username and broker.vhost defaults if they are not present in
    the current configuration. Doing this here prevents other areas that rely
    on config from needing to know that those fields could not be defined by
    the user.
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


def get_config(path):
    """
    Load a merlin configuration file and return a dictionary of the
    configurations.
    """
    filepath = find_config_file(path)

    if filepath is None:
        raise ValueError("App config file not found.")

    config = load_config(filepath)
    load_default_user_names(config)
    return config


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


app_config = get_config(None)
CONFIG = Config(app_config)
