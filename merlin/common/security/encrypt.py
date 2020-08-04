###############################################################################
# Copyright (c) 2019, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.7.2.
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
TODO
"""

import logging
import os

from cryptography.fernet import Fernet

from merlin.config.configfile import CONFIG


LOG = logging.getLogger(__name__)


def _get_key_path():
    """ Loads the redis encryption key path from the file described in config."""
    try:
        key_filepath = CONFIG.results_backend.encryption_key
    except AttributeError:
        key_filepath = "~/.merlin/encrypt_data_key"

    try:
        key_filepath = os.path.abspath(os.path.expanduser(key_filepath))
    except KeyError:
        raise ValueError("Error! No password provided for RabbitMQ")
    return key_filepath


def _gen_key(key_path):
    """ generates an encryption key and writes it to the given key_path"""
    key = Fernet.generate_key()
    parent_dir = os.path.dirname(os.path.normpath(key_path))
    if not os.path.isdir(parent_dir):
        os.makedirs(os.path.dirname(key_path))
    with open(key_path, "wb") as f:
        f.write(key)


def _get_key():
    """ get a valid encryption key. Loads from CONFIG.results_backend.encryption_key if possible,
        initializes that key if it does not yet exist."""
    key_path = _get_key_path()
    try:
        with open(key_path, "rb") as f:
            key = f.read()
    except IOError:
        LOG.info("Generating new encryption key...")
        _gen_key(key_path)
        try:
            with open(key_path, "rb") as f:
                key = f.read()
        except IOError as e:
            LOG.error("Could not read newly generated key... aborting")
            raise e
    return key


def encrypt(payload):
    """
    TODO
    """
    key = _get_key()
    f = Fernet(key)
    del key
    return f.encrypt(payload)


def decrypt(payload):
    """
    TODO
    """
    key = _get_key()
    f = Fernet(key)
    del key
    return f.decrypt(payload)


def init_key():
    """
    Initialize the key to disk on import to prevent race conditions later on, or at least drastically reduce
    the number of corner cases where they could appear.
    """
    Fernet(_get_key())
