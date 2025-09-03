##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""This module handles encryption logic"""

import logging
import os

from cryptography.fernet import Fernet

from merlin.config.configfile import CONFIG


# This disables all the errors about short variable names (like using f to represent a file)
# pylint: disable=invalid-name


LOG = logging.getLogger(__name__)


def _get_key_path() -> str:
    """
    Loads the path to the Redis encryption key from the configuration.

    If the path is not specified in the configuration, it defaults to
    "~/.merlin/encrypt_data_key".

    Returns:
        A string representing the absolute path to the encryption key file.

    Raises:
        ValueError: If there is an issue retrieving the key path from the configuration.
    """
    try:
        key_filepath = CONFIG.results_backend.encryption_key
    except AttributeError:
        key_filepath = "~/.merlin/encrypt_data_key"

    if key_filepath is None:
        raise ValueError("Error! No password provided for RabbitMQ")

    return os.path.abspath(os.path.expanduser(key_filepath))


def _gen_key(key_path: str):
    """
    Generates a new encryption key and writes it to the specified key path.

    Args:
        key_path: The path where the encryption key will be stored.
    """
    key = Fernet.generate_key()
    parent_dir = os.path.dirname(os.path.normpath(key_path))
    if not os.path.isdir(parent_dir):
        os.makedirs(os.path.dirname(key_path))
    with open(key_path, "wb") as f:
        f.write(key)


def _get_key() -> bytes:
    """
    Retrieves a valid encryption key.

    This function attempts to load the key from the path specified in the
    configuration. If the key does not exist, it generates a new key and
    saves it to the specified path.

    Returns:
        The encryption key in bytes format.

    Raises:
        IOError: If there is an issue reading the key file or generating a new key.
    """
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


def encrypt(payload: bytes) -> bytes:
    """
    Encrypts the given payload using a Fernet key.

    Args:
        payload: The data to be encrypted. Must be in bytes format.

    Returns:
        The encrypted data in bytes format.
    """
    key = _get_key()
    f = Fernet(key)
    del key
    return f.encrypt(payload)


def decrypt(payload: bytes) -> bytes:
    """
    Decrypts the given payload using a Fernet key.

    Args:
        payload: The encrypted data to be decrypted. Must be in bytes format.

    Returns:
        The decrypted data in bytes format.
    """
    key = _get_key()
    f = Fernet(key)
    del key
    return f.decrypt(payload)


def init_key():
    """
    Initializes the Fernet key and stores it on disk.

    This function is called on import to prevent race conditions later on,
    or at least drastically reduce the number of corner cases where they could appear.
    """
    Fernet(_get_key())
