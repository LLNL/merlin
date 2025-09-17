##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Functions for encrypting backend traffic.
"""
from typing import Any

import celery.backends.base
from celery.backends.base import Backend

from merlin.common.security import encrypt


encrypt.init_key()

# remember what the original encode / decode are so we can call it in our
# wrapper
old_encode = celery.backends.base.Backend.encode
old_decode = celery.backends.base.Backend.decode


def _encrypt_encode(*args, **kwargs) -> bytes:
    """
    Intercepts calls to the encode method of the Celery backend and encrypts
    the encoded data.

    This function wraps the original encode method, encrypting the result
    after encoding.

    Returns:
        The encrypted encoded data in bytes format.
    """
    return encrypt.encrypt(old_encode(*args, **kwargs))


def _decrypt_decode(self: Backend, payload: bytes) -> Any:
    """
    Intercepts calls to the decode method of the Celery backend and decrypts
    the payload before decoding.

    This function wraps the original decode method, decrypting the payload
    prior to decoding.

    Args:
        self: The instance of the backend from which the decode method is called.
        payload: The encrypted data to be decrypted.

    Returns:
        The decoded data after decryption. Can be any format.
    """
    return old_decode(self, encrypt.decrypt(payload))


def set_backend_funcs():
    """
    Sets the encode and decode methods of the Celery backend to custom
    implementations that handle encryption and decryption.

    This function replaces the default encode and decode methods with
    `_encrypt_encode` and `_decrypt_decode`, respectively, ensuring that
    all data processed by the Celery backend is encrypted and decrypted
    appropriately.
    """
    celery.backends.base.Backend.encode = _encrypt_encode
    celery.backends.base.Backend.decode = _decrypt_decode
