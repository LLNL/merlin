###############################################################################
# Copyright (c) 2019, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.7.3.
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
Functions for encrypting backend traffic.
"""
import celery.backends.base

from merlin.common.security import encrypt


encrypt.init_key()

# remember what the original encode / decode are so we can call it in our
# wrapper
old_encode = celery.backends.base.Backend.encode
old_decode = celery.backends.base.Backend.decode


def _encrypt_encode(*args, **kwargs):
    """
    Intercept all celery.backends.Backend.encode calls and encrypt them after
    encoding
    """
    return encrypt.encrypt(old_encode(*args, **kwargs))


def _decrypt_decode(self, payload):
    """
    Intercept all celery.backends.Backend.decode calls and decrypt them before
    decoding.
    """
    return old_decode(self, encrypt.decrypt(payload))


def set_backend_funcs():
    """
    Set the encode / decode to our own encrypt_encode / encrypt_decode.
    """
    celery.backends.base.Backend.encode = _encrypt_encode
    celery.backends.base.Backend.decode = _decrypt_decode
