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
Module of all Merlin-specific exception types.
"""

# Pylint complains that these exceptions are no different from Exception
# but we don't care, we just need new names for exceptions here
# pylint: disable=W0246

__all__ = (
    "RetryException",
    "SoftFailException",
    "HardFailException",
    "InvalidChainException",
    "RestartException",
)


class RetryException(Exception):
    """
    Exception to signal that a step needs to be
    retried.
    """

    def __init__(self):
        super().__init__()


class SoftFailException(Exception):
    """
    Exception for non-fatal Merlin errors. Workflow
    should continue.
    """

    def __init__(self):
        super().__init__()


class HardFailException(Exception):
    """
    Exception for fatal Merlin errors. Should cause
    workflow to terminate.
    """

    def __init__(self):
        super().__init__()


class InvalidChainException(Exception):
    """
    Exception for invalid Merlin step DAGs.
    """

    def __init__(self):
        super().__init__()


class RestartException(Exception):
    """
    Exception to signal that a step needs to call
    the restart command if present , else retry.
    """

    def __init__(self):
        super().__init__()
