###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.12.2.
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

"""Package for providing enumerations for interfaces"""
from enum import Enum, IntEnum


__all__ = ("ReturnCode",)


class ReturnCode(IntEnum):
    """
    Enum for Merlin return codes.

    This class defines various return codes used in the Merlin system to indicate
    the status of operations. Each return code corresponds to a specific outcome
    of a process.

    Attributes:
        OK (int): Indicates a successful operation. Numeric value: 0.
        ERROR (int): Indicates a general error occurred. Numeric value: 1.
        RESTART (int): Indicates that the process should be restarted. Numeric value: 100.
        SOFT_FAIL (int): Indicates a non-critical failure that allows for recovery. Numeric value: 101.
        HARD_FAIL (int): Indicates a critical failure that cannot be recovered from. Numeric value: 102.
        DRY_OK (int): Indicates a successful operation in a dry run (no changes made). Numeric value: 103.
        RETRY (int): Indicates that the operation should be retried. Numeric value: 104.
        STOP_WORKERS (int): Indicates that worker processes should be stopped. Numeric value: 105.
        RAISE_ERROR (int): Indicates that an error should be raised. Numeric value: 106.
    """

    OK = 0
    ERROR = 1
    RESTART = 100
    SOFT_FAIL = 101
    HARD_FAIL = 102
    DRY_OK = 103
    RETRY = 104
    STOP_WORKERS = 105
    RAISE_ERROR = 106


class WorkerStatus(Enum):
    """
    Status of Merlin workers.

    Attributes:
        RUNNING (str): Indicates the worker is running. String value: "running".
        STALLED (str): Indicates the worker is running but hasn't been processing work. String value: "stalled".
        STOPPED (str): Indicates the worker is not running. String value: "stopped".
        REBOOTING (str): Indicates the worker is actively restarting itself. String value: "rebooting".
    """

    RUNNING = "running"
    STALLED = "stalled"
    STOPPED = "stopped"
    REBOOTING = "rebooting"
