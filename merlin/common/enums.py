##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

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
