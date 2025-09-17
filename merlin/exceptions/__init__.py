##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Module of all Merlin-specific exception types.
"""

# Pylint complains that these exceptions are no different from Exception
# but we don't care, we just need new names for exceptions here
# pylint: disable=W0235

__all__ = (
    "RetryException",
    "SoftFailException",
    "HardFailException",
    "InvalidChainException",
    "RestartException",
    "NoWorkersException",
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
    Exception for invalid Merlin step Directed Acyclic Graphs (DAGs).
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


class NoWorkersException(Exception):
    """
    Exception to signal that no workers were started
    to process a non-empty queue(s).
    """

    def __init__(self, message):
        super().__init__(message)


class MerlinInvalidTaskServerError(Exception):
    """
    Exception to signal that an invalid task server was provided.
    """
