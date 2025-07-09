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

    def __init__(self, message):
        super().__init__(message)


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

    def __init__(self, message):
        super().__init__(message)


class BackendNotSupportedError(Exception):
    """
    Exception to signal that the provided backend is not supported by Merlin.
    """

    def __init__(self, message):
        super().__init__(message)


###############################
# Database-Related Exceptions #
###############################


class EntityNotFoundError(Exception):
    """
    Fallback error for entities that can't be found.
    """


class StudyNotFoundError(EntityNotFoundError):
    """
    Exception to signal that the study you were looking for cannot be found in
    Merlin's database.
    """


class RunNotFoundError(EntityNotFoundError):
    """
    Exception to signal that the run you were looking for cannot be found in
    Merlin's database.
    """


class WorkerNotFoundError(EntityNotFoundError):
    """
    Exception to signal that the worker you were looking for cannot be found in
    Merlin's database.
    """


class UnsupportedDataModelError(Exception):
    """
    Exception to signal that the data model you're trying to use is not supported.
    """


class EntityManagerNotSupportedError(Exception):
    """
    Exception to signal that the provided entity manager is not supported by Merlin.
    """
