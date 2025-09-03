##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Defines an abstract base class for worker handlers in the Merlin workflow framework.

Worker handlers are responsible for launching, stopping, and querying the status
of task server workers (e.g., Celery workers). This interface allows support
for different task servers to be plugged in with consistent behavior.
"""

from abc import ABC, abstractmethod
from typing import Any, List

from merlin.workers.worker import MerlinWorker


class MerlinWorkerHandler(ABC):
    """
    Abstract base class for launching and managing Merlin worker processes.

    Subclasses must implement the methods to launch, stop, and query workers
    using a particular task server (e.g., Celery, Kafka, etc.).

    Methods:
        start_workers: Launch a list of MerlinWorker instances with optional configuration.
        stop_workers: Stop running worker processes managed by this handler.
        query_workers: Query the status of running workers and return summary information.
    """

    def __init__(self):
        """Initialize the worker handler."""

    @abstractmethod
    def start_workers(self, workers: List[MerlinWorker], **kwargs):
        """
        Launch a list of worker instances.

        Args:
            workers (List[MerlinWorker]): The list of workers to launch.
            **kwargs: Optional keyword arguments passed to subclass-specific logic.
        """
        raise NotImplementedError("Subclasses of `MerlinWorkerHandler` must implement a `start_workers` method.")

    @abstractmethod
    def stop_workers(self):
        """
        Stop worker processes.

        This method should terminate any active worker sessions that were previously launched.
        """
        raise NotImplementedError("Subclasses of `MerlinWorkerHandler` must implement a `stop_workers` method.")

    @abstractmethod
    def query_workers(self) -> Any:
        """
        Query the status of all currently running workers.

        Returns:
            Subclasses should return an appropriate data structure summarizing
                the current state of managed workers (e.g., dict, list, string).
        """
        raise NotImplementedError("Subclasses of `MerlinWorkerHandler` must implement a `query_workers` method.")
