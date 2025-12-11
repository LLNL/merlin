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
from typing import List

from merlin.db_scripts.merlin_db import MerlinDatabase
from merlin.workers.worker import MerlinWorker


class MerlinWorkerHandler(ABC):
    """
    Abstract base class for launching and managing Merlin worker processes.

    Subclasses must implement the methods to launch, stop, and query workers
    using a particular task server (e.g., Celery, Kafka, etc.).

    Attributes:
        merlin_db (MerlinDatabase): The database instance used for worker management.

    Methods:
        start_workers: Launch a list of MerlinWorker instances with optional configuration.
        stop_workers: Stop running worker processes managed by this handler.
        query_workers: Query the status of running workers and return summary information.
    """

    def __init__(self, merlin_db: MerlinDatabase = None):
        """
        Initialize the worker handler.

        Args:
            merlin_db: The database instance used for worker management or None.
        """
        self.merlin_db = merlin_db or MerlinDatabase()

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
    def stop_workers(self, queues: List[str] = None, workers: List[str] = None):
        """
        Stop worker processes, optionally filtered by queue or worker name.

        This method terminates active worker processes based on the provided filters.
        The behavior varies by implementation:

        - If both `queues` and `workers` are None, all active workers are stopped.
        - If `queues` is provided, only workers attached to those queues are stopped.
        - If `workers` is provided, only workers matching those names/patterns are stopped.
        - If both are provided, workers must match both criteria (intersection).

        Args:
            queues: Optional list of queue names to filter workers by. Queue names
                will be normalized with the appropriate task server prefix if needed.
            workers: Optional list of worker names or patterns to match. For Celery,
                these can be logical worker names from the spec or regex patterns
                matching physical worker names (e.g., "celery@worker1.*").

        Example:
            ```python
            handler = CeleryWorkerHandler()

            # Stop all workers
            handler.stop_workers()

            # Stop workers on specific queues
            handler.stop_workers(queues=['hello_queue', 'world_queue'])

            # Stop specific workers by name
            handler.stop_workers(workers=['worker1', 'worker2'])

            # Stop workers matching both criteria
            handler.stop_workers(queues=['hello_queue'], workers=['worker1.*'])
            ```

        Raises:
            May raise task-server-specific exceptions if connection fails.
        """
        raise NotImplementedError("Subclasses of `MerlinWorkerHandler` must implement a `stop_workers` method.")

    @abstractmethod
    def query_workers(self, formatter: str, queues: List[str] = None, workers: List[str] = None, local_db: bool = False):
        """
        Query the status of all currently running workers.

        Args:
            formatter: The worker formatter to use (rich or json).
            queues: List of queue names to filter by (optional).
            workers: List of worker names to filter by (optional).
            local_db: Whether to use the local database for querying (optional).
        """
        raise NotImplementedError("Subclasses of `MerlinWorkerHandler` must implement a `query_workers` method.")
