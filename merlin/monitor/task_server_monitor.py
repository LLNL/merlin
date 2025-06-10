##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module defines the `TaskServerMonitor` abstract base class, which serves as a common interface
for monitoring task servers. Task servers are responsible for managing the execution of tasks and
workers in distributed systems, and this class provides an abstraction for monitoring their health
and progress.

The `TaskServerMonitor` class is intended to be subclassed for specific task server implementations
(e.g., Celery, TaskVine). Subclasses must implement all abstract methods to provide task server-specific
functionality, such as waiting for workers and checking task queues.
"""

from abc import ABC, abstractmethod
from typing import List

from merlin.db_scripts.entities.run_entity import RunEntity


class TaskServerMonitor(ABC):
    """
    Abstract base class for monitoring task servers. This class defines the interface
    for monitoring tasks and workers for a specific task server (e.g., Celery, TaskVine).

    Subclasses must implement all abstract methods to provide specific functionality
    for their respective task server.

    Methods:
        wait_for_workers: Wait for workers to start up.
        check_workers_processing: Check if any workers are still processing tasks.
        restart_worker: Restart a dead worker.
        run_worker_health_check: Check the health of workers and restart any that are dead.
        check_tasks: Abstract method to check the status of tasks in a workflow run.
            Must be implemented by subclasses.
    """

    @abstractmethod
    def wait_for_workers(self, workers: List[str], sleep: int):  # TODO should workers list be worker names or IDs?
        """
        Wait for workers to start up.

        Args:
            workers: A list of worker names or IDs to wait for.
            sleep: The interval (in seconds) between checks for worker availability.

        Raises:
            NoWorkersException: When workers don't start in (`self.sleep` * 10) seconds.
        """

    @abstractmethod
    def check_workers_processing(self, queues: List[str]) -> bool:
        """
        Check if any workers are still processing tasks.

        Args:
            queues: A list of queue names to check for active tasks.

        Returns:
            True if workers are processing tasks in the specified queues, False otherwise.
        """

    @abstractmethod
    def run_worker_health_check(self, workers: List[str]):
        """
        Checks the health of the workers provided and restarts any that are dead.

        Args:
            workers: A list of workers to check for worker health.
        """

    @abstractmethod
    def check_tasks(self, run: RunEntity) -> bool:
        """
        Check the status of tasks in the given workflow run.

        Args:
            run: A [`RunEntity`][db_scripts.entities.run_entity.RunEntity] instance representing
                the workflow run whose tasks are being monitored.

        Returns:
            True if tasks are active in the workflow, False otherwise.
        """
