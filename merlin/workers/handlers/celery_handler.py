##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Provides a concrete implementation of the
[`MerlinWorkerHandler`][workers.handlers.worker_handler.MerlinWorkerHandler] for Celery.

This module defines the `CeleryWorkerHandler` class, which is responsible for launching,
stopping, and querying Celery-based worker processes. It supports additional options
such as echoing launch commands, overriding default worker arguments, and disabling logs.
"""

import logging
from typing import Dict, List

from celery import Celery

from merlin.common.enums import WorkerStatus
from merlin.db_scripts.entities.logical_worker_entity import LogicalWorkerEntity
from merlin.db_scripts.merlin_db import MerlinDatabase
from merlin.workers import CeleryWorker
from merlin.workers.formatters.formatter_factory import worker_formatter_factory
from merlin.workers.handlers.worker_handler import MerlinWorkerHandler


LOG = logging.getLogger("merlin")


class CeleryWorkerHandler(MerlinWorkerHandler):
    """
    Worker handler for launching and managing Celery-based Merlin workers.

    This class implements the abstract methods defined in
    [`MerlinWorkerHandler`][workers.handlers.worker_handler.MerlinWorkerHandler] to provide
    Celery-specific behavior, including launching workers with optional command-line overrides,
    stopping workers, and querying their status.

    Attributes:
        merlin_db (MerlinDatabase): The database instance used for worker management.

    Methods:
        start_workers: Launch or echo Celery workers with optional arguments.
        stop_workers: Attempt to stop active Celery workers.
        query_workers: Return a basic summary of Celery worker status.
    """

    def __init__(self, merlin_db: MerlinDatabase = None, app: Celery = None):
        super().__init__(merlin_db=merlin_db)
        if app is None:
            from merlin.celery import app  # pylint: disable=import-outside-toplevel
        self.app = app

    def start_workers(self, workers: List[CeleryWorker], **kwargs):
        """
        Launch or echo Celery workers with optional override behavior.

        Args:
            workers (List[CeleryWorker]): Workers to launch.
            **kwargs:
                - echo_only (bool): If True, print the launch command instead of running it.
                - override_args (str): Arguments to override default worker args.
                - disable_logs (bool): If True, disables logging during worker launch.
        """
        echo_only = kwargs.get("echo_only", False)
        override_args = kwargs.get("override_args", "")
        disable_logs = kwargs.get("disable_logs", False)

        # Launch the workers or echo out the command that will be used to launch the workers
        for worker in workers:
            if echo_only:
                LOG.debug(f"Not launching worker '{worker.name}', just echoing command.")
                launch_cmd = worker.get_launch_command(override_args=override_args, disable_logs=disable_logs)
                print(launch_cmd)
            else:
                LOG.debug(f"Launching worker '{worker.name}'.")
                worker.start(override_args=override_args, disable_logs=disable_logs)

    def stop_workers(self):
        """
        Attempt to stop Celery workers.
        """

    def get_workers_from_app(self) -> List[str]:
        """
        Retrieve a list of all workers connected to the Celery application.

        This method uses the Celery control interface to inspect the current state
        of the application and returns a list of workers that are currently connected.
        If no workers are found, an empty list is returned.

        Args:
            app: The Celery application instance.

        Returns:
            A list of worker names that are currently connected to the Celery application.
                If no workers are connected, an empty list is returned.
        """
        i = self.app.control.inspect()
        workers = i.ping()
        if workers is None:
            return []
        return [*workers]

    def get_active_workers(self) -> Dict[str, List[str]]:
        """
        Retrieve a mapping of active workers to their associated queues for a Celery application.

        This function serves as the inverse of
        [`get_active_celery_queues()`][study.celeryadapter.get_active_celery_queues]. It constructs
        a dictionary where each key is a worker's name and the corresponding value is a
        list of queues that the worker is connected to. This allows for easy identification
        of which queues are being handled by each worker.

        Returns:
            A dictionary mapping active worker names to lists of queue names they are
                attached to. If no active workers are found, an empty dictionary is returned.
        """
        # Get the information we need from celery
        i = self.app.control.inspect()
        active_workers = i.active_queues()
        if active_workers is None:
            active_workers = {}

        # Build the mapping dictionary
        worker_queue_map = {}
        for worker, queues in active_workers.items():
            for queue in queues:
                if worker in worker_queue_map:
                    worker_queue_map[worker].append(queue["name"])
                else:
                    worker_queue_map[worker] = [queue["name"]]

        return worker_queue_map

    def _build_filters(self, queues: List[str], workers: List[str]) -> Dict[str, List[str]]:
        """
        Build filters dictionary for database queries.

        Args:
            queues: List of queue names to filter by.
            workers: List of worker names to filter by.

        Returns:
            Dictionary containing filter criteria.
        """
        filters = {}
        if queues:
            filters["queues"] = [queue if queue.startswith("[merlin]_") else f"[merlin]_{queue}" for queue in queues]
        if workers:
            filters["name"] = workers
        return filters

    def _validate_worker_status(self, logical_workers: List[LogicalWorkerEntity]):
        """
        Cross-check database state with live Celery workers.
        Update status for workers that are actually dead but marked running.

        Args:
            logical_workers: List of logical worker entities to validate.
        """
        # Get actual running workers from Celery
        live_workers = self.get_active_workers()  # Uses Celery inspection

        for logical_worker in logical_workers:
            physical_ids = logical_worker.get_physical_workers()
            for pid in physical_ids:
                physical = self.merlin_db.get("physical_worker", pid)

                # If database says running but Celery doesn't know about it
                if physical.get_worker_status() == WorkerStatus.RUNNING:
                    worker_name = physical.get_name()
                    if worker_name not in live_workers:
                        # Mark as stalled in database
                        LOG.warning(f"Worker {worker_name} marked running but not found in Celery")
                        physical.set_worker_status(WorkerStatus.STALLED)

    def query_workers(self, formatter: str, queues: List[str] = None, workers: List[str] = None):
        """
        Query the status of Celery workers and display using the configured formatter.

        Args:
            formatter: The worker formatter to use (rich or json).
            queues: List of queue names to filter by (optional).
            workers: List of worker names to filter by (optional).
        """
        # Build filters dictionary
        filters = self._build_filters(queues, workers)

        # Retrieve workers from database
        logical_workers = self.merlin_db.get_all("logical_worker", filters=filters)

        # Validate/enrich with live Celery data
        self._validate_worker_status(logical_workers)

        # Use formatter to display the results
        formatter = worker_formatter_factory.create(formatter)
        formatter.format_and_display(logical_workers, filters, self.merlin_db)
