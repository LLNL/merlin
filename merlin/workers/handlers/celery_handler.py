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

    Methods:
        launch_workers: Launch or echo Celery workers with optional arguments.
        stop_workers: Attempt to stop active Celery workers.
        query_workers: Return a basic summary of Celery worker status.
    """

    def __init__(self):
        """ """
        super().__init__()
        self.merlin_db = MerlinDatabase()

    def launch_workers(self, workers: List[CeleryWorker], **kwargs):
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
                worker.launch_worker(override_args=override_args, disable_logs=disable_logs)

    def stop_workers(self):
        """
        Attempt to stop Celery workers.
        """

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
            filters["queues"] = queues
        if workers:
            filters["name"] = workers
        return filters

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

        # Use formatter to display the results
        formatter = worker_formatter_factory.create(formatter)
        formatter.format_and_display(logical_workers, filters, self.merlin_db)
