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
from typing import List

from merlin.workers import CeleryWorker
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
        start_workers: Launch or echo Celery workers with optional arguments.
        stop_workers: Attempt to stop active Celery workers.
        query_workers: Return a basic summary of Celery worker status.
    """

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

    def query_workers(self):
        """
        Query the status of Celery workers.
        """
