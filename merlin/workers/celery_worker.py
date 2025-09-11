##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Implements a Celery-based MerlinWorker.

This module defines the `CeleryWorker` class, which extends the abstract
`MerlinWorker` base class to implement worker launching and management using
Celery. Celery workers are responsible for processing tasks from specified queues
and can be launched either locally or through a batch system.
"""

import logging
import os
import socket
import subprocess
import time
from typing import Dict

from merlin.db_scripts.merlin_db import MerlinDatabase
from merlin.exceptions import MerlinWorkerLaunchError
from merlin.study.configurations import WorkerConfig
from merlin.utils import check_machines
from merlin.workers.worker import MerlinWorker


LOG = logging.getLogger("merlin")


class CeleryWorker(MerlinWorker):
    """
    Concrete implementation of a single Celery-based Merlin worker.

    This class provides logic for validating configuration, constructing launch
    commands, checking launch eligibility, and launching Celery workers that process
    jobs from specific task queues.

    Attributes:
        worker_config (study.configurations.WorkerConfig): The worker configuration object.
        batch_manager (study.batch.BatchManager): A manager object for batch settings.

    Methods:
        _verify_args: Validate and adjust CLI args based on worker setup.
        get_launch_command: Construct the Celery launch command.
        should_launch: Determine whether the worker should be launched based on system state.
        start: Launch the worker using subprocess.
        get_metadata: Return identifying metadata about the worker.
    """

    def __init__(self, worker_config: WorkerConfig):
        """
        Constructor for Celery workers.

        Sets up attributes used throughout this worker object and saves this worker to the database.

        Args:
            worker_config (study.configurations.WorkerConfig): The worker configuration object.
        """
        super().__init__(worker_config)

        # TODO might want to move the below line to the base class? With other task servers
        # we need to see what would be important to store and maybe refactor logical worker entries
        # Add this worker to the database
        merlin_db = MerlinDatabase()
        merlin_db.create("logical_worker", self.worker_config.name, self.worker_config.queues)

    def _verify_args(self, disable_logs: bool = False) -> str:
        """
        Validate and modify the CLI arguments for the Celery worker.

        Adds concurrency and logging-related flags if necessary, and ensures
        the worker name is unique when overlap is allowed.

        Args:
            disable_logs: If True, logging level will not be appended.
        """
        # Use BatchManager to check for parallel configuration
        if self.batch_manager.is_parallel():
            if "--concurrency" not in self.worker_config.args:
                LOG.warning("Missing --concurrency in worker args for parallel tasks.")
            if "--prefetch-multiplier" not in self.worker_config.args:
                LOG.warning("Missing --prefetch-multiplier in worker args for parallel tasks.")
            if "fair" not in self.worker_config.args:
                LOG.warning("Missing -O fair in worker args for parallel tasks.")

        if "-n" not in self.worker_config.args:
            nhash = time.strftime("%Y%m%d-%H%M%S") if self.worker_config.overlap else ""
            self.worker_config.args += f" -n {self.worker_config.name}{nhash}.%%h"

        if not disable_logs and "-l" not in self.worker_config.args:
            self.worker_config.args += f" -l {logging.getLevelName(LOG.getEffectiveLevel())}"

    def get_launch_command(self, override_args: str = "", disable_logs: bool = False) -> str:
        """
        Construct the shell command to launch this Celery worker.

        Args:
            override_args: If provided, these arguments will replace the default `args`.
            disable_logs: If True, logging level will not be added to the command.

        Returns:
            A shell command string suitable for subprocess execution.
        """
        # Override existing arguments if necessary
        if override_args != "":
            self.worker_config.args = override_args

        # Validate args
        self._verify_args(disable_logs=disable_logs)

        # Construct the base celery command
        celery_cmd = f"celery -A merlin worker {self.worker_config.args} -Q {','.join(self.worker_config.queues)}"
        
        # Use BatchManager to create the launch command
        launch_cmd = self.batch_manager.create_worker_launch_command(celery_cmd)
        
        return os.path.expandvars(launch_cmd)

    def should_launch(self) -> bool:
        """
        Determine whether this worker should be launched.

        Performs checks on allowed machines and queue overlap (if applicable).

        Returns:
            True if the worker should be launched, False otherwise.
        """
        if self.worker_config.machines:
            if not check_machines(self.worker_config.machines):
                LOG.error(
                    f"The following machines were provided for worker '{self.worker_config.name}': {self.worker_config.machines}. "
                    f"However, the current machine '{socket.gethostname()}' is not in this list."
                )
                return False

            output_path = self.worker_config.env.get("OUTPUT_PATH")
            if output_path and not os.path.exists(output_path):
                LOG.error(f"{output_path} not accessible on host {socket.gethostname()}")
                return False

        if not self.worker_config.overlap:
            from merlin.study.celeryadapter import get_running_queues  # pylint: disable=import-outside-toplevel

            running_queues = get_running_queues("merlin")
            for queue in self.worker_config.queues:
                if queue in running_queues:
                    LOG.warning(f"Queue {queue} is already being processed by another worker.")
                    return False

        return True

    def start(self, override_args: str = "", disable_logs: bool = False):
        """
        Launch the worker as a subprocess using the constructed launch command.

        Args:
            override_args: Optional CLI arguments to override the default worker args.
            disable_logs: If True, suppresses automatic logging level injection.

        Raises:
            MerlinWorkerLaunchError: If the worker fails to launch.
        """
        if self.should_launch():
            launch_cmd = self.get_launch_command(override_args=override_args, disable_logs=disable_logs)
            try:
                subprocess.Popen(launch_cmd, env=self.worker_config.env, shell=True, universal_newlines=True)  # pylint: disable=R1732
                LOG.debug(f"Launched worker '{self.worker_config.name}' with command: {launch_cmd}.")
            except Exception as e:  # pylint: disable=C0103
                LOG.error(f"Cannot start celery workers, {e}")
                raise MerlinWorkerLaunchError from e
        
    def update_batch_config(self, new_batch_config: Dict):
        """
        Update the batch configuration for this worker.
        
        Args:
            new_batch_config: New batch configuration to apply.
        """
        self.batch_manager.update_config(new_batch_config)
