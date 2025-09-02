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
from merlin.study.batch import batch_check_parallel, batch_worker_launch
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
        name (str): The name of the worker.
        config (dict): Configuration settings for the worker.
        env (dict): Environment variables used by the worker process.
        args (str): Additional CLI arguments passed to Celery.
        queues (List[str]): Queues the worker listens to.
        batch (dict): Optional batch submission settings.
        machines (List[str]): List of hostnames the worker is allowed to run on.
        overlap (bool): Whether this worker can overlap queues with others.

    Methods:
        _verify_args: Validate and adjust CLI args based on worker setup.
        get_launch_command: Construct the Celery launch command.
        should_launch: Determine whether the worker should be launched based on system state.
        start: Launch the worker using subprocess.
        get_metadata: Return identifying metadata about the worker.
    """

    def __init__(
        self,
        name: str,
        config: Dict,
        env: Dict[str, str] = None,
        overlap: bool = False,
    ):
        """
        Constructor for Celery workers.

        Sets up attributes used throughout this worker object and saves this worker to the database.

        Args:
            name: The name of the worker.
            config: A dictionary containing optional configuration settings for this worker including:\n
                - `args`: A string of arguments to pass to the launch command
                - `queues`: A set of task queues for this worker to watch
                - `batch`: A dictionary of specific batch configuration settings to use for this worker
                - `nodes`: The number of nodes to launch this worker on
                - `machines`: A list of machines that this worker is allowed to run on
            env: A dictionary of environment variables set by the user.
            overlap: If True multiple workers can pull tasks from overlapping queues.
        """
        super().__init__(name, config, env)
        self.args = self.config.get("args", "")
        self.queues = self.config.get("queues", {"[merlin]_merlin"})
        self.batch = self.config.get("batch", {})
        self.machines = self.config.get("machines", [])
        self.overlap = overlap

        # Add this worker to the database
        merlin_db = MerlinDatabase()
        merlin_db.create("logical_worker", self.name, self.queues)

    def _verify_args(self, disable_logs: bool = False) -> str:
        """
        Validate and modify the CLI arguments for the Celery worker.

        Adds concurrency and logging-related flags if necessary, and ensures
        the worker name is unique when overlap is allowed.

        Args:
            disable_logs: If True, logging level will not be appended.
        """
        if batch_check_parallel(self.batch):
            if "--concurrency" not in self.args:
                LOG.warning("Missing --concurrency in worker args for parallel tasks.")
            if "--prefetch-multiplier" not in self.args:
                LOG.warning("Missing --prefetch-multiplier in worker args for parallel tasks.")
            if "fair" not in self.args:
                LOG.warning("Missing -O fair in worker args for parallel tasks.")

        if "-n" not in self.args:
            nhash = time.strftime("%Y%m%d-%H%M%S") if self.overlap else ""
            self.args += f" -n {self.name}{nhash}.%%h"

        if not disable_logs and "-l" not in self.args:
            self.args += f" -l {logging.getLevelName(LOG.getEffectiveLevel())}"

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
            self.args = override_args

        # Validate args
        self._verify_args(disable_logs=disable_logs)

        # Construct the launch command
        celery_cmd = f"celery -A merlin worker {self.args} -Q {','.join(self.queues)}"
        nodes = self.batch.get("nodes", None)
        launch_cmd = batch_worker_launch(self.batch, celery_cmd, nodes=nodes)
        return os.path.expandvars(launch_cmd)

    def should_launch(self) -> bool:
        """
        Determine whether this worker should be launched.

        Performs checks on allowed machines and queue overlap (if applicable).

        Returns:
            True if the worker should be launched, False otherwise.
        """
        machines = self.config.get("machines", None)
        queues = self.config.get("queues", ["[merlin]_merlin"])

        if machines:
            if not check_machines(machines):
                LOG.error(
                    f"The following machines were provided for worker '{self.name}': {machines}. "
                    f"However, the current machine '{socket.gethostname()}' is not in this list."
                )
                return False

            output_path = self.env.get("OUTPUT_PATH")
            if output_path and not os.path.exists(output_path):
                LOG.error(f"{output_path} not accessible on host {socket.gethostname()}")
                return False

        if not self.overlap:
            from merlin.study.celeryadapter import get_running_queues  # pylint: disable=import-outside-toplevel

            running_queues = get_running_queues("merlin")
            for queue in queues:
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
                subprocess.Popen(launch_cmd, env=self.env, shell=True, universal_newlines=True)  # pylint: disable=R1732
                LOG.debug(f"Launched worker '{self.name}' with command: {launch_cmd}.")
            except Exception as e:  # pylint: disable=C0103
                LOG.error(f"Cannot start celery workers, {e}")
                raise MerlinWorkerLaunchError from e

    def get_metadata(self) -> Dict:
        """
        Return metadata about this worker instance.

        Returns:
            A dictionary containing key details about this worker.
        """
        return {
            "name": self.name,
            "queues": self.queues,
            "args": self.args,
            "machines": self.machines,
            "batch": self.batch,
        }
