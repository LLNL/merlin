##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Defines an abstract base class for a single Merlin worker instance.

This module provides the `MerlinWorker` interface, which standardizes how individual
task server workers are defined, configured, and launched in the Merlin framework.
Each concrete implementation (e.g., for Celery or other task servers) must provide
logic for constructing the launch command, starting the process, and exposing worker metadata.

This abstraction allows Merlin to support multiple task execution backends while maintaining
a consistent interface for launching and managing worker processes.
"""

from abc import ABC, abstractmethod
from typing import Dict

from merlin.study.batch import BatchManager
from merlin.study.configurations import WorkerConfig


class MerlinWorker(ABC):
    """
    Abstract base class representing a single task server worker.

    This class defines the required interface for constructing and launching
    an individual worker based on its configuration.

    Attributes:
        worker_config (study.configurations.WorkerConfig): The worker configuration object.
        batch_manager (study.batch.BatchManager): A manager object for batch settings.

    Methods:
        get_launch_command: Build the shell command to launch the worker.
        start: Launch the worker process.
        get_metadata: Return identifying metadata about the worker.
    """

    def __init__(self, worker_config: WorkerConfig):
        """
        Initialize a `MerlinWorker` instance.

        Args:
            worker_config (study.configurations.WorkerConfig): The worker configuration object.
        """
        self.worker_config = worker_config

        # Initialize BatchManager for this worker
        self.batch_manager: BatchManager = BatchManager(self.worker_config.get_effective_batch_config())

    @abstractmethod
    def get_launch_command(self, override_args: str = "") -> str:
        """
        Build the command to launch this worker.

        Args:
            override_args: CLI arguments to override the default ones from the spec.

        Returns:
            A shell command string.
        """

    @abstractmethod
    def start(self):
        """
        Launch this worker.
        """

    def get_metadata(self) -> Dict:
        """
        Return metadata about this worker instance.

        Returns:
            A dictionary containing key details about this worker.
        """
        return self.worker_config.to_dict()
