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

import os
from abc import ABC, abstractmethod
from typing import Dict


class MerlinWorker(ABC):
    """
    Abstract base class representing a single task server worker.

    This class defines the required interface for constructing and launching
    an individual worker based on its configuration.

    Attributes:
        name: The name of the worker.
        config: The dictionary configuration for the worker.
        env: A dictionary representing the full environment for the current context.

    Methods:
        get_launch_command: Build the shell command to launch the worker.
        start: Launch the worker process.
        get_metadata: Return identifying metadata about the worker.
    """

    def __init__(self, name: str, config: Dict, env: Dict[str, str] = None):
        """
        Initialize a `MerlinWorker` instance.

        Args:
            name: The name of the worker.
            config: A dictionary containing the worker configuration.
            env: Optional dictionary of environment variables to use; if not provided,
                a copy of the current OS environment is used.
        """
        self.name = name
        self.config = config
        self.env = env or os.environ.copy()

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

    @abstractmethod
    def get_metadata(self) -> Dict:
        """
        Return a dictionary of metadata about this worker (for logging/debugging).

        Returns:
            A metadata dictionary (e.g., name, queues, machines).
        """
