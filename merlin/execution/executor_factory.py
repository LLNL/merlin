##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Task executor factory for selecting and instantiating task executors in Merlin.

This module defines the `MerlinExecutorFactory` class, which serves as an abstraction
layer for managing available task executor implementations. It supports dynamic selection
and instantiation of task executor handlers such as Celery or local runs, based on user input
or system configuration.

The factory maintains mappings of task executor names and aliases, and raises a clear error
if an unsupported task executor is requested.
"""

from typing import Any, Type

from merlin.abstracts import MerlinBaseFactory
from merlin.execution.base import TaskExecutor
from merlin.execution.celery import CeleryExecutor
from merlin.execution.local import LocalExecutor
from merlin.exceptions import TaskExecutorNotSupportedError


class MerlinExecutorFactory(MerlinBaseFactory):
    """
    Factory class for managing and instantiating supported Merlin task executors.

    This subclass of `MerlinBaseFactory` handles registration, validation,
    and instantiation of task executors (e.g., Celery, Local).

    Attributes:
        _registry (Dict[str, TaskExecutor]): Maps canonical task executor names to task executor classes.
        _aliases (Dict[str, str]): Maps legacy or alternate names to canonical task executor names.

    Methods:
        register: Register a new task executor class and optional aliases.
        list_available: Return a list of supported task executor names.
        create: Instantiate a task executor class by name or alias.
        get_component_info: Return metadata about a registered task executor.
    """

    def _register_builtins(self):
        """
        Register built-in task executor implementations.
        """
        self.register("celery", CeleryExecutor)
        self.register("local", LocalExecutor)

    def _validate_component(self, component_class: Any):
        """
        Ensure registered component is a subclass of TaskExecutor.

        Args:
            component_class: The class to validate.

        Raises:
            TypeError: If the component does not subclass TaskExecutor.
        """
        if not issubclass(component_class, TaskExecutor):
            raise TypeError(f"{component_class} must inherit from TaskExecutor")

    def _entry_point_group(self) -> str:
        """
        Entry point group used for discovering task executor plugins.

        Returns:
            The entry point namespace for Merlin task executor plugins.
        """
        return "merlin.execution"

    def _raise_component_error_class(self, msg: str) -> Type[Exception]:
        """
        Raise an appropriate exception for unsupported components.

        This method is used by the base factory logic to determine which
        exception to raise when a requested component is not found or fails
        to initialize.

        Args:
            msg: The message to add to the error being raised.

        Returns:
            The exception class to raise.
        """
        raise TaskExecutorNotSupportedError(msg)


executor_factory = MerlinExecutorFactory()