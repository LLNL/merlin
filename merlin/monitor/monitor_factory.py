##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module provides a factory class to manage and retrieve task server monitors
for supported task servers in Merlin.
"""

from typing import Any, Type

from merlin.abstracts import MerlinBaseFactory
from merlin.exceptions import MerlinInvalidTaskServerError
from merlin.monitor.celery_monitor import CeleryMonitor
from merlin.monitor.task_server_monitor import TaskServerMonitor


class MonitorFactory(MerlinBaseFactory):
    """
    Factory class for managing and instantiating Merlin task server monitors.

    This subclass of `MerlinBaseFactory` is responsible for registering,
    validating, and creating instances of supported `TaskServerMonitor`
    implementations (e.g., `CeleryMonitor`). It also supports plugin-based
    extension via Python entry points.

    Responsibilities:
        - Register built-in task server monitors.
        - Validate that components conform to the `TaskServerMonitor` interface.
        - Support creation and introspection of registered monitor types.
        - Optionally discover external monitor plugins.

    Attributes:
        _registry (Dict[str, TaskServerMonitor]): Maps canonical task server names to monitor classes.
        _aliases (Dict[str, str]): Maps aliases to canonical monitor names.

    Methods:
        register: Register a new monitor class and optional aliases.
        list_available: Return a list of supported monitor names.
        create: Instantiate a monitor class by name or alias.
        get_component_info: Return metadata about a registered monitor.
    """

    def _register_builtins(self):
        """
        Register built-in monitor implementations.
        """
        self.register("celery", CeleryMonitor)

    def _validate_component(self, component_class: Any):
        """
        Ensure registered component is a subclass of TaskServerMonitor.

        Args:
            component_class: The class to validate.

        Raises:
            TypeError: If the component does not subclass TaskServerMonitor.
        """
        if not issubclass(component_class, TaskServerMonitor):
            raise TypeError(f"{component_class} must inherit from TaskServerMonitor")

    def _entry_point_group(self) -> str:
        """
        Entry point group used for discovering monitor plugins.

        Returns:
            The entry point namespace for Merlin monitor plugins.
        """
        return "merlin.monitor"

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
        raise MerlinInvalidTaskServerError(msg)


monitor_factory = MonitorFactory()
