##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module provides a factory class to manage and retrieve task server monitors
for supported task servers in Merlin.
"""

from typing import Any

from merlin.abstracts import MerlinBaseFactory
from merlin.exceptions import MerlinInvalidTaskServerError
from merlin.monitor.celery_monitor import CeleryMonitor
from merlin.monitor.task_server_monitor import TaskServerMonitor


# class MonitorFactory:
#     """
#     A factory class for managing and retrieving task server monitors
#     for supported task servers in Merlin.

#     Attributes:
#         _monitors (Dict[str, TaskServerMonitor]): A dictionary mapping task server names
#             to their corresponding monitor classes.

#     Methods:
#         get_supported_task_servers: Get a list of the supported task servers in Merlin.
#         get_monitor: Get the monitor instance for the specified task server.
#     """

#     def __init__(self):
#         """
#         Initialize the `MonitorFactory` with the supported task server monitors.
#         """
#         self._monitors: Dict[str, TaskServerMonitor] = {
#             "celery": CeleryMonitor,
#         }

#     def get_supported_task_servers(self) -> List[str]:
#         """
#         Get a list of the supported task servers in Merlin.

#         Returns:
#             A list of names representing the supported task servers in Merlin.
#         """
#         return list(self._monitors.keys())

#     def get_monitor(self, task_server: str) -> TaskServerMonitor:
#         """
#         Get the task server monitor for whichever task server the user is utilizing.

#         Args:
#             task_server: The name of the task server to use when loading a task server monitor.

#         Returns:
#             An instantiated [`TaskServerMonitor`][monitor.task_server_monitor.TaskServerMonitor]
#                 object for the specified task server.

#         Raises:
#             MerlinInvalidTaskServerError: If the requested task server is not supported.
#         """
#         monitor_object = self._monitors.get(task_server, None)

#         if monitor_object is None:
#             raise MerlinInvalidTaskServerError(
#                 f"Task server unsupported by Merlin: {task_server}. "
#                 "Supported task servers are: {self.get_supported_task_servers()}"
#             )

#         return monitor_object()

class MonitorFactory(MerlinBaseFactory):
    """
    
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
    
    def _get_component_error_class(self) -> type[Exception]:
        """
        Return the exception type to raise for unsupported components.

        This method is used by the base factory logic to determine which
        exception to raise when a requested component is not found or fails
        to initialize.

        Returns:
            The exception class to raise.
        """
        return MerlinInvalidTaskServerError


monitor_factory = MonitorFactory()
