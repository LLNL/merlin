##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Worker formatter factory for Merlin.

This module provides the `WorkerFormatterFactory`, a central registry and
factory class for managing supported worker formatter implementations.
It allows clients to create worker formatters by name or alias, ensuring
consistent handling of different output formats (e.g., JSON, Rich).
"""

from typing import Any, Type

from merlin.abstracts import MerlinBaseFactory
from merlin.exceptions import MerlinWorkerFormatterNotSupportedError
from merlin.workers.formatters.json_formatter import JSONWorkerFormatter
from merlin.workers.formatters.rich_formatter import RichWorkerFormatter
from merlin.workers.formatters.worker_formatter import WorkerFormatter


class WorkerFormatterFactory(MerlinBaseFactory):
    """
    Factory class for managing and instantiating supported Merlin worker formatters.

    This subclass of `MerlinBaseFactory` handles registration, validation,
    and instantiation of worker formatters (e.g., rich, json).

    Attributes:
        _registry (Dict[str, WorkerFormatter]): Maps canonical formatter names to formatter classes.
        _aliases (Dict[str, str]): Maps legacy or alternate names to canonical formatter names.

    Methods:
        register: Register a new formatter class and optional aliases.
        list_available: Return a list of supported formatter names.
        create: Instantiate a formatter class by name or alias.
        get_component_info: Return metadata about a registered formatter.
    """

    def _register_builtins(self):
        """
        Register built-in worker formatter implementations.
        """
        self.register("json", JSONWorkerFormatter)
        self.register("rich", RichWorkerFormatter)

    def _validate_component(self, component_class: Any):
        """
        Ensure registered component is a subclass of WorkerFormatter.

        Args:
            component_class: The class to validate.

        Raises:
            TypeError: If the component does not subclass WorkerFormatter.
        """
        if not issubclass(component_class, WorkerFormatter):
            raise TypeError(f"{component_class} must inherit from WorkerFormatter")

    def _entry_point_group(self) -> str:
        """
        Entry point group used for discovering worker formatter plugins.

        Returns:
            The entry point namespace for Merlin worker formatter plugins.
        """
        return "merlin.workers.formatters"

    def _raise_component_error_class(self, msg: str) -> Type[Exception]:
        """
        Raise an appropriate exception when an invalid component is requested.

        Subclasses should override this to raise more specific exceptions.

        Args:
            msg: The message to add to the error being raised.

        Raises:
            A subclass of Exception (e.g., ValueError by default).
        """
        raise MerlinWorkerFormatterNotSupportedError(msg)


worker_formatter_factory = WorkerFormatterFactory()
