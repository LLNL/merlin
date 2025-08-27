##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Factory for registering and instantiating Merlin worker handler implementations.

This module defines the `WorkerHandlerFactory`, which manages the lifecycle and registration
of supported task server worker handlers (e.g., Celery). It extends `MerlinBaseFactory` to
provide a pluggable architecture for loading handlers via entry points or direct registration.

The factory enforces type safety by validating that all registered components inherit from
`MerlinWorkerHandler`. It also provides aliasing support and a standard mechanism for plugin
discovery and instantiation.
"""

from typing import Any, Type

from merlin.abstracts import MerlinBaseFactory
from merlin.exceptions import MerlinWorkerHandlerNotSupportedError
from merlin.workers.handlers import CeleryWorkerHandler
from merlin.workers.handlers.worker_handler import MerlinWorkerHandler


class WorkerHandlerFactory(MerlinBaseFactory):
    """
    Factory class for managing and instantiating supported Merlin worker handlers.

    This subclass of `MerlinBaseFactory` handles registration, validation,
    and instantiation of worker handlers (e.g., Celery, Kafka).

    Attributes:
        _registry (Dict[str, MerlinWorkerHandler]): Maps canonical handler names to handler classes.
        _aliases (Dict[str, str]): Maps legacy or alternate names to canonical handler names.

    Methods:
        register: Register a new handler class and optional aliases.
        list_available: Return a list of supported handler names.
        create: Instantiate a handler class by name or alias.
        get_component_info: Return metadata about a registered handler.
    """

    def _register_builtins(self):
        """
        Register built-in worker handler implementations.
        """
        self.register("celery", CeleryWorkerHandler)

    def _validate_component(self, component_class: Any):
        """
        Ensure registered component is a subclass of MerlinWorkerHandler.

        Args:
            component_class: The class to validate.

        Raises:
            TypeError: If the component does not subclass MerlinWorkerHandler.
        """
        if not issubclass(component_class, MerlinWorkerHandler):
            raise TypeError(f"{component_class} must inherit from MerlinWorkerHandler")

    def _entry_point_group(self) -> str:
        """
        Entry point group used for discovering worker handler plugins.

        Returns:
            The entry point namespace for Merlin worker handler plugins.
        """
        return "merlin.workers.handlers"

    def _raise_component_error_class(self, msg: str) -> Type[Exception]:
        """
        Raise an appropriate exception when an invalid component is requested.

        Subclasses should override this to raise more specific exceptions.

        Args:
            msg: The message to add to the error being raised.

        Raises:
            A subclass of Exception (e.g., ValueError by default).
        """
        raise MerlinWorkerHandlerNotSupportedError(msg)


worker_handler_factory = WorkerHandlerFactory()
