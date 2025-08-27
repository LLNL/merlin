##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Factory for registering and instantiating individual Merlin worker implementations.

This module defines the `WorkerFactory`, a subclass of
[`MerlinBaseFactory`][abstracts.factory.MerlinBaseFactory], which manages
the registration, validation, and creation of concrete worker classes such as
[`CeleryWorker`][workers.celery_worker.CeleryWorker]. It supports plugin-based discovery
via Python entry points, enabling extensibility for other task server backends (e.g., Kafka).

The factory ensures that all registered components conform to the `MerlinWorker` interface
and provides useful utilities such as aliasing and error handling for unsupported components.
"""

from typing import Any, Type

from merlin.abstracts import MerlinBaseFactory
from merlin.exceptions import MerlinWorkerNotSupportedError
from merlin.workers import CeleryWorker
from merlin.workers.worker import MerlinWorker


class WorkerFactory(MerlinBaseFactory):
    """
    Factory class for managing and instantiating supported Merlin workers.

    This subclass of `MerlinBaseFactory` handles registration, validation,
    and instantiation of workers (e.g., Celery, Kafka).

    Attributes:
        _registry (Dict[str, MerlinWorker]): Maps canonical worker names to worker classes.
        _aliases (Dict[str, str]): Maps legacy or alternate names to canonical worker names.

    Methods:
        register: Register a new worker class and optional aliases.
        list_available: Return a list of supported worker names.
        create: Instantiate a worker class by name or alias.
        get_component_info: Return metadata about a registered worker.
    """

    def _register_builtins(self):
        """
        Register built-in worker implementations.
        """
        self.register("celery", CeleryWorker)

    def _validate_component(self, component_class: Any):
        """
        Ensure registered component is a subclass of MerlinWorker.

        Args:
            component_class: The class to validate.

        Raises:
            TypeError: If the component does not subclass MerlinWorker.
        """
        if not issubclass(component_class, MerlinWorker):
            raise TypeError(f"{component_class} must inherit from MerlinWorker")

    def _entry_point_group(self) -> str:
        """
        Entry point group used for discovering worker plugins.

        Returns:
            The entry point namespace for Merlin worker plugins.
        """
        return "merlin.workers"

    def _raise_component_error_class(self, msg: str) -> Type[Exception]:
        """
        Raise an appropriate exception when an invalid component is requested.

        Subclasses should override this to raise more specific exceptions.

        Args:
            msg: The message to add to the error being raised.

        Raises:
            A subclass of Exception (e.g., ValueError by default).
        """
        raise MerlinWorkerNotSupportedError(msg)


worker_factory = WorkerFactory()
