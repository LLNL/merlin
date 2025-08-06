##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Backend factory for selecting and instantiating results backends in Merlin.

This module defines the `MerlinBackendFactory` class, which serves as an abstraction
layer for managing available backend implementations. It supports dynamic selection
and instantiation of backend handlers such as Redis or SQLite, based on user input
or system configuration.

The factory maintains mappings of backend names and aliases, and raises a clear error
if an unsupported backend is requested.
"""

from typing import Any, Type

from merlin.abstracts import MerlinBaseFactory
from merlin.backends.redis.redis_backend import RedisBackend
from merlin.backends.results_backend import ResultsBackend
from merlin.backends.sqlite.sqlite_backend import SQLiteBackend
from merlin.exceptions import BackendNotSupportedError


# TODO could this factory replace the functions in config/results_backend.py?
# - Perhaps it should be a class outside of this?
class MerlinBackendFactory(MerlinBaseFactory):
    """
    Factory class for managing and instantiating supported Merlin backends.

    This subclass of `MerlinBaseFactory` handles registration, validation,
    and instantiation of results backends (e.g., Redis, SQLite).

    Attributes:
        _registry (Dict[str, ResultsBackend]): Maps canonical backend names to backend classes.
        _aliases (Dict[str, str]): Maps legacy or alternate names to canonical backend names.

    Methods:
        register: Register a new backend class and optional aliases.
        list_available: Return a list of supported backend names.
        create: Instantiate a backend class by name or alias.
        get_component_info: Return metadata about a registered backend.
    """

    def _register_builtins(self):
        """
        Register built-in backend implementations.
        """
        self.register("redis", RedisBackend, aliases=["rediss"])
        self.register("sqlite", SQLiteBackend)

    def _validate_component(self, component_class: Any):
        """
        Ensure registered component is a subclass of ResultsBackend.

        Args:
            component_class: The class to validate.

        Raises:
            TypeError: If the component does not subclass ResultsBackend.
        """
        if not issubclass(component_class, ResultsBackend):
            raise TypeError(f"{component_class} must inherit from ResultsBackend")

    def _entry_point_group(self) -> str:
        """
        Entry point group used for discovering backend plugins.

        Returns:
            The entry point namespace for Merlin backend plugins.
        """
        return "merlin.backends"

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
        raise BackendNotSupportedError(msg)


backend_factory = MerlinBackendFactory()
