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

from typing import Dict, List

from merlin.backends.redis.redis_backend import RedisBackend
from merlin.backends.results_backend import ResultsBackend
from merlin.backends.sqlite.sqlite_backend import SQLiteBackend
from merlin.exceptions import BackendNotSupportedError


# TODO add register_backend call to this when we create task server interface?
# TODO could this factory replace the functions in config/results_backend.py?
# - Perhaps it should be a class outside of this?
class MerlinBackendFactory:
    """
    Factory class for managing and instantiating supported Merlin backends.

    This class maintains a registry of available results backends (e.g., Redis, SQLite)
    and provides a unified interface for retrieving a backend implementation by name.

    Attributes:
        _backends (Dict[str, backends.results_backend.ResultsBackend]): Mapping of backend names to their classes.
        _backend_aliases (Dict[str, str]): Optional aliases for resolving canonical backend names.

    Methods:
        get_supported_backends: Returns a list of supported backend names.
        get_backend: Instantiates and returns the backend associated with the given name.
    """

    def __init__(self):
        """Initialize the Merlin backend factory."""
        # Map canonical backend names to their classes
        self._backends: Dict[str, ResultsBackend] = {
            "redis": RedisBackend,
            "sqlite": SQLiteBackend,
        }
        # Map aliases to canonical backend names
        self._backend_aliases: Dict[str, str] = {"rediss": "redis"}

    def get_supported_backends(self) -> List[str]:
        """
        Get a list of the supported backends in Merlin.

        Returns:
            A list of names representing the supported backends in Merlin.
        """
        return list(self._backends.keys())

    def get_backend(self, backend: str) -> ResultsBackend:
        """
        Get backend handler for whichever backend the user is using.

        Args:
            backend: The name of the backend to load up.

        Returns:
            An instantiation of a [`ResultsBackend`][backends.results_backend.ResultsBackend] object.

        Raises:
            (exceptions.BackendNotSupportedError): If the requested backend is not supported.
        """
        # Resolve the alias to the canonical backend name
        backend = self._backend_aliases.get(backend, backend)

        # Get the correct backend class
        backend_object = self._backends.get(backend)

        if backend_object is None:
            raise BackendNotSupportedError(f"Backend unsupported by Merlin: {backend}.")

        return backend_object(backend)


backend_factory = MerlinBackendFactory()
