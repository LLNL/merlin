"""
This module contains a backend factory class that acts as an abstraction
layer for loading a results backend.
"""

from typing import Dict

from merlin.backends.redis.redis_backend import RedisBackend
from merlin.backends.results_backend import ResultsBackend
from merlin.exceptions import BackendNotSupportedError


# TODO add register_backend call to this when we create task server interface?
# TODO could this factory replace the functions in config/results_backend.py?
# - Perhaps it should be a class outside of this?
class MerlinBackendFactory:
    """
    This class keeps track of all available backends for Merlin.

    Attributes:
        _backends (Dict[str, backends.results_backend.ResultsBackend]): A dictionary
            of supported backends and their associated classes.

    Methods:
        get_backend: Obtain an instantiation of the backend that's requested.
    """

    def __init__(self):
        # Map canonical backend names to their classes
        self._backends: Dict[str, ResultsBackend] = {"redis": RedisBackend}
        # Map aliases to canonical backend names
        self._backend_aliases: Dict[str, str] = {"rediss": "redis"}

    def get_supported_backends(self):
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
