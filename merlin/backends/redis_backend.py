"""
"""
from typing import Any

from redis import Redis

from merlin.backends.results_backend import ResultsBackend
from merlin.config.results_backend import get_connection_string


class RedisBackend(ResultsBackend):
    """

    TODO might be able to use this in place of RedisConnectionManager for manager
    TODO might be able to make these classes replace the config/results_backend.py file
    - would help get a more OOP approach going within Merlin's codebase
    - instead of calling get_connection_string that logic could be handled in the base class?
    """

    def __init__(self, backend_name: str):
        super().__init__(backend_name)
        self.client = Redis.from_url(url=get_connection_string())

    def get_entry(self, key: Any) -> Any:
        """
        Retrieve a key from the Redis database.

        Args:
            key: The key to query from the Redis database.

        Returns:
            The value associated with `key`.
        """
        return self.client.get(key)

    def set_entry(self, key: Any, value: Any):
        """
        Set a key in the Redis database.

        Args:
            key: The key to update.
            value: The value to update at `key`.
        """
        self.client.set(key, value)

    def delete_entry(self, key: Any):
        """
        Delete a key/value pair from the backend database.

        Args:
            key: The key to delete from the Redis database.
        """
        self.client.delete(key)