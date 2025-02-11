"""
"""
from abc import ABC, abstractmethod
from typing import Any


class ResultsBackend(ABC):
    """
    """

    def __init__(self, backend_name: str):
        self.backend_name = backend_name

    @abstractmethod
    def get_entry(self, key: Any) -> Any:
        """
        Retrieve a key from the backend database.

        Args:
            key: The key to query from the backend database.
        """
        pass

    @abstractmethod
    def set_entry(self, key: Any, value: Any):
        """
        Set a key in the backend database.

        Args:
            key: The key to update.
            value: The value to update at `key`.
        """
        pass

    @abstractmethod
    def delete_entry(self, key: Any):
        """
        Delete a key/value pair from the backend database.

        Args:
            key: The key to delete from the backend database.
        """
        pass