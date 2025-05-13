"""
This module contains the base class for all supported
backends in Merlin.
"""

from abc import ABC, abstractmethod
from typing import List

from merlin.db_scripts.data_models import BaseDataModel


class ResultsBackend(ABC):
    """
    Abstract base class for a results backend, which provides methods to save and retrieve
    information from a backend database.

    This class defines the interface that must be implemented by any concrete backend, and serves
    as the foundation for interacting with various backend types such as Redis, PostgreSQL, etc.

    Attributes:
        backend_name: The name of the backend (e.g., "redis", "postgresql").

    Methods:
        get_name:
            Retrieve the name of the backend.

        get_version:
            Query the backend for the current version.

        get_connection_string:
            Retrieve the connection string used to connect to the backend.

        flush_database:
            Remove every entry in the database.

        save:
            Save an entity (e.g., a study, run, or worker) to the backend database.

        retrieve:
            Retrieve an entity from the backend database using its identifier and store type.

        retrieve_all:
            Retrieve all entities of a specific type from the backend database.

        delete:
            Delete an entity from the backend database using its identifier and store type.
    """

    def __init__(self, backend_name: str):
        """
        Initialize the `ResultsBackend` instance.

        Args:
            backend_name: The name of the backend (e.g., "redis").
        """
        self.backend_name: str = backend_name

    def get_name(self) -> str:
        """
        Get the name of the backend.

        Returns:
            The name of the backend (e.g. redis).
        """
        return self.backend_name

    @abstractmethod
    def get_version(self) -> str:
        """
        Query the backend for the current version.

        Returns:
            A string representing the current version of the backend.
        """
        raise NotImplementedError("Subclasses of `ResultsBackend` must implement a `get_version` method.")

    @abstractmethod
    def get_connection_string(self) -> str:
        """
        Query the backend for the connection string.

        Returns:
            A string representing the connection to the backend.
        """
        raise NotImplementedError("Subclasses of `ResultsBackend` must implement a `get_connection_string` method.")

    @abstractmethod
    def flush_database(self):
        """
        Remove everything stored in the database.
        """
        raise NotImplementedError("Subclasses of `ResultsBackend` must implement a `flush_database` method.")

    @abstractmethod
    def save(self, entity: BaseDataModel):
        """
        Save a `BaseDataModel` object to the backend database.

        Args:
            entity (BaseDataModel): An instance of one of `BaseDataModel`'s inherited classes.

        Raises:
            UnsupportedDataModelError: If the entity type is unsupported.
        """
        raise NotImplementedError("Subclasses of `ResultsBackend` must implement a `save` method.")

    @abstractmethod
    def retrieve(self, entity_identifier: str, store_type: str) -> BaseDataModel:
        """
        Retrieve an object from the appropriate store based on the given query identifier and store type.

        Args:
            entity_identifier (str): The identifier used to query the store.
            store_type (str): The type of store to query. Valid options are:
                - `study`
                - `run`
                - `logical_worker`
                - `physical_worker`

        Returns:
            BaseDataModel: The object retrieved from the specified store.

        Raises:
            ValueError: If the `store_type` is invalid.
        """
        raise NotImplementedError("Subclasses of `ResultsBackend` must implement a `retrieve` method.")

    @abstractmethod
    def retrieve_all(self, store_type: str) -> List[BaseDataModel]:
        """
        Retrieve all objects from the specified store.

        Args:
            store_type (str): The type of store to query. Valid options are:
                - `study`
                - `run`
                - `logical_worker`
                - `physical_worker`

        Returns:
            List[BaseDataModel]: A list of objects retrieved from the specified store.

        Raises:
            ValueError: If the `store_type` is invalid.
        """
        raise NotImplementedError("Subclasses of `ResultsBackend` must implement a `retrieve_all` method.")

    @abstractmethod
    def delete(self, entity_identifier: str, store_type: str):
        """
        Delete an entity from the specified store.

        Args:
            entity_identifier (str): The identifier of the entity to delete.
            store_type (str): The type of store to query. Valid options are:
                - `study`
                - `run`
                - `logical_worker`
                - `physical_worker`

        Raises:
            ValueError: If the `store_type` is invalid.
        """
        raise NotImplementedError("Subclasses of `ResultsBackend` must implement a `delete` method.")
