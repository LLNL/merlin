##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Abstract base class for results backends in the Merlin application.

This module defines `ResultsBackend`, an abstract base class that specifies
the required interface for backend implementations responsible for persisting
and retrieving data in Merlin.

The `ResultsBackend` class encapsulates:
- A unified interface for saving, retrieving, and deleting Merlin data models
- Store type routing for specific model categories (study, run, logical worker, physical worker)
- Support for backend-specific configuration such as version reporting and database flushing

Usage:
    This base class is not meant to be instantiated directly. Instead, it should be subclassed
    by backend-specific implementations such as `RedisBackend` or `SQLiteBackend`.
"""

import logging
import uuid
from abc import ABC, abstractmethod
from typing import Dict, List

from merlin.backends.store_base import StoreBase
from merlin.db_scripts.data_models import BaseDataModel, LogicalWorkerModel, PhysicalWorkerModel, RunModel, StudyModel
from merlin.exceptions import UnsupportedDataModelError


LOG = logging.getLogger(__name__)


class ResultsBackend(ABC):
    """
    Abstract base class for a results backend, which provides methods to save and retrieve
    information from a backend database.

    This class defines the interface that must be implemented by any concrete backend, and serves
    as the foundation for interacting with various backend types such as Redis, PostgreSQL, etc.

    Attributes:
        backend_name (str): The name of the backend (e.g., "redis", "postgresql").
        stores (Dict[str, backends.store_base.StoreBase]): A dictionary of stores that each concrete
            implementation of this class will need to define.

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

    def __init__(self, backend_name: str, stores: Dict[str, StoreBase]):
        """
        Initialize the `ResultsBackend` instance.

        Args:
            backend_name: The name of the backend (e.g., "redis").
        """
        self.backend_name: str = backend_name
        self.stores: Dict[str, StoreBase] = stores

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

    def get_connection_string(self) -> str:
        """
        Query the backend for the connection string.

        Returns:
            A string representing the connection to the backend.
        """
        from merlin.config.results_backend import get_connection_string  # pylint: disable=import-outside-toplevel

        return get_connection_string(include_password=False)

    @abstractmethod
    def flush_database(self):
        """
        Remove everything stored in the database.
        """
        raise NotImplementedError("Subclasses of `ResultsBackend` must implement a `flush_database` method.")

    def _get_store_by_type(self, store_type: str) -> StoreBase:
        """
        Get the appropriate store based on the store type.

        Args:
            store_type (str): The type of store.

        Returns:
            The corresponding store.

        Raises:
            ValueError: If the `store_type` is invalid.
        """
        if store_type not in self.stores:
            raise ValueError(f"Invalid store type '{store_type}'.")
        return self.stores[store_type]

    def _get_store_by_entity(self, entity: BaseDataModel) -> StoreBase:
        """
        Get the appropriate store based on the entity type.

        Args:
            entity (BaseDataModel): The entity to save.

        Returns:
            RedisStore: The corresponding store.

        Raises:
            UnsupportedDataModelError: If the entity type is unsupported.
        """
        if isinstance(entity, StudyModel):
            return self.stores["study"]
        if isinstance(entity, RunModel):
            return self.stores["run"]
        if isinstance(entity, LogicalWorkerModel):
            return self.stores["logical_worker"]
        if isinstance(entity, PhysicalWorkerModel):
            return self.stores["physical_worker"]
        raise UnsupportedDataModelError(f"Unsupported data model of type {type(entity)}.")

    def save(self, entity: BaseDataModel):
        """
        Save a `BaseDataModel` object to the Redis database.

        Args:
            entity (BaseDataModel): An instance of one of `BaseDataModel`'s inherited classes.

        Raises:
            UnsupportedDataModelError: If the entity type is unsupported.
        """
        store = self._get_store_by_entity(entity)
        store.save(entity)

    def retrieve(self, entity_identifier: str, store_type: str) -> BaseDataModel:
        """
        Retrieve an object from the appropriate store based on the given query identifier and store type.

        Args:
            entity_identifier (str): The identifier used to query the store, either an ID (UUID) or a name.
            store_type (str): The type of store to query. Valid options are:
                - `study`
                - `run`
                - `logical_worker`
                - `physical_worker`

        Returns:
            The object retrieved from the specified store.
        """
        LOG.debug(f"Retrieving '{entity_identifier}' from store '{store_type}'.")
        store = self._get_store_by_type(store_type)
        if store_type in ["study", "physical_worker", "run"]:
            try:
                uuid.UUID(entity_identifier)
                return store.retrieve(entity_identifier)
            except ValueError:
                return store.retrieve(entity_identifier, by_name=True)
        else:
            return store.retrieve(entity_identifier)

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
            A list of objects retrieved from the specified store.
        """
        store = self._get_store_by_type(store_type)
        return store.retrieve_all()

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
        """
        store = self._get_store_by_type(store_type)
        if store_type in ["study", "physical_worker"]:
            try:
                uuid.UUID(entity_identifier)
                store.delete(entity_identifier)
            except ValueError:
                store.delete(entity_identifier, by_name=True)
        else:
            store.delete(entity_identifier)
