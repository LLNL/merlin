"""
This module contains the functionality required to interact with a
Redis backend.
"""

import logging
import uuid
from typing import Dict, List

from redis import Redis

from merlin.backends.redis.redis_logical_worker_store import RedisLogicalWorkerStore
from merlin.backends.redis.redis_physical_worker_store import RedisPhysicalWorkerStore
from merlin.backends.redis.redis_run_store import RedisRunStore
from merlin.backends.redis.redis_study_store import RedisStudyStore
from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.data_models import BaseDataModel, LogicalWorkerModel, PhysicalWorkerModel, RunModel, StudyModel
from merlin.exceptions import UnsupportedDataModelError


LOG = logging.getLogger("merlin")


# TODO might be able to make ResultsBackend classes replace the config/results_backend.py file
# - would help get a more OOP approach going within Merlin's codebase
# - instead of calling get_connection_string that logic could be handled in the base class?
class RedisBackend(ResultsBackend):
    """
    A Redis-based implementation of the `ResultsBackend` interface for storing and retrieving
    studies and runs in a Redis database.

    Attributes:
        backend_name (str): The name of the backend (e.g., "redis").
        client (Redis): The Redis client used for database operations.

    Methods:
        get_version:
            Query Redis for the current version.

        get_connection_string:
            Retrieve the connection string used to connect to Redis.

        flush_database:
            Remove every entry in the Redis database.

        save:
            Save a `BaseDataModel` object to the Redis database.

        retrieve:
            Retrieve an entity from the appropriate store based on the given query identifier and store type.

        retrieve_all:
            Retrieve all objects from the specified store.

        delete:
            Delete an entity from the specified store.
    """

    def __init__(self, backend_name: str):
        """
        Initialize the `RedisBackend` instance, setting up the Redis client connection and store mappings.

        Args:
            backend_name: The name of the backend (e.g., "redis").
        """
        super().__init__(backend_name)
        from merlin.config.configfile import CONFIG  # pylint: disable=import-outside-toplevel
        from merlin.config.results_backend import get_connection_string  # pylint: disable=import-outside-toplevel

        # Get the Redis client connection
        redis_config = {"url": get_connection_string(), "decode_responses": True}
        if CONFIG.results_backend.name == "rediss":
            redis_config.update({"ssl_cert_reqs": getattr(CONFIG.results_backend, "cert_reqs", "required")})
        self.client: Redis = Redis.from_url(**redis_config)

        # Create instances of each store in our database
        self.stores: Dict = {
            "study": RedisStudyStore(self.client),
            "run": RedisRunStore(self.client),
            "logical_worker": RedisLogicalWorkerStore(self.client),
            "physical_worker": RedisPhysicalWorkerStore(self.client),
        }

    def get_version(self) -> str:
        """
        Query the Redis backend for the current version.

        Returns:
            A string representing the current version of Redis.
        """
        client_info = self.client.info()
        return client_info.get("redis_version", "N/A")

    def get_connection_string(self):
        """
        Get the connection string to Redis.

        Returns:
            A string representing the connection to Redis.
        """
        from merlin.config.results_backend import get_connection_string  # pylint: disable=import-outside-toplevel

        return get_connection_string(include_password=False)

    def flush_database(self):
        """
        Remove everything stored in Redis.
        """
        self.client.flushdb()

    def _get_store_by_type(self, store_type: str):
        """
        Get the appropriate store based on the store type.

        Args:
            store_type (str): The type of store.

        Returns:
            RedisStore: The corresponding store.

        Raises:
            ValueError: If the `store_type` is invalid.
        """
        if store_type not in self.stores:
            raise ValueError(f"Invalid store type '{store_type}'.")
        return self.stores[store_type]

    def _get_store_by_entity(self, entity: BaseDataModel):
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
        elif isinstance(entity, RunModel):
            return self.stores["run"]
        elif isinstance(entity, LogicalWorkerModel):
            return self.stores["logical_worker"]
        elif isinstance(entity, PhysicalWorkerModel):
            return self.stores["physical_worker"]
        else:
            raise UnsupportedDataModelError(f"Unsupported data model of type {type(entity)}.")

    def save(self, entity: BaseDataModel) -> None:
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
            BaseDataModel: The object retrieved from the specified store.

        Raises:
            ValueError: If the `store_type` is invalid.
        """
        LOG.debug(f"Retrieving '{entity_identifier}' from store '{store_type}'.")
        store = self._get_store_by_type(store_type)
        if store_type in ["study", "physical_worker"]:
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
            List[BaseDataModel]: A list of objects retrieved from the specified store.

        Raises:
            ValueError: If the `store_type` is invalid.
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

        Raises:
            ValueError: If the `store_type` is invalid.
        """
        store = self._get_store_by_type(store_type)
        if store_type in ["study", "physical_worker"]:
            try:
                uuid.UUID(entity_identifier)
                return store.delete(entity_identifier)
            except ValueError:
                return store.delete(entity_identifier, by_name=True)
        else:
            store.delete(entity_identifier)
