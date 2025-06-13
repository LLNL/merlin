##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Base Classes for Redis-Backed Data Stores in Merlin

This module provides foundational classes for implementing Redis-based persistence
in the Merlin system. It defines reusable components that standardize how data models
are stored, retrieved, and managed in Redis.

See also:
    - merlin.backends.store_base: Base class
    - merlin.backends.redis.redis_stores: Concrete store implementations
    - merlin.db_scripts.data_models: Data model definitions
"""

import logging
from typing import Generic, List, Optional, Type

from redis import Redis

from merlin.backends.store_base import StoreBase, T
from merlin.backends.utils import deserialize_entity, get_not_found_error_class, serialize_entity


LOG = logging.getLogger(__name__)


class RedisStoreBase(StoreBase[T], Generic[T]):
    """
    Base class for Redis-based stores.

    This class provides common functionality for saving, retrieving, and deleting
    entities in a Redis database.

    Attributes:
        client (Redis): The Redis client used for database operations.
        key (str): The prefix key used for Redis entries.
        model_class (Type[T]): The model class used for deserialization.

    Methods:
        save: Save or update an entity in the database.
        retrieve: Retrieve an entity from the database by ID.
        retrieve_all: Query the database for all entities of this type.
        delete: Delete an entity from the database by ID.
    """

    def __init__(self, client: Redis, key: str, model_class: Type[T]):
        """
        Initialize the Redis store with a Redis client.

        Args:
            client: A Redis client instance used to interact with the Redis database.
            key: The prefix key used for Redis entries.
            model_class: The model class used for deserialization.
        """
        self.client: Redis = client
        self.key: str = key
        self.model_class: Type[T] = model_class

    def _get_full_key(self, entity_id: str) -> str:
        """
        Get the full Redis key for an entity.

        Args:
            entity_id: The entity ID.

        Returns:
            The full Redis key.
        """
        return entity_id if entity_id.startswith(f"{self.key}:") else f"{self.key}:{entity_id}"

    def save(self, entity: T):
        """
        Save or update an entity in the Redis database.

        Args:
            entity: The entity to save.
        """
        entity_key = f"{self.key}:{entity.id}"

        if self.client.exists(entity_key):
            LOG.debug(f"Attempting to update {self.key} with id '{entity.id}'...")
            # Get the existing data from Redis and convert it to an instance of BaseDataModel
            existing_data = self.client.hgetall(entity_key)
            existing_data_class = deserialize_entity(existing_data, self.model_class)

            # Update the fields and save it to Redis
            existing_data_class.update_fields(entity.to_dict())
            updated_data = serialize_entity(existing_data_class)
            self.client.hset(entity_key, mapping=updated_data)
            LOG.debug(f"Successfully updated {self.key} with id '{entity.id}'.")
        else:
            LOG.debug(f"Creating a {self.key} entry in Redis...")
            serialized_data = serialize_entity(entity)
            self.client.hset(entity_key, mapping=serialized_data)
            LOG.debug(f"Successfully created a {self.key} with id '{entity.id}' in Redis.")

    def retrieve(self, identifier: str) -> Optional[T]:
        """
        Retrieve an entity from the Redis database by ID.

        Args:
            identifier: The ID of the entity to retrieve.

        Returns:
            The entity if found, None otherwise.
        """
        LOG.debug(f"Retrieving identifier {identifier} in RedisStoreBase.")
        entity_key = self._get_full_key(identifier)
        if not self.client.exists(entity_key):
            return None

        data_from_redis = self.client.hgetall(entity_key)
        return deserialize_entity(data_from_redis, self.model_class)

    def retrieve_all(self) -> List[T]:
        """
        Query the Redis database for all entities of this type.

        Returns:
            A list of entities.
        """
        entity_type = f"{self.key}s" if self.key != "study" else "studies"
        LOG.info(f"Fetching all {entity_type} from Redis...")

        pattern = f"{self.key}:*"
        all_entities = []

        # Exclude name mapping key if it exists
        keys_to_exclude = {f"{self.key}:name"}

        # Loop through all entities using scan_iter for better efficiency with large datasets
        for key in self.client.scan_iter(match=pattern):
            if key in keys_to_exclude:
                continue

            entity_id = key.split(":")[1]  # Extract the ID for logging
            try:
                entity_info = self.retrieve(key)
                if entity_info:
                    all_entities.append(entity_info)
                else:
                    LOG.warning(f"{self.key.capitalize()} with id '{entity_id}' could not be retrieved or does not exist.")
            except Exception as exc:  # pylint: disable=broad-except
                LOG.error(f"Error retrieving {self.key} with id '{entity_id}': {exc}")

        LOG.info(f"Successfully retrieved {len(all_entities)} {entity_type} from Redis.")
        return all_entities

    def delete(self, identifier: str):
        """
        Delete an entity from the Redis database by ID.

        Args:
            identifier: The ID of the entity to delete.
        """
        LOG.info(f"Attempting to delete {self.key} with id '{identifier}' from Redis...")

        entity = self.retrieve(identifier)
        if entity is None:
            error_class = get_not_found_error_class(self.model_class)
            raise error_class(f"{self.key.capitalize()} with id '{identifier}' does not exist in the database.")

        # Delete the entity's hash entry
        entity_key = f"{self.key}:{entity.id}"
        LOG.debug(f"Deleting {self.key} hash with key '{entity_key}'...")
        self.client.delete(entity_key)
        LOG.debug(f"Successfully removed {self.key} hash from Redis.")

        LOG.info(f"Successfully deleted {self.key} '{identifier}' from Redis.")


class NameMappingMixin:
    """
    Mixin class that adds name-to-ID mapping functionality to Redis stores.

    This mixin extends Redis stores to support retrieval and deletion by name.

    Methods:
        save: Save or update an entity in the database.
        retrieve: Retrieve an entity from the database by ID or name.
        delete: Delete an entity from the database by ID or name.
    """

    def save(self, entity: Type[T]):
        """
        Save an entity and update the name-to-ID mapping.

        Args:
            entity: The entity to save.
        """
        name_or_ws = entity.name if hasattr(entity, "name") else entity.workspace
        LOG.debug(f"Saving entity {name_or_ws} with id {entity.id} in NameMappingMixin.")
        existing_entity_id = self.client.hget(f"{self.key}:name", name_or_ws)

        # Call the parent class's save method
        super().save(entity)

        # Update name-to-ID mapping if it's a new entity
        if not existing_entity_id:
            LOG.debug(f"Creating a new name-to-ID mapping for {name_or_ws} with id {entity.id}")
            self.client.hset(f"{self.key}:name", name_or_ws, entity.id)

    def retrieve(self, identifier: str, by_name: bool = False) -> Optional[T]:
        """
        Retrieve an entity from the Redis database, either by ID or name.

        Args:
            identifier: The ID or name of the entity to retrieve.
            by_name: If True, interpret the identifier as a name. If False, interpret it as an ID.

        Returns:
            The entity if found, None otherwise.
        """
        LOG.debug(f"Retrieving identifier {identifier} in NameMappingMixin.")
        if by_name:
            # Retrieve the entity ID using the name-to-ID mapping
            entity_id = self.client.hget(f"{self.key}:name", identifier)
            if entity_id is None:
                LOG.debug("Could not retrieve entity id by name-to-ID mapping.")
                return None
            return super().retrieve(entity_id)
        # Use the parent class's retrieve method for ID-based retrieval
        return super().retrieve(identifier)

    def delete(self, identifier: str, by_name: bool = False):
        """
        Delete an entity from the Redis database, either by ID or name.

        Args:
            identifier: The ID or name of the entity to delete.
            by_name: If True, interpret the identifier as a name. If False, interpret it as an ID.
        """
        id_type = "name" if by_name else "id"
        LOG.debug(f"Attempting to delete {self.key} with {id_type} '{identifier}' from Redis...")

        # Retrieve the entity to ensure it exists and get its ID and name
        entity = self.retrieve(identifier, by_name=by_name)
        if entity is None:
            error_class = get_not_found_error_class(self.model_class)
            raise error_class(f"{self.key.capitalize()} with {id_type} '{identifier}' not found in the database.")

        # Delete the entity from the name index and Redis
        name_or_ws = entity.name if hasattr(entity, "name") else entity.workspace
        self.client.hdel(f"{self.key}:name", name_or_ws)
        self.client.delete(f"{self.key}:{entity.id}")

        LOG.info(f"Successfully deleted {self.key} with {id_type} '{identifier}'.")
