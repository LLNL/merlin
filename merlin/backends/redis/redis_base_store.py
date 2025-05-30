"""
Base Classes for Redis-Backed Data Stores in Merlin

This module provides foundational classes for implementing Redis-based persistence
in the Merlin system. It defines reusable components that standardize how data models
are stored, retrieved, and managed in Redis.

See also:
    - merlin.backends.redis.redis_stores: Concrete store implementations
    - merlin.db_scripts.data_models: Data model definitions
"""

import logging
from typing import Generic, List, Optional, Type, TypeVar

from redis import Redis

from merlin.backends.utils import deserialize_object, get_not_found_error_class, serialize_object


LOG = logging.getLogger(__name__)

T = TypeVar("T")


class RedisStoreBase(Generic[T]):
    """
    Base class for Redis-based stores.

    This class provides common functionality for saving, retrieving, and deleting
    objects in a Redis database.

    Attributes:
        client (Redis): The Redis client used for database operations.
        key (str): The prefix key used for Redis entries.
        model_class (Type[T]): The model class used for deserialization.
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

    def _get_full_key(self, obj_id: str) -> str:
        """
        Get the full Redis key for an object.

        Args:
            obj_id: The object ID.

        Returns:
            The full Redis key.
        """
        return obj_id if obj_id.startswith(f"{self.key}:") else f"{self.key}:{obj_id}"

    def save(self, obj: T):
        """
        Save or update an object in the Redis database.

        Args:
            obj: The object to save.
        """
        obj_key = f"{self.key}:{obj.id}"

        if self.client.exists(obj_key):
            LOG.debug(f"Attempting to update {self.key} with id '{obj.id}'...")
            # Get the existing data from Redis and convert it to an instance of BaseDataModel
            existing_data = self.client.hgetall(obj_key)
            existing_data_class = deserialize_object(existing_data, self.model_class)

            # Update the fields and save it to Redis
            existing_data_class.update_fields(obj.to_dict())
            updated_data = serialize_object(existing_data_class)
            self.client.hset(obj_key, mapping=updated_data)
            LOG.debug(f"Successfully updated {self.key} with id '{obj.id}'.")
        else:
            LOG.debug(f"Creating a {self.key} entry in Redis...")
            serialized_data = serialize_object(obj)
            self.client.hset(obj_key, mapping=serialized_data)
            LOG.debug(f"Successfully created a {self.key} with id '{obj.id}' in Redis.")

    def retrieve(self, obj_id: str) -> Optional[T]:
        """
        Retrieve an object from the Redis database by ID.

        Args:
            obj_id: The ID of the object to retrieve.

        Returns:
            The object if found, None otherwise.
        """
        LOG.debug(f"Retrieving identifier {obj_id} in RedisStoreBase.")
        obj_key = self._get_full_key(obj_id)
        if not self.client.exists(obj_key):
            return None

        data_from_redis = self.client.hgetall(obj_key)
        return deserialize_object(data_from_redis, self.model_class)

    def retrieve_all(self) -> List[T]:
        """
        Query the Redis database for all objects of this type.

        Returns:
            A list of objects.
        """
        entity_type = f"{self.key}s" if self.key != "study" else "studies"
        LOG.info(f"Fetching all {entity_type} from Redis...")

        pattern = f"{self.key}:*"
        all_objects = []

        # Exclude name mapping key if it exists
        keys_to_exclude = {f"{self.key}:name"}

        # Loop through all objects using scan_iter for better efficiency with large datasets
        for key in self.client.scan_iter(match=pattern):
            if key in keys_to_exclude:
                continue

            obj_id = key.split(":")[1]  # Extract the ID for logging
            try:
                obj_info = self.retrieve(key)
                if obj_info:
                    all_objects.append(obj_info)
                else:
                    LOG.warning(f"{self.key.capitalize()} with id '{obj_id}' could not be retrieved or does not exist.")
            except Exception as exc:  # pylint: disable=broad-except
                LOG.error(f"Error retrieving {self.key} with id '{obj_id}': {exc}")

        LOG.info(f"Successfully retrieved {len(all_objects)} {entity_type} from Redis.")
        return all_objects

    def delete(self, obj_id: str):
        """
        Delete an object from the Redis database by ID.

        Args:
            obj_id: The ID of the object to delete.
        """
        LOG.info(f"Attempting to delete {self.key} with id '{obj_id}' from Redis...")

        obj = self.retrieve(obj_id)
        if obj is None:
            error_class = get_not_found_error_class(self.model_class)
            raise error_class(f"{self.key.capitalize()} with id '{obj_id}' does not exist in the database.")

        # Delete the object's hash entry
        obj_key = f"{self.key}:{obj.id}"
        LOG.debug(f"Deleting {self.key} hash with key '{obj_key}'...")
        self.client.delete(obj_key)
        LOG.debug(f"Successfully removed {self.key} hash from Redis.")

        LOG.info(f"Successfully deleted {self.key} '{obj_id}' from Redis.")


class NameMappingMixin:
    """
    Mixin class that adds name-to-ID mapping functionality to Redis stores.

    This mixin extends Redis stores to support retrieval and deletion by name.
    """

    def save(self, obj: Type[T]):
        """
        Save an object and update the name-to-ID mapping.

        Args:
            obj: The object to save.
        """
        name_or_ws = obj.name if hasattr(obj, "name") else obj.workspace
        LOG.debug(f"Saving obj {name_or_ws} with id {obj.id} in NameMappingMixin.")
        existing_obj_id = self.client.hget(f"{self.key}:name", name_or_ws)

        # Call the parent class's save method
        super().save(obj)

        # Update name-to-ID mapping if it's a new object
        if not existing_obj_id:
            LOG.debug(f"Creating a new name-to-ID mapping for {name_or_ws} with id {obj.id}")
            self.client.hset(f"{self.key}:name", name_or_ws, obj.id)

    def retrieve(self, identifier: str, by_name: bool = False) -> Optional[object]:
        """
        Retrieve an object from the Redis database, either by ID or name.

        Args:
            identifier: The ID or name of the object to retrieve.
            by_name: If True, interpret the identifier as a name. If False, interpret it as an ID.

        Returns:
            The object if found, None otherwise.
        """
        LOG.debug(f"Retrieving identifier {identifier} in NameMappingMixin.")
        if by_name:
            # Retrieve the object ID using the name-to-ID mapping
            obj_id = self.client.hget(f"{self.key}:name", identifier)
            if obj_id is None:
                LOG.debug("Could not retrieve object id by name-to-ID mapping.")
                return None
            return super().retrieve(obj_id)
        # Use the parent class's retrieve method for ID-based retrieval
        return super().retrieve(identifier)

    def delete(self, identifier: str, by_name: bool = False):
        """
        Delete an object from the Redis database, either by ID or name.

        Args:
            identifier: The ID or name of the object to delete.
            by_name: If True, interpret the identifier as a name. If False, interpret it as an ID.
        """
        id_type = "name" if by_name else "id"
        LOG.debug(f"Attempting to delete {self.key} with {id_type} '{identifier}' from Redis...")

        # Retrieve the object to ensure it exists and get its ID and name
        obj = self.retrieve(identifier, by_name=by_name)
        if obj is None:
            error_class = get_not_found_error_class(self.model_class)
            raise error_class(f"{self.key.capitalize()} with {id_type} '{identifier}' not found in the database.")

        # Delete the object from the name index and Redis
        name_or_ws = obj.name if hasattr(obj, "name") else obj.workspace
        self.client.hdel(f"{self.key}:name", name_or_ws)
        self.client.delete(f"{self.key}:{obj.id}")

        LOG.info(f"Successfully deleted {self.key} with {id_type} '{identifier}'.")
