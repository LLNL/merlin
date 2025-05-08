"""
Utility functions for the Redis backend.
"""

import json
import logging
from datetime import datetime
from typing import Dict

from redis import Redis

from merlin.db_scripts.data_models import BaseDataModel


LOG = logging.getLogger("merlin")


def serialize_data_class(data_class: BaseDataModel) -> Dict[str, str]:
    """
    Given a [`BaseDataModel`][db_scripts.data_models.BaseDataModel] instance,
    convert it's data into a format that the Redis database can interpret.

    Args:
        data_class: A [`BaseDataModel`][db_scripts.data_models.BaseDataModel] instance.

    Returns:
        A dictionary of information that Redis can interpret.
    """
    LOG.debug("Deserializing data from Redis...")
    serialized_data = {}

    for field in data_class.get_instance_fields():
        field_value = getattr(data_class, field.name)
        if isinstance(field_value, set):
            # Explicitly mark this as a set so we can properly deserialize it later
            serialized_data[field.name] = json.dumps({"__set__": list(field_value)})
        elif isinstance(field_value, (list, dict)):
            serialized_data[field.name] = json.dumps(field_value)
        elif isinstance(field_value, datetime):
            serialized_data[field.name] = field_value.isoformat()
        elif field_value is None:
            serialized_data[field.name] = "null"
        else:
            serialized_data[field.name] = str(field_value)

    LOG.debug("Successfully deserialized data.")
    return serialized_data


def deserialize_data_class(retrieval_data: Dict[str, str], data_class: BaseDataModel) -> BaseDataModel:
    """
    Given data that was retrieved by Redis, convert it into a data_class instance.

    Args:
        retrieval_data: The data retrieved by Redis that we need to deserialize.
        data_class: A [`BaseDataModel`][db_scripts.data_models.BaseDataModel] object.

    Returns:
        A [`BaseDataModel`][db_scripts.data_models.BaseDataModel] instance.
    """
    LOG.debug("Deserializing data from Redis...")
    deserialized_data = {}

    for key, val in retrieval_data.items():
        if val.startswith("[") or val.startswith("{"):
            try:
                loaded_val = json.loads(val)
                # Check if this is a set that we specially encoded
                if isinstance(loaded_val, dict) and "__set__" in loaded_val:
                    deserialized_data[key] = set(loaded_val["__set__"])
                else:
                    deserialized_data[key] = loaded_val
            except json.JSONDecodeError as e:
                LOG.error(f"Failed to deserialize JSON for key {key}: {val}")
                LOG.error(f"Error: {str(e)}")
                # Use the original string value as fallback
                deserialized_data[key] = val
        elif val == "null":
            deserialized_data[key] = None
        elif val in ("True", "False"):
            deserialized_data[key] = val == "True"
        elif is_iso_datetime(val):
            deserialized_data[key] = datetime.fromisoformat(val)
        elif val.isdigit():
            deserialized_data[key] = float(val)
        else:
            deserialized_data[key] = str(val)

    LOG.debug("Successfully deserialized data.")
    return data_class.from_dict(deserialized_data)


def is_iso_datetime(value: str) -> bool:
    """
    Check if a string is in ISO 8601 datetime format.

    Args:
        value: The string to check.

    Returns:
        True if the string is in ISO 8601 format, False otherwise.
    """
    try:
        datetime.fromisoformat(value)
        return True
    except ValueError:
        return False


def create_data_class_entry(data_class: BaseDataModel, key: str, client: Redis):
    """
    Given a [`BaseDataModel`][db_scripts.data_models.BaseDataModel] instance and
    a key, store the contents of the `data_class` as a hash at `key` in the Redis database.

    Args:
        data_class: A [`BaseDataModel`][db_scripts.data_models.BaseDataModel] instance.
        key: The location to store the contents of `data_class` in the Redis database.
        client: The Redis client that we're storing the contents of `data_class` in.
    """
    serialized_data = serialize_data_class(data_class)
    client.hset(key, mapping=serialized_data)


def update_data_class_entry(updated_data_class: BaseDataModel, key: str, client: Redis):
    """
    Update an existing data class entry in the Redis database with new information.
    A 'data class' here refers to classes defined in merlin.db_scripts.data_models.

    This function retrieves the existing data of the data class we're updating from Redis, updates
    its fields with the new data provided in the `updated_data_class` object, and saves the updated
    data back to the database.

    Args:
        updated_data_class: A [`BaseDataModel`][db_scripts.data_models.BaseDataModel] instance
            containing the updated information.
        key: The entry that we're updating in the Redis database.
        client: The Redis client.
    """
    # Get the existing data from Redis and convert it to an instance of BaseDataModel
    existing_data = client.hgetall(key)
    existing_data_class = deserialize_data_class(existing_data, type(updated_data_class))

    # Update the fields and save it to Redis
    existing_data_class.update_fields(updated_data_class.to_dict())
    updated_data = serialize_data_class(existing_data_class)
    client.hset(key, mapping=updated_data)
