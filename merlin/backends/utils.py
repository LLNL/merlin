##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Utility functions for backends in the Merlin application.

These utilities are essential for converting in-memory data models into a persistable format,
ensuring compatibility with backend storage systems, and for consistent error handling
when entity lookups fail.
"""

import json
import logging
from datetime import datetime
from typing import Dict, Type, TypeVar

from merlin.db_scripts.data_models import LogicalWorkerModel, PhysicalWorkerModel, RunModel, StudyModel
from merlin.exceptions import RunNotFoundError, StudyNotFoundError, WorkerNotFoundError


T = TypeVar("T")

LOG = logging.getLogger(__name__)


def get_not_found_error_class(model_class: Type[T]) -> Exception:
    """
    Get the appropriate not found error class based on the model type.

    Args:
        model_class: A [`BaseDataModel`][db_scripts.data_models.BaseDataModel] subclass.

    Returns:
        The error class to use.
    """
    error_map = {
        LogicalWorkerModel: WorkerNotFoundError,
        PhysicalWorkerModel: WorkerNotFoundError,
        RunModel: RunNotFoundError,
        StudyModel: StudyNotFoundError,
    }
    return error_map.get(model_class, Exception)


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


def serialize_entity(entity: T) -> Dict[str, str]:
    """
    Given a [`BaseDataModel`][db_scripts.data_models.BaseDataModel] instance,
    convert it's data into a format that the database can interpret.

    Args:
        entity: A [`BaseDataModel`][db_scripts.data_models.BaseDataModel] instance.

    Returns:
        A dictionary of information that the database can interpret.
    """
    LOG.debug("Deserializing data...")
    serialized_data = {}

    for field in entity.get_instance_fields():
        field_value = getattr(entity, field.name)
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


def deserialize_entity(data: Dict[str, str], model_class: T) -> T:
    """
    Given data that was retrieved, convert it into a data_class instance.

    Args:
        data: The data retrieved that we need to deserialize.
        model_class: A [`BaseDataModel`][db_scripts.data_models.BaseDataModel] subclass.

    Returns:
        A [`BaseDataModel`][db_scripts.data_models.BaseDataModel] instance.
    """
    LOG.debug("Deserializing data...")
    deserialized_data = {}

    for key, val in data.items():
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
    return model_class.from_dict(deserialized_data)
