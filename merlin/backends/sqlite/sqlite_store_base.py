##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
SQLite-based generic store implementation for Merlin entities.

This module defines `SQLiteStoreBase`, a generic base class for managing
entity persistence using SQLite as the underlying storage. It provides
core CRUD operations (create, retrieve, update, delete) and dynamic table
creation based on model class field definitions.

This module is intended to be subclassed by entity-specific store classes
in the Merlin backend architecture.

See also:
    - merlin.backends.store_base: Base class
    - merlin.backends.sqlite.sqlite_stores: Concrete store implementations
    - merlin.db_scripts.data_models: Data model definitions
"""

import logging
from datetime import datetime
from typing import Any, Generic, List, Optional, Type

from merlin.backends.sqlite.sqlite_connection import SQLiteConnection
from merlin.backends.store_base import StoreBase, T
from merlin.backends.utils import deserialize_entity, get_not_found_error_class, serialize_entity


LOG = logging.getLogger(__name__)


class SQLiteStoreBase(StoreBase[T], Generic[T]):
    """
    Base class for SQLite-based stores.

    This class provides common functionality for saving, retrieving, and deleting
    entities in a SQLite database.

    Attributes:
        table_name (str): The table name used for SQLite entries.
        model_class (Type[T]): The model class used for deserialization.

    Methods:
        save: Save or update an entity in the database.
        retrieve: Retrieve an entity from the database by ID.
        retrieve_all: Query the database for all entities of this type.
        delete: Delete an entity from the database by ID.
    """

    def __init__(self, table_name: str, model_class: Type[T]):
        """
        Initialize the SQLite store with a SQLite connection.

        Args:
            table_name: The table name used for SQLite entries.
            model_class: The model class used for deserialization.
        """
        self.table_name: str = table_name
        self.model_class: Type[T] = model_class
        self.create_table_if_not_exists()

    def _get_sqlite_type(self, py_type: Any) -> str:
        """
        Map Python types to SQLite types.

        Args:
            py_type: A Python type hint (e.g., str, int, List[str], etc.)

        Returns:
            A string representing the corresponding SQLite column type.
        """
        origin_type = getattr(py_type, "__origin__", py_type)
        result = "TEXT"  # Default fallback

        # Handle generics like List[str], Dict[str, Any], etc.
        if origin_type in (list, dict, set):
            result = "TEXT"  # store as JSON string
        elif py_type == str:
            result = "TEXT"
        elif py_type == int:
            result = "INTEGER"
        elif py_type == float:
            result = "REAL"
        elif py_type == bool:
            result = "INTEGER"  # SQLite uses 0 and 1 for booleans
        elif py_type == datetime:
            result = "TEXT"  # ISO format string

        return result

    def create_table_if_not_exists(self):
        """
        Create the table if it doesn't exist.
        """
        field_defs = []

        for field_obj in self.model_class.get_class_fields():
            col_name = field_obj.name
            col_type = self._get_sqlite_type(field_obj.type)
            field_defs.append(f"{col_name} {col_type}")

        field_defs_str = ", ".join(field_defs)

        with SQLiteConnection() as conn:
            conn.execute(f"CREATE TABLE IF NOT EXISTS {self.table_name} ({field_defs_str});")

    def save(self, entity: T):
        """
        Save or update an entity in the SQLite database.

        Args:
            entity: The entity to save.
        """
        # Try to retrieve any existing entity with this ID
        existing_data = self.retrieve(entity.id)

        # If the entity already exists, update it
        if existing_data:
            LOG.debug(f"Attempting to update {self.table_name} with id '{entity.id}'...")
            existing_data.update_fields(entity.to_dict())
            serialized_data = serialize_entity(existing_data)
            set_str = ", ".join(
                f"{field.name} = :{field.name}" for field in self.model_class.get_class_fields() if field.name != "id"
            )
            with SQLiteConnection() as conn:
                conn.execute(
                    f"""
                    UPDATE {self.table_name}
                    SET {set_str}
                    WHERE id = :id
                """,
                    serialized_data,
                )
            LOG.debug(f"Successfully updated {self.table_name} with id '{entity.id}'.")
        # If the entity does not already exist, create it
        else:
            LOG.debug(f"Creating a {self.table_name} entry in SQLite...")
            serialized_data = serialize_entity(entity)
            fields = [field.name for field in self.model_class.get_class_fields()]
            columns_str = ", ".join(fields)
            placeholders_str = ", ".join(f":{name}" for name in fields)
            with SQLiteConnection() as conn:
                conn.execute(
                    f"""
                    INSERT INTO {self.table_name} ({columns_str})
                    VALUES ({placeholders_str})
                """,
                    serialized_data,
                )
            LOG.debug(f"Successfully created a {self.table_name} with id '{entity.id}' in SQLite.")

    def retrieve(self, identifier: str, by_name: bool = False) -> Optional[T]:
        """
        Retrieve an entity from the SQLite database by ID or name.

        Args:
            identifier: The ID or name of the entity to retrieve.
            by_name: If True, interpret the identifier as a name. If False, interpret it as an ID.

        Returns:
            The entity if found, None otherwise.
        """
        LOG.debug(f"Retrieving identifier {identifier} in SQLiteStoreBase.")

        id_type = "name" if by_name else "id"

        with SQLiteConnection() as conn:
            cursor = conn.execute(f"SELECT * FROM {self.table_name} WHERE {id_type} = :identifier", {"identifier": identifier})
            row = cursor.fetchone()

            if row is None:
                return None

            return deserialize_entity(dict(row), self.model_class)

    def retrieve_all(self) -> List[T]:
        """
        Query the SQLite database for all entities of this type.

        Returns:
            A list of entities.
        """
        entity_type = f"{self.table_name}s" if self.table_name != "study" else "studies"
        LOG.info(f"Fetching all {entity_type} from SQLite...")

        with SQLiteConnection() as conn:
            cursor = conn.execute(f"SELECT * FROM {self.table_name}")
            all_entities = []

            for row in cursor.fetchall():
                try:
                    entity = deserialize_entity(dict(row), self.model_class)
                    if entity:
                        all_entities.append(entity)
                    else:
                        LOG.warning(
                            f"{self.table_name.capitalize()} with id '{row['id']}' could not be retrieved or does not exist."
                        )
                except Exception as exc:  # pylint: disable=broad-except
                    LOG.error(f"Error retrieving {self.table_name} with id '{row['id']}': {exc}")

        LOG.info(f"Successfully retrieved {len(all_entities)} {entity_type} from SQLite.")
        return all_entities

    def delete(self, identifier: str, by_name: bool = False):
        """
        Delete an entity from the SQLite database by ID or name.

        Args:
            identifier: The ID or name of the entity to delete.
            by_name: If True, interpret the identifier as a name. If False, interpret it as an ID.
        """
        id_type = "name" if by_name else "id"
        LOG.info(f"Attempting to delete {self.table_name} with {id_type} '{identifier}' from SQLite...")

        entity = self.retrieve(identifier, by_name=by_name)
        if entity is None:
            error_class = get_not_found_error_class(self.model_class)
            raise error_class(f"{self.table_name.capitalize()} with {id_type} '{identifier}' does not exist in the database.")

        # Delete the entity
        with SQLiteConnection() as conn:
            cursor = conn.execute(f"DELETE FROM {self.table_name} WHERE {id_type} = :identifier", {"identifier": identifier})

            if cursor.rowcount == 0:
                LOG.warning(f"No rows were deleted for {self.table_name} with {id_type} '{identifier}'")
            else:
                LOG.info(f"Successfully deleted {self.table_name} '{identifier}' from SQLite.")
