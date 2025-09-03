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
from typing import Any, Dict, Generic, List, Optional, Tuple, Type

from merlin.backends.sqlite.sqlite_connection import SQLiteConnection
from merlin.backends.store_base import StoreBase, T
from merlin.backends.utils import deserialize_entity, get_not_found_error_class, serialize_entity
from merlin.utils import get_plural_of_entity


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

    def _build_where_clause_and_params(self, filters: Dict[str, Any]) -> Tuple[str, List[Any]]:
        """
        Build the SQL WHERE clause and associated parameter list from a filters dictionary.

        Args:
            filters: Dictionary where keys are column names and values are either
                    single values (for equality) or lists (for IN clauses).

        Returns:
            A tuple of (where_clause: str, params: List[Any])
        """
        if not filters:
            return "", []

        conditions = []
        params = []

        for column, value in filters.items():
            if isinstance(value, list):
                if not value:
                    # Avoid generating invalid SQL like `IN ()`
                    conditions.append("1 = 0")
                else:
                    # We have to use LIKE for lists since they're stored as strings
                    sub_conditions = [f"{column} LIKE ?" for _ in value]
                    conditions.append("(" + " OR ".join(sub_conditions) + ")")
                    params.extend([f"%{v}%" for v in value])
            else:
                conditions.append(f"{column} = ?")
                params.append(value)

        where_clause = "WHERE " + " AND ".join(conditions)
        return where_clause, params

    def _retrieve_by_query(self, filters: Optional[Dict[str, Any]] = None) -> List[T]:
        """
        Internal method to query the SQLite database for entities with optional filters.

        Args:
            filters: Optional dictionary of column filters.

        Returns:
            A list of matching entities.
        """
        entity_type = get_plural_of_entity(self.table_name, split_delimiter="_", join_delimiter=" ")
        log_action = "filtered" if filters else "all"
        LOG.info(f"Fetching {log_action} {entity_type} from SQLite{f' with filters: {filters}' if filters else ''}...")

        where_clause, params = self._build_where_clause_and_params(filters)
        query = f"SELECT * FROM {self.table_name} {where_clause}"
        LOG.debug(f"SQLite query: {query}")
        LOG.debug(f"SQLite params: {params}")

        with SQLiteConnection() as conn:
            cursor = conn.execute(query, params)
            entities = []

            for row in cursor.fetchall():
                try:
                    entity = deserialize_entity(dict(row), self.model_class)
                    if entity:
                        entities.append(entity)
                    else:
                        LOG.warning(
                            f"{self.table_name.capitalize()} with id '{row['id']}' could not be retrieved or does not exist."
                        )
                except Exception as exc:  # pylint: disable=broad-except
                    LOG.error(f"Error retrieving {self.table_name} with id '{row['id']}': {exc}")

        LOG.info(f"Successfully retrieved {len(entities)} {entity_type} from SQLite ({log_action}).")
        return entities

    def retrieve_all(self) -> List[T]:
        """
        Query the SQLite database for all entities of this type.

        Returns:
            A list of entities.
        """
        return self._retrieve_by_query()

    def retrieve_all_filtered(self, filters: Dict[str, Any]) -> List[T]:
        """
        Query the SQLite database for all entities of this type that match the given filters.

        Args:
            filters: A dictionary where keys are column names and values are the values to match.

        Returns:
            A list of filtered entities.
        """
        return self._retrieve_by_query(filters=filters)

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
