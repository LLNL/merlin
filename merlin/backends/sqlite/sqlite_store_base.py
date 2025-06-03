"""
"""
import json
import logging
import sqlite3
from dataclasses import asdict, is_dataclass
from datetime import datetime
from typing import Any, Generic, List, Optional, Type

from merlin.backends.sqlite.sqlite_connection import SQLiteConnection
from merlin.backends.store_base import T, StoreBase
from merlin.backends.utils import deserialize_entity, get_not_found_error_class, serialize_entity

LOG = logging.getLogger(__name__)

# TODO implement save for the rest of the stores and see if this can be moved to this class
# TODO implement the retrieve, retrieve_all, and delete methods for all of the stores
# TODO change all references of "object" or "obj" to "entity"
# TODO fix broken tests
# TODO write tests for these new files

class SQLiteStoreBase(StoreBase[T], Generic[T]):
    """
    Base class for SQLite-based stores.

    This class provides common functionality for saving, retrieving, and deleting
    objects in a SQLite database.

    Attributes:
        connection (sqlite3.Connection): The SQLite connection used for database operations.
        table_name (str): The table name used for SQLite entries.
        model_class (Type[T]): The model class used for deserialization.
    """

    # def __init__(self, connection: sqlite3.Connection, table_name: str, model_class: Type[T]):
    def __init__(self, table_name: str, model_class: Type[T]):
        """
        Initialize the SQLite store with a SQLite connection.

        Args:
            connection: A SQLite connection instance used to interact with the SQLite database.
            table_name: The table name used for SQLite entries.
            model_class: The model class used for deserialization.
        """
        # self.connection: sqlite3.Connection = connection
        self.table_name: str = table_name
        self.model_class: Type[T] = model_class
        self._create_table_if_not_exists()

    def _get_sqlite_type(self, py_type: Any) -> str:
        """
        Map Python types to SQLite types.

        Args:
            py_type: A Python type hint (e.g., str, int, List[str], etc.)

        Returns:
            A string representing the corresponding SQLite column type.
        """
        origin_type = getattr(py_type, '__origin__', py_type)

        # Handle generics like List[str], Dict[str, Any], etc.
        if origin_type in (list, dict, set):
            return "TEXT"  # store as JSON string
        elif py_type == str:
            return "TEXT"
        elif py_type == int:
            return "INTEGER"
        elif py_type == float:
            return "REAL"
        elif py_type == bool:
            return "INTEGER"  # SQLite uses 0 and 1 for booleans
        elif py_type == datetime:
            return "TEXT"  # ISO format string
        else:
            return "TEXT"  # Default fallback

    def _create_table_if_not_exists(self):
        """
        Create the table if it doesn't exist.
        """
        field_defs = []

        for field_obj in self.model_class.get_class_fields():
            col_name = field_obj.name
            col_type = self._get_sqlite_type(field_obj.type)
            field_defs.append(f"{col_name} {col_type}")

        field_defs_str = ", ".join(field_defs)
        # print(f"field_defs_str for model_class '{self.model_class.__name__}': {field_defs_str}")

        with SQLiteConnection() as conn:
            conn.execute(f"CREATE TABLE IF NOT EXISTS {self.table_name} ({field_defs_str});")

        # with SQLiteConnection() as conn:
        #     conn.execute(f"""
        #         CREATE TABLE IF NOT EXISTS {self.table_name} (
        #             id TEXT PRIMARY KEY,
        #             name TEXT UNIQUE,
        #             data TEXT NOT NULL,
        #             created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        #             updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        #         )
        #     """)

        #     # Create index for faster name lookups
        #     conn.execute(f"""
        #         CREATE INDEX IF NOT EXISTS idx_{self.table_name}_name 
        #         ON {self.table_name}(name)
        #     """)


        # self.connection.execute(f"""
        #     CREATE TABLE IF NOT EXISTS {self.table_name} (
        #         id TEXT PRIMARY KEY,
        #         name TEXT UNIQUE,
        #         data TEXT NOT NULL,
        #         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        #         updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        #     )
        # """)
        
        # # Create index for faster name lookups
        # self.connection.execute(f"""
        #     CREATE INDEX IF NOT EXISTS idx_{self.table_name}_name 
        #     ON {self.table_name}(name)
        # """)

    def save(self, obj: T):
        """
        Save or update an object in the SQLite database.

        Args:
            obj: The object to save.
        """
        exists = (self.retrieve(obj.id) is not None)
        if exists:
            LOG.debug(f"Attempting to update {self.table_name} with id '{obj.id}'...")
            LOG.debug(f"Successfully updated {self.table_name} with id '{obj.id}'.")
        else:
            LOG.debug(f"Creating a {self.table_name} entry in SQLite...")
            LOG.debug(f"Successfully created a {self.table_name} with id '{obj.id}' in SQLite.")

    def retrieve(self, identifier: str, by_name: bool = False) -> Optional[T]:
        """
        Retrieve an object from the SQLite database by ID or name.

        Args:
            identifier: The ID or name of the object to retrieve.
            by_name: If True, interpret the identifier as a name. If False, interpret it as an ID.

        Returns:
            The object if found, None otherwise.
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
        Query the SQLite database for all objects of this type.

        Returns:
            A list of objects.
        """
        # entity_type = f"{self.table_name}s" if self.table_name != "study" else "studies"
        # LOG.info(f"Fetching all {entity_type} from SQLite...")

        # cursor = self.connection.execute(f"SELECT id, data FROM {self.table_name}")
        # all_objects = []

        # for row in cursor.fetchall():
        #     obj_id, data = row
        #     try:
        #         obj = deserialize_entity(data, self.model_class)
        #         if obj:
        #             all_objects.append(obj)
        #         else:
        #             LOG.warning(f"{self.table_name.capitalize()} with id '{obj_id}' could not be retrieved or does not exist.")
        #     except Exception as exc:  # pylint: disable=broad-except
        #         LOG.error(f"Error retrieving {self.table_name} with id '{obj_id}': {exc}")

        # LOG.info(f"Successfully retrieved {len(all_objects)} {entity_type} from SQLite.")
        # return all_objects

    def delete(self, identifier: str, by_name: bool = False):
        """
        Delete an object from the SQLite database by ID or name.

        Args:
            identifier: The ID or name of the object to delete.
            by_name: If True, interpret the identifier as a name. If False, interpret it as an ID.
        """
        # id_type = "name" if by_name else "id"
        # LOG.info(f"Attempting to delete {self.table_name} with {id_type} '{identifier}' from SQLite...")

        # obj = self.retrieve(identifier, by_name=by_name)
        # if obj is None:
        #     error_class = get_not_found_error_class(self.model_class)
        #     raise error_class(f"{self.table_name.capitalize()} with {id_type} '{identifier}' does not exist in the database.")

        # # Delete the object
        # cursor = self.connection.execute(f"DELETE FROM {self.table_name} WHERE {id_type} = :identifier", {"identifier": identifier})
        # # if by_name:
        # #     cursor = self.connection.execute(
        # #         f"DELETE FROM {self.table_name} WHERE name = ?", 
        # #         (identifier,)
        # #     )
        # # else:
        # #     cursor = self.connection.execute(
        # #         f"DELETE FROM {self.table_name} WHERE id = ?", 
        # #         (identifier,)
        # #     )
        
        # if cursor.rowcount == 0:
        #     LOG.warning(f"No rows were deleted for {self.table_name} with {id_type} '{identifier}'")
        # else:
        #     LOG.debug(f"Successfully removed {self.table_name} from SQLite.")

        # LOG.info(f"Successfully deleted {self.table_name} '{identifier}' from SQLite.")
