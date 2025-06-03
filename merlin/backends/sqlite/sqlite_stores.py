"""
"""

import logging

from merlin.backends.sqlite.sqlite_connection import SQLiteConnection
from merlin.backends.sqlite.sqlite_store_base import SQLiteStoreBase
from merlin.backends.store_base import T
from merlin.backends.utils import deserialize_entity, serialize_entity
from merlin.db_scripts.data_models import LogicalWorkerModel, PhysicalWorkerModel, RunModel, StudyModel

LOG = logging.getLogger(__name__)


class SQLiteStudyStore(SQLiteStoreBase[StudyModel]):
    """SQLite store for Study entities with built-in name support."""
    
    def __init__(self):
        super().__init__("study", StudyModel)

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
            with SQLiteConnection() as conn:
                conn.execute(f"""
                    UPDATE {self.table_name} 
                    SET name = :name, runs = :runs, additional_data = :additional_data
                    WHERE id = :id
                """, serialized_data)
            LOG.debug(f"Successfully updated {self.table_name} with id '{entity.id}'.")
        # If the entity does not already exist, create it
        else:
            LOG.debug(f"Creating a {self.table_name} entry in SQLite...")
            serialized_data = serialize_entity(entity)
            with SQLiteConnection() as conn:
                conn.execute(f"""
                    INSERT INTO {self.table_name} (id, name, runs, additional_data) 
                    VALUES (:id, :name, :runs, :additional_data)
                """, serialized_data)
            LOG.debug(f"Successfully created a {self.table_name} with id '{entity.id}' in SQLite.")


class SQLiteRunStore(SQLiteStoreBase[RunModel]):
    """SQLite store for Run entities with built-in name support."""
    
    def __init__(self):
        super().__init__("run", RunModel)

    # def save(self, entity: T):
    #     """
    #     Save or update an entity in the SQLite database.

    #     Args:
    #         entity: The entity to save.
    #     """
    #     # Try to retrieve any existing entity with this ID
    #     existing_data = self.retrieve(entity.id)

    #     # If the entity already exists, update it
    #     if existing_data:
    #         LOG.debug(f"Attempting to update {self.table_name} with id '{entity.id}'...")
    #         existing_data.update_fields(entity.to_dict())
    #         serialized_data = serialize_entity(existing_data)
    #         with SQLiteConnection() as conn:
    #             conn.execute(f"""
    #                 UPDATE {self.table_name} 
    #                 SET name = :name, runs = :runs, additional_data = :additional_data
    #                 WHERE id = :id
    #             """, serialized_data)
    #         LOG.debug(f"Successfully updated {self.table_name} with id '{entity.id}'.")
    #     # If the entity does not already exist, create it
    #     else:
    #         LOG.debug(f"Creating a {self.table_name} entry in SQLite...")
    #         serialized_data = serialize_entity(entity)
    #         with SQLiteConnection() as conn:
    #             conn.execute(f"""
    #                 INSERT INTO {self.table_name} (id, name, runs, additional_data) 
    #                 VALUES (:id, :name, :runs, :additional_data)
    #             """, serialized_data)
    #         LOG.debug(f"Successfully created a {self.table_name} with id '{entity.id}' in SQLite.")


class SQLiteLogicalWorkerStore(SQLiteStoreBase[LogicalWorkerModel]):
    """SQLite store for LogicalWorker entities."""
    
    def __init__(self):
        super().__init__("logical_worker", LogicalWorkerModel)


class SQLitePhysicalWorkerStore(SQLiteStoreBase[PhysicalWorkerModel]):
    """SQLite store for PhysicalWorker entities."""
    
    def __init__(self):
        super().__init__("physical_worker", PhysicalWorkerModel)
