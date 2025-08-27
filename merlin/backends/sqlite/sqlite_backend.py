##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
SQLite backend implementation for the Merlin application.

This module defines the `SQLiteBackend` class, which provides a concrete
implementation of the `ResultsBackend` interface using SQLite as the underlying
storage system. It coordinates interactions with entity-specific SQLite store
classes for studies, runs, logical workers, and physical workers.

The backend supports standard CRUD operations, schema initialization, and
database flushing. When used with the `FilterSupportMixin`, it also supports
field-based filtering for entity retrieval.
"""

import logging

from merlin.backends.filter_support_mixin import FilterSupportMixin
from merlin.backends.results_backend import ResultsBackend
from merlin.backends.sqlite.sqlite_connection import SQLiteConnection
from merlin.backends.sqlite.sqlite_stores import (
    SQLiteLogicalWorkerStore,
    SQLitePhysicalWorkerStore,
    SQLiteRunStore,
    SQLiteStudyStore,
)


LOG = logging.getLogger(__name__)


class SQLiteBackend(ResultsBackend, FilterSupportMixin):
    """
    A SQLite-based implementation of the `ResultsBackend` interface for managing
    entity data in a local SQLite database.

    This backend delegates entity-specific operations to corresponding SQLite store
    classes and provides methods for common database operations such as saving,
    retrieving, deleting, and filtering entities. It also supports complete
    database flushing by dropping and recreating schema tables.

    Filtering functionality is enabled via the `FilterSupportMixin`, allowing
    for flexible retrieval of entities based on field-value filters.

    Attributes:
        backend_name (str): The name of the backend (e.g., "sqlite").

    Methods:
        get_version:
            Query SQLite for the current version.

        get_connection_string:
            Retrieve the connection string (file path) used to connect to SQLite.

        flush_database:
            Remove every entry in the SQLite database by dropping and recreating tables.

        save:
            Save a `BaseDataModel` entity to the SQLite database.

        retrieve:
            Retrieve an entity from the appropriate store based on the given query identifier and store type.

        retrieve_all:
            Retrieve all entities from the specified store.

        retrieve_all_filtered:
            Retrieve all entities from the specified store, applying filters if given.

        delete:
            Delete an entity from the specified store.
    """

    def __init__(self):
        """
        Initialize the `SQLiteBackend` instance, setting up the store mappings and tables.
        """
        stores = {
            "study": SQLiteStudyStore(),
            "run": SQLiteRunStore(),
            "logical_worker": SQLiteLogicalWorkerStore(),
            "physical_worker": SQLitePhysicalWorkerStore(),
        }

        super().__init__("sqlite", stores)

        # Initialize database schema
        self._initialize_schema()

    def _initialize_schema(self):
        """Initialize the database schema by creating all necessary tables."""
        for store in self.stores.values():
            store.create_table_if_not_exists()

    def get_version(self) -> str:
        """
        Query SQLite for the current version.

        Returns:
            The SQLite version string.
        """
        with SQLiteConnection() as conn:
            cursor = conn.execute("SELECT sqlite_version()")
            return cursor.fetchone()[0]

    def flush_database(self):
        """
        Remove every entry in the SQLite database by dropping and recreating tables.
        """
        with SQLiteConnection() as conn:
            # Get all table names
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = [row[0] for row in cursor.fetchall()]

            # Drop all tables
            for table in tables:
                conn.execute(f"DROP TABLE IF EXISTS {table}")
