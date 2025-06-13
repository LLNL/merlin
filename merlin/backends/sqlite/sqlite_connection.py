##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
SQLite connection context manager for the Merlin application.

This module defines the `SQLiteConnection` class, which provides a safe and reusable way to
establish and manage SQLite connections using a context manager. It ensures proper configuration
(e.g., enabling WAL mode and foreign key support), handles compatibility with Python versions,
and guarantees cleanup by closing the connection on exit.
"""

import logging
import sqlite3
import sys
from pathlib import Path
from types import TracebackType
from typing import Type


LOG = logging.getLogger(__name__)


class SQLiteConnection:
    """
    Context manager for establishing and safely closing a SQLite database connection.

    This class ensures SQLite connections are created with proper configuration, including:
    - WAL mode for better concurrency
    - Foreign key constraint enforcement
    - Dictionary-style row access via `sqlite3.Row`
    - Compatibility with Python versions < 3.12 and ≥ 3.12 regarding autocommit

    Attributes:
        conn (sqlite3.Connection): The active SQLite connection used within the context.

    Methods:
        __enter__:
            Opens and configures the SQLite connection when entering the context.

        __exit__:
            Closes the SQLite connection when exiting the context, handling any exceptions.
    """

    def __init__(self):
        """Initialize the SQLiteConnection context manager."""
        self.conn: sqlite3.Connection = None

    def __enter__(self) -> sqlite3.Connection:
        """
        Enters the runtime context related to this object and creates a sqlite connection.

        Returns:
            A sqlite connection.
        """
        from merlin.config.results_backend import get_connection_string  # pylint: disable=import-outside-toplevel

        # Get the SQLite database path and ensure the path exists
        db_path = get_connection_string()
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        connection_kwargs = {"check_same_thread": False}
        if sys.version_info < (3, 12):  # Autocommit wasn't added until python 3.12
            connection_kwargs["isolation_level"] = None
        else:
            connection_kwargs["autocommit"] = True

        # Create SQLite connection with optimized settings
        self.conn = sqlite3.connect(db_path, **connection_kwargs)

        # Enable WAL mode for better concurrent access
        self.conn.execute("PRAGMA journal_mode=WAL")
        # Enable foreign key constraints
        self.conn.execute("PRAGMA foreign_keys=ON")

        # This enables name-based access to columns
        self.conn.row_factory = sqlite3.Row

        return self.conn

    def __exit__(self, exc_type: Type[Exception], exc_value: Exception, traceback: TracebackType):
        """
        Exits the runtime context and performs cleanup.

        This method closes the connection if it's still open.

        Args:
            exc_type: The exception type raised, if any.
            exc_value: The exception instance raised, if any.
            traceback: The traceback object, if an exception was raised.
        """
        if self.conn:
            self.conn.close()
