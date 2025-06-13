##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `sqlite_backend.py` module.
"""

import pytest
from pytest_mock import MockerFixture

from merlin.backends.sqlite.sqlite_backend import SQLiteBackend


@pytest.fixture
def sqlite_backend_instance(mocker: MockerFixture) -> SQLiteBackend:
    """
    Fixture to create a `SQLiteBackend` instance with mocked dependencies.

    Args:
        mocker (MockerFixture): The pytest-mock fixture used for mocking.

    Returns:
        SQLiteBackend: An instance with mocked store methods and schema creation.
    """
    # Patch the SQLite store classes to return mocks
    mocker.patch("merlin.backends.sqlite.sqlite_backend.SQLiteStudyStore", return_value=mocker.MagicMock())
    mocker.patch("merlin.backends.sqlite.sqlite_backend.SQLiteRunStore", return_value=mocker.MagicMock())
    mocker.patch("merlin.backends.sqlite.sqlite_backend.SQLiteLogicalWorkerStore", return_value=mocker.MagicMock())
    mocker.patch("merlin.backends.sqlite.sqlite_backend.SQLitePhysicalWorkerStore", return_value=mocker.MagicMock())

    # Patch the initialization method to avoid real DB operations
    mocker.patch.object(SQLiteBackend, "_initialize_schema", return_value=None)

    backend = SQLiteBackend("sqlite")
    return backend


class TestSQLiteBackend:
    """
    Test suite for the `SQLiteBackend` class.

    Validates core functionality such as retrieving version information and flushing the database.
    SQLite connections are mocked to prevent real filesystem or database interactions.
    """

    def test_get_version(self, mocker: MockerFixture, sqlite_backend_instance: SQLiteBackend):
        """
        Test that SQLiteBackend correctly retrieves the SQLite version.

        Args:
            sqlite_backend_instance (SQLiteBackend): The mocked backend.
        """
        mock_conn = mocker.MagicMock()
        mock_cursor = mocker.MagicMock()
        mock_cursor.fetchone.return_value = ["3.43.2"]
        mock_conn.execute.return_value = mock_cursor

        # Patch context manager to return mock connection
        mocker.patch("merlin.backends.sqlite.sqlite_backend.SQLiteConnection", return_value=mock_conn)
        mock_conn.__enter__.return_value = mock_conn

        version = sqlite_backend_instance.get_version()
        assert version == "3.43.2", "SQLite version should be correctly retrieved."

    def test_flush_database(self, mocker: MockerFixture, sqlite_backend_instance: SQLiteBackend):
        """
        Test that SQLiteBackend flushes the database by dropping all tables.

        Args:
            sqlite_backend_instance (SQLiteBackend): The mocked backend.
        """
        mock_conn = mocker.MagicMock()
        mock_cursor = mocker.MagicMock()
        mock_cursor.fetchall.return_value = [("table1",), ("table2",)]
        mock_conn.execute.side_effect = lambda sql: mock_cursor if "SELECT name" in sql else None

        # Patch context manager to return mock connection
        mocker.patch("merlin.backends.sqlite.sqlite_backend.SQLiteConnection", return_value=mock_conn)
        mock_conn.__enter__.return_value = mock_conn

        sqlite_backend_instance.flush_database()

        # Ensure DROP TABLE commands were called for the expected tables
        mock_conn.execute.assert_any_call("DROP TABLE IF EXISTS table1")
        mock_conn.execute.assert_any_call("DROP TABLE IF EXISTS table2")
