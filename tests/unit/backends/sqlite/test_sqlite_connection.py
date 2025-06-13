##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `sqlite_connection.py` module.
"""

import os
import sqlite3
import sys
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from merlin.backends.sqlite.sqlite_store_base import SQLiteConnection
from tests.fixture_types import FixtureDict


@pytest.fixture
def mock_sqlite_components(mocker: MockerFixture) -> FixtureDict[str, MagicMock]:
    """
    Fixture to patch all external dependencies used by SQLiteConnection.

    Args:
        mocker: PyTest mocker fixture.

    Returns:
        A dictionary of mocked sqlite connection components.
    """
    # Patch get_connection_string to return a fake path
    mock_conn_str = os.path.join("tmp", "fake", "merlin.db")
    mock_get_conn_str = mocker.patch("merlin.config.results_backend.get_connection_string", return_value=mock_conn_str)

    # Patch Path.mkdir so it doesn't touch the filesystem
    mock_mkdir = mocker.patch("pathlib.Path.mkdir")

    # Patch sqlite3.connect
    mock_conn = MagicMock(spec=sqlite3.Connection)
    mock_connect = mocker.patch("sqlite3.connect", return_value=mock_conn)

    return {
        "mock_conn_str": mock_conn_str,
        "mock_get_conn_str": mock_get_conn_str,
        "mock_mkdir": mock_mkdir,
        "mock_connect": mock_connect,
        "mock_conn": mock_conn,
    }


def test_connection_enter_sets_up_connection(mock_sqlite_components: FixtureDict[str, MagicMock]):
    """
    Test that `__enter__` correctly initializes the SQLite connection with expected settings.

    Args:
        mock_sqlite_components: A dictionary of mocked sqlite connection components.
    """
    conn_mock = mock_sqlite_components["mock_conn"]
    conn_mock.execute = MagicMock()

    with SQLiteConnection() as conn:
        assert conn is conn_mock

    mock_sqlite_components["mock_get_conn_str"].assert_called_once()
    autocommit_kwargs = {"autocommit": True} if sys.version_info >= (3, 12) else {"isolation_level": None}
    mock_sqlite_components["mock_connect"].assert_called_once_with(
        mock_sqlite_components["mock_conn_str"],
        check_same_thread=False,
        **autocommit_kwargs,
    )
    conn_mock.execute.assert_any_call("PRAGMA journal_mode=WAL")
    conn_mock.execute.assert_any_call("PRAGMA foreign_keys=ON")
    assert conn_mock.row_factory == sqlite3.Row


def test_connection_exit_closes_connection(mock_sqlite_components: FixtureDict[str, MagicMock]):
    """
    Test that `__exit__` closes the SQLite connection.

    Args:
        mock_sqlite_components: A dictionary of mocked sqlite connection components.
    """
    conn_mock = mock_sqlite_components["mock_conn"]
    sqlite_conn = SQLiteConnection()
    sqlite_conn.conn = conn_mock

    sqlite_conn.__exit__(None, None, None)

    conn_mock.close.assert_called_once()
