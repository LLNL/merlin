"""
Tests for the `merlin/backends/sqlite/sqlite_stores.py` module.
"""

import pytest
from pytest_mock import MockerFixture

from merlin.backends.sqlite.sqlite_stores import (
    SQLiteLogicalWorkerStore,
    SQLitePhysicalWorkerStore,
    SQLiteRunStore,
    SQLiteStudyStore,
)
from merlin.db_scripts.data_models import LogicalWorkerModel, PhysicalWorkerModel, RunModel, StudyModel


@pytest.fixture
def mock_create_table(mocker: MockerFixture):
    """
    A fixture to mock the `_create_table_if_not_exists` method so we don't actually
    try to create a connection to the database.

    Args:
        mocker: PyTest mocker fixture.
    """
    mocker.patch("merlin.backends.sqlite.sqlite_store_base.SQLiteStoreBase._create_table_if_not_exists")


class TestSQLiteLogicalWorkerStore:
    """Tests for the SQLiteLogicalWorkerStore implementation."""

    def test_initialization(self, mock_create_table):
        """
        Test that the store initializes with the correct attributes.

        Args:
            mock_create_table: Fixture that patches the `_create_table_if_not_exists` method.
        """
        store = SQLiteLogicalWorkerStore()

        assert store.table_name == "logical_worker"
        assert store.model_class == LogicalWorkerModel


class TestSQLitePhysicalWorkerStore:
    """Tests for the SQLitePhysicalWorkerStore implementation."""

    def test_initialization(self, mock_create_table):
        """
        Test that the store initializes with the correct attributes.

        Args:
            mock_create_table: Fixture that patches the `_create_table_if_not_exists` method.
        """
        store = SQLitePhysicalWorkerStore()

        assert store.table_name == "physical_worker"
        assert store.model_class == PhysicalWorkerModel


class TestSQLiteRunStore:
    """Tests for the SQLiteRunStore implementation."""

    def test_initialization(self, mock_create_table):
        """
        Test that the store initializes with the correct attributes.

        Args:
            mock_create_table: Fixture that patches the `_create_table_if_not_exists` method.
        """
        store = SQLiteRunStore()

        assert store.table_name == "run"
        assert store.model_class == RunModel


class TestSQLiteStudyStore:
    """Tests for the SQLiteStudyStore implementation."""

    def test_initialization(self, mock_create_table):
        """
        Test that the store initializes with the correct attributes.

        Args:
            mock_create_table: Fixture that patches the `_create_table_if_not_exists` method.
        """
        store = SQLiteStudyStore()

        assert store.table_name == "study"
        assert store.model_class == StudyModel
