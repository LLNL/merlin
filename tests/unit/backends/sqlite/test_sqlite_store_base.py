"""
Tests for the `sqlite_store_base.py` module.
"""

from datetime import datetime
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from merlin.backends.sqlite.sqlite_store_base import SQLiteStoreBase
from merlin.db_scripts.data_models import RunModel
from merlin.exceptions import RunNotFoundError
from tests.fixture_types import FixtureDict, FixtureTuple


class TestSQLiteStoreBase:
    """Tests for the SQLiteStoreBase class."""

    @pytest.fixture
    def simple_store(self, mocker: MockerFixture) -> SQLiteStoreBase:
        """
        Create a simple store instance for testing.

        Args:
            mocker: PyTest mocker fixture.

        Returns:
            A simple SQLiteStoreBase implementation for testing.
        """
        # Mock the create_table_if_not_exists method to avoid actual table creation
        mocker.patch.object(SQLiteStoreBase, "create_table_if_not_exists")

        # Create a simple subclass of SQLiteStoreBase for testing
        class TestStore(SQLiteStoreBase):
            pass

        store = TestStore("test_table", RunModel)
        return store

    def test_initialization(self, mocker: MockerFixture):
        """
        Test that the store initializes correctly.

        Args:
            mocker: PyTest mocker fixture.
        """
        # Mock the create_table_if_not_exists method
        mock_create_table = mocker.patch.object(SQLiteStoreBase, "create_table_if_not_exists")

        store = SQLiteStoreBase("test_table", RunModel)

        assert store.table_name == "test_table"
        assert store.model_class == RunModel
        mock_create_table.assert_called_once()

    def test_get_sqlite_type(self, simple_store: SQLiteStoreBase):
        """
        Test the _get_sqlite_type method with various Python types.

        Args:
            simple_store: A fixture providing a SQLiteStoreBase instance.
        """
        # Test basic types
        assert simple_store._get_sqlite_type(str) == "TEXT"
        assert simple_store._get_sqlite_type(int) == "INTEGER"
        assert simple_store._get_sqlite_type(float) == "REAL"
        assert simple_store._get_sqlite_type(bool) == "INTEGER"
        assert simple_store._get_sqlite_type(datetime) == "TEXT"

        # Test generic types (lists, dicts, sets)
        from typing import Dict, List, Set

        assert simple_store._get_sqlite_type(List[str]) == "TEXT"
        assert simple_store._get_sqlite_type(Dict[str, int]) == "TEXT"
        assert simple_store._get_sqlite_type(Set[str]) == "TEXT"

        # Test unknown type (should default to TEXT)
        class CustomType:
            pass

        assert simple_store._get_sqlite_type(CustomType) == "TEXT"

    def testcreate_table_if_not_exists(self, mock_sqlite_connection: FixtureTuple[MagicMock]):
        """
        Test the create_table_if_not_exists method.

        Args:
            mock_sqlite_connection: Fixture providing mocked SQLite connection and cursor.
        """
        # Grab the mocked connection from the SQLiteConnection fixture
        mock_conn, _ = mock_sqlite_connection

        # Mock model class fields
        mock_field1 = MagicMock()
        mock_field1.name = "id"
        mock_field1.type = str
        mock_field2 = MagicMock()
        mock_field2.name = "study_id"
        mock_field2.type = str

        mock_model_class = MagicMock()
        mock_model_class.get_class_fields.return_value = [mock_field1, mock_field2]

        # Create store (this will call create_table_if_not_exists)
        SQLiteStoreBase("test_table", mock_model_class)

        # Verify the SQL execution
        expected_sql = "CREATE TABLE IF NOT EXISTS test_table (id TEXT, study_id TEXT);"
        mock_conn.execute.assert_called_once_with(expected_sql)

    def test_save_new_object(
        self,
        mocker: MockerFixture,
        test_models: FixtureDict,  # Assuming similar fixture structure
        simple_store: SQLiteStoreBase,
        mock_sqlite_connection: FixtureTuple[MagicMock],
    ):
        """
        Test saving a new object to SQLite.

        Args:
            mocker: PyTest mocker fixture.
            test_models: A fixture providing test model instances.
            simple_store: A fixture providing a SQLiteStoreBase instance.
            mock_sqlite_connection: Fixture providing mocked SQLite connection and cursor.
        """
        run = test_models["run"]

        # Mock retrieve to return None (object doesn't exist)
        mocker.patch.object(simple_store, "retrieve", return_value=None)

        # Grab the mocked connection from the SQLiteConnection fixture
        mock_conn, _ = mock_sqlite_connection

        # Mock serialization
        mock_serialize = mocker.patch(
            "merlin.backends.sqlite.sqlite_store_base.serialize_entity", return_value={"id": run.id, "study_id": "study1"}
        )

        # Mock model class fields
        mock_field1 = MagicMock()
        mock_field1.name = "id"
        mock_field2 = MagicMock()
        mock_field2.name = "study_id"
        mocker.patch.object(simple_store.model_class, "get_class_fields", return_value=[mock_field1, mock_field2])

        simple_store.save(run)

        # Verify method calls
        simple_store.retrieve.assert_called_once_with(run.id)
        mock_serialize.assert_called_once_with(run)
        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args
        # Check that the SQL contains the expected structure (whitespace may vary)
        assert "INSERT INTO test_table" in call_args[0][0]
        assert call_args[0][1] == {"id": run.id, "study_id": "study1"}

    def test_save_existing_object(
        self,
        mocker: MockerFixture,
        test_models: FixtureDict,
        simple_store: SQLiteStoreBase,
        mock_sqlite_connection: FixtureTuple[MagicMock],
    ):
        """
        Test updating an existing object in SQLite.

        Args:
            mocker: PyTest mocker fixture.
            test_models: A fixture providing test model instances.
            simple_store: A fixture providing a SQLiteStoreBase instance.
            mock_sqlite_connection: Fixture providing mocked SQLite connection and cursor.
        """
        run = test_models["run"]

        # Mock existing object
        mock_existing = MagicMock()
        mock_existing.update_fields = MagicMock()
        mocker.patch.object(simple_store, "retrieve", return_value=mock_existing)

        # Grab the mocked connection from the SQLiteConnection fixture
        mock_conn, _ = mock_sqlite_connection

        # Mock serialization
        mock_serialize = mocker.patch(
            "merlin.backends.sqlite.sqlite_store_base.serialize_entity", return_value={"id": run.id, "study_id": "study1"}
        )

        # Mock model class fields
        mock_field1 = MagicMock()
        mock_field1.name = "id"
        mock_field2 = MagicMock()
        mock_field2.name = "study_id"
        mocker.patch.object(simple_store.model_class, "get_class_fields", return_value=[mock_field1, mock_field2])

        simple_store.save(run)

        # Verify method calls
        simple_store.retrieve.assert_called_once_with(run.id)
        mock_existing.update_fields.assert_called_once_with(run.to_dict())
        mock_serialize.assert_called_once_with(mock_existing)

        # Verify UPDATE SQL was executed
        mock_conn.execute.assert_called_once()
        call_args = mock_conn.execute.call_args
        assert "UPDATE test_table" in call_args[0][0]
        assert "WHERE id = :id" in call_args[0][0]

    @pytest.mark.parametrize("by_name, identifier_key", [(False, "id"), (True, "name")])
    def test_retrieve_existing_object(
        self,
        mocker: MockerFixture,
        test_models: FixtureDict,
        simple_store: SQLiteStoreBase,
        mock_sqlite_connection: FixtureTuple[MagicMock],
        by_name: bool,
        identifier_key: str,
    ):
        """
        Parametrized test for retrieving an existing object by ID or name from SQLite.

        Args:
            mocker: PyTest mocker fixture.
            test_models: Fixture providing test model instances.
            simple_store: A SQLiteStoreBase instance.
            mock_sqlite_connection: Fixture providing mocked SQLite connection and cursor.
            by_name: Whether to retrieve by name (True) or by ID (False).
            identifier_key: Either "id" or "name", depending on lookup method.
        """
        run = test_models["run"]
        identifier = "test_run" if by_name else run.id

        # Mock connection and cursor
        mock_conn, mock_cursor = mock_sqlite_connection
        mock_row = {"id": run.id, "study_id": "study1", "name": "test_run"}
        mock_cursor.fetchone.return_value = mock_row

        # Mock deserialization
        mock_deserialize = mocker.patch("merlin.backends.sqlite.sqlite_store_base.deserialize_entity", return_value=run)

        result = simple_store.retrieve(identifier, by_name=by_name)

        mock_conn.execute.assert_called_once_with(
            f"SELECT * FROM test_table WHERE {identifier_key} = :identifier", {"identifier": identifier}
        )
        mock_cursor.fetchone.assert_called_once()
        mock_deserialize.assert_called_once_with(mock_row, simple_store.model_class)
        assert result == run

    def test_retrieve_nonexistent_object(
        self,
        simple_store: SQLiteStoreBase,
        mock_sqlite_connection: FixtureTuple[MagicMock],
    ):
        """
        Test retrieving a non-existent object from SQLite.

        Args:
            simple_store: A fixture providing a SQLiteStoreBase instance.
            mock_sqlite_connection: Fixture providing mocked SQLite connection and cursor.
        """
        # Mock SQLiteConnection and cursor
        mock_conn, mock_cursor = mock_sqlite_connection
        mock_cursor.fetchone.return_value = None

        result = simple_store.retrieve("nonexistent_id")

        # Verify method calls
        mock_conn.execute.assert_called_once_with(
            "SELECT * FROM test_table WHERE id = :identifier", {"identifier": "nonexistent_id"}
        )
        mock_cursor.fetchone.assert_called_once()
        assert result is None

    def test_retrieve_all(
        self,
        mocker: MockerFixture,
        test_models: FixtureDict,
        simple_store: SQLiteStoreBase,
        mock_sqlite_connection: FixtureTuple[MagicMock],
    ):
        """
        Test retrieving all objects from SQLite.

        Args:
            mocker: PyTest mocker fixture.
            test_models: A fixture providing test model instances.
            simple_store: A fixture providing a SQLiteStoreBase instance.
            mock_sqlite_connection: Fixture providing mocked SQLite connection and cursor.
        """
        run1 = test_models["run"]
        run2 = RunModel(id="run2", study_id="study1")

        # Mock SQLiteConnection and cursor
        mock_conn, mock_cursor = mock_sqlite_connection
        mock_rows = [{"id": run1.id, "study_id": "study1"}, {"id": run2.id, "study_id": "study1"}]
        mock_cursor.fetchall.return_value = mock_rows

        # Mock deserialization
        mock_deserialize = mocker.patch(
            "merlin.backends.sqlite.sqlite_store_base.deserialize_entity", side_effect=[run1, run2]
        )

        results = simple_store.retrieve_all()

        # Verify method calls
        mock_conn.execute.assert_called_once_with("SELECT * FROM test_table")
        mock_cursor.fetchall.assert_called_once()
        assert mock_deserialize.call_count == 2
        assert len(results) == 2
        assert run1 in results
        assert run2 in results

    def test_retrieve_all_with_deserialization_error(
        self,
        mocker: MockerFixture,
        simple_store: SQLiteStoreBase,
        mock_sqlite_connection: FixtureTuple[MagicMock],
    ):
        """
        Test retrieving all objects when deserialization fails for some objects.

        Args:
            mocker: PyTest mocker fixture.
            simple_store: A fixture providing a SQLiteStoreBase instance.
            mock_sqlite_connection: Fixture providing mocked SQLite connection and cursor.
        """
        # Mock SQLiteConnection and cursor
        _, mock_cursor = mock_sqlite_connection
        mock_rows = [{"id": "run1", "study_id": "study1"}, {"id": "run2", "study_id": "study1"}]  # This one will fail
        mock_cursor.fetchall.return_value = mock_rows

        # Mock deserialization - first succeeds, second fails
        run1 = RunModel(id="run1", study_id="study1")
        mocker.patch(
            "merlin.backends.sqlite.sqlite_store_base.deserialize_entity",
            side_effect=[run1, Exception("Deserialization error")],
        )

        results = simple_store.retrieve_all()

        # Verify that only the successful object is returned
        assert len(results) == 1
        assert results[0] == run1

    @pytest.mark.parametrize(
        "identifier, by_name, identifier_key",
        [
            ("run1", False, "id"),
            ("test_run", True, "name"),
        ],
    )
    def test_delete_existing_object(
        self,
        mocker: MockerFixture,
        test_models: FixtureDict,
        simple_store: SQLiteStoreBase,
        mock_sqlite_connection: FixtureTuple[MagicMock],
        identifier: str,
        by_name: bool,
        identifier_key: str,
    ):
        """
        Parametrized test for deleting an existing object from SQLite
        by ID or name.

        Args:
            mocker: PyTest mocker fixture.
            test_models: A fixture providing test model instances.
            simple_store: A fixture providing a SQLiteStoreBase instance.
            mock_sqlite_connection: Fixture providing mocked SQLite connection and cursor.
            identifier: The value used to identify the object (id or name).
            by_name: Whether the deletion is by name or by id.
            identifier_key: The column to delete on ("id" or "name").
        """
        run = test_models["run"]

        # Mock retrieve to return the object
        mocker.patch.object(simple_store, "retrieve", return_value=run)

        # Mock SQLiteConnection and cursor
        mock_conn, mock_cursor = mock_sqlite_connection
        mock_cursor.rowcount = 1

        # Mock error class
        mocker.patch("merlin.backends.sqlite.sqlite_store_base.get_not_found_error_class", return_value=RunNotFoundError)

        simple_store.delete(identifier, by_name=by_name)

        # Verify calls
        simple_store.retrieve.assert_called_once_with(identifier, by_name=by_name)
        mock_conn.execute.assert_called_once_with(
            f"DELETE FROM test_table WHERE {identifier_key} = :identifier", {"identifier": identifier}
        )

    def test_delete_nonexistent_object(
        self,
        mocker: MockerFixture,
        simple_store: SQLiteStoreBase,
    ):
        """
        Test deleting a non-existent object from SQLite.

        Args:
            mocker: PyTest mocker fixture.
            simple_store: A fixture providing a SQLiteStoreBase instance.
        """
        # Mock retrieve to return None
        mocker.patch.object(simple_store, "retrieve", return_value=None)

        # Mock error class
        mocker.patch("merlin.backends.sqlite.sqlite_store_base.get_not_found_error_class", return_value=RunNotFoundError)

        with pytest.raises(RunNotFoundError, match="Test_table with id 'nonexistent_id' does not exist in the database."):
            simple_store.delete("nonexistent_id")

        # Verify that retrieve was called but execute was not
        simple_store.retrieve.assert_called_once_with("nonexistent_id", by_name=False)

    def test_delete_with_zero_rowcount(
        self,
        mocker: MockerFixture,
        test_models: FixtureDict,
        simple_store: SQLiteStoreBase,
        mock_sqlite_connection: FixtureTuple[MagicMock],
    ):
        """
        Test deleting an object that exists during retrieve but fails to delete (rowcount = 0).

        Args:
            mocker: PyTest mocker fixture.
            test_models: A fixture providing test model instances.
            simple_store: A fixture providing a SQLiteStoreBase instance.
            mock_sqlite_connection: Fixture providing mocked SQLite connection and cursor.
        """
        run = test_models["run"]

        # Mock retrieve to return the object
        mocker.patch.object(simple_store, "retrieve", return_value=run)

        # Mock SQLiteConnection and cursor
        mock_conn, mock_cursor = mock_sqlite_connection
        mock_cursor.rowcount = 0

        # This should not raise an exception, just log a warning
        simple_store.delete(run.id)

        # Verify method calls
        simple_store.retrieve.assert_called_once_with(run.id, by_name=False)
        mock_conn.execute.assert_called_once_with("DELETE FROM test_table WHERE id = :identifier", {"identifier": run.id})
