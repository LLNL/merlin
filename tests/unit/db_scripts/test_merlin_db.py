"""
Tests for the `merlin_db.py` module.
"""

import logging
from typing import Generator
from unittest.mock import MagicMock, patch

import pytest
from _pytest.capture import CaptureFixture

from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.entity_managers.logical_worker_manager import LogicalWorkerManager
from merlin.db_scripts.entity_managers.physical_worker_manager import PhysicalWorkerManager
from merlin.db_scripts.entity_managers.run_manager import RunManager
from merlin.db_scripts.entity_managers.study_manager import StudyManager
from merlin.db_scripts.merlin_db import MerlinDatabase
from merlin.exceptions import EntityManagerNotSupportedError
from tests.fixture_types import FixtureDict


@pytest.fixture
def mock_backend() -> MagicMock:
    """
    Create a mock backend instance.

    Returns:
        A mocked `ResultsBackend` instance.
    """
    mock = MagicMock(spec=ResultsBackend)
    mock.get_name.return_value = "mock_redis"
    mock.get_version.return_value = "1.0"
    mock.get_connection_string.return_value = "redis://localhost:6379/0"
    return mock


@pytest.fixture
def mock_entity_managers() -> FixtureDict[str, MagicMock]:
    """
    Create mock entity managers.

    Returns:
        A dictionary of mocked objects of `DatabaseManager` types.
    """
    study_manager = MagicMock(spec=StudyManager)
    run_manager = MagicMock(spec=RunManager)
    logical_worker_manager = MagicMock(spec=LogicalWorkerManager)
    physical_worker_manager = MagicMock(spec=PhysicalWorkerManager)

    managers = {
        "study": study_manager,
        "run": run_manager,
        "logical_worker": logical_worker_manager,
        "physical_worker": physical_worker_manager,
    }

    return managers


@pytest.fixture
def mock_merlin_db(
    mock_backend: MagicMock, mock_entity_managers: FixtureDict[str, MagicMock]
) -> Generator[MerlinDatabase, None, None]:
    """
    Create a `MerlinDatabase` instance with mocked components.

    Args:
        mock_backend: A mocked `ResultsBackend` instance.
        mock_entity_managers: A dictionary of mocked objects of `DatabaseManager` types.

    Returns:
        A `MerlinDatabase` instance with moocked attributes.
    """
    with (
        patch("merlin.db_scripts.merlin_db.backend_factory") as mock_factory,
        patch("merlin.db_scripts.merlin_db.StudyManager", return_value=mock_entity_managers["study"]),
        patch("merlin.db_scripts.merlin_db.RunManager", return_value=mock_entity_managers["run"]),
        patch("merlin.db_scripts.merlin_db.LogicalWorkerManager", return_value=mock_entity_managers["logical_worker"]),
        patch("merlin.db_scripts.merlin_db.PhysicalWorkerManager", return_value=mock_entity_managers["physical_worker"]),
    ):

        mock_factory.get_backend.return_value = mock_backend
        db = MerlinDatabase()

        # Replace backend and entity managers with mocks
        db.backend = mock_backend
        db._entity_managers = mock_entity_managers

        yield db


class TestMerlinDatabase:
    """Test cases for `MerlinDatabase` class."""

    def test_init(self, mock_backend: MagicMock):
        """
        Test initialization of `MerlinDatabase`.

        Args:
            mock_backend: A mocked `ResultsBackend` instance.
        """
        with (
            patch("merlin.db_scripts.merlin_db.backend_factory") as mock_factory,
            patch("merlin.db_scripts.merlin_db.StudyManager") as mock_study_manager,
            patch("merlin.db_scripts.merlin_db.RunManager") as mock_run_manager,
            patch("merlin.db_scripts.merlin_db.LogicalWorkerManager") as mock_logical_worker_manager,
            patch("merlin.db_scripts.merlin_db.PhysicalWorkerManager") as mock_physical_worker_manager,
            patch("merlin.config.configfile.CONFIG") as mock_config,
        ):

            # Configure mocks
            mock_factory.get_backend.return_value = mock_backend
            mock_config.results_backend.name = "redis"

            # Create instances
            db = MerlinDatabase()

            # Verify backend was created
            mock_factory.get_backend.assert_called_once_with("redis")

            # Verify entity managers were created with the backend
            mock_study_manager.assert_called_once_with(mock_backend)
            mock_run_manager.assert_called_once_with(mock_backend)
            mock_logical_worker_manager.assert_called_once_with(mock_backend)
            mock_physical_worker_manager.assert_called_once_with(mock_backend)

            # Verify entity managers that have set_db_reference method were set up properly
            for manager in [
                mock_study_manager.return_value,
                mock_run_manager.return_value,
                mock_logical_worker_manager.return_value,
                mock_physical_worker_manager.return_value,
            ]:
                if hasattr(manager, "set_db_reference"):
                    manager.set_db_reference.assert_called_once_with(db)

    def test_properties(self, mock_merlin_db: MerlinDatabase, mock_entity_managers: FixtureDict[str, MagicMock]):
        """
        Test property accessors for entity managers.

        Args:
            mock_merlin_db: A `MerlinDatabase` instance with moocked attributes.
            mock_entity_managers: A dictionary of mocked objects of `DatabaseManager` types.
        """
        assert mock_merlin_db.studies is mock_entity_managers["study"]
        assert mock_merlin_db.runs is mock_entity_managers["run"]
        assert mock_merlin_db.logical_workers is mock_entity_managers["logical_worker"]
        assert mock_merlin_db.physical_workers is mock_entity_managers["physical_worker"]

    def test_get_db_type(self, mock_merlin_db: MerlinDatabase, mock_backend: MagicMock):
        """
        Test `get_db_type` method.

        Args:
            mock_merlin_db: A `MerlinDatabase` instance with moocked attributes.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        result = mock_merlin_db.get_db_type()
        mock_backend.get_name.assert_called_once()
        assert result == "mock_redis"

    def test_get_db_version(self, mock_merlin_db: MerlinDatabase, mock_backend: MagicMock):
        """
        Test `get_db_version` method.

        Args:
            mock_merlin_db: A `MerlinDatabase` instance with moocked attributes.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        result = mock_merlin_db.get_db_version()
        mock_backend.get_version.assert_called_once()
        assert result == "1.0"

    def test_get_connection_string(self, mock_merlin_db: MerlinDatabase, mock_backend: MagicMock):
        """
        Test `get_connection_string` method.

        Args:
            mock_merlin_db: A `MerlinDatabase` instance with moocked attributes.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        result = mock_merlin_db.get_connection_string()
        mock_backend.get_connection_string.assert_called_once()
        assert result == "redis://localhost:6379/0"

    def test_validate_entity_type_valid(self, mock_merlin_db: MerlinDatabase):
        """
        Test `_validate_entity_type` with valid entity types.

        Args:
            mock_merlin_db: A `MerlinDatabase` instance with moocked attributes.
        """
        for entity_type in ["study", "run", "logical_worker", "physical_worker"]:
            # Should not raise an exception
            mock_merlin_db._validate_entity_type(entity_type)

    def test_validate_entity_type_invalid(self, mock_merlin_db: MerlinDatabase):
        """
        Test `_validate_entity_type` with invalid entity type.

        Args:
            mock_merlin_db: A `MerlinDatabase` instance with moocked attributes.
        """
        with pytest.raises(EntityManagerNotSupportedError) as excinfo:
            mock_merlin_db._validate_entity_type("invalid_type")
        assert "Entity type not supported: invalid_type" in str(excinfo.value)

    def test_create(self, mock_merlin_db: MerlinDatabase, mock_entity_managers: FixtureDict[str, MagicMock]):
        """
        Test `create` method.

        Args:
            mock_merlin_db: A `MerlinDatabase` instance with moocked attributes.
            mock_entity_managers: A dictionary of mocked objects of `DatabaseManager` types.
        """
        # Test with valid entity type
        entity_type = "study"
        mock_entity_managers[entity_type].create.return_value = "created_study"

        result = mock_merlin_db.create(entity_type, "arg1", key1="value1")

        mock_entity_managers[entity_type].create.assert_called_once_with("arg1", key1="value1")
        assert result == "created_study"

        # Test with invalid entity type
        with pytest.raises(EntityManagerNotSupportedError):
            mock_merlin_db.create("invalid_type")

    def test_get(self, mock_merlin_db: MerlinDatabase, mock_entity_managers: FixtureDict[str, MagicMock]):
        """
        Test `get` method.

        Args:
            mock_merlin_db: A `MerlinDatabase` instance with moocked attributes.
            mock_entity_managers: A dictionary of mocked objects of `DatabaseManager` types.
        """
        # Test with valid entity type
        entity_type = "run"
        mock_entity_managers[entity_type].get.return_value = "run_entity"

        result = mock_merlin_db.get(entity_type, "run_id", extra_param=True)

        mock_entity_managers[entity_type].get.assert_called_once_with("run_id", extra_param=True)
        assert result == "run_entity"

        # Test with invalid entity type
        with pytest.raises(EntityManagerNotSupportedError):
            mock_merlin_db.get("invalid_type", "id")

    def test_get_all(self, mock_merlin_db: MerlinDatabase, mock_entity_managers: FixtureDict[str, MagicMock]):
        """
        Test `get_all` method.

        Args:
            mock_merlin_db: A `MerlinDatabase` instance with moocked attributes.
            mock_entity_managers: A dictionary of mocked objects of `DatabaseManager` types.
        """
        # Test with valid entity type
        entity_type = "logical_worker"
        mock_entity_managers[entity_type].get_all.return_value = ["worker1", "worker2"]

        result = mock_merlin_db.get_all(entity_type)

        mock_entity_managers[entity_type].get_all.assert_called_once()
        assert result == ["worker1", "worker2"]

        # Test with invalid entity type
        with pytest.raises(EntityManagerNotSupportedError):
            mock_merlin_db.get_all("invalid_type")

    def test_delete(self, mock_merlin_db: MerlinDatabase, mock_entity_managers: FixtureDict[str, MagicMock]):
        """
        Test `delete` method.

        Args:
            mock_merlin_db: A `MerlinDatabase` instance with moocked attributes.
            mock_entity_managers: A dictionary of mocked objects of `DatabaseManager` types.
        """
        # Test with valid entity type
        entity_type = "physical_worker"

        mock_merlin_db.delete(entity_type, "worker_id", force=True)

        mock_entity_managers[entity_type].delete.assert_called_once_with("worker_id", force=True)

        # Test with invalid entity type
        with pytest.raises(EntityManagerNotSupportedError):
            mock_merlin_db.delete("invalid_type", "id")

    def test_delete_all(self, mock_merlin_db: MerlinDatabase, mock_entity_managers: FixtureDict[str, MagicMock]):
        """
        Test `delete_all` method.

        Args:
            mock_merlin_db: A `MerlinDatabase` instance with moocked attributes.
            mock_entity_managers: A dictionary of mocked objects of `DatabaseManager` types.
        """
        # Test with valid entity type
        entity_type = "study"

        mock_merlin_db.delete_all(entity_type, force=True)

        mock_entity_managers[entity_type].delete_all.assert_called_once_with(force=True)

        # Test with invalid entity type
        with pytest.raises(EntityManagerNotSupportedError):
            mock_merlin_db.delete_all("invalid_type")

    def test_get_everything(self, mock_merlin_db: MerlinDatabase, mock_entity_managers: FixtureDict[str, MagicMock]):
        """
        Test `get_everything` method.

        Args:
            mock_merlin_db: A `MerlinDatabase` instance with moocked attributes.
            mock_entity_managers: A dictionary of mocked objects of `DatabaseManager` types.
        """
        # Set up mock returns
        mock_entity_managers["study"].get_all.return_value = ["study1", "study2"]
        mock_entity_managers["run"].get_all.return_value = ["run1", "run2", "run3"]
        mock_entity_managers["logical_worker"].get_all.return_value = ["logical_worker1"]
        mock_entity_managers["physical_worker"].get_all.return_value = ["physical_worker1", "physical_worker2"]

        result = mock_merlin_db.get_everything()

        # Verify all get_all methods were called
        for manager in mock_entity_managers.values():
            manager.get_all.assert_called_once()

        # Verify the result contains all entities
        assert result == [
            "study1",
            "study2",
            "run1",
            "run2",
            "run3",
            "logical_worker1",
            "physical_worker1",
            "physical_worker2",
        ]

    def test_delete_everything_confirmed(
        self, caplog: CaptureFixture, mock_merlin_db: MerlinDatabase, mock_backend: MagicMock
    ):
        """
        Test `delete_everything` method with confirmation.

        Args:
            caplog: A built-in fixture from the pytest library to capture logs.
            mock_merlin_db: A `MerlinDatabase` instance with moocked attributes.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        caplog.set_level(logging.INFO)
        with patch("builtins.input", return_value="y"):

            mock_merlin_db.delete_everything()

            # Verify database flush was called
            mock_backend.flush_database.assert_called_once()
            assert "Flushing the database..." in caplog.text
            assert "Database successfully flushed." in caplog.text

    def test_delete_everything_cancelled(
        self, caplog: CaptureFixture, mock_merlin_db: MerlinDatabase, mock_backend: MagicMock
    ):
        """
        Test `delete_everything` method when cancelled.

        Args:
            caplog: A built-in fixture from the pytest library to capture logs.
            mock_merlin_db: A `MerlinDatabase` instance with moocked attributes.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        caplog.set_level(logging.INFO)
        with patch("builtins.input", return_value="n"):

            mock_merlin_db.delete_everything()

            # Verify database flush was NOT called
            mock_backend.flush_database.assert_not_called()
            assert "Database flush cancelled." in caplog.text

    def test_delete_everything_invalid_input_then_confirmed(
        self,
        caplog: CaptureFixture,
        mock_merlin_db: MerlinDatabase,
        mock_backend: MagicMock,
    ):
        """
        Test `delete_everything` method with invalid input followed by confirmation.

        Args:
            caplog: A built-in fixture from the pytest library to capture logs.
            mock_merlin_db: A `MerlinDatabase` instance with moocked attributes.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        caplog.set_level(logging.INFO)
        # First input is invalid, second is valid 'y'
        with patch("builtins.input", side_effect=["invalid", "y"]):

            mock_merlin_db.delete_everything()

            # Verify database flush was called after valid input
            mock_backend.flush_database.assert_called_once()
            assert "Flushing the database..." in caplog.text
            assert "Database successfully flushed." in caplog.text

    def test_delete_everything_force(self, caplog: CaptureFixture, mock_merlin_db: MerlinDatabase, mock_backend: MagicMock):
        """
        Test `delete_everything` method with force=True.

        Args:
            caplog: A built-in fixture from the pytest library to capture logs.
            mock_merlin_db: A `MerlinDatabase` instance with moocked attributes.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        caplog.set_level(logging.INFO)

        mock_merlin_db.delete_everything(force=True)

        # Verify database flush was called without prompting
        mock_backend.flush_database.assert_called_once()
        assert "Flushing the database..." in caplog.text
        assert "Database successfully flushed." in caplog.text
