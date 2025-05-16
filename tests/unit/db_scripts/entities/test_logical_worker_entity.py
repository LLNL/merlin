"""
Tests for the `logical_worker_entity.py` module.
"""

from unittest.mock import MagicMock, patch

import pytest

from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.data_models import LogicalWorkerModel
from merlin.db_scripts.entities.logical_worker_entity import LogicalWorkerEntity
from merlin.db_scripts.entities.physical_worker_entity import PhysicalWorkerEntity


class TestLogicalWorkerEntity:
    """Tests for the `LogicalWorkerEntity` class."""

    @pytest.fixture
    def mock_model(self) -> MagicMock:
        """
        Create a mock `LogicalWorkerModel` for testing.

        Returns:
            A mocked `LogicalWorkerModel` instance.
        """
        model = MagicMock(spec=LogicalWorkerModel)
        model.id = "logical_123"
        model.name = "test_logical_worker"
        model.runs = ["run_1", "run_2"]
        model.queues = ["queue_1", "queue_2"]
        model.physical_workers = ["physical_1", "physical_2"]
        model.additional_data = {"key": "value"}
        return model

    @pytest.fixture
    def mock_backend(self, mock_model: MagicMock) -> MagicMock:
        """
        Create a mock `ResultsBackend` for testing.

        Args:
            mock_model: A mocked `LogicalWorkerModel` instance.

        Returns:
            A mocked `ResultsBackend` instance.
        """
        backend = MagicMock(spec=ResultsBackend)
        backend.get_name.return_value = "mock_backend"
        # Mock the retrieve return so it always returns our test model
        backend.retrieve.return_value = mock_model
        return backend

    @pytest.fixture
    def logical_worker_entity(self, mock_model: MagicMock, mock_backend: MagicMock):
        """
        Create a `LogicalWorkerEntity` instance for testing.

        Args:
            mock_model: A mocked `LogicalWorkerEntity` instance.
            mock_backend: A mocked `ResultsBackend` instance.

        Returns:
            A `LogicalWorkerEntity` instance.
        """
        return LogicalWorkerEntity(mock_model, mock_backend)

    @pytest.fixture
    def mock_physical_worker(self) -> PhysicalWorkerEntity:
        """
        Mock `PhysicalWorkerEntity` for testing.

        Returns:
            A mocked `PhysicalWorkerEntity` instance.
        """
        worker = MagicMock(spec=PhysicalWorkerEntity)
        worker.get_id.return_value = "physical_1"
        worker.get_name.return_value = "test_physical_worker"
        return worker

    def test_get_entity_type(self):
        """Test that _get_entity_type returns the correct value."""
        assert LogicalWorkerEntity._get_entity_type() == "logical_worker"

    def test_repr(self, logical_worker_entity: LogicalWorkerEntity):
        """
        Test the __repr__ method.

        Args:
            logical_worker_entity: A fixture that returns a `LogicalWorkerEntity` instance.
        """
        repr_str = repr(logical_worker_entity)
        assert "LogicalWorkerEntity" in repr_str
        assert f"id={logical_worker_entity.get_id()}" in repr_str
        assert f"name={logical_worker_entity.get_name()}" in repr_str

    def test_str(self, logical_worker_entity: LogicalWorkerEntity, mock_physical_worker: PhysicalWorkerEntity):
        """
        Test the __str__ method.

        Args:
            logical_worker_entity: A fixture that returns a `LogicalWorkerEntity` instance.
            mock_physical_worker: A fixture that returns a `PhysicalWorkerEntity` instance.
        """
        with patch.object(PhysicalWorkerEntity, "load", return_value=mock_physical_worker):
            str_output = str(logical_worker_entity)
            assert f"Logical Worker with ID {logical_worker_entity.get_id()}" in str_output
            assert f"Name: {logical_worker_entity.get_name()}" in str_output
            assert "Physical Workers:" in str_output

    def test_get_physical_workers(self, logical_worker_entity: LogicalWorkerEntity, mock_model: MagicMock):
        """
        Test get_physical_workers returns the correct value.

        Args:
            logical_worker_entity: A fixture that returns a `LogicalWorkerEntity` instance.
            mock_model: A fixture that returns a mocked `LogicalWorkerModel` instance.
        """
        assert logical_worker_entity.get_physical_workers() == mock_model.physical_workers

    def test_add_physical_worker(self, logical_worker_entity: LogicalWorkerEntity, mock_backend: MagicMock):
        """
        Test add_physical_worker adds the worker and saves the model.

        Args:
            logical_worker_entity: A fixture that returns a `LogicalWorkerEntity` instance.
            mock_backend: A fixture that returns a mocked `ResultsBackend` instance.
        """
        new_worker_id = "physical_3"
        logical_worker_entity.add_physical_worker(new_worker_id)
        assert new_worker_id in logical_worker_entity.entity_info.physical_workers
        mock_backend.save.assert_called_once()

    def test_remove_physical_worker(self, logical_worker_entity: LogicalWorkerEntity, mock_backend: MagicMock):
        """
        Test remove_physical_worker removes the worker and saves the model.

        Args:
            logical_worker_entity: A fixture that returns a `LogicalWorkerEntity` instance.
            mock_backend: A fixture that returns a mocked `ResultsBackend` instance.
        """
        worker_id = "physical_1"
        logical_worker_entity.remove_physical_worker(worker_id)
        assert worker_id not in logical_worker_entity.entity_info.physical_workers
        mock_backend.save.assert_called_once()

    def test_get_runs(self, logical_worker_entity: LogicalWorkerEntity, mock_model: MagicMock):
        """
        Test get_runs returns the correct value.

        Args:
            logical_worker_entity: A fixture that returns a `LogicalWorkerEntity` instance.
            mock_model: A fixture that returns a mocked `LogicalWorkerModel` instance.
        """
        assert logical_worker_entity.get_runs() == mock_model.runs

    def test_add_run(self, logical_worker_entity: LogicalWorkerEntity, mock_backend: MagicMock):
        """
        Test add_run adds the run and saves the model.

        Args:
            logical_worker_entity: A fixture that returns a `LogicalWorkerEntity` instance.
            mock_backend: A fixture that returns a mocked `ResultsBackend` instance.
        """
        new_run_id = "run_3"
        logical_worker_entity.add_run(new_run_id)
        assert new_run_id in logical_worker_entity.entity_info.runs
        mock_backend.save.assert_called_once()

    def test_remove_run(self, logical_worker_entity: LogicalWorkerEntity, mock_backend: MagicMock):
        """
        Test remove_run removes the run and saves the model.

        Args:
            logical_worker_entity: A fixture that returns a `LogicalWorkerEntity` instance.
            mock_backend: A fixture that returns a mocked `ResultsBackend` instance.
        """
        run_id = "run_1"
        logical_worker_entity.remove_run(run_id)
        assert run_id not in logical_worker_entity.entity_info.runs
        mock_backend.save.assert_called_once()

    def test_get_queues(self, logical_worker_entity: LogicalWorkerEntity, mock_model: MagicMock):
        """
        Test get_queues returns the correct value.

        Args:
            logical_worker_entity: A fixture that returns a `LogicalWorkerEntity` instance.
            mock_model: A fixture that returns a mocked `LogicalWorkerModel` instance.
        """
        assert logical_worker_entity.get_queues() == mock_model.queues

    @patch.object(LogicalWorkerEntity, "load")
    def test_load(self, mock_load: MagicMock, mock_backend: MagicMock):
        """
        Test the load class method.

        Args:
            mock_load: A mocked version of the `load` method of `LogicalWorkerEntity`.
            mock_backend: A fixture that returns a mocked `ResultsBackend` instance.
        """
        entity_id = "logical_123"
        mock_load.return_value = "loaded_entity"
        result = LogicalWorkerEntity.load(entity_id, mock_backend)
        mock_load.assert_called_once_with(entity_id, mock_backend)
        assert result == "loaded_entity"

    @patch.object(LogicalWorkerEntity, "delete")
    def test_delete(self, mock_delete: MagicMock, mock_backend: MagicMock):
        """
        Test the delete class method.

        Args:
            mock_delete: A mocked version of the `delete` method of `LogicalWorkerEntity`.
            mock_backend: A fixture that returns a mocked `ResultsBackend` instance.
        """
        entity_id = "logical_123"
        LogicalWorkerEntity.delete(entity_id, mock_backend)
        mock_delete.assert_called_once_with(entity_id, mock_backend)
