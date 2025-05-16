"""
Tests for the `physical_worker_entity.py` module.
"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from merlin.backends.results_backend import ResultsBackend
from merlin.common.enums import WorkerStatus
from merlin.db_scripts.data_models import PhysicalWorkerModel
from merlin.db_scripts.entities.physical_worker_entity import PhysicalWorkerEntity


class TestPhysicalWorkerEntity:
    """Tests for the `PhysicalWorkerEntity` class."""

    @pytest.fixture
    def mock_model(self) -> MagicMock:
        """
        Create a mock `PhysicalWorkerModel` for testing.

        Returns:
            A mocked `PhysicalWorkerModel` instance.
        """
        model = MagicMock(spec=PhysicalWorkerModel)
        model.id = "test_id"
        model.name = "test_worker"
        model.logical_worker_id = "logical_worker_123"
        model.launch_cmd = "python worker.py"
        model.args = {"arg1": "value1"}
        model.pid = "12345"
        model.status = WorkerStatus.RUNNING
        model.heartbeat_timestamp = datetime.now().isoformat()
        model.latest_start_time = datetime.now()
        model.host = "test_host"
        model.restart_count = 0
        model.additional_data = {"key": "value"}
        return model

    @pytest.fixture
    def mock_backend(self, mock_model: MagicMock) -> MagicMock:
        """
        Create a mock `ResultsBackend` for testing.

        Args:
            mock_model: A mocked `PhysicalWorkerModel` instance.

        Returns:
            A mocked `ResultsBackend` instance.
        """
        backend = MagicMock(spec=ResultsBackend)
        backend.get_name.return_value = "mock_backend"
        # Mock the retrieve return so it always returns our test model
        backend.retrieve.return_value = mock_model
        return backend

    @pytest.fixture
    def worker_entity(self, mock_model: MagicMock, mock_backend: MagicMock) -> PhysicalWorkerEntity:
        """
        Create a `PhysicalWorkerEntity` instance for testing.

        Args:
            mock_model: A mocked `PhysicalWorkerModel` instance.
            mock_backend: A mocked `ResultsBackend` instance.

        Returns:
            A `PhysicalWorkerEntity` instance.
        """
        return PhysicalWorkerEntity(mock_model, mock_backend)

    def test_get_entity_type(self):
        """Test that _get_entity_type returns the correct value."""
        assert PhysicalWorkerEntity._get_entity_type() == "physical_worker"

    def test_repr(self, worker_entity: PhysicalWorkerEntity):
        """
        Test the __repr__ method.

        Args:
            worker_entity: A fixture that returns a `PhysicalWorkerEntity` instance.
        """
        repr_str = repr(worker_entity)
        assert "PhysicalWorkerEntity" in repr_str
        assert f"id={worker_entity.get_id()}" in repr_str
        assert f"name={worker_entity.get_name()}" in repr_str

    def test_str(self, worker_entity: PhysicalWorkerEntity):
        """
        Test the __str__ method.

        Args:
            worker_entity: A fixture that returns a `PhysicalWorkerEntity` instance.
        """
        str_output = str(worker_entity)
        assert f"Physical Worker with ID {worker_entity.get_id()}" in str_output
        assert f"Name: {worker_entity.get_name()}" in str_output

    def test_get_logical_worker_id(self, worker_entity: PhysicalWorkerEntity, mock_model: MagicMock):
        """
        Test get_logical_worker_id returns the correct value.

        Args:
            worker_entity: A fixture that returns a `PhysicalWorkerEntity` instance.
            mock_model: A fixture that returns a mocked `PhysicalWorkerModel` instance.
        """
        assert worker_entity.get_logical_worker_id() == mock_model.logical_worker_id

    def test_get_launch_cmd(self, worker_entity: PhysicalWorkerEntity, mock_model: MagicMock):
        """
        Test get_launch_cmd returns the correct value.

        Args:
            worker_entity: A fixture that returns a `PhysicalWorkerEntity` instance.
            mock_model: A fixture that returns a mocked `PhysicalWorkerModel` instance.
        """
        assert worker_entity.get_launch_cmd() == mock_model.launch_cmd

    def test_set_launch_cmd(self, worker_entity: PhysicalWorkerEntity, mock_backend: MagicMock):
        """
        Test set_launch_cmd updates the model and saves it.

        Args:
            worker_entity: A fixture that returns a `PhysicalWorkerEntity` instance.
            mock_backend: A fixture that returns a mocked `ResultsBackend` instance.
        """
        new_cmd = "python new_worker.py"
        worker_entity.set_launch_cmd(new_cmd)
        assert worker_entity.entity_info.launch_cmd == new_cmd
        mock_backend.save.assert_called_once()

    def test_get_args(self, worker_entity: PhysicalWorkerEntity, mock_model: MagicMock):
        """
        Test get_args returns the correct value.

        Args:
            worker_entity: A fixture that returns a `PhysicalWorkerEntity` instance.
            mock_model: A fixture that returns a mocked `PhysicalWorkerModel` instance.
        """
        assert worker_entity.get_args() == mock_model.args

    def test_set_args(self, worker_entity: PhysicalWorkerEntity, mock_backend: MagicMock):
        """
        Test set_args updates the model and saves it.

        Args:
            worker_entity: A fixture that returns a `PhysicalWorkerEntity` instance.
            mock_backend: A fixture that returns a mocked `ResultsBackend` instance.
        """
        new_args = {"arg2": "value2"}
        worker_entity.set_args(new_args)
        assert worker_entity.entity_info.args == new_args
        mock_backend.save.assert_called_once()

    def test_get_pid(self, worker_entity: PhysicalWorkerEntity, mock_model: MagicMock):
        """
        Test get_pid returns the correct value.

        Args:
            worker_entity: A fixture that returns a `PhysicalWorkerEntity` instance.
            mock_model: A fixture that returns a mocked `PhysicalWorkerModel` instance.
        """
        assert worker_entity.get_pid() == int(mock_model.pid)

    def test_get_pid_none(self, worker_entity: PhysicalWorkerEntity, mock_model: MagicMock):
        """
        Test get_pid returns None when pid is not set.

        Args:
            worker_entity: A fixture that returns a `PhysicalWorkerEntity` instance.
            mock_model: A fixture that returns a mocked `PhysicalWorkerModel` instance.
        """
        mock_model.pid = None
        assert worker_entity.get_pid() is None

    def test_set_pid(self, worker_entity: PhysicalWorkerEntity, mock_backend: MagicMock):
        """
        Test set_pid updates the model and saves it.

        Args:
            worker_entity: A fixture that returns a `PhysicalWorkerEntity` instance.
            mock_backend: A fixture that returns a mocked `ResultsBackend` instance.
        """
        new_pid = "54321"
        worker_entity.set_pid(new_pid)
        assert worker_entity.entity_info.pid == new_pid
        mock_backend.save.assert_called_once()

    def test_get_status(self, worker_entity: PhysicalWorkerEntity, mock_model: MagicMock):
        """
        Test get_status returns the correct value.

        Args:
            worker_entity: A fixture that returns a `PhysicalWorkerEntity` instance.
            mock_model: A fixture that returns a mocked `PhysicalWorkerModel` instance.
        """
        assert worker_entity.get_status() == mock_model.status

    def test_set_status(self, worker_entity: PhysicalWorkerEntity, mock_backend: MagicMock):
        """
        Test set_status updates the model and saves it.

        Args:
            worker_entity: A fixture that returns a `PhysicalWorkerEntity` instance.
            mock_backend: A fixture that returns a mocked `ResultsBackend` instance.
        """
        new_status = WorkerStatus.STOPPED
        worker_entity.set_status(new_status)
        assert worker_entity.entity_info.status == new_status
        mock_backend.save.assert_called_once()

    def test_get_heartbeat_timestamp(self, worker_entity: PhysicalWorkerEntity, mock_model: MagicMock):
        """
        Test get_heartbeat_timestamp returns the correct value.

        Args:
            worker_entity: A fixture that returns a `PhysicalWorkerEntity` instance.
            mock_model: A fixture that returns a mocked `PhysicalWorkerModel` instance.
        """
        assert worker_entity.get_heartbeat_timestamp() == mock_model.heartbeat_timestamp

    def test_set_heartbeat_timestamp(self, worker_entity: PhysicalWorkerEntity, mock_backend: MagicMock):
        """
        Test set_heartbeat_timestamp updates the model and saves it.

        Args:
            worker_entity: A fixture that returns a `PhysicalWorkerEntity` instance.
            mock_backend: A fixture that returns a mocked `ResultsBackend` instance.
        """
        new_timestamp = datetime.now()
        worker_entity.set_heartbeat_timestamp(new_timestamp)
        assert worker_entity.entity_info.heartbeat_timestamp == new_timestamp
        mock_backend.save.assert_called_once()

    def test_get_latest_start_time(self, worker_entity: PhysicalWorkerEntity, mock_model: MagicMock):
        """
        Test get_latest_start_time returns the correct value.

        Args:
            worker_entity: A fixture that returns a `PhysicalWorkerEntity` instance.
            mock_model: A fixture that returns a mocked `PhysicalWorkerModel` instance.
        """
        assert worker_entity.get_latest_start_time() == mock_model.latest_start_time

    def test_set_latest_start_time(self, worker_entity: PhysicalWorkerEntity, mock_backend: MagicMock):
        """
        Test set_latest_start_time updates the model and saves it.

        Args:
            worker_entity: A fixture that returns a `PhysicalWorkerEntity` instance.
            mock_backend: A fixture that returns a mocked `ResultsBackend` instance.
        """
        new_start_time = datetime.now()
        worker_entity.set_latest_start_time(new_start_time)
        assert worker_entity.entity_info.latest_start_time == new_start_time
        mock_backend.save.assert_called_once()

    def test_get_host(self, worker_entity: PhysicalWorkerEntity, mock_model: MagicMock):
        """
        Test get_host returns the correct value.

        Args:
            worker_entity: A fixture that returns a `PhysicalWorkerEntity` instance.
            mock_model: A fixture that returns a mocked `PhysicalWorkerModel` instance.
        """
        assert worker_entity.get_host() == mock_model.host

    def test_get_restart_count(self, worker_entity: PhysicalWorkerEntity, mock_model: MagicMock):
        """
        Test get_restart_count returns the correct value.

        Args:
            worker_entity: A fixture that returns a `PhysicalWorkerEntity` instance.
            mock_model: A fixture that returns a mocked `PhysicalWorkerModel` instance.
        """
        assert worker_entity.get_restart_count() == mock_model.restart_count

    def test_increment_restart_count(self, worker_entity: PhysicalWorkerEntity, mock_backend: MagicMock):
        """
        Test increment_restart_count increments the count and saves the model.

        Args:
            worker_entity: A fixture that returns a `PhysicalWorkerEntity` instance.
            mock_backend: A fixture that returns a mocked `ResultsBackend` instance.
        """
        initial_count = worker_entity.get_restart_count()
        worker_entity.increment_restart_count()
        assert worker_entity.entity_info.restart_count == initial_count + 1
        mock_backend.save.assert_called_once()

    @patch.object(PhysicalWorkerEntity, "load")
    def test_load(self, mock_load: MagicMock, mock_backend: MagicMock):
        """
        Test the load class method.

        Args:
            mock_load: A mocked version of the `load` method of `PhysicalWorkerEntity`.
            mock_backend: A fixture that returns a mocked `ResultsBackend` instance.
        """
        entity_id = "worker_123"
        mock_load.return_value = "loaded_entity"
        result = PhysicalWorkerEntity.load(entity_id, mock_backend)
        mock_load.assert_called_once_with(entity_id, mock_backend)
        assert result == "loaded_entity"

    @patch.object(PhysicalWorkerEntity, "delete")
    def test_delete(self, mock_delete: MagicMock, mock_backend: MagicMock):
        """
        Test the delete class method.

        Args:
            mock_delete: A mocked version of the `delete` method of `PhysicalWorkerEntity`.
            mock_backend: A fixture that returns a mocked `ResultsBackend` instance.
        """
        entity_id = "worker_123"
        PhysicalWorkerEntity.delete(entity_id, mock_backend)
        mock_delete.assert_called_once_with(entity_id, mock_backend)
