"""
Tests for the `physical_worker_manager.py` module.
"""

import pytest
from unittest.mock import MagicMock, patch, call

from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.data_models import PhysicalWorkerModel
from merlin.db_scripts.entities.physical_worker_entity import PhysicalWorkerEntity
from merlin.db_scripts.entity_managers.physical_worker_manager import PhysicalWorkerManager
from merlin.db_scripts.merlin_db import MerlinDatabase
from merlin.exceptions import WorkerNotFoundError


class TestPhysicalWorkerManager:
    """Tests for the `PhysicalWorkerManager` class."""

    @pytest.fixture
    def mock_backend(self) -> MagicMock:
        """
        Create a mock `ResultsBackend` for testing.

        Returns:
            A mocked `ResultsBackend` instance.
        """
        return MagicMock(spec=ResultsBackend)

    @pytest.fixture
    def mock_db(self) -> MagicMock:
        """
        Create a mock `MerlinDatabase` for testing.

        Returns:
            A mocked `MerlinDatabase` instance.
        """
        db = MagicMock(spec=MerlinDatabase)
        db.logical_workers = MagicMock()
        return db

    @pytest.fixture
    def worker_manager(self, mock_backend: MagicMock, mock_db: MagicMock) -> PhysicalWorkerManager:
        """
        Create a `PhysicalWorkerManager` instance for testing.

        Args:
            mock_backend: A mocked `ResultsBackend` instance.
            mock_db: A mocked `MerlinDatabase` instance.

        Returns:
            A `PhysicalWorkerManager` instance.
        """
        manager = PhysicalWorkerManager(mock_backend)
        manager.set_db_reference(mock_db)
        return manager

    def test_create_worker(self, worker_manager: PhysicalWorkerManager, mock_backend: MagicMock):
        """
        Test creating a new physical worker.
        
        Args:
            worker_manager: A `PhysicalWorkerManager` instance.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        # Setup
        worker_name = "test_worker"
        
        # Mock the backend get call to simulate worker doesn't exist
        mock_backend.retrieve.return_value = None
        
        # Execute
        worker = worker_manager.create(worker_name)
        
        # Assert
        assert isinstance(worker, PhysicalWorkerEntity)
        mock_backend.retrieve.assert_called_once_with(worker_name, "physical_worker")
        mock_backend.save.assert_called_once()
        saved_model = mock_backend.save.call_args[0][0]
        assert saved_model.name == worker_name

    def test_create_worker_with_additional_attrs(self, worker_manager: PhysicalWorkerManager, mock_backend: MagicMock):
        """
        Test creating a worker with additional attributes.
        
        Args:
            worker_manager: A `PhysicalWorkerManager` instance.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        # Setup
        worker_name = "test_worker"
        attrs = {"host": "localhost", "pid": 1234}
        
        # Mock the backend get call to simulate worker doesn't exist
        mock_backend.retrieve.return_value = None
        
        # Execute
        worker_manager.create(worker_name, **attrs)
        
        # Assert
        saved_model = mock_backend.save.call_args[0][0]
        assert saved_model.name == worker_name
        assert saved_model.host == "localhost"
        assert saved_model.pid == 1234

    def test_create_existing_worker(self, worker_manager: PhysicalWorkerManager, mock_backend: MagicMock):
        """
        Test creating a worker that already exists returns the existing worker.
        
        Args:
            worker_manager: A `PhysicalWorkerManager` instance.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        # Setup
        worker_name = "existing_worker"
        
        existing_model = PhysicalWorkerModel(name=worker_name)
        mock_backend.retrieve.return_value = existing_model
        
        # Execute
        worker = worker_manager.create(worker_name)
        
        # Assert
        assert isinstance(worker, PhysicalWorkerEntity)
        mock_backend.retrieve.assert_called_once_with(worker_name, "physical_worker")
        mock_backend.save.assert_not_called()

    def test_get_worker(self, worker_manager: PhysicalWorkerManager, mock_backend: MagicMock):
        """
        Test retrieving a worker by ID or name.
        
        Args:
            worker_manager: A `PhysicalWorkerManager` instance.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        # Setup
        worker_id = "worker_name"  # In this case, ID is the same as name
        mock_model = PhysicalWorkerModel(name=worker_id)
        mock_backend.retrieve.return_value = mock_model
        
        # Execute
        worker = worker_manager.get(worker_id)
        
        # Assert
        assert isinstance(worker, PhysicalWorkerEntity)
        mock_backend.retrieve.assert_called_once_with(worker_id, "physical_worker")

    def test_get_all_workers(self, worker_manager: PhysicalWorkerManager, mock_backend: MagicMock):
        """
        Test retrieving all workers.
        
        Args:
            worker_manager: A `PhysicalWorkerManager` instance.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        # Setup
        mock_models = [
            PhysicalWorkerModel(name="worker1"),
            PhysicalWorkerModel(name="worker2")
        ]
        mock_backend.retrieve_all.return_value = mock_models
        
        # Execute
        workers = worker_manager.get_all()
        
        # Assert
        assert len(workers) == 2
        assert all(isinstance(w, PhysicalWorkerEntity) for w in workers)
        mock_backend.retrieve_all.assert_called_once_with("physical_worker")

    def test_delete_worker(self, worker_manager: PhysicalWorkerManager, mock_backend: MagicMock, mock_db: MagicMock):
        """
        Test deleting a worker and cleanup of associated logical worker.
        
        Args:
            worker_manager: A `PhysicalWorkerManager` instance.
            mock_backend: A mocked `ResultsBackend` instance.
            mock_db: A mocked `MerlinDatabase` instance.
        """
        # Setup
        worker_id = "worker_name"
        
        mock_worker = MagicMock(spec=PhysicalWorkerEntity)
        mock_worker.get_id.return_value = worker_id
        mock_worker.get_logical_worker_id.return_value = "logical_worker_id"
        
        # Mock the get method to return our mock worker
        worker_manager._get_entity = MagicMock(return_value=mock_worker)
        
        # Setup mock logical worker
        mock_logical_worker = MagicMock()
        mock_db.logical_workers.get.return_value = mock_logical_worker

        with patch.object(PhysicalWorkerEntity, 'delete') as mock_delete:
            # Execute
            worker_manager.delete(worker_id)
            
            # Assert
            worker_manager._get_entity.assert_called_once_with(PhysicalWorkerEntity, worker_id)
            mock_db.logical_workers.get.assert_called_once_with(worker_id="logical_worker_id")
            mock_logical_worker.remove_physical_worker.assert_called_once_with(worker_id)
            mock_delete.assert_called_once_with(worker_id, mock_backend)

    def test_delete_worker_with_missing_logical_worker(
        self, worker_manager: PhysicalWorkerManager, mock_backend: MagicMock, mock_db: MagicMock
    ):
        """
        Test deleting a worker when the associated logical worker isn't found.
        
        Args:
            worker_manager: A `PhysicalWorkerManager` instance.
            mock_backend: A mocked `ResultsBackend` instance.
            mock_db: A mocked `MerlinDatabase` instance.
        """
        # Setup
        worker_id = "worker_name"
        
        mock_worker = MagicMock(spec=PhysicalWorkerEntity)
        mock_worker.get_id.return_value = worker_id
        mock_worker.get_logical_worker_id.return_value = "missing_logical_worker"
        
        # Mock the get method to return our mock worker
        worker_manager._get_entity = MagicMock(return_value=mock_worker)
        
        # Setup logical worker not found
        mock_db.logical_workers.get.side_effect = WorkerNotFoundError
        
        with patch.object(PhysicalWorkerEntity, 'delete') as mock_delete:
            # Execute
            worker_manager.delete(worker_id)
            
            # Assert
            mock_db.logical_workers.get.assert_called_once_with(worker_id="missing_logical_worker")
            mock_delete.assert_called_once_with(worker_id, mock_backend)

    def test_delete_all_workers(self, worker_manager: PhysicalWorkerManager):
        """
        Test deleting all workers.
        
        Args:
            worker_manager: A `PhysicalWorkerManager` instance.
        """
        # Setup
        mock_workers = [
            MagicMock(spec=PhysicalWorkerEntity),
            MagicMock(spec=PhysicalWorkerEntity)
        ]
        mock_workers[0].get_id.return_value = "worker1"
        mock_workers[1].get_id.return_value = "worker2"
        
        worker_manager.get_all = MagicMock(return_value=mock_workers)
        worker_manager.delete = MagicMock()
        
        # Execute
        worker_manager.delete_all()
        
        # Assert
        worker_manager.get_all.assert_called_once()
        worker_manager.delete.assert_has_calls([
            call("worker1"), 
            call("worker2")
        ])
