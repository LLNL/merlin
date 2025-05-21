"""
Tests for the `logical_worker_manager.py` module.
"""

import pytest
from unittest.mock import MagicMock, patch, call

from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.data_models import LogicalWorkerModel
from merlin.db_scripts.entities.logical_worker_entity import LogicalWorkerEntity
from merlin.db_scripts.entity_managers.logical_worker_manager import LogicalWorkerManager
from merlin.db_scripts.merlin_db import MerlinDatabase
from merlin.exceptions import RunNotFoundError


class TestLogicalWorkerManager:
    """Tests for the `LogicalWorkerManager` class."""
    
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
        db.runs = MagicMock()
        return db

    @pytest.fixture
    def worker_manager(self, mock_backend: MagicMock, mock_db: MagicMock) -> LogicalWorkerManager:
        """
        Create a `LogicalWorkerManager` instance for testing.

        Args:
            mock_backend: A mocked `ResultsBackend` instance.
            mock_db: A mocked `MerlinDatabase` instance.

        Returns:
            A `LogicalWorkerManager` instance.
        """
        manager = LogicalWorkerManager(mock_backend)
        manager.set_db_reference(mock_db)
        return manager

    def test_create_worker(self, worker_manager: LogicalWorkerManager, mock_backend: MagicMock):
        """
        Test creating a new logical worker.
        
        Args:
            worker_manager: A `LogicalWorkerManager` instance.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        # Setup
        worker_name = "test_worker"
        queues = ["queue1", "queue2"]
        worker_id = LogicalWorkerModel.generate_id(worker_name, queues)
        
        # Mock the backend get call to simulate worker doesn't exist
        mock_backend.retrieve.return_value = None
        
        # Execute
        worker = worker_manager.create(worker_name, queues)
        
        # Assert
        assert isinstance(worker, LogicalWorkerEntity)
        mock_backend.retrieve.assert_called_once_with(worker_id, "logical_worker")
        mock_backend.save.assert_called_once()
        saved_model = mock_backend.save.call_args[0][0]
        assert saved_model.id == worker_id
        assert saved_model.name == worker_name
        assert saved_model.queues == queues

    def test_create_existing_worker(self, worker_manager: LogicalWorkerManager, mock_backend: MagicMock):
        """
        Test creating a worker that already exists returns the existing worker.
        
        Args:
            worker_manager: A `LogicalWorkerManager` instance.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        # Setup
        worker_name = "existing_worker"
        queues = ["queue1"]
        worker_id = LogicalWorkerModel.generate_id(worker_name, queues)
        
        existing_model = LogicalWorkerModel(name=worker_name, queues=queues)
        mock_backend.retrieve.return_value = existing_model
        
        # Execute
        worker = worker_manager.create(worker_name, queues)
        
        # Assert
        assert isinstance(worker, LogicalWorkerEntity)
        mock_backend.retrieve.assert_called_once_with(worker_id, "logical_worker")
        mock_backend.save.assert_not_called()

    def test_get_worker_by_id(self, worker_manager: LogicalWorkerManager, mock_backend: MagicMock):
        """
        Test retrieving a worker by ID.
        
        Args:
            worker_manager: A `LogicalWorkerManager` instance.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        # Setup
        worker_id = "worker_id_123"
        mock_model = LogicalWorkerModel(id=worker_id, name="test", queues=["q1"])
        mock_backend.retrieve.return_value = mock_model
        
        # Execute
        worker = worker_manager.get(worker_id=worker_id)
        
        # Assert
        assert isinstance(worker, LogicalWorkerEntity)
        mock_backend.retrieve.assert_called_once_with(worker_id, "logical_worker")

    def test_get_worker_by_name_queues(self, worker_manager: LogicalWorkerManager, mock_backend: MagicMock):
        """
        Test retrieving a worker by name and queues.
        
        Args:
            worker_manager: A `LogicalWorkerManager` instance.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        # Setup
        worker_name = "test_worker"
        queues = ["queue1", "queue2"]
        worker_id = LogicalWorkerModel.generate_id(worker_name, queues)
        
        mock_model = LogicalWorkerModel(id=worker_id, name=worker_name, queues=queues)
        mock_backend.retrieve.return_value = mock_model
        
        # Execute
        worker = worker_manager.get(worker_name=worker_name, queues=queues)
        
        # Assert
        assert isinstance(worker, LogicalWorkerEntity)
        mock_backend.retrieve.assert_called_once_with(worker_id, "logical_worker")

    def test_get_worker_invalid_args(self, worker_manager: LogicalWorkerManager):
        """
        Test that `get` raises ValueError with invalid arguments.
        
        Args:
            worker_manager: A `LogicalWorkerManager` instance.
        """
        # Missing both worker_id and (worker_name, queues)
        with pytest.raises(ValueError):
            worker_manager.get()
        
        # Missing queues when worker_name is provided
        with pytest.raises(ValueError):
            worker_manager.get(worker_name="test")
        
        # Missing worker_name when queues is provided
        with pytest.raises(ValueError):
            worker_manager.get(queues=["q1"])
        
        # Providing both worker_id and (worker_name, queues)
        with pytest.raises(ValueError):
            worker_manager.get(worker_id="id", worker_name="test", queues=["q1"])

    def test_get_all_workers(self, worker_manager: LogicalWorkerManager, mock_backend: MagicMock):
        """
        Test retrieving all workers.
        
        Args:
            worker_manager: A `LogicalWorkerManager` instance.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        # Setup
        mock_models = [
            LogicalWorkerModel(name="worker1", queues=["q1"]),
            LogicalWorkerModel(name="worker2", queues=["q2"])
        ]
        mock_backend.retrieve_all.return_value = mock_models
        
        # Execute
        workers = worker_manager.get_all()
        
        # Assert
        assert len(workers) == 2
        assert all(isinstance(w, LogicalWorkerEntity) for w in workers)
        mock_backend.retrieve_all.assert_called_once_with("logical_worker")

    def test_delete_worker(self, worker_manager: LogicalWorkerManager, mock_backend: MagicMock, mock_db: MagicMock):
        """
        Test deleting a worker and cleanup of associated runs.
        
        Args:
            worker_manager: A `LogicalWorkerManager` instance.
            mock_backend: A mocked `ResultsBackend` instance.
            mock_db: A mocked `MerlinDatabase` instance.
        """
        # Setup worker entity
        worker_id = "worker_id_123"
        mock_worker = MagicMock(spec=LogicalWorkerEntity)
        mock_worker.get_id.return_value = worker_id
        mock_worker.get_runs.return_value = ["run1", "run2"]
        
        # Mock the get methods to return our mock worker
        worker_manager.get = MagicMock(return_value=mock_worker)
        worker_manager._get_entity = MagicMock(return_value=mock_worker)
        
        # Setup mock runs
        mock_run1 = MagicMock()
        mock_db.runs.get.side_effect = [mock_run1, RunNotFoundError]

        with patch.object(LogicalWorkerEntity, 'delete') as mock_delete:
            # Execute
            worker_manager.delete(worker_id=worker_id)
            
            # Assert
            worker_manager.get.assert_called_once_with(
                worker_id=worker_id, worker_name=None, queues=None
            )
            worker_manager._get_entity.assert_called_once_with(LogicalWorkerEntity, worker_id)
            mock_db.runs.get.assert_has_calls([call("run1"), call("run2")])
            mock_run1.remove_worker.assert_called_once_with(worker_id)
            mock_delete.assert_called_once_with(worker_id, mock_backend)

    def test_delete_all_workers(self, worker_manager: LogicalWorkerManager):
        """
        Test deleting all workers.
        
        Args:
            worker_manager: A `LogicalWorkerManager` instance.
        """
        # Setup
        mock_workers = [
            MagicMock(spec=LogicalWorkerEntity),
            MagicMock(spec=LogicalWorkerEntity)
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
