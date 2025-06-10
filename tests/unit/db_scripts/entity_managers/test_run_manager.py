##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `run_manager.py` module.
"""

from unittest.mock import MagicMock, call, patch

import pytest

from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.data_models import RunModel
from merlin.db_scripts.entities.run_entity import RunEntity
from merlin.db_scripts.entities.study_entity import StudyEntity
from merlin.db_scripts.entity_managers.run_manager import RunManager
from merlin.db_scripts.merlin_db import MerlinDatabase
from merlin.exceptions import StudyNotFoundError, WorkerNotFoundError


class TestRunManager:
    """Tests for the `RunManager` class."""

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
        db.studies = MagicMock()
        db.logical_workers = MagicMock()
        return db

    @pytest.fixture
    def run_manager(self, mock_backend: MagicMock, mock_db: MagicMock) -> RunManager:
        """
        Create a `RunManager` instance for testing.

        Args:
            mock_backend: A mocked `ResultsBackend` instance.
            mock_db: A mocked `MerlinDatabase` instance.

        Returns:
            A `RunManager` instance.
        """
        manager = RunManager(mock_backend)
        manager.set_db_reference(mock_db)
        return manager

    def test_create_run(self, run_manager: RunManager, mock_backend: MagicMock, mock_db: MagicMock):
        """
        Test creating a new run.

        Args:
            run_manager: A `RunManager` instance.
            mock_backend: A mocked `ResultsBackend` instance.
            mock_db: A mocked `MerlinDatabase` instance.
        """
        # Setup
        study_name = "test_study"
        workspace = "test_workspace"
        queues = ["queue1", "queue2"]

        # Mock study creation
        mock_study = MagicMock(spec=StudyEntity)
        mock_study.get_id.return_value = "study_id_123"
        mock_db.studies.create.return_value = mock_study

        # Execute
        run = run_manager.create(study_name, workspace, queues)

        # Assert
        assert isinstance(run, RunEntity)
        mock_db.studies.create.assert_called_once_with(study_name)
        mock_backend.save.assert_called_once()
        saved_model = mock_backend.save.call_args[0][0]
        assert saved_model.study_id == "study_id_123"
        assert saved_model.workspace == workspace
        assert saved_model.queues == queues
        mock_study.add_run.assert_called_once()

    def test_create_run_with_additional_args(self, run_manager: RunManager, mock_backend: MagicMock, mock_db: MagicMock):
        """
        Test creating a run with additional valid and invalid arguments.

        Args:
            run_manager: A `RunManager` instance.
            mock_backend: A mocked `ResultsBackend` instance.
            mock_db: A mocked `MerlinDatabase` instance.
        """
        # Setup
        study_name = "test_study"
        workspace = "test_workspace"
        queues = ["queue1"]

        # Valid RunModel field and additional data
        valid_field = "run_complete"
        valid_value = False
        invalid_field = "non_model_field"
        invalid_value = "extra_data"

        # Mock study creation
        mock_study = MagicMock(spec=StudyEntity)
        mock_study.get_id.return_value = "study_id_123"
        mock_db.studies.create.return_value = mock_study

        # Execute
        run_manager.create(study_name, workspace, queues, **{valid_field: valid_value, invalid_field: invalid_value})

        # Assert
        saved_model = mock_backend.save.call_args[0][0]
        assert saved_model.run_complete == valid_value
        assert saved_model.additional_data == {invalid_field: invalid_value}

    def test_get_run(self, run_manager: RunManager, mock_backend: MagicMock):
        """
        Test retrieving a run by ID or workspace.

        Args:
            run_manager: A `RunManager` instance.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        # Setup
        run_id = "run_id_123"
        mock_model = RunModel(id=run_id, study_id="study_1", workspace="test_workspace", queues=["q1"])
        mock_backend.retrieve.return_value = mock_model

        # Execute
        run = run_manager.get(run_id)

        # Assert
        assert isinstance(run, RunEntity)
        mock_backend.retrieve.assert_called_once_with(run_id, "run")

    def test_get_all_runs(self, run_manager: RunManager, mock_backend: MagicMock):
        """
        Test retrieving all runs.

        Args:
            run_manager: A `RunManager` instance.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        # Setup
        mock_models = [
            RunModel(study_id="study_1", workspace="workspace1", queues=["q1"]),
            RunModel(study_id="study_2", workspace="workspace2", queues=["q2"]),
        ]
        mock_backend.retrieve_all.return_value = mock_models

        # Execute
        runs = run_manager.get_all()

        # Assert
        assert len(runs) == 2
        assert all(isinstance(r, RunEntity) for r in runs)
        mock_backend.retrieve_all.assert_called_once_with("run")

    def test_delete_run(self, run_manager: RunManager, mock_backend: MagicMock, mock_db: MagicMock):
        """
        Test deleting a run and cleanup of associated entities.

        Args:
            run_manager: A `RunManager` instance.
            mock_backend: A mocked `ResultsBackend` instance.
            mock_db: A mocked `MerlinDatabase` instance.
        """
        # Setup
        run_id = "run_id_123"

        mock_run = MagicMock(spec=RunEntity)
        mock_run.get_id.return_value = run_id
        mock_run.get_study_id.return_value = "study_id_123"
        mock_run.get_workers.return_value = ["worker1", "worker2"]

        # Mock the get method to return our mock run
        run_manager._get_entity = MagicMock(return_value=mock_run)

        # Setup mock study and workers
        mock_study = MagicMock()
        mock_worker1 = MagicMock()

        # Mock study get success, worker1 get success, worker2 not found
        mock_db.studies.get.return_value = mock_study
        mock_db.logical_workers.get.side_effect = [mock_worker1, WorkerNotFoundError]

        with patch.object(RunEntity, "delete") as mock_delete:
            # Execute
            run_manager.delete(run_id)

            # Assert
            run_manager._get_entity.assert_called_once_with(RunEntity, run_id)

            # Study cleanup
            mock_db.studies.get.assert_called_once_with("study_id_123")
            mock_study.remove_run.assert_called_once_with(run_id)

            # Worker cleanup
            mock_db.logical_workers.get.assert_has_calls([call(worker_id="worker1"), call(worker_id="worker2")])
            mock_worker1.remove_run.assert_called_once_with(run_id)

            # Run deletion
            mock_delete.assert_called_once_with(run_id, mock_backend)

    def test_delete_run_with_missing_study(self, run_manager: RunManager, mock_backend: MagicMock, mock_db: MagicMock):
        """
        Test deleting a run when the associated study isn't found.

        Args:
            run_manager: A `RunManager` instance.
            mock_backend: A mocked `ResultsBackend` instance.
            mock_db: A mocked `MerlinDatabase` instance.
        """
        # Setup
        run_id = "run_id_123"

        mock_run = MagicMock(spec=RunEntity)
        mock_run.get_id.return_value = run_id
        mock_run.get_study_id.return_value = "missing_study"
        mock_run.get_workers.return_value = []

        # Mock the get method to return our mock run
        run_manager._get_entity = MagicMock(return_value=mock_run)

        # Setup study not found
        mock_db.studies.get.side_effect = StudyNotFoundError

        with patch.object(RunEntity, "delete") as mock_delete:
            # Execute
            run_manager.delete(run_id)

            # Assert
            mock_db.studies.get.assert_called_once_with("missing_study")
            mock_delete.assert_called_once_with(run_id, mock_backend)

    def test_delete_all_runs(self, run_manager: RunManager):
        """
        Test deleting all runs.

        Args:
            run_manager: A `RunManager` instance.
        """
        # Setup
        mock_runs = [MagicMock(spec=RunEntity), MagicMock(spec=RunEntity)]
        mock_runs[0].get_id.return_value = "run1"
        mock_runs[1].get_id.return_value = "run2"

        run_manager.get_all = MagicMock(return_value=mock_runs)
        run_manager.delete = MagicMock()

        # Execute
        run_manager.delete_all()

        # Assert
        run_manager.get_all.assert_called_once()
        run_manager.delete.assert_has_calls([call("run1"), call("run2")])
