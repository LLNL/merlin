"""
Tests for the `study_manager.py` module.
"""

import pytest
from unittest.mock import MagicMock, patch, call

from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.data_models import StudyModel
from merlin.db_scripts.entities.study_entity import StudyEntity
from merlin.db_scripts.entity_managers.study_manager import StudyManager
from merlin.db_scripts.merlin_db import MerlinDatabase


class TestStudyManager:
    """Tests for the `StudyManager` class."""

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
    def study_manager(self, mock_backend: MagicMock, mock_db: MagicMock) -> StudyManager:
        """
        Create a `StudyManager` instance for testing.

        Args:
            mock_backend: A mocked `ResultsBackend` instance.
            mock_db: A mocked `MerlinDatabase` instance.

        Returns:
            A `StudyManager` instance.
        """
        manager = StudyManager(mock_backend)
        manager.set_db_reference(mock_db)
        return manager

    def test_create_study(self, study_manager: StudyManager, mock_backend: MagicMock):
        """
        Test creating a new study.
        
        Args:
            study_manager: A `StudyManager` instance.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        # Setup
        study_name = "test_study"
        
        # Mock the backend get call to simulate study doesn't exist
        mock_backend.retrieve.return_value = None
        
        # Execute
        study = study_manager.create(study_name)
        
        # Assert
        assert isinstance(study, StudyEntity)
        mock_backend.retrieve.assert_called_once_with(study_name, "study")
        mock_backend.save.assert_called_once()
        saved_model = mock_backend.save.call_args[0][0]
        assert saved_model.name == study_name

    def test_create_existing_study(self, study_manager: StudyManager, mock_backend: MagicMock):
        """
        Test creating a study that already exists returns the existing study.
        
        Args:
            study_manager: A `StudyManager` instance.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        # Setup
        study_name = "existing_study"
        
        existing_model = StudyModel(name=study_name)
        mock_backend.retrieve.return_value = existing_model
        
        # Execute
        study = study_manager.create(study_name)
        
        # Assert
        assert isinstance(study, StudyEntity)
        mock_backend.retrieve.assert_called_once_with(study_name, "study")
        mock_backend.save.assert_not_called()

    def test_get_study(self, study_manager: StudyManager, mock_backend: MagicMock):
        """
        Test retrieving a study by ID or name.
        
        Args:
            study_manager: A `StudyManager` instance.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        # Setup
        study_id = "study_name"  # In this case, ID is the same as name
        mock_model = StudyModel(name=study_id)
        mock_backend.retrieve.return_value = mock_model
        
        # Execute
        study = study_manager.get(study_id)
        
        # Assert
        assert isinstance(study, StudyEntity)
        mock_backend.retrieve.assert_called_once_with(study_id, "study")

    def test_get_all_studies(self, study_manager: StudyManager, mock_backend: MagicMock):
        """
        Test retrieving all studies.
        
        Args:
            study_manager: A `StudyManager` instance.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        # Setup
        mock_models = [
            StudyModel(name="study1"),
            StudyModel(name="study2")
        ]
        mock_backend.retrieve_all.return_value = mock_models
        
        # Execute
        studies = study_manager.get_all()
        
        # Assert
        assert len(studies) == 2
        assert all(isinstance(s, StudyEntity) for s in studies)
        mock_backend.retrieve_all.assert_called_once_with("study")

    def test_delete_study_with_runs(self, study_manager: StudyManager, mock_backend: MagicMock, mock_db: MagicMock):
        """
        Test deleting a study with associated runs.
        
        Args:
            study_manager: A `StudyManager` instance.
            mock_backend: A mocked `ResultsBackend` instance.
            mock_db: A mocked `MerlinDatabase` instance.
        """
        # Setup
        study_id = "study_name"
        
        mock_study = MagicMock(spec=StudyEntity)
        mock_study.get_id.return_value = study_id
        mock_study.get_runs.return_value = ["run1", "run2"]
        
        # Mock the get method to return our mock study
        study_manager._get_entity = MagicMock(return_value=mock_study)
        
        with patch.object(StudyEntity, 'delete') as mock_delete:
            # Execute
            study_manager.delete(study_id, remove_associated_runs=True)
            
            # Assert
            study_manager._get_entity.assert_called_once_with(StudyEntity, study_id)
            mock_db.runs.delete.assert_has_calls([
                call("run1"),
                call("run2")
            ])
            mock_delete.assert_called_once_with(study_id, mock_backend)

    def test_delete_study_without_runs(self, study_manager: StudyManager, mock_backend: MagicMock, mock_db: MagicMock):
        """
        Test deleting a study without removing associated runs.
        
        Args:
            study_manager: A `StudyManager` instance.
            mock_backend: A mocked `ResultsBackend` instance.
            mock_db: A mocked `MerlinDatabase` instance.
        """
        # Setup
        study_id = "study_name"
        
        mock_study = MagicMock(spec=StudyEntity)
        mock_study.get_id.return_value = study_id
        mock_study.get_runs.return_value = ["run1", "run2"]
        
        # Mock the get method to return our mock study
        study_manager._get_entity = MagicMock(return_value=mock_study)
        
        with patch.object(StudyEntity, 'delete') as mock_delete:
            # Execute
            study_manager.delete(study_id, remove_associated_runs=False)
            
            # Assert
            mock_db.runs.delete.assert_not_called()
            mock_delete.assert_called_once_with(study_id, mock_backend)

    def test_delete_all_studies(self, study_manager: StudyManager):
        """
        Test deleting all studies.
        
        Args:
            study_manager: A `StudyManager` instance.
        """
        # Setup
        mock_studies = [
            MagicMock(spec=StudyEntity),
            MagicMock(spec=StudyEntity)
        ]
        mock_studies[0].get_id.return_value = "study1"
        mock_studies[1].get_id.return_value = "study2"
        
        study_manager.get_all = MagicMock(return_value=mock_studies)
        study_manager.delete = MagicMock()
        
        # Execute
        study_manager.delete_all(remove_associated_runs=True)
        
        # Assert
        study_manager.get_all.assert_called_once()
        study_manager.delete.assert_has_calls([
            call("study1", remove_associated_runs=True), 
            call("study2", remove_associated_runs=True)
        ])
