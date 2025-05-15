"""
Tests for the `study_entity.py` module.
"""

import pytest
from unittest.mock import MagicMock, patch

from merlin.db_scripts.data_models import StudyModel
from merlin.db_scripts.entities.study_entity import StudyEntity
from merlin.backends.results_backend import ResultsBackend


class TestStudyEntity:
    """Tests for the `StudyEntity` class."""

    @pytest.fixture
    def mock_model(self) -> MagicMock:
        """
        Create a mock `StudyModel` for testing.

        Returns:
            A mocked `StudyModel` instance.
        """
        model = MagicMock(spec=StudyModel)
        model.id = "study_123"
        model.name = "test_study"
        model.runs = ["run_1", "run_2"]
        model.additional_data = {"key": "value"}
        return model

    @pytest.fixture
    def mock_backend(self, mock_model: MagicMock) -> MagicMock:
        """
        Create a mock `ResultsBackend` for testing.

        Args:
            mock_model: A mocked `StudyModel` instance.

        Returns:
            A mocked `ResultsBackend` instance.
        """
        backend = MagicMock(spec=ResultsBackend)
        backend.get_name.return_value = "mock_backend"
        backend.retrieve.return_value = mock_model
        return backend

    @pytest.fixture
    def study_entity(self, mock_model: MagicMock, mock_backend: MagicMock) -> StudyEntity:
        """
        Create a `StudyEntity` instance for testing.

        Args:
            mock_model: A mocked `StudyModel` instance.
            mock_backend: A mocked `ResultsBackend` instance.

        Returns:
            A `StudyEntity` instance.
        """
        return StudyEntity(mock_model, mock_backend)

    def test_get_entity_type(self):
        """
        Test that `_get_entity_type` returns the correct value.
        """
        assert StudyEntity._get_entity_type() == "study"

    def test_repr(self, study_entity: StudyEntity):
        """
        Test the `__repr__` method.

        Args:
            study_entity: A fixture that returns a `StudyEntity` instance.
        """
        repr_str = repr(study_entity)
        assert "StudyEntity" in repr_str
        assert f"id={study_entity.get_id()}" in repr_str
        assert f"name={study_entity.get_name()}" in repr_str
        assert "runs=" in repr_str

    def test_str(self, study_entity: StudyEntity):
        """
        Test the `__str__` method.

        Args:
            study_entity: A fixture that returns a `StudyEntity` instance.
        """
        str_output = str(study_entity)
        assert f"Study with ID {study_entity.get_id()}" in str_output
        assert f"Name: {study_entity.get_name()}" in str_output
        assert "Runs:" in str_output

    def test_get_runs(self, study_entity: StudyEntity, mock_model: MagicMock):
        """
        Test `get_runs` returns the correct value.

        Args:
            study_entity: A fixture that returns a `StudyEntity` instance.
            mock_model: A fixture that returns a mocked `StudyModel` instance.
        """
        assert study_entity.get_runs() == mock_model.runs

    def test_add_run(self, study_entity: StudyEntity, mock_backend: MagicMock):
        """
        Test `add_run` adds the run and saves the model.

        Args:
            study_entity: A fixture that returns a `StudyEntity` instance.
            mock_backend: A fixture that returns a mocked `ResultsBackend` instance.
        """
        new_run_id = "run_3"
        study_entity.add_run(new_run_id)
        assert new_run_id in study_entity.entity_info.runs
        mock_backend.save.assert_called_once()

    def test_remove_run(self, study_entity: StudyEntity, mock_backend: MagicMock):
        """
        Test `remove_run` removes the run and saves the model.

        Args:
            study_entity: A fixture that returns a `StudyEntity` instance.
            mock_backend: A fixture that returns a mocked `ResultsBackend` instance.
        """
        run_id = "run_1"
        study_entity.remove_run(run_id)
        assert run_id not in study_entity.entity_info.runs
        mock_backend.save.assert_called_once()

    def test_get_name(self, study_entity: StudyEntity, mock_model: MagicMock):
        """
        Test `get_name` returns the correct value.

        Args:
            study_entity: A fixture that returns a `StudyEntity` instance.
            mock_model: A fixture that returns a mocked `StudyModel` instance.
        """
        assert study_entity.get_name() == mock_model.name

    @patch.object(StudyEntity, 'load')
    def test_load(self, mock_load: MagicMock, mock_backend: MagicMock):
        """
        Test the `load` class method.

        Args:
            mock_load: A mocked version of the `load` method of `StudyEntity`.
            mock_backend: A fixture that returns a mocked `ResultsBackend` instance.
        """
        entity_id = "study_123"
        mock_load.return_value = "loaded_entity"
        result = StudyEntity.load(entity_id, mock_backend)
        mock_load.assert_called_once_with(entity_id, mock_backend)
        assert result == "loaded_entity"

    @patch.object(StudyEntity, 'delete')
    def test_delete(self, mock_delete: MagicMock, mock_backend: MagicMock):
        """
        Test the `delete` class method.

        Args:
            mock_delete: A mocked version of the `delete` method of `StudyEntity`.
            mock_backend: A fixture that returns a mocked `ResultsBackend` instance.
        """
        entity_id = "study_123"
        StudyEntity.delete(entity_id, mock_backend)
        mock_delete.assert_called_once_with(entity_id, mock_backend)
