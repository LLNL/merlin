##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `run_entity.py` module.
"""

from unittest.mock import MagicMock, patch

import pytest

from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.data_models import RunModel
from merlin.db_scripts.entities.run_entity import RunEntity
from merlin.db_scripts.entities.study_entity import StudyEntity


class TestRunEntity:
    """Tests for the `RunEntity` class."""

    @pytest.fixture
    def mock_model(self) -> MagicMock:
        """
        Create a mock `RunModel` for testing.

        Returns:
            A mocked `RunModel` instance.
        """
        model = MagicMock(spec=RunModel)
        model.id = "run_123"
        model.study_id = "study_123"
        model.workspace = "/test/workspace"
        model.queues = ["queue_1", "queue_2"]
        model.workers = ["worker_1", "worker_2"]
        model.parent = "parent_run"
        model.child = "child_run"
        model.run_complete = False
        model.additional_data = {"key": "value"}
        return model

    @pytest.fixture
    def mock_backend(self, mock_model: MagicMock) -> MagicMock:
        """
        Create a mock `ResultsBackend` for testing.

        Args:
            mock_model: A mocked `RunModel` instance.

        Returns:
            A mocked `ResultsBackend` instance.
        """
        backend = MagicMock(spec=ResultsBackend)
        backend.get_name.return_value = "mock_backend"
        backend.retrieve.return_value = mock_model
        return backend

    @pytest.fixture
    def run_entity(self, mock_model: MagicMock, mock_backend: MagicMock) -> RunEntity:
        """
        Create a `RunEntity` instance for testing.

        Args:
            mock_model: A mocked `RunModel` instance.
            mock_backend: A mocked `ResultsBackend` instance.

        Returns:
            A `RunEntity` instance.
        """
        with patch("os.path.join", return_value="/test/workspace/merlin_info/run_metadata.json"):
            return RunEntity(mock_model, mock_backend)

    @pytest.fixture
    def mock_study(self) -> MagicMock:
        """
        Mock `StudyEntity` for testing.

        Returns:
            A mocked `StudyEntity` instance.
        """
        study = MagicMock(spec=StudyEntity)
        study.get_id.return_value = "study_123"
        study.get_name.return_value = "test_study"
        return study

    def test_get_entity_type(self):
        """
        Test that `_get_entity_type` returns the correct value.
        """
        assert RunEntity._get_entity_type() == "run"

    def test_init(self, mock_model: MagicMock, mock_backend: MagicMock):
        """
        Test that initialization sets the `_metadata_file` correctly.

        Args:
            mock_model: A mocked `RunModel` instance.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        with patch("os.path.join", return_value="/test/workspace/merlin_info/run_metadata.json"):
            run_entity = RunEntity(mock_model, mock_backend)
            assert run_entity._metadata_file == "/test/workspace/merlin_info/run_metadata.json"

    def test_repr(self, run_entity: RunEntity):
        """
        Test the `__repr__` method.

        Args:
            run_entity: A fixture that returns a `RunEntity` instance.
        """
        repr_str = repr(run_entity)
        assert "RunEntity" in repr_str
        assert f"id={run_entity.get_id()}" in repr_str
        assert f"study_id={run_entity.get_study_id()}" in repr_str

    def test_str(self, run_entity: RunEntity, mock_study: MagicMock):
        """
        Test the `__str__` method.

        Args:
            run_entity: A fixture that returns a `RunEntity` instance.
            mock_study: A fixture that returns a mocked `StudyEntity` instance.
        """
        with patch.object(StudyEntity, "load", return_value=mock_study):
            str_output = str(run_entity)
            assert f"Run with ID {run_entity.get_id()}" in str_output
            assert f"Workspace: {run_entity.get_workspace()}" in str_output
            assert "Study:" in str_output

    def test_run_complete_property(self, run_entity: RunEntity, mock_model: MagicMock):
        """
        Test the `run_complete` property getter and setter.

        Args:
            run_entity: A fixture that returns a `RunEntity` instance.
            mock_model: A fixture that returns a mocked `RunModel` instance.
        """
        assert run_entity.run_complete == mock_model.run_complete
        run_entity.run_complete = True
        assert run_entity.entity_info.run_complete is True

    def test_get_metadata_file(self, run_entity: RunEntity):
        """
        Test `get_metadata_file` returns the correct value.

        Args:
            run_entity: A fixture that returns a `RunEntity` instance.
        """
        assert run_entity.get_metadata_file() == "/test/workspace/merlin_info/run_metadata.json"

    def test_get_metadata_filepath(self):
        """
        Test `get_metadata_filepath` class method returns the correct path.
        """
        with patch("os.path.join", return_value="/test/workspace/merlin_info/run_metadata.json"):
            result = RunEntity.get_metadata_filepath("/test/workspace")
            assert result == "/test/workspace/merlin_info/run_metadata.json"

    def test_get_study_id(self, run_entity: RunEntity, mock_model: MagicMock):
        """
        Test `get_study_id` returns the correct value.

        Args:
            run_entity: A fixture that returns a `RunEntity` instance.
            mock_model: A fixture that returns a mocked `RunModel` instance.
        """
        assert run_entity.get_study_id() == mock_model.study_id

    def test_get_workspace(self, run_entity: RunEntity, mock_model: MagicMock):
        """
        Test `get_workspace` returns the correct value.

        Args:
            run_entity: A fixture that returns a `RunEntity` instance.
            mock_model: A fixture that returns a mocked `RunModel` instance.
        """
        assert run_entity.get_workspace() == mock_model.workspace

    def test_get_workers(self, run_entity: RunEntity, mock_model: MagicMock):
        """
        Test `get_workers` returns the correct value.

        Args:
            run_entity: A fixture that returns a `RunEntity` instance.
            mock_model: A fixture that returns a mocked `RunModel` instance.
        """
        assert run_entity.get_workers() == mock_model.workers

    def test_add_worker(self, run_entity: RunEntity, mock_backend: MagicMock):
        """
        Test `add_worker` adds the worker and saves the model.

        Args:
            run_entity: A fixture that returns a `RunEntity` instance.
            mock_backend: A fixture that returns a mocked `ResultsBackend` instance.
        """
        new_worker_id = "worker_3"
        run_entity.add_worker(new_worker_id)
        assert new_worker_id in run_entity.entity_info.workers
        mock_backend.save.assert_called_once()

    def test_remove_worker(self, run_entity: RunEntity, mock_backend: MagicMock):
        """
        Test `remove_worker` removes the worker and saves the model.

        Args:
            run_entity: A fixture that returns a `RunEntity` instance.
            mock_backend: A fixture that returns a mocked `ResultsBackend` instance.
        """
        worker_id = "worker_1"
        run_entity.remove_worker(worker_id)
        assert worker_id not in run_entity.entity_info.workers
        mock_backend.save.assert_called_once()

    def test_get_parent(self, run_entity: RunEntity, mock_model: MagicMock):
        """
        Test `get_parent` returns the correct value.

        Args:
            run_entity: A fixture that returns a `RunEntity` instance.
            mock_model: A fixture that returns a mocked `RunModel` instance.
        """
        assert run_entity.get_parent() == mock_model.parent

    def test_get_child(self, run_entity: RunEntity, mock_model: MagicMock):
        """
        Test `get_child` returns the correct value.

        Args:
            run_entity: A fixture that returns a `RunEntity` instance.
            mock_model: A fixture that returns a mocked `RunModel` instance.
        """
        assert run_entity.get_child() == mock_model.child

    def test_get_queues(self, run_entity: RunEntity, mock_model: MagicMock):
        """
        Test `get_queues` returns the correct value.

        Args:
            run_entity: A fixture that returns a `RunEntity` instance.
            mock_model: A fixture that returns a mocked `RunModel` instance.
        """
        assert run_entity.get_queues() == mock_model.queues

    def test_post_save_hook(self, run_entity: RunEntity):
        """
        Test `_post_save_hook` calls dump_metadata.

        Args:
            run_entity: A fixture that returns a `RunEntity` instance.
        """
        with patch.object(run_entity, "dump_metadata") as mock_dump:
            run_entity._post_save_hook()
            mock_dump.assert_called_once()

    def test_dump_metadata(self, run_entity: RunEntity):
        """
        Test `dump_metadata` calls dump_to_json_file on the model.

        Args:
            run_entity: A fixture that returns a `RunEntity` instance.
        """
        run_entity.dump_metadata()
        run_entity.entity_info.dump_to_json_file.assert_called_once_with(run_entity.get_metadata_file())

    @patch("os.path.isdir", return_value=True)
    @patch("os.path.exists", return_value=True)
    @patch("merlin.db_scripts.data_models.RunModel.load_from_json_file")
    def test_load_from_workspace(
        self,
        mock_load_json: MagicMock,
        mock_exists: MagicMock,
        mock_isdir: MagicMock,
        mock_model: MagicMock,
        mock_backend: MagicMock,
    ):
        """
        Test `load` method when loading from workspace.

        Args:
            mock_load_json: A mocked version of the `load_from_json_file` method of `RunModel`.
            mock_exists: A mocked version of the `os.path.exists` function.
            mock_isdir: A mocked version of the `os.path.isdir` function.
            mock_model: A fixture that returns a mocked `RunModel` instance.
            mock_backend: A fixture that returns a mocked `ResultsBackend` instance.
        """
        workspace = "/test/workspace"
        mock_load_json.return_value = mock_model

        with patch(
            "merlin.db_scripts.entities.run_entity.RunEntity.get_metadata_filepath",
            return_value="/test/workspace/merlin_info/run_metadata.json",
        ):
            run = RunEntity.load(workspace, mock_backend)
            mock_load_json.assert_called_once_with("/test/workspace/merlin_info/run_metadata.json")
            assert run.entity_info == mock_model

    @patch("os.path.isdir", return_value=False)
    def test_load_from_id(self, mock_isdir: MagicMock, mock_model: MagicMock, mock_backend: MagicMock):
        """
        Test `load` method when loading from ID.

        Args:
            mock_isdir: A mocked version of the `os.path.isdir` function.
            mock_model: A fixture that returns a mocked `RunModel` instance.
            mock_backend: A fixture that returns a mocked `ResultsBackend` instance.
        """
        run_id = "run_123"
        mock_backend.retrieve.return_value = mock_model

        run = RunEntity.load(run_id, mock_backend)
        mock_backend.retrieve.assert_called_once_with(run_id, "run")
        assert run.entity_info == mock_model

    @patch("merlin.db_scripts.entities.run_entity.RunEntity.load")
    def test_delete(self, mock_load: MagicMock, mock_backend: MagicMock):
        """
        Test `delete` method.

        Args:
            mock_load: A mocked version of the `load` method of `StudyEntity`.
            mock_backend: A fixture that returns a mocked `ResultsBackend` instance.
        """
        run_id = "run_123"
        mock_run = MagicMock()
        mock_run.get_id.return_value = run_id
        mock_load.return_value = mock_run

        RunEntity.delete(run_id, mock_backend)
        mock_load.assert_called_once_with(run_id, mock_backend)
        mock_backend.delete.assert_called_once_with(run_id, "run")
