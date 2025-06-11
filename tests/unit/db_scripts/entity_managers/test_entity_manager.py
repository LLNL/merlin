##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `entity_manager.py` module.
"""

from dataclasses import dataclass
from typing import Any, List
from unittest.mock import MagicMock, call

import pytest
from _pytest.capture import CaptureFixture

from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.data_models import BaseDataModel
from merlin.db_scripts.entities.db_entity import DatabaseEntity
from merlin.db_scripts.entity_managers.entity_manager import EntityManager
from merlin.exceptions import RunNotFoundError, StudyNotFoundError, WorkerNotFoundError


@dataclass
class TestDataModel(BaseDataModel):
    """A concrete subclass of `BaseDataModel` for testing."""

    id: str = "test_id"
    name: str = "default"
    attr1: str = "val"

    @property
    def fields_allowed_to_be_updated(self):
        return ["name", "attr1"]


class TestEntity(DatabaseEntity):
    """A concrete test implementation of a database entity."""

    def __init__(self, entity_info: TestDataModel, backend: ResultsBackend):
        self.entity_info = entity_info
        self.backend = backend

    def __repr__(self):
        """Return the official string representation of the entity."""
        return f"TestEntity(id={self.get_id()})"

    def __str__(self) -> str:
        """Return a user-friendly string representation of the entity."""
        return f"Test Entity {self.get_id()}"

    @classmethod
    def load(cls, identifier: str, backend: ResultsBackend) -> "TestEntity":
        try:
            entity_info = backend.retrieve(identifier, "test_entity")
            return cls(entity_info, backend)
        except KeyError:
            raise WorkerNotFoundError(f"Test entity {identifier} not found")

    @classmethod
    def delete(cls, identifier: str, backend: ResultsBackend):
        backend.delete(identifier, "test_entity")


class TestEntityManager(EntityManager[TestEntity, TestDataModel]):
    """A concrete test implementation of an entity manager."""

    def create(self, name: str, **kwargs: Any) -> TestEntity:
        return self._create_entity_if_not_exists(
            TestEntity,
            TestDataModel,
            name,
            f"Test entity {name} already exists, loading it.",
            f"Creating new test entity {name}",
            name=name,
            **kwargs,
        )

    def get(self, identifier: str) -> TestEntity:
        return self._get_entity(TestEntity, identifier)

    def get_all(self) -> List[TestEntity]:
        return self._get_all_entities(TestEntity, "test_entity")

    def delete(self, identifier: str, **kwargs: Any):
        cleanup_fn = kwargs.get("cleanup_fn")
        self._delete_entity(TestEntity, identifier, cleanup_fn=cleanup_fn)

    def delete_all(self, **kwargs: Any):
        self._delete_all_by_type(self.get_all, self.delete, "test entities", **kwargs)


@pytest.fixture
def mock_backend() -> MagicMock:
    """
    Create a mock backend instance.

    Returns:
        A mocked `ResultsBackend` instance.
    """
    backend = MagicMock(spec=ResultsBackend)
    return backend


@pytest.fixture
def entity_manager(mock_backend: MagicMock) -> TestEntityManager:
    """
    Create an instance of the base `EntityManager` for testing.

    Args:
        mock_backend: A mocked `ResultsBackend` instance.

    Returns:
        An instance of the base `EntityManager` for testing.
    """
    return TestEntityManager(mock_backend)


class TestEntityManagerBase:
    def test_init(self, entity_manager: TestEntityManager, mock_backend: MagicMock):
        """
        Test initialization of `EntityManager`.

        Args:
            entity_manager: An instance of the base `EntityManager` for testing.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        assert entity_manager.backend == mock_backend
        assert entity_manager.db is None

    def test_create_entity_if_not_exists_new(self, entity_manager: TestEntityManager, mock_backend: MagicMock):
        """
        Test the `_create_entity_if_not_exists` method when the entity is new (does not yet
        exist in the database).

        Args:
            entity_manager: An instance of the base `EntityManager` for testing.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        # Setup backend to raise an error, simulating entity not found
        mock_backend.retrieve.side_effect = KeyError()

        # Create a new entity
        entity = entity_manager.create("test1", attr1="value1")

        # Verify the entity was created with correct attributes
        assert isinstance(entity, TestEntity)
        assert entity.entity_info.name == "test1"
        assert entity.entity_info.attr1 == "value1"

        # Verify backend interactions
        mock_backend.retrieve.assert_called_once_with("test1", "test_entity")
        mock_backend.save.assert_called_once()

    def test_create_entity_if_not_exists_existing(self, entity_manager: TestEntityManager, mock_backend: MagicMock):
        """
        Test the `_create_entity_if_not_exists` method when the entity is already exists in the database.

        Args:
            entity_manager: An instance of the base `EntityManager` for testing.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        # Setup backend to return an existing entity
        existing_model = TestDataModel(name="test1", attr1="value1")
        mock_backend.retrieve.return_value = existing_model

        # Create/get the entity
        entity = entity_manager.create("test1", attr2="value2")

        # Verify the existing entity was returned
        assert isinstance(entity, TestEntity)
        assert entity.entity_info.name == "test1"
        assert entity.entity_info.attr1 == "value1"
        assert not hasattr(entity.entity_info, "attr2")  # Should not have the new attribute

        # Verify backend interactions
        mock_backend.retrieve.assert_called_once_with("test1", "test_entity")
        mock_backend.save.assert_not_called()

    def test_get_entity(self, entity_manager: TestEntityManager, mock_backend: MagicMock):
        """
        Test the `get_entity` method with an existing entity.

        Args:
            entity_manager: An instance of the base `EntityManager` for testing.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        # Setup backend to return an entity
        existing_model = TestDataModel(name="test1", attr1="value1")
        mock_backend.retrieve.return_value = existing_model

        # Get the entity
        entity = entity_manager.get("test1")

        # Verify the entity was returned correctly
        assert isinstance(entity, TestEntity)
        assert entity.entity_info.name == "test1"
        assert entity.entity_info.attr1 == "value1"

        # Verify backend interactions
        mock_backend.retrieve.assert_called_once_with("test1", "test_entity")

    def test_get_entity_not_found(self, entity_manager: TestEntityManager, mock_backend: MagicMock):
        """
        Test the `get_entity` method with a non-existing entity.

        Args:
            entity_manager: An instance of the base `EntityManager` for testing.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        # Setup backend to raise an error
        mock_backend.retrieve.side_effect = KeyError()

        # Attempt to get a non-existent entity
        with pytest.raises(WorkerNotFoundError):
            entity_manager.get("nonexistent")

        # Verify backend interactions
        mock_backend.retrieve.assert_called_once_with("nonexistent", "test_entity")

    def test_get_all_entities_empty(self, entity_manager: TestEntityManager, mock_backend: MagicMock):
        """
        Test the `get_all_entities` method with no entities in the database.

        Args:
            entity_manager: An instance of the base `EntityManager` for testing.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        # Setup backend to return no entities
        mock_backend.retrieve_all.return_value = []

        # Get all entities
        entities = entity_manager.get_all()

        # Verify result is empty
        assert entities == []

        # Verify backend interactions
        mock_backend.retrieve_all.assert_called_once_with("test_entity")

    def test_get_all_entities(self, entity_manager: TestEntityManager, mock_backend: MagicMock):
        """
        Test the `get_all_entities` method with entities in the database.

        Args:
            entity_manager: An instance of the base `EntityManager` for testing.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        # Setup backend to return multiple entities
        model1 = TestDataModel(name="test1", attr1="value1")
        model2 = TestDataModel(name="test2", attr1="value2")
        mock_backend.retrieve_all.return_value = [model1, model2]

        # Get all entities
        entities = entity_manager.get_all()

        # Verify correct entities were returned
        assert len(entities) == 2
        assert all(isinstance(entity, TestEntity) for entity in entities)
        assert entities[0].entity_info.name == "test1"
        assert entities[1].entity_info.name == "test2"

        # Verify backend interactions
        mock_backend.retrieve_all.assert_called_once_with("test_entity")

    def test_delete_entity(self, entity_manager: TestEntityManager, mock_backend: MagicMock):
        """
        Test the `delete_entity` method with an existing entity.

        Args:
            entity_manager: An instance of the base `EntityManager` for testing.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        # Setup backend to return an entity for load
        existing_model = TestDataModel(name="test1", attr1="value1")
        mock_backend.retrieve.return_value = existing_model

        # Delete the entity
        entity_manager.delete("test1")

        # Verify backend interactions
        mock_backend.retrieve.assert_called_once_with("test1", "test_entity")
        mock_backend.delete.assert_called_once_with("test1", "test_entity")

    def test_delete_entity_with_cleanup(self, entity_manager: TestEntityManager, mock_backend: MagicMock):
        """
        Test the `delete_entity` method with a cleanup function.

        Args:
            entity_manager: An instance of the base `EntityManager` for testing.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        # Setup backend to return an entity for load
        existing_model = TestDataModel(name="test1", attr1="value1")
        mock_backend.retrieve.return_value = existing_model

        # Create a cleanup function
        cleanup_fn = MagicMock()

        # Delete the entity with cleanup
        entity_manager.delete("test1", cleanup_fn=cleanup_fn)

        # Verify cleanup function was called with entity
        cleanup_fn.assert_called_once()
        called_entity = cleanup_fn.call_args[0][0]
        assert isinstance(called_entity, TestEntity)
        assert called_entity.entity_info.name == "test1"

        # Verify backend interactions
        mock_backend.retrieve.assert_called_once_with("test1", "test_entity")
        mock_backend.delete.assert_called_once_with("test1", "test_entity")

    def test_delete_all_empty(self, caplog: CaptureFixture, entity_manager: TestEntityManager, mock_backend: MagicMock):
        """
        Test the `delete_all` method with a no existing entities in the database.

        Args:
            caplog: A built-in fixture from the pytest library to capture logs.
            entity_manager: An instance of the base `EntityManager` for testing.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        # Setup backend to return no entities
        mock_backend.retrieve_all.return_value = []

        # Delete all entities
        entity_manager.delete_all()

        # Verify log warning was issued
        assert "No test entities found in the database." in caplog.text

        # Verify backend interactions
        mock_backend.retrieve_all.assert_called_once_with("test_entity")
        mock_backend.delete.assert_not_called()

    def test_delete_all(self, entity_manager: TestEntityManager, mock_backend: MagicMock):
        """
        Test the `delete_all` method with a existing entities.

        Args:
            entity_manager: An instance of the base `EntityManager` for testing.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        # Setup backend to return multiple entities
        model1 = TestDataModel(name="test1", attr1="value1")
        model2 = TestDataModel(name="test2", attr1="value2")
        mock_backend.retrieve_all.return_value = [model1, model2]

        # Delete all entities
        entity_manager.delete_all()

        # Verify backend interactions
        mock_backend.retrieve_all.assert_called_once_with("test_entity")
        assert mock_backend.delete.call_count == 2
        mock_backend.delete.assert_has_calls([call("test_id", "test_entity"), call("test_id", "test_entity")], any_order=True)

    def test_delete_all_with_cleanup(self, entity_manager: TestEntityManager, mock_backend: MagicMock):
        """
        Test the `delete_all` method with a cleanup function.

        Args:
            entity_manager: An instance of the base `EntityManager` for testing.
            mock_backend: A mocked `ResultsBackend` instance.
        """
        # Setup backend to return multiple entities
        model1 = TestDataModel(name="test1", attr1="value1")
        model2 = TestDataModel(name="test2", attr1="value2")
        mock_backend.retrieve_all.return_value = [model1, model2]

        # Create a cleanup function
        cleanup_fn = MagicMock()

        # Delete all entities with cleanup
        entity_manager.delete_all(cleanup_fn=cleanup_fn)

        # Verify backend interactions
        mock_backend.retrieve_all.assert_called_once_with("test_entity")
        assert mock_backend.delete.call_count == 2
        mock_backend.delete.assert_has_calls([call("test_id", "test_entity"), call("test_id", "test_entity")], any_order=True)

        # Verify cleanup function was called for each entity
        assert cleanup_fn.call_count == 2

    @pytest.mark.parametrize("exception_type", [WorkerNotFoundError, StudyNotFoundError, RunNotFoundError])
    def test_create_entity_handles_various_not_found_errors(
        self, entity_manager: TestEntityManager, mock_backend: MagicMock, exception_type: Exception
    ):
        """
        Test the `create` method to make sure it handles all of the exception types that could be raised.

        Args:
            entity_manager: An instance of the base `EntityManager` for testing.
            mock_backend: A mocked `ResultsBackend` instance.
            exception_type: The type of exception that's being tested.
        """
        # Setup backend to raise different types of not found errors
        mock_backend.retrieve.side_effect = exception_type("Entity not found")

        # Create a new entity
        entity = entity_manager.create("test1", attr1="value1")

        # Verify the entity was created
        assert isinstance(entity, TestEntity)
        assert entity.entity_info.name == "test1"
        assert entity.entity_info.attr1 == "value1"

        # Verify backend interactions
        mock_backend.retrieve.assert_called_once()
        mock_backend.save.assert_called_once()
