import pytest
from unittest.mock import Mock
from typing import Dict, List
from pytest_mock import MockerFixture

from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.data_models import BaseDataModel
from merlin.exceptions import EntityNotFoundError, RunNotFoundError, StudyNotFoundError, WorkerNotFoundError

# Import the module to test
from merlin.db_scripts.entities.db_entity import DatabaseEntity


class MockEntityInfo(BaseDataModel):
    """
    A mock implementation of `BaseDataModel` used for testing `DatabaseEntity`.

    Attributes:
        id (str): The ID of the mock entity.
        additional_data (Dict, optional): Any extra data associated with the entity.
    """
    def __init__(self, id: str, additional_data: Dict = None):
        """
        Constructor method.

        Args:
            id (str): The ID of the mock entity.
            additional_data (Dict, optional): Any extra data associated with the entity.
        """
        self.id = id
        self.additional_data = additional_data or {}
    
    @property
    def fields_allowed_to_be_updated(self) -> List:
        """
        Return the list of fields allowed to be updated.

        Returns:
            An empty list, as updates are not allowed for this mock.
        """
        return []


class TestEntity(DatabaseEntity[MockEntityInfo]):
    """
    A concrete implementation of `DatabaseEntity` for testing purposes.
    """
    def __repr__(self) -> str:
        """Return the official string representation of the entity."""
        return f"TestEntity(id={self.get_id()})"

    def __str__(self) -> str:
        """Return a user-friendly string representation of the entity."""
        return f"Test Entity {self.get_id()}"
    
    @classmethod
    def _get_entity_type(cls) -> str:
        """
        Return the type of entity used for backend operations.

        Returns:
            The string `"test_entity"`.
        """
        return "test_entity"


class DefaultTypeEntity(DatabaseEntity[MockEntityInfo]):
    """
    A DatabaseEntity subclass that does not override `_get_entity_type`,
    used to test default entity type inference.
    """
    def __repr__(self) -> str:
        """Return the official string representation of the entity."""
        return f"DefaultTypeEntity(id={self.get_id()})"

    def __str__(self) -> str:
        """Return a user-friendly string representation of the entity."""
        return f"Default Type Entity {self.get_id()}"


class TestDatabaseEntity:
    """Test suite for the `DatabaseEntity` base class."""
    
    @pytest.fixture
    def mock_backend(self, mocker: MockerFixture) -> Mock:
        """
        Fixture to provide a mocked `ResultsBackend`.

        Args:
            mocker (MockerFixture): A pytest-mock fixture for mocking.

        Returns:
            A mock `ResultsBackend` instance.
        """
        return mocker.Mock(spec=ResultsBackend)
    
    @pytest.fixture
    def entity_info(self) -> MockEntityInfo:
        """
        Fixture to provide a sample `MockEntityInfo` instance.

        Returns:
            A mock data model for testing.
        """
        return MockEntityInfo(id="test-123", additional_data={"key": "value"})
    
    @pytest.fixture
    def entity(self, entity_info: MockEntityInfo, mock_backend: Mock) -> TestEntity:
        """
        Fixture to provide a `TestEntity` instance.

        Args:
            entity_info (MockEntityInfo): The mock entity data.
            mock_backend (Mock): The mock backend.

        Returns:
            The entity under test.
        """
        return TestEntity(entity_info, mock_backend)
    
    def test_init(self, entity: TestEntity, entity_info: MockEntityInfo, mock_backend: Mock):
        """
        Test that the entity is initialized with the correct info and backend.
        
        Args:
            entity: A fixture that returns an entity object for testing.
            entity_info: A fixture that returns a mocked data model instance.
            mock_backend: A fixture that returns a mocked results backend instance.
        """
        assert entity.entity_info == entity_info
        assert entity.backend == mock_backend
    
    def test_get_id(self, entity: TestEntity):
        """
        Test that get_id returns the correct entity ID.
        
        Args:
            entity: A fixture that returns an entity object for testing.
        """
        assert entity.get_id() == "test-123"
    
    def test_repr(self, entity: TestEntity):
        """
        Test the __repr__ output.
        
        Args:
            entity: A fixture that returns an entity object for testing.
        """
        assert repr(entity) == "TestEntity(id=test-123)"
    
    def test_str(self, entity: TestEntity):
        """
        Test the __str__ output.
        
        Args:
            entity: A fixture that returns an entity object for testing.
        """
        assert str(entity) == "Test Entity test-123"
    
    def test_entity_type_custom(self, entity: TestEntity):
        """
        Test that a custom entity type is correctly returned.
        
        Args:
            entity: A fixture that returns an entity object for testing.
        """
        assert entity.entity_type == "test_entity"
    
    def test_entity_type_default(self, entity_info: MockEntityInfo, mock_backend: Mock):
        """
        Test that the default entity type is inferred from the class name.

        Should return 'defaulttype' for `DefaultTypeEntity`.

        Args:
            entity_info: A fixture that returns a mocked data model instance.
            mock_backend: A fixture that returns a mocked results backend instance.
        """
        default_entity = DefaultTypeEntity(entity_info, mock_backend)
        # Should use class name without "entity" suffix, lowercase
        assert default_entity.entity_type == "defaulttype"
    
    def test_reload_data_success(self, entity: TestEntity, mock_backend: Mock):
        """
        Test successful reload of entity data from the backend.

        Args:
            entity: A fixture that returns an entity object for testing.
            mock_backend: A fixture that returns a mocked results backend instance.
        """
        new_entity_info = MockEntityInfo(id="test-123", additional_data={"key": "updated"})
        mock_backend.retrieve.return_value = new_entity_info
        
        entity.reload_data()
        
        mock_backend.retrieve.assert_called_once_with("test-123", "test_entity")
        assert entity.entity_info == new_entity_info
    
    def test_reload_data_not_found(self, entity: TestEntity, mock_backend: Mock):
        """
        Test that `EntityNotFoundError` is raised when the entity is not found.

        Args:
            entity: A fixture that returns an entity object for testing.
            mock_backend: A fixture that returns a mocked results backend instance.
        """
        mock_backend.retrieve.return_value = None
        
        with pytest.raises(EntityNotFoundError) as excinfo:
            entity.reload_data()
        
        assert "Test_entity with ID test-123 not found" in str(excinfo.value)
        mock_backend.retrieve.assert_called_once_with("test-123", "test_entity")
    
    def test_get_additional_data(self, entity: TestEntity, mock_backend: Mock):
        """
        Test that additional data can be retrieved correctly.

        Args:
            entity: A fixture that returns an entity object for testing.
            mock_backend: A fixture that returns a mocked results backend instance.
        """
        new_entity_info = MockEntityInfo(id="test-123", additional_data={"key": "updated"})
        mock_backend.retrieve.return_value = new_entity_info
        
        result = entity.get_additional_data()
        
        assert result == {"key": "updated"}
        mock_backend.retrieve.assert_called_once_with("test-123", "test_entity")
    
    def test_save(self, mocker: MockerFixture, entity: TestEntity, mock_backend: Mock):
        """
        Test that the entity is saved correctly and `_post_save_hook` is called.

        Args:
            mocker: A pytest-mock fixture for mocking.
            entity: A fixture that returns an entity object for testing.
            mock_backend: A fixture that returns a mocked results backend instance.
        """
        # Mock _post_save_hook to verify it's called
        entity._post_save_hook = mocker.Mock()
        
        entity.save()
        
        mock_backend.save.assert_called_once_with(entity.entity_info)
        entity._post_save_hook.assert_called_once()
    
    def test_post_save_hook(self, entity: TestEntity):
        """
        Test that the default `_post_save_hook` implementation runs without error.

        Args:
            entity: A fixture that returns an entity object for testing.
        """
        # Default implementation should do nothing, but be callable
        entity._post_save_hook()  # Should not raise any exception
    
    def test_load_success(self, mock_backend: Mock):
        """
        Test successful loading of an entity from the backend.

        Args:
            mock_backend: A fixture that returns a mocked results backend instance.
        """
        entity_info = MockEntityInfo(id="test-123")
        mock_backend.retrieve.return_value = entity_info
        
        entity = TestEntity.load("test-123", mock_backend)
        
        assert isinstance(entity, TestEntity)
        assert entity.get_id() == "test-123"
        mock_backend.retrieve.assert_called_once_with("test-123", "test_entity")
    
    def test_load_not_found(self, mock_backend: Mock):
        """
        Test that loading a non-existent entity raises `EntityNotFoundError`.

        Args:
            mock_backend: A fixture that returns a mocked results backend instance.
        """
        mock_backend.retrieve.return_value = None
        
        with pytest.raises(EntityNotFoundError) as excinfo:
            TestEntity.load("test-123", mock_backend)
        
        assert "Test_entity with ID test-123 not found" in str(excinfo.value)
        mock_backend.retrieve.assert_called_once_with("test-123", "test_entity")
    
    def test_delete(self, mock_backend: Mock):
        """
        Test that the `delete` method calls the backend with the correct arguments.

        Args:
            mock_backend: A fixture that returns a mocked results backend instance.
        """
        TestEntity.delete("test-123", mock_backend)
        
        mock_backend.delete.assert_called_once_with("test-123", "test_entity")
    
    def test_error_classes_mapping(self):
        """
        Test that error class mappings for known entity types are correct.
        """
        # Test the error class mapping for different entity types
        assert TestEntity._error_classes["run"] == RunNotFoundError
        assert TestEntity._error_classes["study"] == StudyNotFoundError
        assert TestEntity._error_classes["logical_worker"] == WorkerNotFoundError
        assert TestEntity._error_classes["physical_worker"] == WorkerNotFoundError
    
    def test_specific_error_classes(self, entity_info: MockEntityInfo, mock_backend: Mock):
        """
        Test that appropriate error classes are raised for specific entity types.

        Args:
            entity_info: A fixture that returns a mocked data model instance.
            mock_backend: A fixture that returns a mocked results backend instance.
        """
        # Create entities with types that map to specific error classes
        class RunEntity(TestEntity):
            @classmethod
            def _get_entity_type(cls) -> str:
                return "run"
                
        class StudyEntity(TestEntity):
            @classmethod
            def _get_entity_type(cls) -> str:
                return "study"
        
        # Test run entity not found error
        mock_backend.retrieve.return_value = None
        with pytest.raises(RunNotFoundError):
            run_entity = RunEntity(entity_info, mock_backend)
            run_entity.reload_data()
            
        # Test study entity not found error
        with pytest.raises(StudyNotFoundError):
            study_entity = StudyEntity(entity_info, mock_backend)
            study_entity.reload_data()
