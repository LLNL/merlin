"""
Tests for the `merlin/backends/redis/redis_base_store.py` module.
"""

import pytest
from pytest_mock import MockerFixture

from merlin.backends.redis.redis_base_store import RedisStoreBase, NameMappingMixin
from merlin.db_scripts.data_models import (
    LogicalWorkerModel, 
    PhysicalWorkerModel, 
    RunModel, 
    StudyModel
)
from merlin.exceptions import WorkerNotFoundError, RunNotFoundError, StudyNotFoundError
from tests.fixture_types import FixtureCallable, FixtureDict, FixtureRedis


class TestRedisStoreBase:
    """Tests for the RedisStoreBase class."""
    
    @pytest.fixture
    def simple_store(self, redis_stores_mock_redis: FixtureRedis) -> RedisStoreBase:
        """
        Create a simple store instance for testing.

        Args:
            redis_stores_mock_redis: A fixture providing a mocked Redis client.
            
        Returns:
            A simple RedisStoreBase implementation for testing.
        """
        # Create a simple subclass of RedisStoreBase for testing
        class TestStore(RedisStoreBase):
            pass
        
        store = TestStore(redis_stores_mock_redis, "test", RunModel)
        return store
    
    def test_initialization(self, redis_stores_mock_redis: FixtureRedis):
        """
        Test that the store initializes correctly.
        
        Args:
            redis_stores_mock_redis: A fixture providing a mocked Redis client.
        """
        store = RedisStoreBase(redis_stores_mock_redis, "test_key", RunModel)
        
        assert store.client == redis_stores_mock_redis
        assert store.key == "test_key"
        assert store.model_class == RunModel
    
    def test_get_full_key(self, simple_store: RedisStoreBase):
        """
        Test the _get_full_key method.
        
        Args:
            simple_store: A fixture providing a RedisStoreBase instance.
        """
        # When the ID already has the prefix
        key = simple_store._get_full_key("test:123")
        assert key == "test:123"
        
        # When the ID doesn't have the prefix
        key = simple_store._get_full_key("123")
        assert key == "test:123"
    
    def test_save_new_object(
        self,
        mocker: MockerFixture,
        redis_stores_test_models: FixtureRedis,
        simple_store: RedisStoreBase,
    ):
        """
        Test saving a new object to Redis.
        
        Args:
            mocker: PyTest mocker fixture.
            redis_stores_test_models: A fixture providing test model instances.
            simple_store: A fixture providing a RedisStoreBase instance.
        """
        # Setup
        run = redis_stores_test_models["run"]
        simple_store.client.exists.return_value = False
        
        # Mock the create_data_class_entry function
        mocked_update = mocker.patch("merlin.backends.redis.redis_base_store.create_data_class_entry")
        
        # Call the method
        simple_store.save(run)
        
        # Assertions
        simple_store.client.exists.assert_called_once_with(f"{simple_store.key}:{run.id}")
        mocked_update.assert_called_once_with(run, f"{simple_store.key}:{run.id}", simple_store.client)
    
    def test_save_existing_object(
        self,
        mocker: MockerFixture,
        redis_stores_test_models: FixtureRedis,
        simple_store: RedisStoreBase,
    ):
        """
        Test updating an existing object in Redis.
        
        Args:
            mocker: PyTest mocker fixture.
            redis_stores_test_models: A fixture providing test model instances.
            simple_store: A fixture providing a RedisStoreBase instance.
        """
        # Setup
        run = redis_stores_test_models["run"]
        simple_store.client.exists.return_value = True
        
        # Mock the update_data_class_entry function
        mocked_update = mocker.patch("merlin.backends.redis.redis_base_store.update_data_class_entry")
        
        # Call the method
        simple_store.save(run)
        
        # Assertions
        simple_store.client.exists.assert_called_once_with(f"{simple_store.key}:{run.id}")
        mocked_update.assert_called_once_with(run, f"{simple_store.key}:{run.id}", simple_store.client)
    
    def test_retrieve_existing_object(
        self,
        mocker: MockerFixture,
        redis_stores_test_models: FixtureRedis,
        redis_stores_create_redis_hash_data: FixtureCallable,
        simple_store: RedisStoreBase,
    ):
        """
        Test retrieving an existing object from Redis.
        
        Args:
            mocker: PyTest mocker fixture.
            redis_stores_test_models: A fixture providing test model instances.
            redis_stores_create_redis_hash_data: A fixture that creates Redis hash data.
            simple_store: A fixture providing a RedisStoreBase instance.
        """
        # Setup
        run = redis_stores_test_models["run"]
        simple_store.client.exists.return_value = True
        simple_store.client.hgetall.return_value = redis_stores_create_redis_hash_data(run)
        
        # Mock the deserialize_data_class function
        mocked_deserialize = mocker.patch(
            "merlin.backends.redis.redis_base_store.deserialize_data_class",
            return_value=run
        )
        
        # Call the method
        result = simple_store.retrieve(run.id)
        
        # Assertions
        simple_store.client.exists.assert_called_once_with(f"{simple_store.key}:{run.id}")
        simple_store.client.hgetall.assert_called_once_with(f"{simple_store.key}:{run.id}")
        mocked_deserialize.assert_called_once_with(
            simple_store.client.hgetall.return_value, 
            simple_store.model_class
        )
        assert result == run
    
    def test_retrieve_nonexistent_object(self, simple_store: RedisStoreBase):
        """
        Test retrieving a non-existent object from Redis.
        
        Args:
            simple_store: A fixture providing a RedisStoreBase instance.
        """
        # Setup
        simple_store.client.exists.return_value = False
        
        # Call the method
        result = simple_store.retrieve("nonexistent_id")
        
        # Assertions
        simple_store.client.exists.assert_called_once_with(f"{simple_store.key}:nonexistent_id")
        simple_store.client.hgetall.assert_not_called()
        assert result is None
    
    def test_retrieve_all(
        self,
        mocker: MockerFixture,
        redis_stores_test_models: FixtureRedis,
        simple_store: RedisStoreBase,
    ):
        """
        Test retrieving all objects from Redis.
        
        Args:
            mocker: PyTest mocker fixture.
            redis_stores_test_models: A fixture providing test model instances.
            simple_store: A fixture providing a RedisStoreBase instance.
        """
        # Setup
        run1 = redis_stores_test_models["run"]
        run2 = RunModel(id="run2", study_id="study1")
        
        keys = [f"{simple_store.key}:{run1.id}", f"{simple_store.key}:{run2.id}"]
        simple_store.client.scan_iter.return_value = keys
        
        # Mock the retrieve method to return our test models
        mocker.patch.object(
            simple_store, 
            "retrieve", 
            side_effect=[run1, run2]
        )
        
        # Call the method
        results = simple_store.retrieve_all()
        
        # Assertions
        simple_store.client.scan_iter.assert_called_once_with(match=f"{simple_store.key}:*")
        assert simple_store.retrieve.call_count == 2
        assert len(results) == 2
        assert run1 in results
        assert run2 in results
    
    def test_delete_existing_object(
        self,
        mocker: MockerFixture,
        redis_stores_test_models: FixtureRedis,
        simple_store: RedisStoreBase,
    ):
        """
        Test deleting an existing object from Redis.
        
        Args:
            mocker: PyTest mocker fixture.
            redis_stores_test_models: A fixture providing test model instances.
            simple_store: A fixture providing a RedisStoreBase instance.
        """
        # Setup
        run = redis_stores_test_models["run"]
        
        # Mock the retrieve method to return our test model
        mocker.patch.object(simple_store, "retrieve", return_value=run)
        mocker.patch.object(simple_store, "_get_not_found_error_class", return_value=RunNotFoundError)
        
        # Call the method
        simple_store.delete(run.id)
        
        # Assertions
        simple_store.retrieve.assert_called_once_with(run.id)
        simple_store.client.delete.assert_called_once_with(f"{simple_store.key}:{run.id}")
    
    def test_delete_nonexistent_object(
        self,
        mocker: MockerFixture,
        simple_store: RedisStoreBase,
    ):
        """
        Test deleting a non-existent object from Redis.
        
        Args:
            mocker: PyTest mocker fixture.
            simple_store: A fixture providing a RedisStoreBase instance.
        """
        # Setup
        mocker.patch.object(simple_store, "retrieve", return_value=None)
        mocker.patch.object(simple_store, "_get_not_found_error_class", return_value=RunNotFoundError)
        
        # Call the method and assert it raises the correct exception
        with pytest.raises(RunNotFoundError):
            simple_store.delete("nonexistent_id")
        
        # Assertions
        simple_store.retrieve.assert_called_once_with("nonexistent_id")
        simple_store.client.delete.assert_not_called()
    
    def test_get_not_found_error_class(
        self,
        redis_stores_mock_redis: FixtureRedis
    ):
        """
        Test the _get_not_found_error_class method returns the correct error class.
        
        Args:
            redis_stores_mock_redis: A fixture providing a mocked Redis client.
        """        
        # Create stores with different model classes
        logical_worker_store = RedisStoreBase(redis_stores_mock_redis, "logical_worker", LogicalWorkerModel)
        physical_worker_store = RedisStoreBase(redis_stores_mock_redis, "physical_worker", PhysicalWorkerModel)
        run_store = RedisStoreBase(redis_stores_mock_redis, "run", RunModel)
        study_store = RedisStoreBase(redis_stores_mock_redis, "study", StudyModel)
        
        # Test error classes
        assert logical_worker_store._get_not_found_error_class() == WorkerNotFoundError
        assert physical_worker_store._get_not_found_error_class() == WorkerNotFoundError
        assert run_store._get_not_found_error_class() == RunNotFoundError
        assert study_store._get_not_found_error_class() == StudyNotFoundError


class TestNameMappingMixin:
    """Tests for the NameMappingMixin class."""
    
    @pytest.fixture
    def name_mapping_store(self, redis_stores_mock_redis: FixtureRedis) -> RedisStoreBase:
        """
        Create a store with NameMappingMixin for testing.
        
        Args:
            redis_stores_mock_redis: A fixture providing a mocked Redis client.

        Returns:
            A store instance that implements NameMappingMixin.
        """
        # Create a test class that uses the NameMappingMixin
        class TestStore(NameMappingMixin, RedisStoreBase):
            pass
        
        store = TestStore(redis_stores_mock_redis, "test", StudyModel)
        return store
    
    def test_save_new_object(
        self,
        mocker: MockerFixture,
        redis_stores_test_models: FixtureDict,
        name_mapping_store: RedisStoreBase,
    ):
        """
        Test saving a new object with name mapping.
        
        Args:
            mocker: PyTest mocker fixture.
            redis_stores_test_models: A fixture providing test model instances.
            name_mapping_store: A fixture providing a store with NameMappingMixin.
        """
        # Setup
        study = redis_stores_test_models["study"]
        name_mapping_store.client.hget.return_value = None  # No existing object with this name
        
        # Mock the parent class's save method
        mocker.patch.object(RedisStoreBase, "save")
        
        # Call the method
        name_mapping_store.save(study)
        
        # Assertions
        name_mapping_store.client.hget.assert_called_once_with(f"{name_mapping_store.key}:name", study.name)
        RedisStoreBase.save.assert_called_once_with(study)
        name_mapping_store.client.hset.assert_called_once_with(
            f"{name_mapping_store.key}:name", 
            study.name, 
            study.id
        )
    
    def test_save_existing_object(
        self,
        mocker: MockerFixture,
        redis_stores_test_models: FixtureDict,
        name_mapping_store: RedisStoreBase,
    ):
        """
        Test updating an existing object with name mapping.
        
        Args:
            mocker: PyTest mocker fixture.
            redis_stores_test_models: A fixture providing test model instances.
            name_mapping_store: A fixture providing a store with NameMappingMixin.
        """
        # Setup
        study = redis_stores_test_models["study"]
        name_mapping_store.client.hget.return_value = study.id  # Existing object with this name
        
        # Mock the parent class's save method
        mocker.patch.object(RedisStoreBase, "save")
        
        # Call the method
        name_mapping_store.save(study)
        
        # Assertions
        name_mapping_store.client.hget.assert_called_once_with(f"{name_mapping_store.key}:name", study.name)
        RedisStoreBase.save.assert_called_once_with(study)
        name_mapping_store.client.hset.assert_not_called()  # Should not update name mapping
    
    def test_retrieve_by_id(
        self,
        mocker: MockerFixture,
        redis_stores_test_models: FixtureDict,
        name_mapping_store: RedisStoreBase,
    ):
        """
        Test retrieving an object by ID with name mapping.
        
        Args:
            mocker: PyTest mocker fixture.
            redis_stores_test_models: A fixture providing test model instances.
            name_mapping_store: A fixture providing a store with NameMappingMixin.
        """
        # Setup
        study = redis_stores_test_models["study"]
        
        # Mock the parent class's retrieve method
        mocker.patch.object(RedisStoreBase, "retrieve", return_value=study)
        
        # Call the method
        result = name_mapping_store.retrieve(study.id, by_name=False)
        
        # Assertions
        RedisStoreBase.retrieve.assert_called_once_with(study.id)
        assert result == study
    
    def test_retrieve_by_name_existing(
        self,
        mocker: MockerFixture,
        redis_stores_test_models: FixtureDict,
        name_mapping_store: RedisStoreBase,
    ):
        """
        Test retrieving an existing object by name with name mapping.
        
        Args:
            mocker: PyTest mocker fixture.
            redis_stores_test_models: A fixture providing test model instances.
            name_mapping_store: A fixture providing a store with NameMappingMixin.
        """
        # Setup
        study = redis_stores_test_models["study"]
        name_mapping_store.client.hget.return_value = study.id
        
        # Mock the parent class's retrieve method
        mocker.patch.object(RedisStoreBase, "retrieve", return_value=study)
        
        # Call the method
        result = name_mapping_store.retrieve(study.name, by_name=True)
        
        # Assertions
        name_mapping_store.client.hget.assert_called_once_with(f"{name_mapping_store.key}:name", study.name)
        RedisStoreBase.retrieve.assert_called_once_with(study.id)
        assert result == study
    
    def test_retrieve_by_name_nonexistent(self, name_mapping_store: RedisStoreBase):
        """
        Test retrieving a non-existent object by name with name mapping.
        
        Args:
            name_mapping_store: A fixture providing a store with NameMappingMixin.
        """
        # Setup
        name_mapping_store.client.hget.return_value = None
        
        # Call the method
        result = name_mapping_store.retrieve("nonexistent_name", by_name=True)
        
        # Assertions
        name_mapping_store.client.hget.assert_called_once_with(f"{name_mapping_store.key}:name", "nonexistent_name")
        assert result is None
    
    def test_delete_by_id(
        self,
        mocker: MockerFixture,
        redis_stores_test_models: FixtureDict,
        name_mapping_store: RedisStoreBase,
    ):
        """
        Test deleting an object by ID with name mapping.
        
        Args:
            mocker: PyTest mocker fixture.
            redis_stores_test_models: A fixture providing test model instances.
            name_mapping_store: A fixture providing a store with NameMappingMixin.
        """
        # Setup
        study = redis_stores_test_models["study"]
        
        # Mock the retrieve method to return our test model
        mocker.patch.object(name_mapping_store, "retrieve", return_value=study)
        mocker.patch.object(name_mapping_store, "_get_not_found_error_class", return_value=StudyNotFoundError)
        
        # Call the method
        name_mapping_store.delete(study.id, by_name=False)
        
        # Assertions
        name_mapping_store.retrieve.assert_called_once_with(study.id, by_name=False)
        name_mapping_store.client.hdel.assert_called_once_with(f"{name_mapping_store.key}:name", study.name)
        name_mapping_store.client.delete.assert_called_once_with(f"{name_mapping_store.key}:{study.id}")
    
    def test_delete_by_name(
        self,
        mocker: MockerFixture,
        redis_stores_test_models: FixtureDict,
        name_mapping_store: RedisStoreBase,
    ):
        """
        Test deleting an object by name with name mapping.
        
        Args:
            mocker: PyTest mocker fixture.
            redis_stores_test_models: A fixture providing test model instances.
            name_mapping_store: A fixture providing a store with NameMappingMixin.
        """
        # Setup
        study = redis_stores_test_models["study"]
        
        # Mock the retrieve method to return our test model
        mocker.patch.object(name_mapping_store, "retrieve", return_value=study)
        mocker.patch.object(name_mapping_store, "_get_not_found_error_class", return_value=StudyNotFoundError)
        
        # Call the method
        name_mapping_store.delete(study.name, by_name=True)
        
        # Assertions
        name_mapping_store.retrieve.assert_called_once_with(study.name, by_name=True)
        name_mapping_store.client.hdel.assert_called_once_with(f"{name_mapping_store.key}:name", study.name)
        name_mapping_store.client.delete.assert_called_once_with(f"{name_mapping_store.key}:{study.id}")
    
    def test_delete_nonexistent_object(self, mocker: MockerFixture, name_mapping_store: RedisStoreBase):
        """
        Test deleting a non-existent object with name mapping.
        
        Args:
            mocker: PyTest mocker fixture.
            name_mapping_store: A fixture providing a store with NameMappingMixin.
        """
        # Setup
        mocker.patch.object(name_mapping_store, "retrieve", return_value=None)
        mocker.patch.object(name_mapping_store, "_get_not_found_error_class", return_value=StudyNotFoundError)
        
        # Call the method and assert it raises the correct exception
        with pytest.raises(StudyNotFoundError):
            name_mapping_store.delete("nonexistent_name", by_name=True)
        
        # Assertions
        name_mapping_store.retrieve.assert_called_once_with("nonexistent_name", by_name=True)
        name_mapping_store.client.hdel.assert_not_called()
        name_mapping_store.client.delete.assert_not_called()