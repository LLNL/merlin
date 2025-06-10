"""
Tests for the `merlin/backends/redis/redis_store_base.py` module.
"""

from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from merlin.backends.redis.redis_store_base import NameMappingMixin, RedisStoreBase
from merlin.db_scripts.data_models import RunModel, StudyModel
from merlin.exceptions import RunNotFoundError, StudyNotFoundError
from tests.fixture_types import FixtureCallable, FixtureDict, FixtureRedis


class TestRedisStoreBase:
    """Tests for the RedisStoreBase class."""

    @pytest.fixture
    def simple_store(self, mock_redis: FixtureRedis) -> RedisStoreBase:
        """
        Create a simple store instance for testing.

        Args:
            mock_redis: A fixture providing a mocked Redis client.

        Returns:
            A simple RedisStoreBase implementation for testing.
        """

        # Create a simple subclass of RedisStoreBase for testing
        class TestStore(RedisStoreBase):
            pass

        store = TestStore(mock_redis, "test", RunModel)
        return store

    def test_initialization(self, mock_redis: FixtureRedis):
        """
        Test that the store initializes correctly.

        Args:
            mock_redis: A fixture providing a mocked Redis client.
        """
        store = RedisStoreBase(mock_redis, "test_key", RunModel)

        assert store.client == mock_redis
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
        test_models: FixtureRedis,
        simple_store: RedisStoreBase,
    ):
        """
        Test saving a new object to Redis.

        Args:
            mocker: PyTest mocker fixture.
            test_models: A fixture providing test model instances.
            simple_store: A fixture providing a RedisStoreBase instance.
        """
        run = test_models["run"]
        simple_store.client.exists.return_value = False
        mock_serialize = mocker.patch(
            "merlin.backends.redis.redis_store_base.serialize_entity", return_value={"id": run.id, "foo": "bar"}
        )

        simple_store.save(run)

        simple_store.client.exists.assert_called_once_with(f"{simple_store.key}:{run.id}")
        mock_serialize.assert_called_once_with(run)
        simple_store.client.hset.assert_called_once_with(f"{simple_store.key}:{run.id}", mapping={"id": run.id, "foo": "bar"})

    def test_save_existing_object(
        self,
        mocker: MockerFixture,
        test_models: FixtureRedis,
        simple_store: RedisStoreBase,
    ):
        """
        Test updating an existing object in Redis.

        Args:
            mocker: PyTest mocker fixture.
            test_models: A fixture providing test model instances.
            simple_store: A fixture providing a RedisStoreBase instance.
        """
        run = test_models["run"]
        simple_store.client.exists.return_value = True
        # Mock deserialization of existing data
        mock_deserialize = mocker.patch("merlin.backends.redis.redis_store_base.deserialize_entity")
        mock_existing = MagicMock()
        mock_deserialize.return_value = mock_existing
        simple_store.client.hgetall.return_value = {"id": run.id, "foo": "old"}
        # Mock update_fields and to_dict
        mock_existing.update_fields = MagicMock()
        mock_existing.to_dict.return_value = {"id": run.id, "foo": "bar"}
        mock_serialize = mocker.patch(
            "merlin.backends.redis.redis_store_base.serialize_entity", return_value={"id": run.id, "foo": "bar"}
        )

        simple_store.save(run)

        simple_store.client.exists.assert_called_once_with(f"{simple_store.key}:{run.id}")
        simple_store.client.hgetall.assert_called_once_with(f"{simple_store.key}:{run.id}")
        mock_deserialize.assert_called_once_with({"id": run.id, "foo": "old"}, RunModel)
        mock_existing.update_fields.assert_called_once_with(run.to_dict())
        mock_serialize.assert_called_once_with(mock_existing)
        simple_store.client.hset.assert_called_once_with(f"{simple_store.key}:{run.id}", mapping={"id": run.id, "foo": "bar"})

    def test_retrieve_existing_object(
        self,
        mocker: MockerFixture,
        test_models: FixtureRedis,
        create_redis_hash_data: FixtureCallable,
        simple_store: RedisStoreBase,
    ):
        """
        Test retrieving an existing object from Redis.

        Args:
            mocker: PyTest mocker fixture.
            test_models: A fixture providing test model instances.
            create_redis_hash_data: A fixture that creates Redis hash data.
            simple_store: A fixture providing a RedisStoreBase instance.
        """
        run = test_models["run"]
        simple_store.client.exists.return_value = True
        redis_data = create_redis_hash_data(run)
        simple_store.client.hgetall.return_value = redis_data
        mock_deserialize = mocker.patch("merlin.backends.redis.redis_store_base.deserialize_entity", return_value=run)

        result = simple_store.retrieve(run.id)

        simple_store.client.exists.assert_called_once_with(f"{simple_store.key}:{run.id}")
        simple_store.client.hgetall.assert_called_once_with(f"{simple_store.key}:{run.id}")
        mock_deserialize.assert_called_once_with(redis_data, RunModel)
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
        test_models: FixtureRedis,
        simple_store: RedisStoreBase,
    ):
        """
        Test retrieving all objects from Redis.

        Args:
            mocker: PyTest mocker fixture.
            test_models: A fixture providing test model instances.
            simple_store: A fixture providing a RedisStoreBase instance.
        """
        # Setup
        run1 = test_models["run"]
        run2 = RunModel(id="run2", study_id="study1")

        keys = [f"{simple_store.key}:{run1.id}", f"{simple_store.key}:{run2.id}"]
        simple_store.client.scan_iter.return_value = keys

        # Mock the retrieve method to return our test models
        mocker.patch.object(simple_store, "retrieve", side_effect=[run1, run2])

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
        test_models: FixtureRedis,
        simple_store: RedisStoreBase,
    ):
        """
        Test deleting an existing object from Redis.

        Args:
            mocker: PyTest mocker fixture.
            test_models: A fixture providing test model instances.
            simple_store: A fixture providing a RedisStoreBase instance.
        """
        run = test_models["run"]
        mocker.patch.object(simple_store, "retrieve", return_value=run)
        mocker.patch("merlin.backends.redis.redis_store_base.get_not_found_error_class", return_value=RunNotFoundError)

        simple_store.delete(run.id)

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
        mocker.patch.object(simple_store, "retrieve", return_value=None)
        mocker.patch("merlin.backends.redis.redis_store_base.get_not_found_error_class", return_value=RunNotFoundError)

        with pytest.raises(RunNotFoundError):
            simple_store.delete("nonexistent_id")

        simple_store.retrieve.assert_called_once_with("nonexistent_id")
        simple_store.client.delete.assert_not_called()


class TestNameMappingMixin:
    """Tests for the NameMappingMixin class."""

    @pytest.fixture
    def name_mapping_store(self, mock_redis: FixtureRedis) -> RedisStoreBase:
        """
        Create a store with NameMappingMixin for testing.

        Args:
            mock_redis: A fixture providing a mocked Redis client.

        Returns:
            A store instance that implements NameMappingMixin.
        """

        # Create a test class that uses the NameMappingMixin
        class TestStore(NameMappingMixin, RedisStoreBase):
            pass

        store = TestStore(mock_redis, "test", StudyModel)
        return store

    def test_save_new_object(
        self,
        mocker: MockerFixture,
        test_models: FixtureDict,
        name_mapping_store: RedisStoreBase,
    ):
        """
        Test saving a new object with name mapping.

        Args:
            mocker: PyTest mocker fixture.
            test_models: A fixture providing test model instances.
            name_mapping_store: A fixture providing a store with NameMappingMixin.
        """
        # Setup
        study = test_models["study"]
        name_mapping_store.client.hget.return_value = None  # No existing object with this name

        # Mock the parent class's save method
        mocker.patch.object(RedisStoreBase, "save")

        # Call the method
        name_mapping_store.save(study)

        # Assertions
        name_mapping_store.client.hget.assert_called_once_with(f"{name_mapping_store.key}:name", study.name)
        RedisStoreBase.save.assert_called_once_with(study)
        name_mapping_store.client.hset.assert_called_once_with(f"{name_mapping_store.key}:name", study.name, study.id)

    def test_save_existing_object(
        self,
        mocker: MockerFixture,
        test_models: FixtureDict,
        name_mapping_store: RedisStoreBase,
    ):
        """
        Test updating an existing object with name mapping.

        Args:
            mocker: PyTest mocker fixture.
            test_models: A fixture providing test model instances.
            name_mapping_store: A fixture providing a store with NameMappingMixin.
        """
        # Setup
        study = test_models["study"]
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
        test_models: FixtureDict,
        name_mapping_store: RedisStoreBase,
    ):
        """
        Test retrieving an object by ID with name mapping.

        Args:
            mocker: PyTest mocker fixture.
            test_models: A fixture providing test model instances.
            name_mapping_store: A fixture providing a store with NameMappingMixin.
        """
        # Setup
        study = test_models["study"]

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
        test_models: FixtureDict,
        name_mapping_store: RedisStoreBase,
    ):
        """
        Test retrieving an existing object by name with name mapping.

        Args:
            mocker: PyTest mocker fixture.
            test_models: A fixture providing test model instances.
            name_mapping_store: A fixture providing a store with NameMappingMixin.
        """
        # Setup
        study = test_models["study"]
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
        test_models: FixtureDict,
        name_mapping_store: RedisStoreBase,
    ):
        """
        Test deleting an object by ID with name mapping.

        Args:
            mocker: PyTest mocker fixture.
            test_models: A fixture providing test model instances.
            name_mapping_store: A fixture providing a store with NameMappingMixin.
        """
        study = test_models["study"]

        # Patch get_not_found_error_class at the correct location
        mocker.patch("merlin.backends.redis.redis_store_base.get_not_found_error_class", return_value=StudyNotFoundError)

        # Patch retrieve to call the parent method (simulate as if the object exists)
        mocker.patch.object(NameMappingMixin, "retrieve", return_value=study)

        # Call the method
        name_mapping_store.delete(study.id, by_name=False)

        # Assertions
        NameMappingMixin.retrieve.assert_called_once_with(study.id, by_name=False)
        name_mapping_store.client.hdel.assert_called_once_with(f"{name_mapping_store.key}:name", study.name)
        name_mapping_store.client.delete.assert_called_once_with(f"{name_mapping_store.key}:{study.id}")

    def test_delete_by_name(
        self,
        mocker: MockerFixture,
        test_models: FixtureDict,
        name_mapping_store: RedisStoreBase,
    ):
        """
        Test deleting an object by name with name mapping.

        Args:
            mocker: PyTest mocker fixture.
            test_models: A fixture providing test model instances.
            name_mapping_store: A fixture providing a store with NameMappingMixin.
        """
        study = test_models["study"]

        # Patch get_not_found_error_class at the correct location
        mocker.patch("merlin.backends.redis.redis_store_base.get_not_found_error_class", return_value=StudyNotFoundError)

        # Patch retrieve to call the parent method (simulate as if the object exists)
        mocker.patch.object(NameMappingMixin, "retrieve", return_value=study)

        # Call the method
        name_mapping_store.delete(study.name, by_name=True)

        # Assertions
        NameMappingMixin.retrieve.assert_called_once_with(study.name, by_name=True)
        name_mapping_store.client.hdel.assert_called_once_with(f"{name_mapping_store.key}:name", study.name)
        name_mapping_store.client.delete.assert_called_once_with(f"{name_mapping_store.key}:{study.id}")

    def test_delete_nonexistent_object(self, mocker: MockerFixture, name_mapping_store: RedisStoreBase):
        """
        Test deleting a non-existent object with name mapping.

        Args:
            mocker: PyTest mocker fixture.
            name_mapping_store: A fixture providing a store with NameMappingMixin.
        """
        # Patch get_not_found_error_class at the correct location
        mocker.patch("merlin.backends.redis.redis_store_base.get_not_found_error_class", return_value=StudyNotFoundError)

        # Patch retrieve to return None
        mocker.patch.object(NameMappingMixin, "retrieve", return_value=None)

        # Call the method and assert it raises the correct exception
        with pytest.raises(StudyNotFoundError):
            name_mapping_store.delete("nonexistent_name", by_name=True)

        # Assertions
        NameMappingMixin.retrieve.assert_called_once_with("nonexistent_name", by_name=True)
        name_mapping_store.client.hdel.assert_not_called()
        name_mapping_store.client.delete.assert_not_called()
