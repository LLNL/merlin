"""
Tests for the `redis_backend.py` module.
"""

import uuid

import pytest
from pytest_mock import MockerFixture

from merlin.backends.redis.redis_backend import RedisBackend
from merlin.db_scripts.data_models import BaseDataModel, LogicalWorkerModel, PhysicalWorkerModel, RunModel, StudyModel
from merlin.exceptions import UnsupportedDataModelError
from tests.fixture_types import FixtureStr


class TestRedisBackend:
    """
    Test suite for the `RedisBackend` class.

    This class contains unit tests to validate the functionality of the `RedisBackend` implementation,
    which provides an interface for interacting with Redis as a backend for storing and retrieving entities.

    Fixtures and mocking are used to isolate the tests from the actual Redis implementation, ensuring that the tests focus
    on the behavior of the `RedisBackend` class.

    Parametrization is employed to reduce redundancy and ensure comprehensive coverage across different entity types
    and operations.
    """

    def test_get_version(self, redis_backend_instance: RedisBackend):
        """
        Test that RedisBackend correctly retrieves the Redis version.

        Args:
            redis_backend_instance (RedisBackend): A fixture representing a `RedisBackend` instance with
                mocked Redis client and stores.
        """
        redis_backend_instance.client.info.return_value = {"redis_version": "6.2.6"}
        version = redis_backend_instance.get_version()
        assert version == "6.2.6", "Redis version should be correctly retrieved."

    def test_get_connection_string(
        self,
        mocker: MockerFixture,
        redis_backend_instance: RedisBackend,
        redis_backend_connection_string: FixtureStr,
    ):
        """
        Test that RedisBackend correctly retrieves the connection string.

        Args:
            mocker (MockerFixture): A built-in fixture from the pytest-mock library to create a Mock object.
            redis_backend_instance (RedisBackend): A fixture representing a `RedisBackend` instance with
                mocked Redis client and stores.
            redis_backend_connection_string (FixtureStr): A mock Redis connection string.
        """
        # The `redis_backend_instance` has mocked `get_connection_string` already
        connection_string = redis_backend_instance.get_connection_string()
        assert connection_string == redis_backend_connection_string, "Connection string should match the mocked value."

    def test_flush_database(self, redis_backend_instance: RedisBackend):
        """
        Test that RedisBackend flushes the database.

        Args:
            redis_backend_instance (RedisBackend): A fixture representing a `RedisBackend` instance with
                mocked Redis client and stores.
        """
        redis_backend_instance.flush_database()
        redis_backend_instance.client.flushdb.assert_called_once()

    @pytest.mark.parametrize(
        "db_model, model_type",
        [
            (StudyModel, "study"),
            (RunModel, "run"),
            (LogicalWorkerModel, "logical_worker"),
            (PhysicalWorkerModel, "physical_worker"),
        ],
    )
    def test_save_valid_entity(
        self,
        mocker: MockerFixture,
        redis_backend_instance: RedisBackend,
        db_model: BaseDataModel,
        model_type: str,
    ):
        """
        Test saving a valid entity to the Redis store.

        This is a parametrized test that ensures saving to the Redis store works for all valid model types.
        Currently this includes:
        - studies
        - runs
        - logical workers
        - physical workers

        Args:
            mocker (MockerFixture): A built-in fixture from the pytest-mock library to create a Mock object.
            redis_backend_instance (RedisBackend): A fixture representing a `RedisBackend` instance with
                mocked Redis client and stores.
            db_model (BaseDataModel): The database model class representing the entity type being tested.
            model_type (str): A string identifier for the type of entity being tested. This corresponds to
                the key used in the `RedisBackend.stores` dictionary.
        """
        entity = mocker.MagicMock(spec=db_model)
        redis_backend_instance.save(entity)
        redis_backend_instance.stores[model_type].save.assert_called_once_with(entity)

    def test_save_invalid_entity(self, mocker: MockerFixture, redis_backend_instance: RedisBackend):
        """
        Test saving an invalid entity raises UnsupportedDataModelError.

        Args:
            mocker (MockerFixture): A built-in fixture from the pytest-mock library to create a Mock object.
            redis_backend_instance (RedisBackend): A fixture representing a `RedisBackend` instance with
                mocked Redis client and stores.
        """
        invalid_entity = mocker.MagicMock(spec=object)  # Not a subclass of BaseDataModel
        with pytest.raises(UnsupportedDataModelError, match="Unsupported data model of type"):
            redis_backend_instance.save(invalid_entity)

    @pytest.mark.parametrize(
        "db_model, model_type",
        [
            (StudyModel, "study"),
            (RunModel, "run"),
            (LogicalWorkerModel, "logical_worker"),
            (PhysicalWorkerModel, "physical_worker"),
        ],
    )
    def test_retrieve_valid_entity_by_id(
        self,
        mocker: MockerFixture,
        redis_backend_instance: RedisBackend,
        db_model: BaseDataModel,
        model_type: str,
    ):
        """
        Test retrieving a valid entity from the Redis store by ID.

        Args:
            mocker (MockerFixture): A built-in fixture from the pytest-mock library to create a Mock object.
            redis_backend_instance (RedisBackend): A fixture representing a `RedisBackend` instance with
                mocked Redis client and stores.
            db_model (BaseDataModel): The database model class representing the entity type being tested.
            model_type (str): A string identifier for the type of entity being tested. This corresponds to
                the key used in the `RedisBackend.stores` dictionary.
        """
        test_id = str(uuid.uuid4())
        redis_backend_instance.stores[model_type].retrieve.return_value = mocker.MagicMock(spec=db_model)
        entity = redis_backend_instance.retrieve(test_id, model_type)
        redis_backend_instance.stores[model_type].retrieve.assert_called_once_with(test_id)
        assert isinstance(entity, db_model), f"Retrieved entity should be of type {type(db_model)}."

    @pytest.mark.parametrize(
        "db_model, model_type",
        [(StudyModel, "study"), (PhysicalWorkerModel, "physical_worker")],
    )
    def test_retrieve_valid_entity_by_name(
        self,
        mocker: MockerFixture,
        redis_backend_instance: RedisBackend,
        db_model: BaseDataModel,
        model_type: str,
    ):
        """
        Test retrieving a valid entity from the Redis store by name. This test only applies to study and
        physical worker entities.

        Args:
            mocker (MockerFixture): A built-in fixture from the pytest-mock library to create a Mock object.
            redis_backend_instance (RedisBackend): A fixture representing a `RedisBackend` instance with
                mocked Redis client and stores.
            db_model (BaseDataModel): The database model class representing the entity type being tested.
            model_type (str): A string identifier for the type of entity being tested. This corresponds to
                the key used in the `RedisBackend.stores` dictionary.
        """
        test_name = "entity_name"
        redis_backend_instance.stores[model_type].retrieve.return_value = mocker.MagicMock(spec=db_model)
        entity = redis_backend_instance.retrieve(test_name, model_type)
        redis_backend_instance.stores[model_type].retrieve.assert_called_once_with(test_name, by_name=True)
        assert isinstance(entity, db_model), f"Retrieved entity should be of type {type(db_model)}."

    def test_retrieve_invalid_store_type(self, redis_backend_instance: RedisBackend):
        """
        Test retrieving from an invalid store type raises ValueError.

        Args:
            redis_backend_instance (RedisBackend): A fixture representing a `RedisBackend` instance with
                mocked Redis client and stores.
        """
        invalid_store_type = "invalid_store"
        with pytest.raises(ValueError, match=f"Invalid store type '{invalid_store_type}'."):
            redis_backend_instance.retrieve(str(uuid.uuid4()), invalid_store_type)

    @pytest.mark.parametrize(
        "db_model, model_type",
        [
            (StudyModel, "study"),
            (RunModel, "run"),
            (LogicalWorkerModel, "logical_worker"),
            (PhysicalWorkerModel, "physical_worker"),
        ],
    )
    def test_retrieve_all(
        self,
        mocker: MockerFixture,
        redis_backend_instance: RedisBackend,
        db_model: BaseDataModel,
        model_type: str,
    ):
        """
        Test retrieving all entities from a valid store.

        Args:
            mocker (MockerFixture): A built-in fixture from the pytest-mock library to create a Mock object.
            redis_backend_instance (RedisBackend): A fixture representing a `RedisBackend` instance with
                mocked Redis client and stores.
            db_model (BaseDataModel): The database model class representing the entity type being tested.
            model_type (str): A string identifier for the type of entity being tested. This corresponds to
                the key used in the `RedisBackend.stores` dictionary.
        """
        redis_backend_instance.stores[model_type].retrieve_all.return_value = [mocker.MagicMock(spec=db_model)]
        entities = redis_backend_instance.retrieve_all(model_type)
        redis_backend_instance.stores[model_type].retrieve_all.assert_called_once()
        assert len(entities) == 1, "Should retrieve one entity."
        assert isinstance(entities[0], db_model), f"Retrieved entity should be of type {type(db_model)}."

    @pytest.mark.parametrize("model_type", ["study", "run", "logical_worker", "physical_worker"])
    def test_delete_valid_entity_by_id(self, redis_backend_instance: RedisBackend, model_type: str):
        """
        Test deleting a valid entity from the Redis store by ID.

        Args:
            redis_backend_instance (RedisBackend): A fixture representing a `RedisBackend` instance with
                mocked Redis client and stores.
            model_type (str): A string identifier for the type of entity being tested. This corresponds to
                the key used in the `RedisBackend.stores` dictionary.
        """
        test_id = str(uuid.uuid4())
        redis_backend_instance.delete(test_id, model_type)
        redis_backend_instance.stores[model_type].delete.assert_called_once_with(test_id)

    @pytest.mark.parametrize("model_type", ["study", "physical_worker"])
    def test_delete_valid_entity_by_name(self, redis_backend_instance: RedisBackend, model_type: str):
        """
        Test deleting a valid entity from the Redis store by name.

        Args:
            redis_backend_instance (RedisBackend): A fixture representing a `RedisBackend` instance with
                mocked Redis client and stores.
            model_type (str): A string identifier for the type of entity being tested. This corresponds to
                the key used in the `RedisBackend.stores` dictionary.
        """
        test_name = "entity_name"
        redis_backend_instance.delete(test_name, model_type)
        redis_backend_instance.stores[model_type].delete.assert_called_once_with(test_name, by_name=True)

    def test_delete_invalid_store_type(self, redis_backend_instance: RedisBackend):
        """
        Test deleting from an invalid store type raises ValueError.

        Args:
            redis_backend_instance (RedisBackend): A fixture representing a `RedisBackend` instance with
                mocked Redis client and stores.
        """
        invalid_store_type = "invalid_store"
        with pytest.raises(ValueError, match=f"Invalid store type '{invalid_store_type}'."):
            redis_backend_instance.delete(str(uuid.uuid4()), invalid_store_type)
