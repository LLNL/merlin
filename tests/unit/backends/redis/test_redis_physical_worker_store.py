"""
Tests for the `merlin/backends/redis/redis_physical_worker_store.py` module.
"""
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from merlin.backends.redis.redis_physical_worker_store import RedisPhysicalWorkerStore
from merlin.db_scripts.data_models import PhysicalWorkerModel
from merlin.exceptions import WorkerNotFoundError
from tests.fixture_types import FixtureStr

class TestRedisPhysicalWorkerStore:
    """
    Test suite for the `RedisPhysicalWorkerStore` class.

    This class contains unit tests to validate the functionality of the `RedisPhysicalWorkerStore`, which is 
    responsible for managing physical worker data in a Redis database. The `RedisPhysicalWorkerStore` provides 
    methods for saving, retrieving, updating, and deleting physical workers, as well as retrieving all workers.

    Fixtures and mocking are extensively used to isolate the tests from the actual Redis database, ensuring 
    that the tests focus on the behavior of the `RedisPhysicalWorkerStore` class. Mock Redis clients and worker 
    models are used to simulate real-world scenarios without interacting with an actual database.

    These tests ensure the robustness and correctness of the `RedisPhysicalWorkerStore` class, which is critical 
    for managing physical workers in the Merlin framework.
    """

    def test_save_new_worker(
        self,
        mocker: MockerFixture,
        redis_physical_worker_store_id: FixtureStr,
        redis_physical_worker_store_name: FixtureStr,
        redis_physical_worker_store_instance: RedisPhysicalWorkerStore,
        redis_backend_mock_redis_client: MagicMock,
        redis_physical_worker_store_mock_worker: PhysicalWorkerModel
    ):
        """
        Tests saving a new physical worker.

        Args:
            mocker (MockerFixture): Used to patch dependencies.
            redis_physical_worker_store_id (FixtureStr): The unique ID of the physical worker.
            redis_physical_worker_store_name (FixtureStr): The name of the physical worker.
            redis_physical_worker_store_instance (RedisPhysicalWorkerStore): The store instance being tested.
            redis_backend_mock_redis_client (MagicMock): The mocked Redis client.
            redis_physical_worker_store_mock_worker (PhysicalWorkerModel): The mocked physical worker model.
        """
        # Patch the functions that are called from within `save`
        mock_create_data_class_entry = mocker.patch("merlin.backends.redis.redis_physical_worker_store.create_data_class_entry")
        redis_backend_mock_redis_client.hget.return_value = None

        # Run the test
        redis_physical_worker_store_instance.save(redis_physical_worker_store_mock_worker)

        # Check that the Redis client's `hget` method is called once with the correct input
        redis_backend_mock_redis_client.hget.assert_called_once_with(
            f"{redis_physical_worker_store_instance.key}:name", redis_physical_worker_store_name
        )

        # Check that the creation function is called once with the right arguments
        mock_create_data_class_entry.assert_called_once_with(
            redis_physical_worker_store_mock_worker,
            f"{redis_physical_worker_store_instance.key}:{redis_physical_worker_store_id}",
            redis_backend_mock_redis_client,
        )
        redis_backend_mock_redis_client.hset.assert_called_once_with(
            f"{redis_physical_worker_store_instance.key}:name",
            redis_physical_worker_store_name,
            redis_physical_worker_store_mock_worker.id,
        )

    def test_save_existing_worker(
        self,
        mocker: MockerFixture,
        redis_physical_worker_store_id: FixtureStr,
        redis_physical_worker_store_name: FixtureStr,
        redis_physical_worker_store_instance: RedisPhysicalWorkerStore,
        redis_backend_mock_redis_client: MagicMock,
        redis_physical_worker_store_mock_worker: PhysicalWorkerModel
    ):
        """
        Test updating an existing physical worker.

        Args:
            mocker (MockerFixture): Used to patch dependencies.
            redis_physical_worker_store_id (FixtureStr): The unique ID of the physical worker.
            redis_physical_worker_store_name (FixtureStr): The name of the physical worker.
            redis_physical_worker_store_instance (RedisPhysicalWorkerStore): The store instance being tested.
            redis_backend_mock_redis_client (MagicMock): The mocked Redis client.
            redis_physical_worker_store_mock_worker (PhysicalWorkerModel): The mocked physical worker model.
        """
        # Patch the functions that are called from within `save`
        mock_update_data_class_entry = mocker.patch("merlin.backends.redis.redis_physical_worker_store.update_data_class_entry")
        redis_backend_mock_redis_client.hget.return_value = redis_physical_worker_store_id

        # Run the test
        redis_physical_worker_store_instance.save(redis_physical_worker_store_mock_worker)

        # Check that the Redis client's `hget` method is called once with the correct input
        redis_backend_mock_redis_client.hget.assert_called_once_with(
            f"{redis_physical_worker_store_instance.key}:name", redis_physical_worker_store_name
        )

        # Check that the update function is called once with the right arguments
        mock_update_data_class_entry.assert_called_once_with(
            redis_physical_worker_store_mock_worker,
            f"{redis_physical_worker_store_instance.key}:{redis_physical_worker_store_id}",
            redis_backend_mock_redis_client
        )

    def test_retrieve_existing_worker_by_id(
        self,
        mocker: MockerFixture,
        redis_physical_worker_store_id: FixtureStr,
        redis_physical_worker_store_name: FixtureStr,
        redis_physical_worker_store_instance: RedisPhysicalWorkerStore,
        redis_backend_mock_redis_client: MagicMock,
        redis_physical_worker_store_mock_worker: PhysicalWorkerModel,
    ):
        """
        Test retrieving an existing worker by ID.

        Args:
            mocker (MockerFixture): Used to patch dependencies.
            redis_physical_worker_store_id (FixtureStr): The unique ID of the physical worker.
            redis_physical_worker_store_name (FixtureStr): The name of the physical worker.
            redis_physical_worker_store_instance (RedisPhysicalWorkerStore): The store instance being tested.
            redis_backend_mock_redis_client (MagicMock): The mocked Redis client.
            redis_physical_worker_store_mock_worker (PhysicalWorkerModel): The mocked physical worker model.
        """
        # Patch the functions that are called from within `retrieve`
        redis_backend_mock_redis_client.exists.return_value = True
        redis_backend_mock_redis_client.hgetall.return_value = {
            "id": redis_physical_worker_store_id,
            "name": redis_physical_worker_store_name,
        }
        mock_deserialize_data_class = mocker.patch(
            "merlin.backends.redis.redis_physical_worker_store.deserialize_data_class",
            return_value=redis_physical_worker_store_mock_worker
        )

        # Run the test
        worker = redis_physical_worker_store_instance.retrieve(redis_physical_worker_store_id)

        # Check that the worker that's retrieved is what we're expecting
        assert worker == redis_physical_worker_store_mock_worker

        # Check that every function we're expecting to be called is called with the correct parameters
        redis_backend_mock_redis_client.exists.assert_called_once_with(
            f"{redis_physical_worker_store_instance.key}:{redis_physical_worker_store_id}"
        )
        redis_backend_mock_redis_client.hgetall.assert_called_once_with(
            f"{redis_physical_worker_store_instance.key}:{redis_physical_worker_store_id}"
        )
        mock_deserialize_data_class.assert_called_once_with(
            redis_backend_mock_redis_client.hgetall.return_value, PhysicalWorkerModel
        )

    def test_retrieve_existing_worker_by_name(
        self,
        mocker: MockerFixture,
        redis_physical_worker_store_id: FixtureStr,
        redis_physical_worker_store_name: FixtureStr,
        redis_physical_worker_store_instance: RedisPhysicalWorkerStore,
        redis_backend_mock_redis_client: MagicMock,
        redis_physical_worker_store_mock_worker: PhysicalWorkerModel,
    ):
        """
        Test retrieving an existing worker by name.

        Args:
            mocker (MockerFixture): Used to patch dependencies.
            redis_physical_worker_store_id (FixtureStr): The unique ID of the physical worker.
            redis_physical_worker_store_name (FixtureStr): The name of the physical worker.
            redis_physical_worker_store_instance (RedisPhysicalWorkerStore): The store instance being tested.
            redis_backend_mock_redis_client (MagicMock): The mocked Redis client.
            redis_physical_worker_store_mock_worker (PhysicalWorkerModel): The mocked physical worker model.
        """
        # Patch the functions that are called from within `retrieve`
        redis_backend_mock_redis_client.hget.return_value = redis_physical_worker_store_id
        redis_backend_mock_redis_client.exists.return_value = True
        redis_backend_mock_redis_client.hgetall.return_value = {
            "id": redis_physical_worker_store_id,
            "name": redis_physical_worker_store_name,
        }
        mock_deserialize_data_class = mocker.patch(
            "merlin.backends.redis.redis_physical_worker_store.deserialize_data_class",
            return_value=redis_physical_worker_store_mock_worker
        )

        # Run the test
        worker = redis_physical_worker_store_instance.retrieve(redis_physical_worker_store_name, by_name=True)

        # Check that the worker that's retrieved is what we're expecting
        assert worker == redis_physical_worker_store_mock_worker

        # Check that every function we're expecting to be called is called with the correct parameters
        redis_backend_mock_redis_client.hget.assert_called_once_with(
            f"{redis_physical_worker_store_instance.key}:name", redis_physical_worker_store_name
        )
        redis_backend_mock_redis_client.exists.assert_called_once_with(
            f"{redis_physical_worker_store_instance.key}:{redis_physical_worker_store_id}"
        )
        redis_backend_mock_redis_client.hgetall.assert_called_once_with(
            f"{redis_physical_worker_store_instance.key}:{redis_physical_worker_store_id}"
        )
        mock_deserialize_data_class.assert_called_once_with(
            redis_backend_mock_redis_client.hgetall.return_value, PhysicalWorkerModel
        )

    def test_retrieve_nonexistent_worker(
        self,
        redis_physical_worker_store_instance: RedisPhysicalWorkerStore,
        redis_backend_mock_redis_client: MagicMock,
    ):
        """
        Test retrieving a nonexisting logical worker.
        
        Args:
            redis_physical_worker_store_instance (RedisLogicalWorkerStore): The store instance being tested.
            redis_backend_mock_redis_client (MagicMock): The mocked Redis client.
        """
        nonexistent_id = "nonexistent"
        redis_backend_mock_redis_client.exists.return_value = False

        worker = redis_physical_worker_store_instance.retrieve(nonexistent_id)

        assert worker is None
        redis_backend_mock_redis_client.exists.assert_called_once_with(
            f"{redis_physical_worker_store_instance.key}:{nonexistent_id}"
        )

    def test_retrieve_all_workers(
        self,
        mocker: MockerFixture,
        redis_physical_worker_store_id: FixtureStr,
        redis_physical_worker_store_instance: RedisPhysicalWorkerStore,
        redis_backend_mock_redis_client: MagicMock,
        redis_physical_worker_store_mock_worker: PhysicalWorkerModel,
    ):
        """
        Test retrieving an existing worker by name.

        Args:
            mocker (MockerFixture): Used to patch dependencies.
            redis_physical_worker_store_id (FixtureStr): The unique ID of the physical worker.
            redis_physical_worker_store_instance (RedisPhysicalWorkerStore): The store instance being tested.
            redis_backend_mock_redis_client (MagicMock): The mocked Redis client.
            redis_physical_worker_store_mock_worker (PhysicalWorkerModel): The mocked physical worker model.
        """
        existing_key = f"{redis_physical_worker_store_instance.key}:{redis_physical_worker_store_id}"
        nonexisting_key = f"{redis_physical_worker_store_instance.key}:nonexistent"

        # Make it so that the Redis client has an existing key and a nonexisting key
        redis_backend_mock_redis_client.keys.return_value = [
            f"{redis_physical_worker_store_instance.key}:name", existing_key, nonexisting_key
        ]
        redis_physical_worker_store_instance.retrieve = mocker.MagicMock(
            side_effect=[redis_physical_worker_store_mock_worker, None]
        )

        # Run the test
        workers = redis_physical_worker_store_instance.retrieve_all()

        # Make sure only the existing key is retrieved but both keys are queried
        assert len(workers) == 1
        redis_physical_worker_store_instance.retrieve.assert_any_call(existing_key)
        redis_physical_worker_store_instance.retrieve.assert_any_call(nonexisting_key)

    def test_delete_existing_worker(
        self,
        mocker: MockerFixture,
        redis_physical_worker_store_id: FixtureStr,
        redis_physical_worker_store_name: FixtureStr,
        redis_physical_worker_store_instance: RedisPhysicalWorkerStore,
        redis_backend_mock_redis_client: MagicMock,
        redis_physical_worker_store_mock_worker: PhysicalWorkerModel,
    ):
        """
        Test deleting an existing physical worker.

        Args:
            mocker (MockerFixture): Used to patch dependencies.
            redis_physical_worker_store_id (FixtureStr): The unique ID of the physical worker.
            redis_physical_worker_store_name (FixtureStr): The name of the physical worker.
            redis_physical_worker_store_instance (RedisPhysicalWorkerStore): The store instance being tested.
            redis_backend_mock_redis_client (MagicMock): The mocked Redis client.
            redis_physical_worker_store_mock_worker (PhysicalWorkerModel): The mocked physical worker model.
        """
        # Mock the retrieval return value
        redis_physical_worker_store_instance.retrieve = mocker.MagicMock(
            return_value=redis_physical_worker_store_mock_worker
        )

        # Run the test
        redis_physical_worker_store_instance.delete(redis_physical_worker_store_id)

        # Check that the correct methods are called with the right parameters
        redis_physical_worker_store_instance.retrieve.assert_called_once_with(redis_physical_worker_store_id, by_name=False)
        redis_backend_mock_redis_client.hdel.assert_called_once_with(
            f"{redis_physical_worker_store_instance.key}:name", redis_physical_worker_store_name
        )
        redis_backend_mock_redis_client.delete.assert_called_once_with(
            f"{redis_physical_worker_store_instance.key}:{redis_physical_worker_store_id}"
        )

    def test_delete_nonexistent_worker(
        self,
        mocker: MockerFixture,
        redis_physical_worker_store_id: FixtureStr,
        redis_physical_worker_store_instance: RedisPhysicalWorkerStore,
        redis_backend_mock_redis_client: MagicMock,
    ):
        """
        Test deleting a nonexisting physical worker.

        Args:
            mocker (MockerFixture): Used to patch dependencies.
            redis_physical_worker_store_id (FixtureStr): The unique ID of the physical worker.
            redis_physical_worker_store_instance (RedisPhysicalWorkerStore): The store instance being tested.
            redis_backend_mock_redis_client (MagicMock): The mocked Redis client.
        """
        # Mock the return value of the retrieval method so it returns None
        redis_physical_worker_store_instance.retrieve = mocker.MagicMock(return_value=None)

        # Check that the delete method raises a WorkerNotFoundError
        with pytest.raises(WorkerNotFoundError, match=f"Worker with id '{redis_physical_worker_store_id}' not found in the database."):
            redis_physical_worker_store_instance.delete(redis_physical_worker_store_id)

        redis_physical_worker_store_instance.retrieve.assert_called_once_with(redis_physical_worker_store_id, by_name=False)
        redis_backend_mock_redis_client.delete.assert_not_called()
