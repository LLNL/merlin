"""
Tests for the `merlin/backends/redis/redis_logical_worker_store.py` module.
"""
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from merlin.backends.redis.redis_logical_worker_store import RedisLogicalWorkerStore
from merlin.db_scripts.data_models import LogicalWorkerModel
from merlin.exceptions import WorkerNotFoundError
from tests.fixture_types import FixtureList, FixtureStr


class TestRedisLogicalWorkerStore:
    """
    Test suite for the `RedisLogicalWorkerStore` class.

    This class contains unit tests to validate the functionality of the `RedisLogicalWorkerStore`, which is 
    responsible for managing logical worker data in a Redis database. The `RedisLogicalWorkerStore` provides 
    methods for saving, retrieving, updating, and deleting logical workers, as well as retrieving all workers.

    Fixtures and mocking are extensively used to isolate the tests from the actual Redis database, ensuring 
    that the tests focus on the behavior of the `RedisLogicalWorkerStore` class. Mock Redis clients and worker 
    models are used to simulate real-world scenarios without interacting with an actual database.

    These tests ensure the robustness and correctness of the `RedisLogicalWorkerStore` class, which is critical 
    for managing logical workers in the Merlin framework.
    """

    def test_save_new_worker(
        self,
        mocker: MockerFixture,
        redis_logical_worker_store_id: FixtureStr,
        redis_logical_worker_store_instance: RedisLogicalWorkerStore,
        redis_backend_mock_redis_client: MagicMock,
        redis_logical_worker_store_mock_worker: LogicalWorkerModel
    ):
        """
        Tests saving a new logical worker.

        Args:
            mocker (MockerFixture): Used to patch dependencies.
            redis_logical_worker_store_id (str): The unique ID of the logical worker.
            redis_logical_worker_store_instance (RedisLogicalWorkerStore): The store instance being tested.
            redis_backend_mock_redis_client (MagicMock): The mocked Redis client.
            redis_logical_worker_store_mock_worker (LogicalWorkerModel): The mocked logical worker model.
        """
        # Patch the functions that are called from within `save`
        mock_create_data_class_entry = mocker.patch("merlin.backends.redis.redis_logical_worker_store.create_data_class_entry")
        redis_backend_mock_redis_client.exists.return_value = False

        # Run the test
        redis_logical_worker_store_instance.save(redis_logical_worker_store_mock_worker)

        # Check that the Redis client's `exist` method is called once with the correct input
        redis_backend_mock_redis_client.exists.assert_called_once_with(
            f"{redis_logical_worker_store_instance.key}:{redis_logical_worker_store_id}"
        )

        # Check that the creation function is called once with the right arguments
        mock_create_data_class_entry.assert_called_once_with(
            redis_logical_worker_store_mock_worker,
            f"{redis_logical_worker_store_instance.key}:{redis_logical_worker_store_id}",
            redis_backend_mock_redis_client,
        )

    def test_save_existing_worker(
        self,
        mocker: MockerFixture,
        redis_logical_worker_store_id: FixtureStr,
        redis_logical_worker_store_instance: RedisLogicalWorkerStore,
        redis_backend_mock_redis_client: MagicMock,
        redis_logical_worker_store_mock_worker: LogicalWorkerModel
    ):
        """
        Test updating an existing logical worker.
        
        Args:
            mocker (MockerFixture): Used to patch dependencies.
            redis_logical_worker_store_id (str): The unique ID of the logical worker.
            redis_logical_worker_store_instance (RedisLogicalWorkerStore): The store instance being tested.
            redis_backend_mock_redis_client (MagicMock): The mocked Redis client.
            redis_logical_worker_store_mock_worker (LogicalWorkerModel): The mocked logical worker model.
        """
        # Patch the functions that are called from within `save`
        mock_update_data_class_entry = mocker.patch("merlin.backends.redis.redis_logical_worker_store.update_data_class_entry")
        redis_backend_mock_redis_client.exists.return_value = True

        # Run the test
        redis_logical_worker_store_instance.save(redis_logical_worker_store_mock_worker)

        # Check that the Redis client's `exist` method is called once with the correct input
        redis_backend_mock_redis_client.exists.assert_called_once_with(
            f"{redis_logical_worker_store_instance.key}:{redis_logical_worker_store_id}"
        )

        # Check that the update function is called once with the right arguments
        mock_update_data_class_entry.assert_called_once_with(
            redis_logical_worker_store_mock_worker,
            f"{redis_logical_worker_store_instance.key}:{redis_logical_worker_store_id}",
            redis_backend_mock_redis_client
        )

    def test_retrieve_existing_worker(
        self,
        mocker: MockerFixture,
        redis_logical_worker_store_id: FixtureStr,
        redis_logical_worker_store_name: FixtureStr,
        redis_logical_worker_store_queues: FixtureList[str],
        redis_logical_worker_store_instance: RedisLogicalWorkerStore,
        redis_backend_mock_redis_client: MagicMock,
        redis_logical_worker_store_mock_worker: LogicalWorkerModel,
    ):
        """
        Test retrieving an existing logical worker.
        
        Args:
            mocker (MockerFixture): Used to patch dependencies.
            redis_logical_worker_store_id (str): The unique ID of the logical worker.
            redis_logical_worker_store_name (FixtureStr): The name of the logical worker.
            redis_logical_worker_store_queues (FixtureList[str]): The queues of the logical worker.
            redis_logical_worker_store_instance (RedisLogicalWorkerStore): The store instance being tested.
            redis_backend_mock_redis_client (MagicMock): The mocked Redis client.
            redis_logical_worker_store_mock_worker (LogicalWorkerModel): The mocked logical worker model.
        """
        # Patch the functions/methods that are called within `retrieve`
        redis_backend_mock_redis_client.exists.return_value = True
        redis_backend_mock_redis_client.hgetall.return_value = {
            "id": redis_logical_worker_store_id,
            "name": redis_logical_worker_store_name,
            "queues": redis_logical_worker_store_queues,
        }
        mock_update_data_class_entry = mocker.patch(
            "merlin.backends.redis.redis_logical_worker_store.deserialize_data_class",
            return_value=redis_logical_worker_store_mock_worker
        )

        # Run the test
        worker = redis_logical_worker_store_instance.retrieve(redis_logical_worker_store_id)

        # Check that the worker that's retrieved is what we're expecting
        assert worker == redis_logical_worker_store_mock_worker

        # Check that every function we're expecting to be called is called with the correct parameters
        redis_backend_mock_redis_client.exists.assert_called_once_with(
            f"{redis_logical_worker_store_instance.key}:{redis_logical_worker_store_id}"
        )
        redis_backend_mock_redis_client.hgetall.assert_called_once_with(
            f"{redis_logical_worker_store_instance.key}:{redis_logical_worker_store_id}"
        )
        mock_update_data_class_entry.assert_called_once_with(
            redis_backend_mock_redis_client.hgetall.return_value, LogicalWorkerModel
        )

    def test_retrieve_nonexistent_worker(
        self,
        redis_logical_worker_store_instance: RedisLogicalWorkerStore,
        redis_backend_mock_redis_client: MagicMock,
    ):
        """
        Test retrieving a nonexisting logical worker.
        
        Args:
            redis_logical_worker_store_instance (RedisLogicalWorkerStore): The store instance being tested.
            redis_backend_mock_redis_client (MagicMock): The mocked Redis client.
        """
        nonexistent_id = "nonexistent"
        redis_backend_mock_redis_client.exists.return_value = False

        worker = redis_logical_worker_store_instance.retrieve(nonexistent_id)

        assert worker is None
        redis_backend_mock_redis_client.exists.assert_called_once_with(
            f"{redis_logical_worker_store_instance.key}:{nonexistent_id}"
        )

    def test_retrieve_all_workers(
        self,
        mocker: MockerFixture,
        redis_logical_worker_store_id: FixtureStr,
        redis_logical_worker_store_instance: RedisLogicalWorkerStore,
        redis_backend_mock_redis_client: MagicMock,
        redis_logical_worker_store_mock_worker: LogicalWorkerModel,
    ):
        """
        Test retrieving all logical workers.
        
        Args:
            mocker (MockerFixture): Used to patch dependencies.
            redis_logical_worker_store_id (str): The unique ID of the logical worker.
            redis_logical_worker_store_instance (RedisLogicalWorkerStore): The store instance being tested.
            redis_backend_mock_redis_client (MagicMock): The mocked Redis client.
            redis_logical_worker_store_mock_worker (LogicalWorkerModel): The mocked logical worker model.
        """
        existing_key = f"{redis_logical_worker_store_instance.key}:{redis_logical_worker_store_id}"
        nonexisting_key = f"{redis_logical_worker_store_instance.key}:nonexistent"

        # Make it so that the Redis client has an existing key and a nonexisting key
        redis_backend_mock_redis_client.scan_iter.return_value = [existing_key,nonexisting_key]
        redis_logical_worker_store_instance.retrieve = mocker.MagicMock(
            side_effect=[redis_logical_worker_store_mock_worker, None]
        )

        # Run the test
        workers = redis_logical_worker_store_instance.retrieve_all()

        # Make sure only the existing key is retrieved but both keys are queried
        assert len(workers) == 1
        redis_logical_worker_store_instance.retrieve.assert_any_call(existing_key)
        redis_logical_worker_store_instance.retrieve.assert_any_call(nonexisting_key)

    def test_delete_existing_worker(
        self,
        mocker: MockerFixture,
        redis_logical_worker_store_id: FixtureStr,
        redis_logical_worker_store_instance: RedisLogicalWorkerStore,
        redis_backend_mock_redis_client: MagicMock,
        redis_logical_worker_store_mock_worker: LogicalWorkerModel,
    ):
        """
        Test deleting an existing logical worker.
        
        Args:
            mocker (MockerFixture): Used to patch dependencies.
            redis_logical_worker_store_id (str): The unique ID of the logical worker.
            redis_logical_worker_store_instance (RedisLogicalWorkerStore): The store instance being tested.
            redis_backend_mock_redis_client (MagicMock): The mocked Redis client.
            redis_logical_worker_store_mock_worker (LogicalWorkerModel): The mocked logical worker model.
        """
        # Mock the retrieval return value
        redis_logical_worker_store_instance.retrieve = mocker.MagicMock(
            return_value=redis_logical_worker_store_mock_worker
        )

        # Run the test
        redis_logical_worker_store_instance.delete(redis_logical_worker_store_id)

        # Check that the correct methods are called with the right parameters
        redis_logical_worker_store_instance.retrieve.assert_called_once_with(redis_logical_worker_store_id)
        redis_backend_mock_redis_client.delete.assert_called_once_with(
            f"{redis_logical_worker_store_instance.key}:{redis_logical_worker_store_id}"
        )

    def test_delete_nonexistent_worker(
        self,
        mocker: MockerFixture,
        redis_logical_worker_store_id: FixtureStr,
        redis_logical_worker_store_instance: RedisLogicalWorkerStore,
        redis_backend_mock_redis_client: MagicMock,
    ):
        """
        Test deleting a non-existent logical worker.

        Args:
            mocker (MockerFixture): Used to patch dependencies.
            redis_logical_worker_store_id (str): The unique ID of the logical worker.
            redis_logical_worker_store_instance (RedisLogicalWorkerStore): The store instance being tested.
            redis_backend_mock_redis_client (MagicMock): The mocked Redis client.
        """
        # Mock the return value of the retrieval method so it returns None
        redis_logical_worker_store_instance.retrieve = mocker.MagicMock(return_value=None)

        # Check that the delete method raises a WorkerNotFoundError
        with pytest.raises(WorkerNotFoundError, match=f"Worker with id '{redis_logical_worker_store_id}' does not exist in the database."):
            redis_logical_worker_store_instance.delete(redis_logical_worker_store_id)

        # Check that the methods are called (or not) depending on what were expecting
        redis_logical_worker_store_instance.retrieve.assert_called_once_with(redis_logical_worker_store_id)
        redis_backend_mock_redis_client.delete.assert_not_called()
