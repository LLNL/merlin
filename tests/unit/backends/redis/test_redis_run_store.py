"""
Tests for the `merlin/backends/redis/redis_run_store.py` module.
"""
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from merlin.backends.redis.redis_run_store import RedisRunStore
from merlin.db_scripts.data_models import RunModel
from merlin.exceptions import RunNotFoundError
from tests.fixture_types import FixtureStr

class TestRedisRunStore:
    """
    Test suite for the `RedisRunStore` class.

    This class contains unit tests to validate the functionality of the `RedisRunStore`, which is 
    responsible for managing run data in a Redis database. The `RedisRunStore` provides methods for
    saving, retrieving, updating, and deleting runs, as well as retrieving all runs.

    Fixtures and mocking are extensively used to isolate the tests from the actual Redis database, ensuring 
    that the tests focus on the behavior of the `RedisRunStore` class. Mock Redis clients and run 
    models are used to simulate real-world scenarios without interacting with an actual database.

    These tests ensure the robustness and correctness of the `RedisRunStore` class, which is critical 
    for managing runs in the Merlin framework.
    """

    def test_save_new_run(
        self,
        mocker: MockerFixture,
        redis_run_store_id: FixtureStr,
        redis_run_store_instance: RedisRunStore,
        redis_backend_mock_redis_client: MagicMock,
        redis_run_store_mock_run: RunModel
    ):
        """
        Tests saving a new run.

        Args:
            mocker (MockerFixture): Used to patch dependencies.
            redis_run_store_id (str): The unique ID of the run.
            redis_run_store_instance (RedisRunStore): The store instance being tested.
            redis_backend_mock_redis_client (MagicMock): The mocked Redis client.
            redis_run_store_mock_run (RunModel): The mocked run model.
        """
        # Patch the functions that are called from within `save`
        mock_create_data_class_entry = mocker.patch("merlin.backends.redis.redis_run_store.create_data_class_entry")
        redis_backend_mock_redis_client.exists.return_value = False

        # Run the test
        redis_run_store_instance.save(redis_run_store_mock_run)

        # Check that the Redis client's `exists` method is called once with the correct input
        redis_backend_mock_redis_client.exists.assert_called_once_with(
            f"{redis_run_store_instance.key}:{redis_run_store_id}"
        )

        # Check that the creation function is called once with the right arguments
        mock_create_data_class_entry.assert_called_once_with(
            redis_run_store_mock_run,
            f"{redis_run_store_instance.key}:{redis_run_store_id}",
            redis_backend_mock_redis_client,
        )

    def test_save_existing_run(
        self,
        mocker: MockerFixture,
        redis_run_store_id: FixtureStr,
        redis_run_store_instance: RedisRunStore,
        redis_backend_mock_redis_client: MagicMock,
        redis_run_store_mock_run: RunModel
    ):
        """
        Test updating an existing run.

        Args:
            mocker (MockerFixture): Used to patch dependencies.
            redis_run_store_id (str): The unique ID of the run.
            redis_run_store_instance (RedisRunStore): The store instance being tested.
            redis_backend_mock_redis_client (MagicMock): The mocked Redis client.
            redis_run_store_mock_run (RunModel): The mocked run model.
        """
        # Patch the functions that are called from within `save`
        mock_update_data_class_entry = mocker.patch("merlin.backends.redis.redis_run_store.update_data_class_entry")
        redis_backend_mock_redis_client.exists.return_value = True

        # Run the test
        redis_run_store_instance.save(redis_run_store_mock_run)

        # Check that the Redis client's `exists` method is called once with the correct input
        redis_backend_mock_redis_client.exists.assert_called_once_with(
            f"{redis_run_store_instance.key}:{redis_run_store_id}"
        )

        # Check that the update function is called once with the right arguments
        mock_update_data_class_entry.assert_called_once_with(
            redis_run_store_mock_run,
            f"{redis_run_store_instance.key}:{redis_run_store_id}",
            redis_backend_mock_redis_client
        )

    def test_retrieve_existing_run(
        self,
        mocker: MockerFixture,
        redis_run_store_id: FixtureStr,
        redis_run_store_instance: RedisRunStore,
        redis_backend_mock_redis_client: MagicMock,
        redis_run_store_mock_run: RunModel,
    ):
        """
        Test retrieving an existing run.

        Args:
            mocker (MockerFixture): Used to patch dependencies.
            redis_run_store_id (str): The unique ID of the run.
            redis_run_store_instance (RedisRunStore): The store instance being tested.
            redis_backend_mock_redis_client (MagicMock): The mocked Redis client.
            redis_run_store_mock_run (RunModel): The mocked run model.
        """
        # Patch the functions that are called from within `retrieve`
        redis_backend_mock_redis_client.exists.return_value = True
        redis_backend_mock_redis_client.hgetall.return_value = {
            "id": redis_run_store_id,
            "study_id": "study_1",
            "workspace": "run_workspace",
        }
        mock_deserialize_data_class = mocker.patch(
            "merlin.backends.redis.redis_run_store.deserialize_data_class",
            return_value=redis_run_store_mock_run
        )

        # Run the test
        run = redis_run_store_instance.retrieve(redis_run_store_id)

        # Check that the worker that's retrieved is what we're expecting
        assert run == redis_run_store_mock_run

        # Check that every function we're expecting to be called is called with the correct parameters
        redis_backend_mock_redis_client.exists.assert_called_once_with(
            f"{redis_run_store_instance.key}:{redis_run_store_id}"
        )
        redis_backend_mock_redis_client.hgetall.assert_called_once_with(
            f"{redis_run_store_instance.key}:{redis_run_store_id}"
        )
        mock_deserialize_data_class.assert_called_once_with(
            redis_backend_mock_redis_client.hgetall.return_value, RunModel
        )

    def test_retrieve_nonexistent_run(
        self,
        redis_run_store_instance: RedisRunStore,
        redis_backend_mock_redis_client: MagicMock,
    ):
        """
        Test retrieving a nonexisting run.

        Args:
            redis_run_store_instance (RedisRunStore): The store instance being tested.
            redis_backend_mock_redis_client (MagicMock): The mocked Redis client.
        """
        nonexistent_id = "nonexistent"
        redis_backend_mock_redis_client.exists.return_value = False

        run = redis_run_store_instance.retrieve(nonexistent_id)

        assert run is None
        redis_backend_mock_redis_client.exists.assert_called_once_with(
            f"{redis_run_store_instance.key}:{nonexistent_id}"
        )

    def test_retrieve_all_runs(
        self,
        mocker: MockerFixture,
        redis_run_store_id: FixtureStr,
        redis_run_store_instance: RedisRunStore,
        redis_backend_mock_redis_client: MagicMock,
        redis_run_store_mock_run: RunModel,
    ):
        """
        Test retrieving all runs.

        Args:
            mocker (MockerFixture): Used to patch dependencies.
            redis_run_store_id (str): The unique ID of the run.
            redis_run_store_instance (RedisRunStore): The store instance being tested.
            redis_backend_mock_redis_client (MagicMock): The mocked Redis client.
            redis_run_store_mock_run (RunModel): The mocked run model.
        """
        existing_key = f"{redis_run_store_instance.key}:{redis_run_store_id}"
        nonexisting_key = f"{redis_run_store_instance.key}:nonexistent"

        # Make it so that the Redis client has an existing key and a nonexisting key
        redis_backend_mock_redis_client.scan_iter.return_value = [existing_key, nonexisting_key]
        redis_run_store_instance.retrieve = mocker.MagicMock(
            side_effect=[redis_run_store_mock_run, None]
        )

        # Run the test
        runs = redis_run_store_instance.retrieve_all()

        # Make sure only the existing key is retrieved but both keys are queried
        assert len(runs) == 1
        redis_run_store_instance.retrieve.assert_any_call(existing_key)
        redis_run_store_instance.retrieve.assert_any_call(nonexisting_key)

    def test_delete_existing_run(
        self,
        mocker: MockerFixture,
        redis_run_store_id: FixtureStr,
        redis_run_store_instance: RedisRunStore,
        redis_backend_mock_redis_client: MagicMock,
        redis_run_store_mock_run: RunModel,
    ):
        """
        Test deleting an existing run.

        Args:
            mocker (MockerFixture): Used to patch dependencies.
            redis_run_store_id (str): The unique ID of the run.
            redis_run_store_instance (RedisRunStore): The store instance being tested.
            redis_backend_mock_redis_client (MagicMock): The mocked Redis client.
            redis_run_store_mock_run (RunModel): The mocked run model.
        """
        # Mock the retrieval return value
        redis_run_store_instance.retrieve = mocker.MagicMock(
            return_value=redis_run_store_mock_run
        )

        # Run the test
        redis_run_store_instance.delete(redis_run_store_id)

        # Check that the correct methods are called with the right parameters
        redis_run_store_instance.retrieve.assert_called_once_with(redis_run_store_id)
        redis_backend_mock_redis_client.delete.assert_called_once_with(
            f"{redis_run_store_instance.key}:{redis_run_store_id}"
        )

    def test_delete_nonexistent_run(
        self,
        mocker: MockerFixture,
        redis_run_store_id: FixtureStr,
        redis_run_store_instance: RedisRunStore,
        redis_backend_mock_redis_client: MagicMock,
    ):
        """
        Test deleting a nonexisting run.

        Args:
            mocker (MockerFixture): Used to patch dependencies.
            redis_run_store_id (str): The unique ID of the run.
            redis_run_store_instance (RedisRunStore): The store instance being tested.
            redis_backend_mock_redis_client (MagicMock): The mocked Redis client.
        """
        # Mock the return value of the retrieval method so it returns None
        redis_run_store_instance.retrieve = mocker.MagicMock(return_value=None)

        # Check that the delete method raises an appropriate error
        with pytest.raises(RunNotFoundError, match=f"Run with id '{redis_run_store_id}' does not exist in the database."):
            redis_run_store_instance.delete(redis_run_store_id)

        redis_run_store_instance.retrieve.assert_called_once_with(redis_run_store_id)
        redis_backend_mock_redis_client.delete.assert_not_called()
