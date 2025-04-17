"""
"""
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture
from redis import Redis

from merlin.backends.redis.redis_logical_worker_store import RedisLogicalWorkerStore
from merlin.db_scripts.data_models import LogicalWorkerModel
from tests.fixture_types import FixtureList, FixtureStr


@pytest.fixture
def redis_logical_worker_store_name() -> FixtureStr:
    """
    Fixture to provide the name of a logical worker.

    Returns:
        The name of the logical worker.
    """
    return "worker1"


@pytest.fixture
def redis_logical_worker_store_queues() -> FixtureList[str]:
    """
    Fixture to provide the queues of a logical worker.

    Returns:
        A list of queue names assigned to the logical worker.
    """
    return ["queue1", "queue2"]


@pytest.fixture
def redis_logical_worker_store_id(
) -> FixtureStr:
    """
    Fixture to provide the ID of a logical worker.

    Returns:
        The unique ID of the logical worker.
    """
    return "logical_worker_id"


@pytest.fixture
def redis_logical_worker_store_mock_worker(
    mocker: MockerFixture,
    redis_logical_worker_store_id: FixtureStr,
    redis_logical_worker_store_name: FixtureStr,
    redis_logical_worker_store_queues: FixtureList[str],
) -> LogicalWorkerModel:
    """
    Mocks a `LogicalWorkerModel` instance.

    Args:
        mocker (MockerFixture): Used to patch dependencies.
        redis_logical_worker_store_id (FixtureStr): The unique ID of the logical worker.
        redis_logical_worker_store_name (FixtureStr): The name of the logical worker.
        redis_logical_worker_store_queues (FixtureList[str]): The queues assigned to the logical worker.

    Returns:
        A mocked `LogicalWorkerModel` instance.
    """
    mocker.patch("merlin.db_scripts.data_models.LogicalWorkerModel.generate_id", return_value=redis_logical_worker_store_id)
    return LogicalWorkerModel(
        id=redis_logical_worker_store_id, name=redis_logical_worker_store_name, queues=redis_logical_worker_store_queues
    )


@pytest.fixture
def redis_logical_worker_store_mock_redis_client(mocker: MockerFixture) -> MagicMock:
    """
    Mocks the Redis client.

    Args:
        mocker (MockerFixture): Used to create a mock Redis client.

    Returns:
        A mocked Redis client.
    """
    return mocker.MagicMock(spec=Redis)


@pytest.fixture
def redis_logical_worker_store_instance(redis_logical_worker_store_mock_redis_client) -> RedisLogicalWorkerStore:
    """
    Creates an instance of `RedisLogicalWorkerStore` with a mocked Redis client.

    Args:
        redis_logical_worker_store_mock_redis_client (MagicMock): The mocked Redis client.

    Returns:
        An instance of `RedisLogicalWorkerStore`.
    """
    return RedisLogicalWorkerStore(client=redis_logical_worker_store_mock_redis_client)
