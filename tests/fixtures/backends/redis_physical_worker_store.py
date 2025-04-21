"""
Fixtures for the `redis_logical_worker_store.py` module.
"""
from unittest.mock import MagicMock

import pytest

from merlin.backends.redis.redis_physical_worker_store import RedisPhysicalWorkerStore
from merlin.db_scripts.data_models import PhysicalWorkerModel
from tests.fixture_types import FixtureStr


@pytest.fixture
def redis_physical_worker_store_name() -> FixtureStr:
    """
    Fixture to provide the name of a physical worker.

    Returns:
        The name of the physical worker.
    """
    return "celery@worker1.%hostname"


@pytest.fixture
def redis_physical_worker_store_id() -> FixtureStr:
    """
    Fixture to provide the ID of a physical worker.

    Returns:
        The unique ID of the physical worker.
    """
    return "physical_worker_id"


@pytest.fixture
def redis_physical_worker_store_mock_worker(
    redis_physical_worker_store_id: FixtureStr,
    redis_physical_worker_store_name: FixtureStr,
) -> PhysicalWorkerModel:
    """
    Mocks a `PhysicalWorkerModel` instance.

    Args:
        redis_physical_worker_store_id (FixtureStr): The unique ID of the physical worker.
        redis_physical_worker_store_name (FixtureStr): The name of the physical worker.

    Returns:
        A mocked `PhysicalWorkerModel` instance.
    """
    return PhysicalWorkerModel(id=redis_physical_worker_store_id, name=redis_physical_worker_store_name)


@pytest.fixture
def redis_physical_worker_store_instance(redis_backend_mock_redis_client: MagicMock) -> RedisPhysicalWorkerStore:
    """
    Creates an instance of `RedisPhysicalWorkerStore` with a mocked Redis client.

    Args:
        redis_backend_mock_redis_client (MagicMock): The mocked Redis client.

    Returns:
        An instance of `RedisPhysicalWorkerStore`.
    """
    return RedisPhysicalWorkerStore(client=redis_backend_mock_redis_client)
