"""
Fixtures for the `redis_run-store.py` module.
"""
from unittest.mock import MagicMock

import pytest

from merlin.backends.redis.redis_run_store import RedisRunStore
from merlin.db_scripts.data_models import RunModel
from tests.fixture_types import FixtureStr


@pytest.fixture
def redis_run_store_id() -> FixtureStr:
    """
    Fixture to provide the ID of a run.

    Returns:
        The unique ID of the run.
    """
    return "run_id"


@pytest.fixture
def redis_run_store_mock_run(redis_run_store_id: FixtureStr) -> RunModel:
    """
    Mocks a `RunModel` instance.

    Args:
        mocker (MockerFixture): Used to patch dependencies.
        redis_run_store_id (FixtureStr): The unique ID of the run.
        redis_run_store_workspace (FixtureStr): The workspace of the run.

    Returns:
        A mocked `LogicalWorkerModel` instance.
    """
    return RunModel(id=redis_run_store_id)


@pytest.fixture
def redis_run_store_instance(redis_backend_mock_redis_client: MagicMock) -> RedisRunStore:
    """
    Creates an instance of `RedisRunStore` with a mocked Redis client.

    Args:
        redis_backend_mock_redis_client (MagicMock): The mocked Redis client.

    Returns:
        An instance of `RedisRunStore`.
    """
    return RedisRunStore(client=redis_backend_mock_redis_client)
