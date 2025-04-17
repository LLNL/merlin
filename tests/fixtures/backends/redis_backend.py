"""
Fixtures for the `redis_backend.py` module.
"""
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture
from redis import Redis

from merlin.backends.redis.redis_backend import RedisBackend
from tests.fixture_types import FixtureModification, FixtureStr


@pytest.fixture
def redis_backend_connection_string() -> FixtureStr:
    """
    Fixture to provide a mock Redis connection string.

    This fixture returns a Redis connection string that can be used in tests to simulate
    connecting to a Redis server. It ensures that tests relying on a Redis connection
    string do not require a real Redis instance and remain isolated.

    Returns:
        A mock Redis connection string.
    """
    return "redis://localhost:6379"


@pytest.fixture
def redis_backend_mock_redis_client(mocker: MockerFixture) -> MagicMock:
    """
    Mocks the Redis client.

    Args:
        mocker (MockerFixture): Used to create a mock Redis client.

    Returns:
        A mocked Redis client.
    """
    return mocker.MagicMock(spec=Redis)


@pytest.fixture
def redis_backend_instance(
    mocker: MockerFixture,
    redis_results_backend_config_class: FixtureModification,
    redis_backend_connection_string: FixtureStr,
    redis_backend_mock_redis_client: MagicMock,
) -> RedisBackend:
    """
    Fixture to create a `RedisBackend` instance with mocked dependencies.
    
    This fixture sets up a `RedisBackend` instance with its Redis client and store mappings mocked,
    allowing tests to run without requiring an actual Redis server. It uses the `mocker` library 
    to patch external dependencies such as the Redis client, connection string retrieval, and 
    configuration settings.

    Args:
        mocker (MockerFixture): The pytest-mock fixture used for mocking objects and patching 
            external dependencies.
        redis_results_backend_config_class (FixtureModification): A fixture that sets the `CONFIG`
            object to point to a Redis backend.

    Returns:
        A `RedisBackend` instance with mocked Redis client and stores.
    """
    # Mock the `info()` return value
    redis_backend_mock_redis_client.info.return_value = {"redis_version": "6.2.5"}

    # Patch Redis.from_url to return the mock client
    mocker.patch("merlin.backends.redis.redis_backend.Redis.from_url", return_value=redis_backend_mock_redis_client)

    # Patch the connection string retrieval
    mocker.patch("merlin.config.results_backend.get_connection_string", return_value=redis_backend_connection_string)

    # Initialize RedisBackend
    backend = RedisBackend("redis")

    # Override the client and stores with mocked objects
    backend.client = redis_backend_mock_redis_client
    backend.stores = {
        "study": mocker.MagicMock(),
        "run": mocker.MagicMock(),
        "logical_worker": mocker.MagicMock(),
        "physical_worker": mocker.MagicMock(),
    }

    return backend
