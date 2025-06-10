"""
Tests for the `merlin/backends/redis/redis_stores.py` module.
"""

from merlin.backends.redis.redis_stores import (
    RedisLogicalWorkerStore,
    RedisPhysicalWorkerStore,
    RedisRunStore,
    RedisStudyStore,
)
from merlin.db_scripts.data_models import LogicalWorkerModel, PhysicalWorkerModel, RunModel, StudyModel
from tests.fixture_types import FixtureRedis


class TestRedisLogicalWorkerStore:
    """Tests for the RedisLogicalWorkerStore implementation."""

    def test_initialization(self, mock_redis: FixtureRedis):
        """
        Test that the store initializes with the correct parameters.

        Args:
            mock_redis: A fixture providing a mocked Redis client.
        """
        store = RedisLogicalWorkerStore(mock_redis)

        assert store.client == mock_redis
        assert store.key == "logical_worker"
        assert store.model_class == LogicalWorkerModel


class TestRedisPhysicalWorkerStore:
    """Tests for the RedisPhysicalWorkerStore implementation."""

    def test_initialization(self, mock_redis: FixtureRedis):
        """
        Test that the store initializes with the correct parameters.

        Args:
            mock_redis: A fixture providing a mocked Redis client.
        """
        store = RedisPhysicalWorkerStore(mock_redis)

        assert store.client == mock_redis
        assert store.key == "physical_worker"
        assert store.model_class == PhysicalWorkerModel


class TestRedisRunStore:
    """Tests for the RedisRunStore implementation."""

    def test_initialization(self, mock_redis: FixtureRedis):
        """
        Test that the store initializes with the correct parameters.

        Args:
            mock_redis: A fixture providing a mocked Redis client.
        """
        store = RedisRunStore(mock_redis)

        assert store.client == mock_redis
        assert store.key == "run"
        assert store.model_class == RunModel


class TestRedisStudyStore:
    """Tests for the RedisStudyStore implementation."""

    def test_initialization(self, mock_redis: FixtureRedis):
        """
        Test that the store initializes with the correct parameters.

        Args:
            mock_redis: A fixture providing a mocked Redis client.
        """
        store = RedisStudyStore(mock_redis)

        assert store.client == mock_redis
        assert store.key == "study"
        assert store.model_class == StudyModel
