##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Redis Store Implementations for Merlin Data Models

This module provides Redis-backed storage implementations for various Merlin system models.
These store classes offer persistence, retrieval, and management capabilities for core
system entities like studies, runs, and workers.

Each store implements a consistent interface through inheritance from RedisStoreBase,
with specialized functionality added through mixins (like NameMappingMixin for
name-based lookups). The stores handle serialization/deserialization, CRUD operations,
and maintain appropriate Redis key structures.

See also:
    - merlin.backends.redis.redis_base_store: Base classes and mixins
    - merlin.db_scripts.data_models: Data model definitions
"""

from redis import Redis

from merlin.backends.redis.redis_store_base import NameMappingMixin, RedisStoreBase
from merlin.db_scripts.data_models import LogicalWorkerModel, PhysicalWorkerModel, RunModel, StudyModel


class RedisLogicalWorkerStore(RedisStoreBase[LogicalWorkerModel]):
    """
    A Redis-based store for managing [`LogicalWorkerModel`][db_scripts.data_models.LogicalWorkerModel]
    objects.
    """

    def __init__(self, client: Redis):
        """
        Initialize the `RedisLogicalWorkerStore` with a Redis client.

        Args:
            client: A Redis client instance used to interact with the Redis database.
        """
        super().__init__(client, "logical_worker", LogicalWorkerModel)


class RedisPhysicalWorkerStore(NameMappingMixin, RedisStoreBase[PhysicalWorkerModel]):
    """
    A Redis-based store for managing [`PhysicalWorkerModel`][db_scripts.data_models.PhysicalWorkerModel]
    objects.
    """

    def __init__(self, client: Redis):
        """
        Initialize the `RedisPhysicalWorkerStore` with a Redis client.

        Args:
            client: A Redis client instance used to interact with the Redis database.
        """
        super().__init__(client, "physical_worker", PhysicalWorkerModel)


class RedisRunStore(NameMappingMixin, RedisStoreBase[RunModel]):
    """
    A Redis-based store for managing [`RunModel`][db_scripts.data_models.RunModel]
    objects.
    """

    def __init__(self, client: Redis):
        """
        Initialize the `RedisRunStore` with a Redis client.

        Args:
            client: A Redis client instance used to interact with the Redis database.
        """
        super().__init__(client, "run", RunModel)


class RedisStudyStore(NameMappingMixin, RedisStoreBase[StudyModel]):
    """
    A Redis-based store for managing [`StudyModel`][db_scripts.data_models.StudyModel]
    objects.
    """

    def __init__(self, client: Redis):
        """
        Initialize the `RedisStudyStore` with a Redis client.

        Args:
            client: A Redis client instance used to interact with the Redis database.
        """
        super().__init__(client, "study", StudyModel)
