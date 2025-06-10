##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Redis backend implementation for the Merlin application.

This module provides a concrete implementation of the `ResultsBackend` interface using Redis
as the underlying database. It defines the `RedisBackend` class, which manages interactions
with Redis-specific store classes for different data models, including CRUD operations and
database flushing.
"""

import logging

from redis import Redis

from merlin.backends.redis.redis_stores import (
    RedisLogicalWorkerStore,
    RedisPhysicalWorkerStore,
    RedisRunStore,
    RedisStudyStore,
)
from merlin.backends.results_backend import ResultsBackend


LOG = logging.getLogger("merlin")


# TODO might be able to make ResultsBackend classes replace the config/results_backend.py file
# - would help get a more OOP approach going within Merlin's codebase
# - instead of calling get_connection_string that logic could be handled in the base class?
class RedisBackend(ResultsBackend):
    """
    A Redis-based implementation of the `ResultsBackend` interface for interacting with a Redis database.

    Attributes:
        backend_name (str): The name of the backend (e.g., "redis").
        client (Redis): The Redis client used for database operations.

    Methods:
        get_version:
            Query Redis for the current version.

        get_connection_string:
            Retrieve the connection string used to connect to Redis.

        flush_database:
            Remove every entry in the Redis database.

        save:
            Save a `BaseDataModel` entity to the Redis database.

        retrieve:
            Retrieve an entity from the appropriate store based on the given query identifier and store type.

        retrieve_all:
            Retrieve all entities from the specified store.

        delete:
            Delete an entity from the specified store.
    """

    def __init__(self, backend_name: str):
        """
        Initialize the `RedisBackend` instance, setting up the Redis client connection and store mappings.

        Args:
            backend_name (str): The name of the backend (e.g., "redis").
        """
        from merlin.config.configfile import CONFIG  # pylint: disable=import-outside-toplevel
        from merlin.config.results_backend import get_connection_string  # pylint: disable=import-outside-toplevel

        # Get the Redis client connection
        redis_config = {"url": get_connection_string(), "decode_responses": True}
        if CONFIG.results_backend.name == "rediss":
            redis_config.update({"ssl_cert_reqs": getattr(CONFIG.results_backend, "cert_reqs", "required")})
        self.client: Redis = Redis.from_url(**redis_config)

        # Create instances of each store in our database
        stores = {
            "study": RedisStudyStore(self.client),
            "run": RedisRunStore(self.client),
            "logical_worker": RedisLogicalWorkerStore(self.client),
            "physical_worker": RedisPhysicalWorkerStore(self.client),
        }

        super().__init__(backend_name, stores)

    def get_version(self) -> str:
        """
        Query the Redis backend for the current version.

        Returns:
            A string representing the current version of Redis.
        """
        client_info = self.client.info()
        return client_info.get("redis_version", "N/A")

    def flush_database(self):
        """
        Remove everything stored in Redis.
        """
        self.client.flushdb()
