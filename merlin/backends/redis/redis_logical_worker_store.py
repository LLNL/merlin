"""
Module for managing logical workers in a Redis database using the `RedisLogicalWorkerStore` class.

This module provides functionality to save, retrieve, and delete logical workers stored in a Redis database.
It uses the [`LogicalWorkerModel`][db_scripts.data_models.LogicalWorkerModel] module to represent
logical worker data.
"""

import logging
from typing import List

from redis import Redis

from merlin.backends.redis.redis_utils import create_data_class_entry, deserialize_data_class, update_data_class_entry
from merlin.db_scripts.data_models import LogicalWorkerModel
from merlin.exceptions import WorkerNotFoundError


LOG = logging.getLogger("merlin")


class RedisLogicalWorkerStore:
    """
    A Redis-based store for managing [`LogicalWorkerModel`][db_scripts.data_models.LogicalWorkerModel]
    objects.

    This class provides methods to save, retrieve, and delete logical workers in a Redis database.
    Each logical worker is stored as a Redis hash, and logical workers are associated with runs
    and physical workers. Each logical worker entry is unique to its name and queues.

    Attributes:
        client (Redis): The Redis client used for database operations.

    Methods:
        save:
            Save or update a logical worker in the Redis database.
        retrieve:
            Retrieve a logical worker by ID.
        retrieve_all:
            Retrieve all logical workers stored in the Redis database.
        delete:
            Delete a logical worker by ID.
    """

    def __init__(self, client: Redis):
        """
        Initialize the `RedisLogicalWorkerStore` with a Redis client.

        Args:
            client: A Redis client instance used to interact with the Redis database.
        """
        self.client: Redis = client
        self.key = "logical_worker"

    def save(self, worker: LogicalWorkerModel):
        """
        Given a [`LogicalWorkerModel`][db_scripts.data_models.LogicalWorkerModel] object, enter
        all of it's information to the backend database.

        Args:
            worker: A [`LogicalWorkerModel`][db_scripts.data_models.LogicalWorkerModel] instance.
        """
        worker_key = f"{self.key}:{worker.id}"
        if self.client.exists(worker_key):
            LOG.info(f"Attempting to update logical worker with id '{worker.id}'...")
            update_data_class_entry(worker, worker_key, self.client)
            LOG.info(f"Successfully updated logical worker with id '{worker.id}'.")
        else:
            LOG.info("Creating a logical worker entry in Redis...")
            create_data_class_entry(worker, worker_key, self.client)
            LOG.info(f"Successfully created a logical worker with id '{worker.id}' in Redis.")

    def retrieve(self, worker_id: str) -> LogicalWorkerModel:
        """
        Given a worker's id, retrieve it from the backend database.

        Args:
            worker_id: The ID of the worker to retrieve.

        Returns:
            A [`LogicalWorkerModel`][db_scripts.data_models.LogicalWorkerModel] instance.
        """
        worker_key = f"{self.key}:{worker_id}"
        worker_key = worker_id if worker_id.startswith(f"{self.key}:") else f"{self.key}:{worker_id}"
        if not self.client.exists(worker_key):
            return None

        data_from_redis = self.client.hgetall(worker_key)
        return deserialize_data_class(data_from_redis, LogicalWorkerModel)

    def retrieve_all(self) -> List[LogicalWorkerModel]:
        """
        Query the backend database for every logical worker that's currently stored.

        Returns:
            A list of [`LogicalWorkerModel`][db_scripts.data_models.LogicalWorkerModel] objects.
        """
        LOG.info("Fetching all logical workers from Redis...")

        pattern = f"{self.key}:*"
        all_logical_workers = []

        # Loop through all runs
        for key in self.client.scan_iter(match=pattern):
            logical_worker_id = key.split(":")[1]  # Extract the ID for logging
            try:
                worker_info = self.retrieve(key)
                if worker_info:
                    all_logical_workers.append(worker_info)
                else:
                    # Shouldn't hit this since we're looping with scan
                    LOG.warning(f"Logical worker with id '{logical_worker_id}' could not be retrieved or does not exist.")
            except Exception as exc:  # pylint: disable=broad-except
                LOG.error(f"Error retrieving logical worker with id '{logical_worker_id}': {exc}")

        LOG.info(f"Successfully retrieved {len(all_logical_workers)} logical workers from Redis.")
        return all_logical_workers

    def delete(self, worker_id: str):
        """
        Given a worker id, find it in the database and remove that entry.

        Args:
            worker_id: The id of the worker to delete.
        """
        LOG.info(f"Attempting to delete run with id '{worker_id}' from Redis...")
        worker = self.retrieve(worker_id)
        if worker is None:
            raise WorkerNotFoundError(f"Worker with id '{worker_id}' does not exist in the database.")

        # Delete the worker's hash entry
        worker_key = f"{self.key}:{worker.id}"
        LOG.debug(f"Deleting logical worker hash with key '{worker_key}'...")
        self.client.delete(worker_key)
        LOG.debug("Successfully removed logical worker hash from Redis.")

        LOG.info(f"Successfully deleted logical worker '{worker_id}' and all associated data from Redis.")
