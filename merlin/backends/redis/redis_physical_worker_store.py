"""
Module for managing physical workers in a Redis database using the `RedisPhysicalWorkerStore` class.

This module provides functionality to save, retrieve, and delete physical workers stored in a Redis database.
It uses the [`PhysicalWorkerModel`][db_scripts.data_models.PhysicalWorkerModel] module to represent worker data.
"""

import logging
from typing import List

from redis import Redis

from merlin.backends.redis.redis_utils import create_data_class_entry, deserialize_data_class, update_data_class_entry
from merlin.db_scripts.data_models import PhysicalWorkerModel
from merlin.exceptions import WorkerNotFoundError


LOG = logging.getLogger("merlin")


class RedisPhysicalWorkerStore:
    """
    A Redis-based store for managing [`PhysicalWorkerModel`][db_scripts.data_models.PhysicalWorkerModel] objects.

    This class provides methods to save, retrieve and delete physical workers in a Redis database.
    Each worker is stored as a Redis hash, and a separate name-to-ID mapping is maintained for efficient lookups by name.

    Attributes:
        client (Redis): The Redis client used for database operations.

    Methods:
        save:
            Save or update a worker in the Redis database.
        retrieve:
            Retrieve a worker by ID or name.
        retrieve_all:
            Retrieve all workers stored in the Redis database.
        delete:
            Delete a worker by ID or name.
    """

    def __init__(self, client: Redis):
        """
        Initialize the `RedisPhysicalWorkerStore` with a Redis client.

        Args:
            client: A Redis client instance used to interact with the Redis database.
        """
        self.client: Redis = client
        self.key: str = "physical_worker"

    def save(self, worker: PhysicalWorkerModel):
        """
        Given a [`PhysicalWorkerModel`][db_scripts.data_models.PhysicalWorkerModel] object, enter
        all of it's information to the backend database.

        Args:
            worker: A [`PhysicalWorkerModel`][db_scripts.data_models.PhysicalWorkerModel] instance.
        """
        existing_worker_id = self.client.hget(f"{self.key}:name", worker.name)
        worker_key = f"{self.key}:{worker.id}"
        if existing_worker_id:
            LOG.info(f"Attempting to update worker with id '{worker.id}'...")
            update_data_class_entry(worker, worker_key, self.client)
            LOG.info(f"Successfully updated worker with id '{worker.id}'.")
        else:
            LOG.info("Creating a worker entry in Redis...")
            create_data_class_entry(worker, worker_key, self.client)
            self.client.hset(f"{self.key}:name", worker.name, worker.id)
            LOG.info(f"Successfully created a worker with id '{worker.id}' in Redis.")

    def retrieve(self, identifier: str, by_name: bool = False) -> PhysicalWorkerModel:
        """
        Retrieve a worker from the backend database, either by its ID or name.

        Args:
            identifier: The ID or name of the worker to retrieve.
            by_name: If True, interpret the identifier as a name. If False, interpret it as an ID.

        Returns:
            A [`PhysicalWorkerModel`][db_scripts.data_models.PhysicalWorkerModel] instance,
                or None if the worker does not exist in the database.
        """
        if by_name:
            # Retrieve the worker ID using the name-to-ID mapping
            worker_id = self.client.hget(f"{self.key}:name", identifier)
            if worker_id is None:
                return None
            worker_id = f"{self.key}:{worker_id}"
        else:
            # Ensure the worker ID has the correct prefix
            worker_id = identifier if identifier.startswith(f"{self.key}:") else f"{self.key}:{identifier}"

        # Check if the worker exists in Redis
        if worker_id is None or not self.client.exists(worker_id):
            return None

        # Retrieve the worker data from Redis and deserialize it
        data_from_redis = self.client.hgetall(worker_id)
        return deserialize_data_class(data_from_redis, PhysicalWorkerModel)

    def retrieve_all(self) -> List[PhysicalWorkerModel]:
        """
        Query the backend database for every worker that's currently stored.

        Returns:
            A list of [`WorkerModel`][db_scripts.data_models.WorkerModel] objects.
        """
        LOG.info("Retrieving all workers from Redis...")

        # Retrieve all worker ids
        worker_ids = self.client.keys(f"{self.key}:*")
        if not worker_ids:
            return None
        worker_ids.remove(f"{self.key}:name")
        LOG.debug(f"Found {len(worker_ids)} workers in Redis.")

        all_workers = []

        # Loop through each worker id and retrieve its WorkerModel
        for worker_id in worker_ids:
            try:
                worker_info = self.retrieve(worker_id)
                if worker_info:
                    all_workers.append(worker_info)
                else:
                    LOG.warning(f"Worker with ID '{worker_id}' could not be retrieved or does not exist.")
            except Exception as exc:  # pylint: disable=broad-except
                LOG.error(f"Error retrieving worker with ID '{worker_id}': {exc}")

        # Return the list of WorkerModel objects
        LOG.info(f"Successfully retrieved {len(all_workers)} workers from Redis.")
        return all_workers

    def delete(self, identifier: str, by_name: bool = False):
        """
        Given a worker id, find it in the database and remove that entry.

        Args:
            worker_id: The id of the worker to delete.
        """
        id_type = "name" if by_name else "id"
        LOG.info(f"Attempting to delete worker with {id_type} '{identifier}' from Redis...")

        # Retrieve the worker to ensure it exists and get its name
        worker = self.retrieve(identifier, by_name=by_name)
        if worker is None:
            raise WorkerNotFoundError(f"Worker with {id_type} '{identifier}' not found in the database.")

        # Delete the worker from the name index and Redis
        self.client.hdel(f"{self.key}:name", worker.name)
        self.client.delete(f"{self.key}:{worker.id}")

        LOG.info(f"Successfully deleted worker with {id_type} '{identifier}'.")
