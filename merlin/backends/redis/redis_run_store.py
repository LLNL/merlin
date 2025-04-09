"""
Module for managing runs in a Redis database using the `RedisRunStore` class.

This module provides functionality to save, retrieve, and delete runs stored in a Redis database.
It uses the [`RunModel`][db_scripts.data_models.RunModel] module to represent run data.
"""

import logging
from typing import List

from redis import Redis

from merlin.backends.redis.redis_utils import create_data_class_entry, deserialize_data_class, update_data_class_entry
from merlin.db_scripts.data_models import RunModel


LOG = logging.getLogger("merlin")


class RedisRunStore:
    """
    A Redis-based store for managing [`RunModel`][db_scripts.data_models.RunModel] objects.

    This class provides methods to save, retrieve, and delete runs in a Redis database.
    Each run is stored as a Redis hash, and runs are associated with studies via their `study_id`.

    Attributes:
        client (Redis): The Redis client used for database operations.

    Methods:
        save:
            Save or update a run in the Redis database.
        retrieve:
            Retrieve a run by its ID.
        retrieve_all:
            Retrieve all runs stored in the Redis database.
        delete:
            Delete a run by its ID and update the associated study's run list.
    """

    def __init__(self, client: Redis):
        """
        Initialize the `RedisRunStore` with a Redis client.

        Args:
            client: A Redis client instance used to interact with the Redis database.
        """
        self.client: Redis = client
        self.key: str = "run"

    def save(self, run: RunModel):
        """
        Given a `RunModel` object, enter all of it's information to the backend database.

        Args:
            run: A [`RunModel`][db_scripts.data_models.RunModel] instance.
        """
        run_key = f"{self.key}:{run.id}"
        if self.client.exists(run_key):
            LOG.info(f"Attempting to update run with id '{run.id}'...")
            update_data_class_entry(run, run_key, self.client)
            LOG.info(f"Successfully updated run with id '{run.id}'.")
        else:
            LOG.info("Creating a run entry in Redis...")
            create_data_class_entry(run, run_key, self.client)
            LOG.info(f"Successfully created a run with id '{run.id}' in Redis.")

    def retrieve(self, run_id: str) -> RunModel:
        """
        Given a run's id, retrieve it from the Redis database.

        Args:
            run_id: The id of a run to retrieve.
        """
        run_key = run_id if run_id.startswith(f"{self.key}:") else f"{self.key}:{run_id}"
        if not self.client.exists(run_key):
            return None

        data_from_redis = self.client.hgetall(run_key)
        return deserialize_data_class(data_from_redis, RunModel)

    def retrieve_all(self) -> List[RunModel]:
        """
        Query the Redis database for every run that's currently stored.

        Returns:
            A list of [`RunModel`][db_scripts.data_models.RunModel] objects.
        """
        LOG.info("Fetching all runs from Redis...")

        run_pattern = f"{self.key}:*"
        all_runs = []

        # Loop through all runs
        for run_key in self.client.scan_iter(match=run_pattern):
            run_id = run_key.split(":")[1]  # Extract the ID
            try:
                run_info = self.retrieve(run_id)
                if run_info:
                    all_runs.append(run_info)
                else:
                    # Shouldn't hit this since we're looping with scan
                    LOG.warning(f"Run with id '{run_id}' could not be retrieved or does not exist.")
            except Exception as exc:  # pylint: disable=broad-except
                LOG.error(f"Error retrieving run with id '{run_id}': {exc}")

        LOG.info(f"Successfully retrieved {len(all_runs)} runs from Redis.")
        return all_runs

    def delete(self, run_id: str):
        """
        Given a run id, find it in the database and remove that entry. This will also
        delete the run id from the list of runs in the associated study's entry.

        Args:
            run_id: The id of the run to delete.
        """
        LOG.info(f"Attempting to delete run with id '{run_id}' from Redis...")
        run = self.retrieve(run_id)
        if run is None:
            raise ValueError(f"Run with id '{run_id}' does not exist in the database.")

        # Delete the run's hash entry
        run_key = f"{self.key}:{run.id}"
        LOG.debug(f"Deleting run hash with key '{run_key}'...")
        self.client.delete(run_key)
        LOG.debug("Successfully removed run hash from Redis.")

        LOG.info(f"Successfully deleted run '{run_id}' and all associated data from Redis.")
