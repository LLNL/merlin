"""
Module for managing studies in a Redis database using the `RedisStudyStore` class.

This module provides functionality to save, retrieve, and delete studies stored in a Redis database.
It uses the [`StudyModel`][db_scripts.data_models.StudyModel] object to represent study data and
provides helper functions for serialization and deserialization of study objects.
"""

import logging
from typing import List

from redis import Redis

from merlin.backends.redis.redis_utils import create_data_class_entry, deserialize_data_class, update_data_class_entry
from merlin.db_scripts.data_models import StudyModel


LOG = logging.getLogger("merlin")


class RedisStudyStore:
    """
    A Redis-based store for managing [`StudyModel`][db_scripts.data_models.StudyModel] objects.

    This class provides methods to save, retrieve, and delete studies in a Redis database.
    Each study is stored as a Redis hash, and a separate name-to-ID mapping is maintained for
    efficient lookups by name.

    Attributes:
        client (Redis): The Redis client used for database operations.

    Methods:
        save:
            Save or update a study in the Redis database.
        retrieve:
            Retrieve a study by ID or name.
        retrieve_all:
            Retrieve all studies stored in the Redis database.
        delete:
            Delete a study by ID or name.
    """

    def __init__(self, client: Redis):
        """
        Initialize the `RedisStudyStore` with a Redis client.

        Args:
            client: A Redis client instance used to interact with the Redis database.
        """
        self.client: Redis = client
        self.key: str = "study"

    def save(self, study: StudyModel):
        """
        Given a `StudyModel` object, enter all of it's information to the Redis database.
        If a study with `study.id` already exists, this will override everything in that
        entry.

        Args:
            study: A [`StudyModel`][db_scripts.data_models.StudyModel] instance.
        """
        existing_study_id = self.client.hget(f"{self.key}:name", study.name)

        # Study already exists so update the existing study
        if existing_study_id:
            LOG.debug(f"existing_study_id: {existing_study_id}.")
            LOG.debug(f"study.id: {study.id}")
            LOG.info(f"Attempting to update study with id '{study.id}'...")
            update_data_class_entry(study, f"{self.key}:{study.id}", self.client)
            LOG.info(f"Successfully updated study with id '{study.id}'.")
        # Study does not yet exist so create a new study entry
        else:
            LOG.info("Creating a study entry in Redis...")
            create_data_class_entry(study, f"{self.key}:{study.id}", self.client)
            self.client.hset(f"{self.key}:name", study.name, study.id)
            LOG.info(f"Successfully created a study with id '{study.id}' in Redis.")

        LOG.info(f"Study with name '{study.name}' saved to Redis under id '{study.id}'.")

    def retrieve(self, identifier: str, by_name: bool = False) -> StudyModel:
        """
        Retrieve a study from the Redis database, either by its ID or name.

        Args:
            identifier: The ID or name of the study to retrieve.
            by_name: If True, interpret the identifier as a name. If False, interpret it as an ID.

        Returns:
            A [`StudyModel`][db_scripts.data_models.StudyModel] instance
                or None if the study does not yet exist in the database.
        """
        if by_name:
            # Retrieve the study ID using the name-to-ID mapping
            study_id = self.client.hget(f"{self.key}:name", identifier)
            if study_id is None:
                return None
            study_id = f"{self.key}:{study_id}"
        else:
            # Ensure the study ID has the correct prefix
            study_id = identifier if identifier.startswith(f"{self.key}:") else f"{self.key}:{identifier}"

        # Check if the study exists in Redis
        if study_id is None or not self.client.exists(study_id):
            return None

        # Retrieve the study data from Redis and deserialize it
        data_from_redis = self.client.hgetall(study_id)
        return deserialize_data_class(data_from_redis, StudyModel)

    def retrieve_all(self) -> List[StudyModel]:
        """
        Query the Redis database for every study that's currently stored.

        Returns:
            A list of [`StudyModel`][db_scripts.data_models.StudyModel] objects.
        """
        LOG.info("Retrieving all studies from Redis...")

        # Retrieve all study ids
        study_ids = self.client.keys(f"{self.key}:*")
        if not study_ids:
            return None
        study_ids.remove(f"{self.key}:name")
        LOG.debug(f"Found {len(study_ids)} studies in Redis.")

        all_studies = []

        # Loop through each study id and retrieve its StudyModel
        for study_id in study_ids:
            try:
                study_info = self.retrieve(study_id)
                if study_info:
                    all_studies.append(study_info)
                else:
                    LOG.warning(f"Study with ID '{study_id}' could not be retrieved or does not exist.")
            except Exception as exc:  # pylint: disable=broad-except
                LOG.error(f"Error retrieving study with ID '{study_id}': {exc}")

        # Return the list of StudyModel objects
        LOG.info(f"Successfully retrieved {len(all_studies)} studies from Redis.")
        return all_studies

    def delete(self, identifier: str, by_name: bool = False):
        """
        Delete a study from the database, either by its ID or name.

        Args:
            identifier: The ID or name of the study to delete.
            by_name: If True, interpret the identifier as a name. If False, interpret it as an ID.
        """
        id_type = "name" if by_name else "id"
        LOG.info(f"Attempting to delete study with {id_type} '{identifier}' from Redis...")

        # Retrive by name or id
        study = self.retrieve(identifier, by_name=by_name)
        if study is None:
            raise ValueError(f"Study with {id_type} '{identifier}' does not exist in the database.")

        # Delete the study's hash entry
        study_key = f"{self.key}:{study.id}"
        LOG.info(f"Deleting study hash with key '{study_key}'...")
        self.client.delete(study_key)

        # Remove the study's name-to-ID mapping
        LOG.info(f"Removing study name-to-ID mapping for '{study.name}'...")
        self.client.hdel(f"{self.key}:name", study.name)

        LOG.info(f"Successfully deleted study with {id_type} '{identifier}' and all associated data from Redis.")
