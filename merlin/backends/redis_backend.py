"""
"""
import logging
from dataclasses import fields
from typing import Any, Dict, List

import json
from redis import Redis

from merlin.backends.results_backend import ResultsBackend
from merlin.config.results_backend import get_connection_string
from merlin.db_scripts.data_formats import BaseDataClass, RunInfo, StudyInfo

LOG = logging.getLogger("merlin")


# TODO might be able to use this in place of RedisConnectionManager for manager
# TODO might be able to make ResultsBackend classes replace the config/results_backend.py file
# - would help get a more OOP approach going within Merlin's codebase
# - instead of calling get_connection_string that logic could be handled in the base class?
class RedisBackend(ResultsBackend):
    """
    A Redis-based implementation of the `ResultsBackend` interface for storing and retrieving 
    studies and runs in a Redis database.

    Attributes:
        backend_name (str): The name of the backend (e.g., "redis").
        client: The Redis client used for database operations.

    Methods:
        _create_data_class_entry: Store a [`BaseDataClass`][merlin.db_scripts.data_formats.BaseDataClass] 
            instance as a hash in the Redis database.
        _deserialize_data_class: Convert data retrieved from Redis into a 
            [`BaseDataClass`][merlin.db_scripts.data_formats.BaseDataClass] instance.
        _serialize_data_class: Convert a [`BaseDataClass`][merlin.db_scripts.data_formats.BaseDataClass] 
            instance into a format that Redis can interpret.
        _update_run: Update an existing run entry in the Redis database.
        _update_study: Update an existing study entry in the Redis database.
        retrieve_all_studies: Retrieve all studies currently stored in the Redis database.
        retrieve_run: Retrieve a [`RunInfo`][merlin.db_scripts.data_formats.RunInfo] object 
            from the Redis database by its ID.
        retrieve_study: Retrieve a [`StudyInfo`][merlin.db_scripts.data_formats.StudyInfo] object 
            from the Redis database by its name.
        save_run: Save a [`RunInfo`][merlin.db_scripts.data_formats.RunInfo] object to the Redis database.
        save_study: Save a [`StudyInfo`][merlin.db_scripts.data_formats.StudyInfo] object to the Redis database.
    """

    def __init__(self, backend_name: str):
        super().__init__(backend_name)
        self.client: Redis = Redis.from_url(url=get_connection_string(), decode_responses=True)

    def _serialize_data_class(self, data_class: BaseDataClass) -> Dict[str, str]:
        """
        Given a [`BaseDataClass`][merlin.db_scripts.data_formats.BaseDataClass] instance,
        convert it's data into a format that the Redis database can interpret.

        Args:
            data_class: A [`BaseDataClass`][merlin.db_scripts.data_formats.BaseDataClass] instance.

        Returns:
            A dictionary of information that Redis can interpret.
        """
        LOG.info("Deserializing data from Redis...")
        serialized_data = {}

        for field in data_class.fields():
            field_value = getattr(data_class, field.name)
            if isinstance(field_value, (list, dict)):
                serialized_data[field.name] = json.dumps(field_value)
            elif field_value is None:
                serialized_data[field.name] = "null"
            else:
                serialized_data[field.name] = str(field_value)

        LOG.info("Successfully deserialized data.")
        return serialized_data

    def _deserialize_data_class(self, retrieval_data: Dict[str, str], data_class: BaseDataClass) -> BaseDataClass:
        """
        Given data that was retrieved by Redis, convert it into a data_class instance.

        Args:
            retrieval_data: The data retrieved by Redis that we need to deserialize.
            data_class: A [`BaseDataClass`][merlin.db_scripts.data_formats.BaseDataClass] object.

        Returns:
            A [`BaseDataClass`][merlin.db_scripts.data_formats.BaseDataClass] instance.
        """
        LOG.info("Deserializing data from Redis...")
        deserialized_data = {}

        for key, val in retrieval_data.items():
            if val.startswith("[") or val.startswith("{"):
                deserialized_data[key] = json.loads(val)
            elif val == "null":
                deserialized_data[key] = None
            elif val == "True" or val == "False":
                deserialized_data[key] = True if val == "True" else False
            elif val.isdigit():
                deserialized_data[key] = float(val)
            else:
                deserialized_data[key] = str(val)

        LOG.info("Successfully deserialized data.")
        return data_class.from_dict(deserialized_data)

    def _create_data_class_entry(self, data_class: BaseDataClass, key: str):
        """
        Given a [`BaseDataClass`][merlin.db_scripts.data_formats.BaseDataClass] instance and
        a key, store the contents of the `data_class` as a hash at `key` in the Redis database.

        Args:
            data_class: A [`BaseDataClass`][merlin.db_scripts.data_formats.BaseDataClass] instance.
            key: The location to store the contents of `data_class` in the Redis database.
        """
        serialized_data = self._serialize_data_class(data_class)
        self.client.hset(key, mapping=serialized_data)

    def _update_study(self, updated_study: StudyInfo):
        """
        Update an existing study entry in the Redis database with new information.

        This method retrieves the existing study data from Redis, merges the runs from the 
        existing study with the new runs (avoiding duplicates), and updates the study entry 
        in the database.

        Args:
            updated_study: A [`StudyInfo`][merlin.db_scripts.data_formats.StudyInfo] object 
                containing the updated study information.
        """
        LOG.info("Updating an existing study entry in Redis...")
        # Study exists, retrieve the existing data
        existing_study_key = f"study:{updated_study.id}"
        existing_study_data = self.client.hgetall(existing_study_key)

        # Merge existing runs with new runs (avoiding duplicates)
        existing_runs = json.loads(existing_study_data.get("runs", []))
        updated_runs = list(set(existing_runs + updated_study.runs))

        # Update the study data
        updated_study_info = StudyInfo(
            id=updated_study.id,
            name=updated_study.name,
            runs=updated_runs,
        )
        updated_study_data = self._serialize_data_class(updated_study_info)
        self.client.hset(existing_study_key, mapping=updated_study_data)
        LOG.info(f"Successfully updated study '{updated_study.name}' with id '{updated_study.id}'.")

    def save_study(self, study: StudyInfo):
        """
        Given a StudyInfo object, enter all of it's information to the Redis database.

        Args:
            study: A [`StudyInfo`][merlin.db_scripts.data_formats.StudyInfo] instance.
        """
        existing_study_id = self.client.hget("study:name", study.name)

        # Study already exists so update the existing study
        if existing_study_id:
            LOG.debug(f"existing_study_id: {existing_study_id}.")
            LOG.debug(f"study.id: {study.id}")
            if study.id != existing_study_id:
                raise ValueError(
                    f"ID mismatch. StudyInfo contains ID '{study.id}' but ID in database is '{existing_study_id}'." \
                    "When the StudyInfo object was created, did you load it in with DatabaseStudy.load?"
                )
            self._update_study(study)
        # Study does not yet exist so create a new study entry
        else:
            LOG.info("Creating a study entry in Redis...")
            self._create_data_class_entry(study, f"study:{study.id}")
            self.client.hset("study:name", study.name, study.id)
            LOG.info("Successfully created a study entry.")

        LOG.info(f"Study with name '{study.name}' saved to Redis under id '{study.id}'.")

    def retrieve_study(self, study_name: str) -> StudyInfo:
        """
        Given a study's name, retrieve it from the Redis database.

        Args:
            study_name: The name of the study to retrieve.
            
        Returns:
            A [`StudyInfo`][merlin.db_scripts.data_formats.StudyInfo] instance
                or None if the study does not yet exist in the database.
        """
        study_id = self.client.hget("study:name", study_name)

        if study_id is None:
            return None

        data_from_redis = self.client.hgetall(f"study:{study_id}")
        return self._deserialize_data_class(data_from_redis, StudyInfo)   

    def retrieve_all_studies(self) -> List[StudyInfo]:
        """
        Query the Redis database for every study that's currently stored.

        Returns:
            A list of [`StudyInfo`][merlin.db_scripts.data_formats.StudyInfo] objects.
        """
        LOG.info("Fetching all studies from Redis...")

        # Retrieve all study names
        study_names = self.client.hkeys("study:name")
        LOG.debug(f"Found {len(study_names)} studies in Redis.")

        all_studies = []

        # Loop through each study name and retrieve its StudyInfo
        for study_name in study_names:
            try:
                # Use the existing retrieve_study method to get the StudyInfo object
                study_info = self.retrieve_study(study_name)
                if study_info:
                    all_studies.append(study_info)
                else:
                    LOG.warning(f"Study '{study_name}' could not be retrieved or does not exist.")
            except Exception as e:
                LOG.error(f"Error retrieving study '{study_name}': {e}")

        # Return the list of StudyInfo objects
        LOG.info(f"Successfully retrieved {len(all_studies)} studies from Redis.")
        return all_studies

    def _update_run(self, updated_run: RunInfo):
        """
        Update an existing run entry in the Redis database with new information.

        This method retrieves the existing run data from Redis, updates its fields with the 
        new data provided in the `updated_run` object, and saves the updated run back to the database.

        Args:
            updated_run: A [`RunInfo`][merlin.db_scripts.data_formats.RunInfo] object 
                containing the updated run information.
        """
        # Get the existing data from Redis and convert it to a RunInfo object
        run_key = f"run:{updated_run.id}"
        existing_data = self.client.hgetall(run_key)
        existing_run = self._deserialize_data_class(existing_data, RunInfo)

        # Update the fields in the existing run and save it to Redis
        existing_run.update_fields(updated_run.to_dict())
        updated_run_data = self._serialize_data_class(existing_run)
        self.client.hset(run_key, mapping=updated_run_data)

        LOG.info(f"Successfully updated run with id '{updated_run.id}'.")
        
    def save_run(self, run: RunInfo):
        """
        Given a RunInfo object, enter all of it's information to the backend database.

        Args:
            run: A [`RunInfo`][merlin.db_scripts.data_formats.RunInfo] instance.
        """
        run_key = f"run:{run.id}"
        if self.client.exists(run_key):
            self._update_run(run)
        else:
            self._create_data_class_entry(run, run_key)

    def retrieve_run(self, run_id: str) -> RunInfo:
        """
        Given a run's id, retrieve it from the Redis database.

        Args:
            run_id: The id of a run to retrieve.
        """
        run_key = f"run:{run_id}"
        if not self.client.exists(run_key):
            return None

        data_from_redis = self.client.hgetall(run_key)
        return self._deserialize_data_class(data_from_redis, RunInfo)   
