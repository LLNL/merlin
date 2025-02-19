"""
"""
import logging
from dataclasses import fields
from typing import Any, Dict, List

import json
from redis import Redis

from merlin.backends.results_backend import ResultsBackend
from merlin.config.results_backend import get_backend_password
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
        _create_data_class_entry:
            Store a [`BaseDataClass`][merlin.db_scripts.data_formats.BaseDataClass] instance
            as a hash in the Redis database.

        _update_data_class_entry:
            Update an existing data class entry in the Redis database with new information.

        _serialize_data_class:
            Convert a [`BaseDataClass`][merlin.db_scripts.data_formats.BaseDataClass] instance
            into a format that Redis can interpret.

        _deserialize_data_class:
            Convert data retrieved from Redis into a
            [`BaseDataClass`][merlin.db_scripts.data_formats.BaseDataClass] instance.

        save_study:
            Save a [`StudyInfo`][merlin.db_scripts.data_formats.StudyInfo] object to the Redis database.
            If a study with the same name exists, it will update the existing entry.

        retrieve_study:
            Retrieve a [`StudyInfo`][merlin.db_scripts.data_formats.StudyInfo] object from the Redis
            database by its name.

        retrieve_all_studies:
            Retrieve all studies currently stored in the Redis database.

        delete_study:
            Delete a study from the Redis database by its name. Optionally, remove all associated runs.

        save_run:
            Save a [`RunInfo`][merlin.db_scripts.data_formats.RunInfo] object to the Redis database. If a run
            with the same ID exists, it will update the existing entry.

        retrieve_run:
            Retrieve a [`RunInfo`][merlin.db_scripts.data_formats.RunInfo] object from the Redis database
            by its ID.

        retrieve_all_runs:
            Retrieve all runs currently stored in the Redis database.

        delete_run:
            Delete a run from the Redis database by its ID. This will also remove the run from the associated
            study's list of runs.
    """

    def __init__(self, backend_name: str):
        super().__init__(backend_name)
        # TODO have this database use a different db number than Celery does
        # - do we want a new database for each type of information? i.e. one for studies, one for runs, etc.?
        from merlin.config.configfile import CONFIG  # pylint: disable=import-outside-toplevel

        password_file = CONFIG.results_backend.password if hasattr(CONFIG.results_backend, "password") else None
        server = CONFIG.results_backend.server if hasattr(CONFIG.results_backend, "server") else None
        port = CONFIG.results_backend.port if hasattr(CONFIG.results_backend, "port") else None
        results_db_num = CONFIG.results_backend.db_num if hasattr(CONFIG.results_backend, "db_num") else None
        username = CONFIG.results_backend.username if hasattr(CONFIG.results_backend, "username") else None
        has_ssl = hasattr(CONFIG.results_backend, "cert_reqs")
        ssl_cert_reqs = CONFIG.results_backend.cert_reqs if has_ssl else "required"

        password = None
        if password_file is not None:
            try:
                password = get_backend_password(password_file)
            except IOError:
                if hasattr(CONFIG.results_backend, "password"):
                    password = CONFIG.results_backend.password

        self.client: Redis = Redis(
            host=server,
            port=port,
            db=results_db_num,
            username=username,
            password=password,
            decode_responses=True,
            ssl=has_ssl,
            ssl_cert_reqs=ssl_cert_reqs,
        )

    def _serialize_data_class(self, data_class: BaseDataClass) -> Dict[str, str]:
        """
        Given a [`BaseDataClass`][merlin.db_scripts.data_formats.BaseDataClass] instance,
        convert it's data into a format that the Redis database can interpret.

        Args:
            data_class: A [`BaseDataClass`][merlin.db_scripts.data_formats.BaseDataClass] instance.

        Returns:
            A dictionary of information that Redis can interpret.
        """
        LOG.debug("Deserializing data from Redis...")
        serialized_data = {}

        for field in data_class.fields():
            field_value = getattr(data_class, field.name)
            if isinstance(field_value, (list, dict)):
                serialized_data[field.name] = json.dumps(field_value)
            elif field_value is None:
                serialized_data[field.name] = "null"
            else:
                serialized_data[field.name] = str(field_value)

        LOG.debug("Successfully deserialized data.")
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
        LOG.debug("Deserializing data from Redis...")
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

        LOG.debug("Successfully deserialized data.")
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

    def _update_data_class_entry(self, updated_data_class: BaseDataClass, key: str):
        """
        Update an existing data class entry in the Redis database with new information.
        A 'data class' here refers to classes defined in merlin.db_scripts.data_formats.

        This method retrieves the existing data of the data class we're updating from Redis, updates
        its fields with the new data provided in the `updated_data_class` object, and saves the updated
        data back to the database.

        Args:
            updated_data_class: A [`BaseDataClass`][merlin.db_scripts.data_formats.BaseDataClass] instance 
                containing the updated information.
        """
        # Get the existing data from Redis and convert it to an instance of BaseDataClass
        existing_data = self.client.hgetall(key)
        existing_data_class = self._deserialize_data_class(existing_data, type(updated_data_class))

        # Update the fields and save it to Redis
        existing_data_class.update_fields(updated_data_class.to_dict())
        updated_study_data = self._serialize_data_class(existing_data_class)
        self.client.hset(key, mapping=updated_study_data)

    def get_version(self) -> str:
        """
        Query the Redis backend for the current version.

        Returns:
            A string representing the current version of Redis.
        """
        client_info = self.client.info()
        return client_info.get("redis_version", "N/A")        

    def save_study(self, study: StudyInfo):
        """
        Given a StudyInfo object, enter all of it's information to the Redis database.
        If a study with `study.id` already exists, this will override everything in that
        entry.

        Args:
            study: A [`StudyInfo`][merlin.db_scripts.data_formats.StudyInfo] instance.
        """
        existing_study_id = self.client.hget("study:name", study.name)

        # Study already exists so update the existing study
        if existing_study_id:
            LOG.debug(f"existing_study_id: {existing_study_id}.")
            LOG.debug(f"study.id: {study.id}")
            LOG.info(f"Attempting to update study with id '{study.id}'...")
            self._update_data_class_entry(study, f"study:{study.id}")
            LOG.info(f"Successfully updated study with id '{study.id}'.")
        # Study does not yet exist so create a new study entry
        else:
            LOG.info("Creating a study entry in Redis...")
            self._create_data_class_entry(study, f"study:{study.id}")
            self.client.hset("study:name", study.name, study.id)
            LOG.info(f"Successfully created a study with id '{study.id}' in Redis.")

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

    def delete_study(self, study_name: str, remove_associated_runs: bool = True):
        """
        Given the name of the study, find it in the database and remove that entry.

        Args:
            study_name: The name of the study to remove from the database.
            remove_associated_runs: If true, remove the runs associated with this study.
        """
        LOG.info(f"Attempting to delete study '{study_name}' from Redis...")

        # Retrieve the study using the retrieve_study method
        study = self.retrieve_study(study_name)
        if study is None:
            raise ValueError(f"Study with name '{study_name}' does not exist in the database.")

        # Delete all associated runs
        if remove_associated_runs:
            for run_id in study.runs:
                self.delete_run(run_id)  # Use the existing delete_run method

        # Delete the study's hash entry
        study_key = f"study:{study.id}"
        LOG.info(f"Deleting study hash with key '{study_key}'...")
        self.client.delete(study_key)

        # Remove the study's name-to-ID mapping
        LOG.info(f"Removing study name-to-ID mapping for '{study_name}'...")
        self.client.hdel("study:name", study_name)

        LOG.info(f"Successfully deleted study '{study_name}' and all associated data from Redis.")

    def save_run(self, run: RunInfo):
        """
        Given a RunInfo object, enter all of it's information to the backend database.

        Args:
            run: A [`RunInfo`][merlin.db_scripts.data_formats.RunInfo] instance.
        """
        run_key = f"run:{run.id}"
        if self.client.exists(run_key):
            LOG.info(f"Attempting to update run with id '{run.id}'...")
            self._update_data_class_entry(run, run_key)
            LOG.info(f"Successfully updated run with id '{run.id}'.")
        else:
            LOG.info("Creating a run entry in Redis...")
            self._create_data_class_entry(run, run_key)
            LOG.info(f"Successfully created a run with id '{run.id}' in Redis.")

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

    def retrieve_all_runs(self) -> List[RunInfo]:
        """
        Query the Redis database for every study that's currently stored.

        Returns:
            A list of [`RunInfo`][merlin.db_scripts.data_formats.RunInfo] objects.
        """
        LOG.info("Fetching all runs from Redis...")

        run_pattern = "run:*"
        all_runs = []

        # Loop through all runs
        for run_key in self.client.scan_iter(match=run_pattern):
            run_id = run_key.split(":")[1]  # Extract the ID
            try:
                run_info = self.retrieve_run(run_id)
                if run_info:
                    all_runs.append(run_info)
                else:
                    # Shouldn't hit this since we're looping with scan
                    LOG.warning(f"Run with id '{run_id}' could not be retrieved or does not exist.")
            except Exception as e:
                LOG.error(f"Error retrieving run with id '{run_id}': {e}")

        LOG.info(f"Successfully retrieved {len(all_runs)} runs from Redis.")
        return all_runs

    def delete_run(self, run_id: str):
        """
        Given a run id, find it in the database and remove that entry. This will also
        delete the run id from the list of runs in the associated study's entry.

        Args:
            run_id: The id of the run to delete.
        """
        LOG.info(f"Attempting to delete run with id '{run_id}' from Redis...")
        run = self.retrieve_run(run_id)
        if run is None:
            raise ValueError(f"Run with id '{run_id}' does not exist in the database.")
        
        LOG.debug(f"The run being deleted is associated with study '{run.study_id}'. Removing this run from that study's list of runs...")
        study_data = self.client.hgetall(f"study:{run.study_id}")
        if not study_data:
            LOG.warning(
                f"Study with id '{run.study_id}' does not exist in the database. Ignoring the removal of this run from that study's list of runs."
            )
        else:
            study_info = self._deserialize_data_class(study_data, StudyInfo)
            study_info.runs.remove(run_id)
            self.save_study(study_info)
            LOG.debug("Successfully removed this run from the associated study.")

        # Delete the run's hash entry
        run_key = f"run:{run.id}"
        LOG.debug(f"Deleting run hash with key '{run_key}'...")
        self.client.delete(run_key)
        LOG.debug("Successfully removed run hash from Redis.")

        LOG.info(f"Successfully deleted run '{run_id}' and all associated data from Redis.")
