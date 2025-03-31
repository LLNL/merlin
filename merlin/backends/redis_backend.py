"""
This module contains the functionality required to interact with a
Redis backend.
"""

import json
import logging
from datetime import datetime
from typing import Dict, List

from redis import Redis

from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.data_models import BaseDataModel, RunModel, StudyModel, WorkerModel
from merlin.exceptions import WorkerNotFoundError


LOG = logging.getLogger("merlin")

# TODO
# 1. Add the work that Ryan did in celeryadapter where the worker launch command is saved to Redis
#  - Will need to instantiate this RedisBackend class I think... not sure
# 2. Integrate this work with the monitor command to enable worker restarts


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
        client (Redis): The Redis client used for database operations.

    Methods:
        _create_data_class_entry:
            Store a [`BaseDataModel`][db_scripts.data_models.BaseDataModel] instance
            as a hash in the Redis database.

        _update_data_class_entry:
            Update an existing data class entry in the Redis database with new information.

        _serialize_data_class:
            Convert a [`BaseDataModel`][db_scripts.data_models.BaseDataModel] instance
            into a format that Redis can interpret.

        _deserialize_data_class:
            Convert data retrieved from Redis into a
            [`BaseDataModel`][db_scripts.data_models.BaseDataModel] instance.

        get_version:
            Query Redis for the current version.

        get_connection_string:
            Retrieve the connection string used to connect to Redis.

        flush_database:
            Remove every entry in the Redis database.

        save_study:
            Save a [`StudyModel`][db_scripts.data_models.StudyModel] object to the Redis database.
            If a study with the same name exists, it will update the existing entry.

        retrieve_study:
            Retrieve a [`StudyModel`][db_scripts.data_models.StudyModel] object from the Redis
            database by its name.

        retrieve_all_studies:
            Retrieve all studies currently stored in the Redis database.

        delete_study:
            Delete a study from the Redis database by its name. Optionally, remove all associated runs.

        save_run:
            Save a [`RunModel`][db_scripts.data_models.RunModel] object to the Redis database. If a run
            with the same ID exists, it will update the existing entry.

        retrieve_run:
            Retrieve a [`RunModel`][db_scripts.data_models.RunModel] object from the Redis database
            by its ID.

        retrieve_all_runs:
            Retrieve all runs currently stored in the Redis database.

        delete_run:
            Delete a run from the Redis database by its ID. This will also remove the run from the associated
            study's list of runs.
    """

    def __init__(self, backend_name: str):
        super().__init__(backend_name)
        from merlin.config.configfile import CONFIG  # pylint: disable=import-outside-toplevel
        from merlin.config.results_backend import get_connection_string  # pylint: disable=import-outside-toplevel

        redis_config = {"url": get_connection_string(), "decode_responses": True}
        if CONFIG.results_backend.name == "rediss":
            redis_config.update({"ssl_cert_reqs": getattr(CONFIG.results_backend, "cert_reqs", "required")})

        self.client: Redis = Redis.from_url(**redis_config)

    def _serialize_data_class(self, data_class: BaseDataModel) -> Dict[str, str]:
        """
        Given a [`BaseDataModel`][db_scripts.data_models.BaseDataModel] instance,
        convert it's data into a format that the Redis database can interpret.

        Args:
            data_class: A [`BaseDataModel`][db_scripts.data_models.BaseDataModel] instance.

        Returns:
            A dictionary of information that Redis can interpret.
        """
        LOG.debug("Deserializing data from Redis...")
        serialized_data = {}

        for field in data_class.get_instance_fields():
            field_value = getattr(data_class, field.name)
            if isinstance(field_value, (list, dict)):
                serialized_data[field.name] = json.dumps(field_value)
            elif isinstance(field_value, datetime):
                serialized_data[field.name] = field_value.isoformat()
            elif field_value is None:
                serialized_data[field.name] = "null"
            else:
                serialized_data[field.name] = str(field_value)

        LOG.debug("Successfully deserialized data.")
        return serialized_data

    def _deserialize_data_class(self, retrieval_data: Dict[str, str], data_class: BaseDataModel) -> BaseDataModel:
        """
        Given data that was retrieved by Redis, convert it into a data_class instance.

        Args:
            retrieval_data: The data retrieved by Redis that we need to deserialize.
            data_class: A [`BaseDataModel`][db_scripts.data_models.BaseDataModel] object.

        Returns:
            A [`BaseDataModel`][db_scripts.data_models.BaseDataModel] instance.
        """
        LOG.debug("Deserializing data from Redis...")
        deserialized_data = {}

        for key, val in retrieval_data.items():
            if val.startswith("[") or val.startswith("{"):
                deserialized_data[key] = json.loads(val)
            elif val == "null":
                deserialized_data[key] = None
            elif val in ("True", "False"):
                deserialized_data[key] = val == "True"
            elif self._is_iso_datetime(val):
                deserialized_data[key] = datetime.fromisoformat(val)
            elif val.isdigit():
                deserialized_data[key] = float(val)
            else:
                deserialized_data[key] = str(val)

        LOG.debug("Successfully deserialized data.")
        return data_class.from_dict(deserialized_data)

    def _is_iso_datetime(self, value: str) -> bool:
        """
        Check if a string is in ISO 8601 datetime format.

        Args:
            value: The string to check.

        Returns:
            True if the string is in ISO 8601 format, False otherwise.
        """
        try:
            datetime.fromisoformat(value)
            return True
        except ValueError:
            return False

    def _create_data_class_entry(self, data_class: BaseDataModel, key: str):
        """
        Given a [`BaseDataModel`][db_scripts.data_models.BaseDataModel] instance and
        a key, store the contents of the `data_class` as a hash at `key` in the Redis database.

        Args:
            data_class: A [`BaseDataModel`][db_scripts.data_models.BaseDataModel] instance.
            key: The location to store the contents of `data_class` in the Redis database.
        """
        serialized_data = self._serialize_data_class(data_class)
        self.client.hset(key, mapping=serialized_data)

    def _update_data_class_entry(self, updated_data_class: BaseDataModel, key: str):
        """
        Update an existing data class entry in the Redis database with new information.
        A 'data class' here refers to classes defined in merlin.db_scripts.data_models.

        This method retrieves the existing data of the data class we're updating from Redis, updates
        its fields with the new data provided in the `updated_data_class` object, and saves the updated
        data back to the database.

        Args:
            updated_data_class: A [`BaseDataModel`][db_scripts.data_models.BaseDataModel] instance
                containing the updated information.
        """
        # Get the existing data from Redis and convert it to an instance of BaseDataModel
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
    
    def get_connection_string(self):
        """
        Get the connection string to Redis.

        Returns:
            A string representing the connection to Redis.
        """
        from merlin.config.results_backend import get_connection_string  # pylint: disable=import-outside-toplevel
        return get_connection_string(include_password=False)
    
    def flush_database(self):
        """
        Remove everything stored in Redis.
        """
        self.client.flushdb()

    def save_study(self, study: StudyModel):
        """
        Given a StudyModel object, enter all of it's information to the Redis database.
        If a study with `study.id` already exists, this will override everything in that
        entry.

        Args:
            study: A [`StudyModel`][db_scripts.data_models.StudyModel] instance.
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

    def retrieve_study(self, study_id: str) -> StudyModel:
        """
        Given a study's id, retrieve it from the Redis database.

        Args:
            study_id: The name of the study to retrieve.

        Returns:
            A [`StudyModel`][db_scripts.data_models.StudyModel] instance
                or None if the study does not yet exist in the database.
        """
        if not study_id.startswith("study:"):
            study_id = f"study:{study_id}"

        if not self.client.exists(study_id):
            return None

        data_from_redis = self.client.hgetall(study_id)
        return self._deserialize_data_class(data_from_redis, StudyModel)

    def retrieve_study_by_name(self, study_name: str) -> StudyModel:
        """
        Given a study's name, retrieve it from the Redis database.

        Args:
            study_name: The name of the study to retrieve.

        Returns:
            A [`StudyModel`][db_scripts.data_models.StudyModel] instance
                or None if the study does not yet exist in the database.
        """
        study_id = self.client.hget("study:name", study_name)

        if study_id is None:
            return None

        data_from_redis = self.client.hgetall(f"study:{study_id}")
        return self._deserialize_data_class(data_from_redis, StudyModel)

    def retrieve_all_studies(self) -> List[StudyModel]:
        """
        Query the Redis database for every study that's currently stored.

        Returns:
            A list of [`StudyModel`][db_scripts.data_models.StudyModel] objects.
        """
        LOG.info("Retrieving all studies from Redis...")

        # Retrieve all study ids
        study_ids = self.client.keys("study:*")
        if not study_ids:
            return None
        study_ids.remove("study:name")
        LOG.debug(f"Found {len(study_ids)} studies in Redis.")

        all_studies = []

        # Loop through each study id and retrieve its StudyModel
        for study_id in study_ids:
            try:
                study_info = self.retrieve_study(study_id)
                if study_info:
                    all_studies.append(study_info)
                else:
                    LOG.warning(f"Study with ID '{study_id}' could not be retrieved or does not exist.")
            except Exception as exc:  # pylint: disable=broad-except
                LOG.error(f"Error retrieving study with ID '{study_id}': {exc}")

        # Return the list of StudyModel objects
        LOG.info(f"Successfully retrieved {len(all_studies)} studies from Redis.")
        return all_studies

    def delete_study(self, study_id: str, remove_associated_runs: bool = True):
        """
        Given the id of the study, find it in the database and remove that entry.

        Args:
            study_id: The id of the study to remove from the database.
            remove_associated_runs: If true, remove the runs associated with this study.
        """
        LOG.info(f"Attempting to delete study with id '{study_id}' from Redis...")

        # Retrieve the study using the retrieve_study method
        study = self.retrieve_study(study_id)
        if study is None:
            raise ValueError(f"Study with id '{study_id}' does not exist in the database.")

        # Delete all associated runs
        if remove_associated_runs:
            for run_id in study.runs:
                self.delete_run(run_id)  # Use the existing delete_run method

        # Delete the study's hash entry
        study_key = f"study:{study_id}"
        LOG.info(f"Deleting study hash with key '{study_key}'...")
        self.client.delete(study_key)

        # Remove the study's name-to-ID mapping
        LOG.info(f"Removing study name-to-ID mapping for '{study.name}'...")
        self.client.hdel("study:name", study.name)

        LOG.info(f"Successfully deleted study with id '{study_id}' and all associated data from Redis.")

    def save_run(self, run: RunModel):
        """
        Given a RunModel object, enter all of it's information to the backend database.

        Args:
            run: A [`RunModel`][db_scripts.data_models.RunModel] instance.
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

    def retrieve_run(self, run_id: str) -> RunModel:
        """
        Given a run's id, retrieve it from the Redis database.

        Args:
            run_id: The id of a run to retrieve.
        """
        run_key = f"run:{run_id}"
        if not self.client.exists(run_key):
            return None

        data_from_redis = self.client.hgetall(run_key)
        return self._deserialize_data_class(data_from_redis, RunModel)

    def retrieve_all_runs(self) -> List[RunModel]:
        """
        Query the Redis database for every study that's currently stored.

        Returns:
            A list of [`RunModel`][db_scripts.data_models.RunModel] objects.
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
            except Exception as exc:  # pylint: disable=broad-except
                LOG.error(f"Error retrieving run with id '{run_id}': {exc}")

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

        LOG.debug(
            f"The run being deleted is associated with study '{run.study_id}'. "
            "Removing this run from that study's list of runs..."
        )
        study_data = self.client.hgetall(f"study:{run.study_id}")
        if not study_data:
            LOG.warning(
                f"Study with id '{run.study_id}' does not exist in the database. "
                "Ignoring the removal of this run from that study's list of runs."
            )
        else:
            study_info = self._deserialize_data_class(study_data, StudyModel)
            study_info.runs.remove(run_id)
            self.save_study(study_info)
            LOG.debug("Successfully removed this run from the associated study.")

        # Delete the run's hash entry
        run_key = f"run:{run.id}"
        LOG.debug(f"Deleting run hash with key '{run_key}'...")
        self.client.delete(run_key)
        LOG.debug("Successfully removed run hash from Redis.")

        LOG.info(f"Successfully deleted run '{run_id}' and all associated data from Redis.")

    def save_worker(self, worker: WorkerModel):
        """
        Given a [`WorkerModel`][db_scripts.data_models.WorkerModel] object, enter
        all of it's information to the backend database.

        Args:
            worker: A [`WorkerModel`][db_scripts.data_models.WorkerModel] instance.
        """
        existing_worker_id = self.client.hget("worker:name", worker.name)
        worker_key = f"worker:{worker.id}"
        if existing_worker_id:
            LOG.info(f"Attempting to update worker with id '{worker.id}'...")
            self._update_data_class_entry(worker, worker_key)
            LOG.info(f"Successfully updated worker with id '{worker.id}'.")
        else:
            LOG.info("Creating a worker entry in Redis...")
            self._create_data_class_entry(worker, worker_key)
            self.client.hset("worker:name", worker.name, worker.id)
            LOG.info(f"Successfully created a worker with id '{worker.id}' in Redis.")

    def retrieve_worker(self, worker_id: str) -> WorkerModel:
        """
        Given a worker's id, retrieve it from the backend database.

        Args:
            worker_id: The ID of the worker to retrieve.

        Returns:
            A [`WorkerModel`][db_scripts.data_models.WorkerModel] instance.
        """
        if not worker_id.startswith("worker:"):
            worker_id = f"worker:{worker_id}"

        if not self.client.exists(worker_id):
            return None

        data_from_redis = self.client.hgetall(worker_id)
        return self._deserialize_data_class(data_from_redis, WorkerModel)

    def retrieve_worker_by_name(self, worker_name: str) -> WorkerModel:
        """
        Given a worker's name, retrieve it from the backend database.

        Args:
            worker_name: The name of the worker to retrieve.

        Returns:
            A [`WorkerModel`][db_scripts.data_models.WorkerModel] instance.
        """
        worker_id = self.client.hget("worker:name", worker_name)

        if worker_id is None:
            return None

        data_from_redis = self.client.hgetall(f"worker:{worker_id}")
        return self._deserialize_data_class(data_from_redis, WorkerModel)

    def retrieve_all_workers(self) -> List[WorkerModel]:
        """
        Query the backend database for every worker that's currently stored.

        Returns:
            A list of [`WorkerModel`][db_scripts.data_models.WorkerModel] objects.
        """
        LOG.info("Retrieving all workers from Redis...")

        # Retrieve all worker ids
        worker_ids = self.client.keys("worker:*")
        if not worker_ids:
            return None
        worker_ids.remove("worker:name")
        LOG.debug(f"Found {len(worker_ids)} workers in Redis.")

        all_workers = []

        # Loop through each worker id and retrieve its WorkerModel
        for worker_id in worker_ids:
            try:
                worker_info = self.retrieve_worker(worker_id)
                if worker_info:
                    all_workers.append(worker_info)
                else:
                    LOG.warning(f"Worker with ID '{worker_id}' could not be retrieved or does not exist.")
            except Exception as exc:  # pylint: disable=broad-except
                LOG.error(f"Error retrieving worker with ID '{worker_id}': {exc}")

        # Return the list of WorkerModel objects
        LOG.info(f"Successfully retrieved {len(all_workers)} workers from Redis.")
        return all_workers

    def delete_worker(self, worker_id: str):
        """
        Given a worker id, find it in the database and remove that entry.

        Args:
            worker_id: The id of the worker to delete.
        """
        LOG.info(f"Attempting to delete worker with id '{worker_id}' from Redis...")

        # Retrieve the worker to ensure it exists and get its name
        worker = self.retrieve_worker(worker_id)
        if worker is None:
            raise WorkerNotFoundError(f"Worker with ID '{worker_id}' not found in the database.")

        # Delete the worker from the name index and Redis
        self.client.hdel("worker:name", worker.name)
        self.client.delete(f"worker:{worker.id}")

        LOG.info(f"Successfully deleted worker with ID '{worker_id}'.")
