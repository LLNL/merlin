"""
Module for managing database entities related to physical workers.

This module provides functionality for interacting with physical workers stored in a database,
including creating, retrieving, updating, and deleting them. It defines the `WorkerEntity`
class, which extends the abstract base class [`DatabaseEntity`][db_scripts.db_entity.DatabaseEntity],
to encapsulate worker-specific operations and behaviors.
"""

import logging
from datetime import datetime
from typing import Dict

from merlin.backends.results_backend import ResultsBackend
from merlin.common.abstracts.enums import WorkerStatus
from merlin.db_scripts.db_entity import DatabaseEntity
from merlin.exceptions import WorkerNotFoundError


LOG = logging.getLogger("merlin")

# TODO
# - implement this class
#   - I think it should act as an overarching class that interacts with both Logical and Physical workers
#   - will need to update the redis_backend.py file to accommodate these changes
# - remove the db_worker.py file once this is done
#   - change all references to worker_entity instead


class PhysicalWorkerEntity(DatabaseEntity):
    """
    A class representing a physical worker in the database.

    This class provides methods to interact with and manage a worker's data, including
    retrieving information about the worker, updating its state, and saving or deleting
    it from the database.

    Attributes:
        entity_info (db_scripts.data_models.PhysicalWorkerModel): An instance of the `PhysicalWorkerModel`
            class containing the physical worker's metadata.
        backend (backends.results_backend.ResultsBackend): An instance of the `ResultsBackend`
            class used to interact with the database.

    Methods:
        __repr__:
            Provide a string representation of the `DatabaseWorker` instance.

        __str__:
            Provide a human-readable string representation of the `DatabaseWorker` instance.

        reload_data:
            Reload the latest data for this worker from the database.

        get_id:
            Retrieve the ID of the worker. _Implementation found in
                [`DatabaseEntity.get_id`][db_scripts.db_entity.DatabaseEntity.get_id]._

        get_additional_data:
            Retrieve any additional data saved to this worker. _Implementation found in
                [`DatabaseEntity.get_additional_data`][db_scripts.db_entity.DatabaseEntity.get_additional_data]._

        get_name:
            Retrieve the name of this worker.

        get_launch_cmd:
            Retrieve the command used to launch this worker.

        set_launch_cmd:
            Update the launch command used to start this worker.

        get_queues:
            Retrieve the task queues assigned to this worker.

        set_queues:
            Update the queues this worker is watching.

        get_args:
            Retrieve the arguments for this worker.

        set_args:
            Update the arguments used by this worker.

        get_pid:
            Retrieve the process ID for this worker.

        set_pid:
            Update the process ID for this worker.

        get_status:
            Retrieve the status of this worker.

        set_status:
            Update the status of this worker.

        get_heartbeat_timestamp:
            Retrieve the last heartbeat timestamp of this worker.

        set_heartbeat_timestamp:
            Update the latest heartbeat timestamp of this worker.

        get_latest_start_time:
            Retrieve the time this worker was last started.

        set_latest_start_time:
            Update the latest start time of this worker.

        get_host:
            Retrieve the hostname where this worker is running.

        set_host:
            Update the hostname for this worker.

        get_restart_count:
            Retrieve the number of times this worker has been restarted.

        increment_restart_count:
            Increment the restart count for this worker.

        save:
            Save the current state of this worker to the database.

        load:
            (classmethod) Load a `DatabaseWorker` instance from the database by its ID.

        load_by_name:
            (classmethod) Load a `DatabaseWorker` instance from the database by its name.

        delete:
            (classmethod) Delete a worker from the database by its ID.
    """

    def __repr__(self) -> str:
        """
        Provide a string representation of the `PhysicalWorkerEntity` instance.

        Returns:
            A human-readable string representation of the `PhysicalWorkerEntity` instance.
        """
        return (
            f"PhysicalWorkerEntity("
            f"id={self.get_id()}, "
            f"name={self.get_name()}, "
            f"launch_cmd={self.get_launch_cmd()}, "
            f"args={self.get_args()}, "
            f"pid={self.get_pid()}, "
            f"status={self.get_status()}, "
            f"heartbeat_timestamp={self.get_heartbeat_timestamp()}, "
            f"latest_start_time={self.get_latest_start_time()}, "
            f"host={self.get_host()}, "
            f"restart_count={self.get_restart_count()}, "
            f"additional_data={self.get_additional_data()}, "
            f"backend={self.backend.get_name()})"
        )

    def __str__(self) -> str:
        """
        Provide a string representation of the `DatabaseWorker` instance.

        Returns:
            A human-readable string representation of the `DatabaseWorker` instance.
        """
        worker_id = self.get_id()
        return (
            f"Worker with ID {worker_id}\n"
            f"------------{'-' * len(worker_id)}\n"
            f"Name: {self.get_name()}\n"
            f"Launch Command: {self.get_launch_cmd()}\n"
            f"Args: {self.get_args()}\n"
            f"Process ID: {self.get_pid()}\n"
            f"Status: {self.get_status()}\n"
            f"Last Heartbeat: {self.get_heartbeat_timestamp()}\n"
            f"Last Spinup: {self.get_latest_start_time()}\n"
            f"Host: {self.get_host()}\n"
            f"Restart Count: {self.get_restart_count()}\n"
            f"Additional Data: {self.get_additional_data()}\n\n"
        )

    def reload_data(self):
        """
        Reload the latest data for this worker from the database and update the
        [`PhysicalWorkerModel`][db_scripts.db_formats.PhysicalWorkerModel] object
        stored in `self.entity_info`.

        Raises:
            (exceptions.WorkerNotFoundError): If an entry for this worker was
                not found in the database.
        """
        worker_id = self.get_id()
        updated_entity_info = self.backend.retrieve(worker_id, "physical_worker")
        if not updated_entity_info:
            raise WorkerNotFoundError(f"Worker with ID {worker_id} not found in the database.")
        self.entity_info = updated_entity_info

    def get_name(self) -> str:
        """
        Get the name of this worker.

        Returns:
            The name for this worker.
        """
        return self.entity_info.name

    def get_launch_cmd(self) -> str:
        """
        Get the command used to launch this worker.

        Returns:
            The command used to launch this worker.
        """
        return self.entity_info.launch_cmd

    def set_launch_cmd(self, launch_cmd: str):
        """
        Set the launch command used to start this worker.

        Args:
            launch_cmd: The launch command used to start this worker.
        """
        self.entity_info.launch_cmd = launch_cmd
        self.save()

    def get_args(self) -> Dict:
        """
        Get the arguments for this worker.

        Returns:
            A dictionary of arguments for this worker.
        """
        return self.entity_info.args

    def set_args(self, args: str):
        """
        Set the arguments used by this worker.

        Args:
            args: The arguments used by this worker.
        """
        self.entity_info.args = args
        self.save()

    def get_pid(self) -> str:
        """
        Get the process ID for this worker.

        Returns:
            The process ID for this worker.
        """
        self.reload_data()
        return self.entity_info.pid

    def set_pid(self, pid: str):
        """
        Set the PID of this worker.

        Args:
            pid: The new PID of this worker.
        """
        self.entity_info.pid = pid
        self.save()

    def get_status(self) -> WorkerStatus:
        """
        Get the status of this worker.

        Returns:
            A [`WorkerStatus`][common.abstracts.enums.WorkerStatus] enum representing
                the status of this worker.
        """
        self.reload_data()
        return self.entity_info.status

    def set_status(self, status: WorkerStatus):
        """
        Set the status of this worker.

        Args:
            status: A [`WorkerStatus`][common.abstracts.enums.WorkerStatus] enum representing
                the new status of the worker.
        """
        self.entity_info.status = status
        self.save()

    def get_heartbeat_timestamp(self) -> str:
        """
        Get the last heartbeat timestamp of this worker.

        Returns:
            The last heartbeat timestamp we received from this worker
        """
        self.reload_data()
        return self.entity_info.heartbeat_timestamp

    def set_heartbeat_timestamp(self, heartbeat_timestamp: datetime):
        """
        Set the latest heartbeat timestamp of this worker.

        Args:
            heartbeat_timestamp: The latest heartbeat timestamp of this worker.
        """
        self.entity_info.heartbeat_timestamp = heartbeat_timestamp
        self.save()

    def get_latest_start_time(self) -> datetime:
        """
        Get the time that this worker was last started.

        Returns:
            A datetime object representing the last time this worker was started.
        """
        self.reload_data()
        return self.entity_info.latest_start_time

    def set_latest_start_time(self, latest_start_time: datetime):
        """
        Set the latest start time of this worker. This will be set on worker
        startup followed by any time the worker is restarted.

        Args:
            latest_start_time: The latest start time of this worker.
        """
        self.entity_info.latest_start_time = latest_start_time
        self.save()

    def get_host(self) -> str:
        """
        Get the hostname where this worker is running.

        Returns:
            The name of the host that this worker is running on.
        """
        return self.entity_info.host

    def get_restart_count(self) -> int:
        """
        Get the number of times that this worker has been restarted.

        Returns:
            The number of times that this worker has been restarted.
        """
        self.reload_data()
        return self.entity_info.restart_count

    def increment_restart_count(self):
        """
        Add another restart to the restart count.
        """
        self.entity_info.restart_count = self.get_restart_count() + 1
        self.save()

    def save(self):
        """
        Save the current state of this worker to the database.
        """
        self.backend.save_worker(self.entity_info)

    @classmethod
    def load(cls, entity_id: str, backend: ResultsBackend) -> "PhysicalWorkerEntity":
        """
        Load a worker from the database by ID.

        Args:
            entity_id: The ID of the worker to load.
            backend: A [`ResultsBackend`][backends.results_backend.ResultsBackend] instance.

        Returns:
            A `PhysicalWorkerEntity` instance.

        Raises:
            (exceptions.WorkerNotFoundError): If an entry for worker with id `entity_id` was
                not found in the database.
        """
        entity_info = backend.retrieve_worker(entity_id)
        if not entity_info:
            raise WorkerNotFoundError(f"Worker with ID {entity_id} not found in the database.")

        return cls(entity_info, backend)

    @classmethod
    def load_by_name(cls, worker_name: str, backend: ResultsBackend) -> "PhysicalWorkerEntity":
        """
        Load a worker from the database by its name.

        Args:
            worker_name: The name of the worker to load.
            backend: A [`ResultsBackend`][backends.results_backend.ResultsBackend] instance.

        Returns:
            A `PhysicalWorkerEntity` instance.

        Raises:
            (exceptions.WorkerNotFoundError): If no worker with the given name is found in
                the database.
        """
        entity_info = backend.retrieve_worker_by_name(worker_name)
        if not entity_info:
            raise WorkerNotFoundError(f"Worker with name '{worker_name}' not found in the database.")

        return cls(entity_info, backend)

    @classmethod
    def delete(cls, entity_id: str, backend: ResultsBackend):
        """
        Delete a worker from the database.

        Args:
            entity_id: The ID of the worker to delete.
            backend: A [`ResultsBackend`][backends.results_backend.ResultsBackend] instance.
        """
        LOG.info(f"Deleting worker with id '{entity_id}' from the database...")
        backend.delete_worker(entity_id)
        LOG.info(f"Worker with id '{entity_id}' has been successfully deleted.")
