##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Module for managing database entities related to physical workers.

This module provides functionality for interacting with physical workers stored in a database,
including creating, retrieving, updating, and deleting them. It defines the `WorkerEntity`
class, which extends the abstract base class [`DatabaseEntity`][db_scripts.entities.db_entity.DatabaseEntity],
to encapsulate worker-specific operations and behaviors.
"""

import logging
from datetime import datetime
from typing import Dict, Optional

from merlin.common.enums import WorkerStatus
from merlin.db_scripts.data_models import PhysicalWorkerModel
from merlin.db_scripts.entities.db_entity import DatabaseEntity
from merlin.db_scripts.entities.mixins.name import NameMixin


LOG = logging.getLogger("merlin")


class PhysicalWorkerEntity(DatabaseEntity[PhysicalWorkerModel], NameMixin):
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
            Provide a string representation of the `PhysicalWorkerEntity` instance.
        __str__:
            Provide a human-readable string representation of the `PhysicalWorkerEntity` instance.
        reload_data:
            Reload the latest data for this worker from the database.
        get_id:
            Retrieve the ID of the worker. _Implementation found in
                [`DatabaseEntity.get_id`][db_scripts.entities.db_entity.DatabaseEntity.get_id]._
        get_additional_data:
            Retrieve any additional data saved to this worker. _Implementation found in
                [`DatabaseEntity.get_additional_data`][db_scripts.entities.db_entity.DatabaseEntity.get_additional_data]._
        get_name:
            Retrieve the name of this worker.
        get_logical_worker_id:
            Retrieve the ID of the logical worker that this physical worker was created from.
        get_launch_cmd:
            Retrieve the command used to launch this worker.
        set_launch_cmd:
            Update the launch command used to start this worker.
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
        get_restart_count:
            Retrieve the number of times this worker has been restarted.
        increment_restart_count:
            Increment the restart count for this worker.
        save:
            Save the current state of this worker to the database.
        load:
            (classmethod) Load a `PhysicalWorkerEntity` instance from the database by its ID or name.
        delete:
            (classmethod) Delete a worker from the database by its ID or name.
    """

    @classmethod
    def _get_entity_type(cls) -> str:
        return "physical_worker"

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
            f"logical_worker_id={self.get_logical_worker_id()}, "
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
        Provide a string representation of the `PhysicalWorkerEntity` instance.

        Returns:
            A human-readable string representation of the `PhysicalWorkerEntity` instance.
        """
        worker_id = self.get_id()
        return (
            f"Physical Worker with ID {worker_id}\n"
            f"------------{'-' * len(worker_id)}\n"
            f"Name: {self.get_name()}\n"
            f"Logical Worker ID: {self.get_logical_worker_id()}\n"
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

    def get_logical_worker_id(self) -> str:
        """
        Get the ID of the logical worker that this physical worker was created from.

        Returns:
            The ID of the logical worker that this physical worker was created from.
        """
        return self.entity_info.logical_worker_id

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

    def get_pid(self) -> Optional[int]:
        """
        Get the process ID for this worker.

        Returns:
            The process ID for this worker or None if not set.
        """
        self.reload_data()
        return int(self.entity_info.pid) if self.entity_info.pid else None

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
            A [`WorkerStatus`][common.enums.WorkerStatus] enum representing
                the status of this worker.
        """
        self.reload_data()
        return self.entity_info.status

    def set_status(self, status: WorkerStatus):
        """
        Set the status of this worker.

        Args:
            status: A [`WorkerStatus`][common.enums.WorkerStatus] enum representing
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
