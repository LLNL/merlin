##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Module for managing database entities related to logical workers.

This module defines the `LogicalWorkerEntity` class, which extends the abstract base class
[`DatabaseEntity`][db_scripts.entities.db_entity.DatabaseEntity], to encapsulate logical-worker-specific
operations and behaviors.
"""

import logging
from typing import List

from merlin.db_scripts.data_models import LogicalWorkerModel
from merlin.db_scripts.entities.db_entity import DatabaseEntity
from merlin.db_scripts.entities.mixins.name import NameMixin
from merlin.db_scripts.entities.mixins.queue_management import QueueManagementMixin
from merlin.db_scripts.entities.mixins.run_management import RunManagementMixin
from merlin.db_scripts.entities.physical_worker_entity import PhysicalWorkerEntity


LOG = logging.getLogger("merlin")


class LogicalWorkerEntity(DatabaseEntity[LogicalWorkerModel], RunManagementMixin, QueueManagementMixin, NameMixin):
    """
    A class representing a logical worker in the database.

    This class provides methods to interact with and manage a logical worker's data, including
    retrieving, adding, and removing run IDs and physical worker IDs from their respective lists,
    as well as saving or deleting the logical worker itself from the database.

    Attributes:
        entity_info (db_scripts.data_models.LogicalWorkerModel): An instance of the `LogicalWorkerModel`
            class containing the logical worker's metadata.
        backend (backends.results_backend.ResultsBackend): An instance of the `ResultsBackend`
            class used to interact with the database.

    Methods:
        __repr__:
            Provide a string representation of the `LogicalWorkerEntity` instance.

        __str__:
            Provide a human-readable string representation of the `LogicalWorkerEntity` instance.

        reload_data:
            Reload the latest data for this logical worker from the database.

        get_id:
            Retrieve the unique ID of the logical worker. _Implementation found in
                [`DatabaseEntity.get_id`][db_scripts.entities.db_entity.DatabaseEntity.get_id]._

        get_additional_data:
            Retrieve any additional metadata associated with the logical worker. _Implementation found in
                [`DatabaseEntity.get_additional_data`][db_scripts.entities.db_entity.DatabaseEntity.get_additional_data]._

        get_name:
            Retrieve the name of the logical worker.

        get_runs:
            Retrieve the IDs of the runs using this logical worker.

        add_run:
            Add a run ID to the list of runs.

        remove_run:
            Remove a run ID from the list of runs.

        get_physical_workers:
            Retrieve the IDs of the physical workers created from this logical worker.

        add_physical_worker:
            Add a physical worker ID to the list of physical workers.

        remove_physical_worker:
            Remove a physical worker ID from the list of physical workers.

        get_queues:
            Retrieve the list of queues that this worker is assigned to.

        save:
            Save the current state of the logical worker to the database.

        load:
            (classmethod) Load a `LogicalWorkerEntity` instance from the database by its ID.

        delete:
            (classmethod) Delete a logical worker from the database by its ID.
    """

    @classmethod
    def _get_entity_type(cls) -> str:
        return "logical_worker"

    def __repr__(self) -> str:
        """
        Provide a string representation of the `LogicalWorkerEntity` instance.

        Returns:
            A human-readable string representation of the `LogicalWorkerEntity` instance.
        """
        return (
            f"LogicalWorkerEntity("
            f"id={self.get_id()}, "
            f"name={self.get_name()}, "
            f"runs={self.get_runs()}, "
            f"queues={self.get_queues()}, "
            f"physical_workers={self.get_physical_workers()}, "
            f"additional_data={self.get_additional_data()}, "
            f"backend={self.backend.get_name()})"
        )

    def __str__(self) -> str:
        """
        Provide a string representation of the `LogicalWorkerEntity` instance.

        Returns:
            A human-readable string representation of the `LogicalWorkerEntity` instance.
        """
        worker_id = self.get_id()
        physical_workers = [
            PhysicalWorkerEntity.load(physical_worker_id, self.backend) for physical_worker_id in self.get_physical_workers()
        ]
        physical_worker_str = ""
        if physical_workers:
            for physical_worker in physical_workers:
                physical_worker_str += f"  - ID: {physical_worker.get_id()}\n    Name: {physical_worker.get_name()}\n"
        else:
            physical_worker_str = "  No physical workers found.\n"
        return (
            f"Logical Worker with ID {worker_id}\n"
            f"------------{'-' * len(worker_id)}\n"
            f"Name: {self.get_name()}\n"
            f"Runs:\n{self.construct_run_string()}"
            f"Queues: {self.get_queues()}\n"
            f"Physical Workers:\n{physical_worker_str}"
            f"Additional Data: {self.get_additional_data()}\n\n"
        )

    def get_physical_workers(self) -> List[str]:
        """
        Get the physical instances of this logical worker.

        Returns:
            A list of physical worker IDs.
        """
        self.reload_data()
        return self.entity_info.physical_workers

    def add_physical_worker(self, physical_worker_id: str):
        """
        Add a new physical worker id to the list of physical workers.

        Args:
            physical_worker_id: The id of the physical worker to add.
        """
        self.entity_info.physical_workers.append(physical_worker_id)
        self.save()

    def remove_physical_worker(self, physical_worker_id: str):
        """
        Remove a physical worker id from the list of physical workers.

        Does *not* delete a [`PhysicalWorkerEntity`][db_scripts.entities.physical_worker_entity.PhysicalWorkerEntity]
        from the database. This will only remove the physical worker's id from the list in this worker.

        Args:
            physical_worker_id: The ID of the physical worker to remove.
        """
        self.reload_data()
        self.entity_info.physical_workers.remove(physical_worker_id)
        self.save()
