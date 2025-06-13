##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Module for managing logical worker entities.

This module defines the `LogicalWorkerManager` class, which provides high-level operations
for creating, retrieving, and deleting logical workers stored in the database. It extends
the generic [`EntityManager`][db_scripts.entity_managers.entity_manager.EntityManager] class
with logic specific to logical worker entities, such as ID resolution based on worker name
and queues.

The manager also integrates cleanup routines to maintain consistency across related entities,
e.g., by removing logical workers from associated runs before deletion.
"""

from __future__ import annotations

import logging
from typing import List

from merlin.db_scripts.data_models import LogicalWorkerModel
from merlin.db_scripts.entities.logical_worker_entity import LogicalWorkerEntity
from merlin.db_scripts.entity_managers.entity_manager import EntityManager
from merlin.exceptions import RunNotFoundError


LOG = logging.getLogger("merlin")

# Purposefully ignoring this pylint message as each entity will have different parameter requirements
# pylint: disable=arguments-differ,arguments-renamed


class LogicalWorkerManager(EntityManager[LogicalWorkerEntity, LogicalWorkerModel]):
    """
    Manager class for handling logical worker entities.

    This class provides methods to create, retrieve, and delete logical workers from the
    backend. It also handles internal ID resolution and cleanup of references to logical
    workers from related entities such as runs.

    Attributes:
        backend: The backend interface used for storing and retrieving logical workers.
        db: Reference to the main database interface, used for cross-entity operations
            such as detaching workers from runs.

    Methods:
        create: Create a logical worker with the given name and queue list.
        get: Retrieve a logical worker either by ID or by name and queues.
        get_all: Retrieve all logical workers in the system.
        delete: Delete a logical worker and remove it from all associated runs.
        delete_all: Delete all logical workers currently stored in the backend.
        set_db_reference: Set a reference to the main database object for accessing related entities.
    """

    def _resolve_worker_id(self, worker_id: str = None, worker_name: str = None, queues: List[str] = None) -> str:
        """
        Resolve the logical worker ID based on provided parameters.

        Either a `worker_id` must be provided, or both `worker_name` and `queues`.

        Args:
            worker_id (str, optional): The ID of the logical worker.
            worker_name (str, optional): The name of the logical worker.
            queues (List[str], optional): The queues the worker handles.

        Returns:
            The resolved logical worker ID.

        Raises:
            ValueError: If input arguments are invalid or insufficient.
        """
        # Same implementation as in the original class
        if worker_id is not None:
            if worker_name is not None or queues is not None:
                raise ValueError("Provide either `worker_id` or (`worker_name` and `queues`), but not both.")
            return worker_id
        if worker_name is None or queues is None:
            raise ValueError("You must provide either `worker_id` or both `worker_name` and `queues`.")

        return LogicalWorkerModel.generate_id(worker_name, queues)

    def create(self, name: str, queues: List[str]) -> LogicalWorkerEntity:
        """
        Create a new logical worker entity, or return it if it already exists.

        Args:
            name (str): The name of the logical worker.
            queues (List[str]): A list of queue names the worker is assigned to.

        Returns:
            The created or existing logical worker entity.
        """
        logical_worker_id = self._resolve_worker_id(worker_name=name, queues=queues)
        log_message_create = (
            f"Logical worker with name '{name}' and queues '{queues}' does not yet have "
            "an entry in the database. Creating one."
        )
        log_message_exists = f"Logical worker with name '{name}' and queues '{queues}' already has an entry in the database."
        return self._create_entity_if_not_exists(
            entity_class=LogicalWorkerEntity,
            model_class=LogicalWorkerModel,
            identifier=logical_worker_id,
            log_message_exists=log_message_exists,
            log_message_create=log_message_create,
            name=name,
            queues=queues,
        )

    def get(self, worker_id: str = None, worker_name: str = None, queues: List[str] = None) -> LogicalWorkerEntity:
        """
        Retrieve a logical worker entity by ID, or by name and queues.

        Args:
            worker_id (str, optional): The unique identifier of the logical worker.
            worker_name (str, optional): The name of the logical worker.
            queues (List[str], optional): The queues the worker handles.

        Returns:
            The retrieved logical worker entity.

        Raises:
            ValueError: If input arguments are invalid or insufficient.
            WorkerNotFoundError: If the specified worker does not exist.
        """
        worker_id = self._resolve_worker_id(worker_id=worker_id, worker_name=worker_name, queues=queues)
        return self._get_entity(LogicalWorkerEntity, worker_id)

    def get_all(self) -> List[LogicalWorkerEntity]:
        """
        Retrieve all logical worker entities from the backend.

        Returns:
            A list of all logical worker entities.
        """
        return self._get_all_entities(LogicalWorkerEntity, "logical_worker")

    def delete(self, worker_id: str = None, worker_name: str = None, queues: List[str] = None):
        """
        Delete a logical worker entity and clean up any references from associated runs.

        The method ensures the logical worker is removed from all runs that reference it
        before deleting it from the backend.

        Args:
            worker_id (str, optional): The ID of the logical worker.
            worker_name (str, optional): The name of the logical worker.
            queues (List[str], optional): The queues the worker handles.

        Raises:
            ValueError: If input arguments are invalid or insufficient.
        """
        logical_worker = self.get(worker_id=worker_id, worker_name=worker_name, queues=queues)

        def cleanup_logical_worker(worker):
            runs_using_worker = worker.get_runs()
            for run_id in runs_using_worker:
                try:
                    run = self.db.runs.get(run_id)
                    run.remove_worker(worker.get_id())
                except RunNotFoundError:
                    LOG.warning(f"Couldn't find run with id {run_id}. Continuing with logical worker delete.")

        self._delete_entity(LogicalWorkerEntity, logical_worker.get_id(), cleanup_fn=cleanup_logical_worker)

    def delete_all(self):
        """
        Delete all logical worker entities currently stored in the backend.

        Runs cleanup on each logical worker before deletion to remove dependencies.
        """
        self._delete_all_by_type(get_all_fn=self.get_all, delete_fn=self.delete, entity_name="logical workers")

    def set_db_reference(self, db: MerlinDatabase):  # noqa: F821  pylint: disable=undefined-variable
        """
        Set a reference to the main Merlin database object for cross-entity operations.

        This allows the manager to access other entity managers (e.g., runs) when
        performing operations like cleanup during deletions.

        Args:
            db (db_scripts.merlin_db.MerlinDatabase): The database object that provides
                access to related entity managers.
        """
        self.db = db
