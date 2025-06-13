##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Module for managing database entities related to physical workers.

This module defines the `PhysicalWorkerManager` class, which provides high-level operations for
creating, retrieving, and deleting physical worker entities stored in the database. It acts as a
controller that encapsulates logic around
[`PhysicalWorkerEntity`][db_scripts.entities.physical_worker_entity.PhysicalWorkerEntity]
objects and their corresponding [`PhysicalWorkerModel`][db_scripts.data_models.PhysicalWorkerModel]
representations.

The manager interacts with the results backend and optionally references the main database
object to support operations that involve other entities, such as cleanup of related logical workers.
"""

from __future__ import annotations

import logging
from typing import Any, List

from merlin.db_scripts.data_models import PhysicalWorkerModel
from merlin.db_scripts.entities.physical_worker_entity import PhysicalWorkerEntity
from merlin.db_scripts.entity_managers.entity_manager import EntityManager
from merlin.exceptions import WorkerNotFoundError


LOG = logging.getLogger("merlin")

# Purposefully ignoring this pylint message as each entity will have different parameter requirements
# pylint: disable=arguments-differ,arguments-renamed


class PhysicalWorkerManager(EntityManager[PhysicalWorkerEntity, PhysicalWorkerModel]):
    """
    Manager for physical worker entities.

    This manager handles the creation, retrieval, and deletion of physical worker entities.
    It abstracts lower-level backend interactions and optionally performs cleanup logic
    that involves related logical workers through a reference to the main
    [`MerlinDatabase`][db_scripts.merlin_db.MerlinDatabase].

    Attributes:
        backend (backends.results_backend.ResultsBackend): The backend used for database operations.
        db (db_scripts.merlin_db.MerlinDatabase): Optional reference to the main database for cross-entity logic.

    Methods:
        create: Create a new physical worker if it does not already exist.
        get: Retrieve a physical worker entity by its ID or name.
        get_all: Retrieve all physical worker entities from the database.
        delete: Delete a specific physical worker, performing cleanup on related logical workers.
        delete_all: Delete all physical worker entities from the database.
        set_db_reference: Set a reference to the main database object for cross-entity operations.
    """

    def create(self, name: str, **kwargs: Any) -> PhysicalWorkerEntity:
        """
        Create a physical worker entity if it does not already exist.

        This method checks whether a physical worker with the specified name exists.
        If not, it creates a new one using the provided attributes.

        Args:
            name (str): The name of the physical worker.
            **kwargs (Any): Additional attributes to pass to the
                [`PhysicalWorkerModel`][db_scripts.data_models.PhysicalWorkerModel] constructor.

        Returns:
            The created or pre-existing physical worker entity.
        """
        log_message_create = f"Physical worker with name '{name}' does not yet have an " "entry in the database. Creating one."
        return self._create_entity_if_not_exists(
            entity_class=PhysicalWorkerEntity,
            model_class=PhysicalWorkerModel,
            identifier=name,
            log_message_exists=f"Physical worker with name '{name}' already has an entry in the database.",
            log_message_create=log_message_create,
            name=name,
            **kwargs,
        )

    def get(self, worker_id_or_name: str) -> PhysicalWorkerEntity:
        """
        Retrieve a physical worker entity by its ID or name.

        Args:
            worker_id_or_name (str): The ID or name of the physical worker to retrieve.

        Returns:
            The physical worker entity corresponding to the provided identifier.

        Raises:
            WorkerNotFoundError: If the specified worker does not exist.
        """
        return self._get_entity(PhysicalWorkerEntity, worker_id_or_name)

    def get_all(self) -> List[PhysicalWorkerEntity]:
        """
        Retrieve all physical worker entities from the database.

        Returns:
            A list of all physical workers stored in the database.
        """
        return self._get_all_entities(PhysicalWorkerEntity, "physical_worker")

    def delete(self, worker_id_or_name: str):
        """
        Delete a physical worker entity by its ID or name.

        This method will also attempt to remove the deleted physical worker's ID
        from its associated logical worker. If the logical worker cannot be found,
        a warning is logged and deletion continues.

        Args:
            worker_id_or_name (str): The ID or name of the physical worker to delete.
        """

        def cleanup_physical_worker(worker):
            logical_worker_id = worker.get_logical_worker_id()
            try:
                logical_worker = self.db.logical_workers.get(worker_id=logical_worker_id)
                logical_worker.remove_physical_worker(worker.get_id())
            except WorkerNotFoundError:
                LOG.warning(
                    f"Couldn't find logical worker with id {logical_worker_id}. Continuing with physical worker delete."
                )

        self._delete_entity(PhysicalWorkerEntity, worker_id_or_name, cleanup_fn=cleanup_physical_worker)

    def delete_all(self):
        """
        Delete all physical worker entities from the database.

        This operation also performs cleanup on associated logical workers as needed.
        """
        self._delete_all_by_type(get_all_fn=self.get_all, delete_fn=self.delete, entity_name="physical workers")

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
