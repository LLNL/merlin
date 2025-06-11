##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Module for managing run entities within the Merlin database.

This module defines the `RunManager` class, which extends the generic
[`EntityManager`][db_scripts.entity_managers.entity_manager.EntityManager] base
class to provide CRUD operations for runs associated with studies, workspaces,
and queues. The manager coordinates with the study and logical worker managers
for consistent data handling during create and delete operations.
"""

from __future__ import annotations

import logging
from typing import Any, List

from merlin.db_scripts.data_models import RunModel
from merlin.db_scripts.entities.run_entity import RunEntity
from merlin.db_scripts.entity_managers.entity_manager import EntityManager
from merlin.exceptions import StudyNotFoundError, WorkerNotFoundError


LOG = logging.getLogger("merlin")

# Purposefully ignoring this pylint message as each entity will have different parameter requirements
# pylint: disable=arguments-differ,arguments-renamed


class RunManager(EntityManager[RunEntity, RunModel]):
    """
    Manager for run entities.

    This class handles creation, retrieval, updating, and deletion of runs in the database.
    It maintains consistency by coordinating with study and logical worker entities during
    operations such as run creation and deletion.

    Attributes:
        backend: The database backend used for storing run entities.
        db: Reference to the main Merlin database, allowing access to other entity managers
            such as studies and logical workers.

    Methods:
        create: Create a new run associated with a study and workspace.
        get: Retrieve a run by its ID or workspace identifier.
        get_all: Retrieve all runs from the database.
        delete: Delete a run and perform cleanup of related entities.
        delete_all: Delete all runs in the database.
        set_db_reference: Set the reference to the main Merlin database for cross-entity operations.
    """

    def create(self, study_name: str, workspace: str, queues: List[str], **kwargs: Any) -> RunEntity:
        """
        Create a new run associated with a study, workspace, and queues.

        This method ensures the study exists (creating it if necessary), then creates
        and saves a new run entity. Additional keyword arguments that correspond to valid
        [`RunModel`][db_scripts.data_models.RunModel] fields are included; other kwargs are
        stored as additional data.

        Args:
            study_name (str): Name of the study this run belongs to.
            workspace (str): Workspace identifier for the run.
            queues (List[str]): List of queues associated with the run.
            **kwargs (Any): Additional optional fields for the run entity.

        Returns:
            The created run entity.
        """
        # Create the study if it doesn't exist
        study_entity = self.db.studies.create(study_name)

        # Filter valid fields for the RunModel
        valid_fields = {f.name for f in RunModel.get_class_fields()}
        valid_kwargs = {key: val for key, val in kwargs.items() if key in valid_fields}
        additional_data = {key: val for key, val in kwargs.items() if key not in valid_fields}

        # Create the RunModel and save it
        new_run = RunModel(
            study_id=study_entity.get_id(),
            workspace=workspace,
            queues=queues,
            **valid_kwargs,
            additional_data=additional_data,
        )
        run_entity = RunEntity(new_run, self.backend)
        run_entity.save()

        # Add the run ID to the study
        study_entity.add_run(run_entity.get_id())

        return run_entity

    def get(self, run_id_or_workspace: str) -> RunEntity:
        """
        Retrieve a run entity by its unique ID or workspace identifier.

        Args:
            run_id_or_workspace (str): The unique identifier or workspace string of the run.

        Returns:
            The run entity corresponding to the given identifier.

        Raises:
            RunNotFoundError: If no run is found matching the identifier.
        """
        return self._get_entity(RunEntity, run_id_or_workspace)

    def get_all(self) -> List[RunEntity]:
        """
        Retrieve all run entities stored in the database.

        Returns:
            A list of all run entities.
        """
        return self._get_all_entities(RunEntity, "run")

    def delete(self, run_id_or_workspace: str):
        """
        Delete a run entity by its ID or workspace identifier.

        Performs cleanup operations to maintain consistency:\n
        - Removes the run from its associated study.
        - Removes the run from all logical workers referencing it.

        Args:
            run_id_or_workspace (str): The unique identifier or workspace string of the run to delete.

        Raises:
            RunNotFoundError: If no run is found matching the identifier.
        """

        def cleanup_run(run):
            # Remove from study
            try:
                study = self.db.studies.get(run.get_study_id())
                study.remove_run(run.get_id())
            except StudyNotFoundError:
                LOG.warning(f"Couldn't find study with id {run.get_study_id()}. Continuing with run delete.")

            # Remove from logical workers
            for worker_id in run.get_workers():
                try:
                    logical_worker = self.db.logical_workers.get(worker_id=worker_id)
                    logical_worker.remove_run(run.get_id())
                except WorkerNotFoundError:
                    LOG.warning(f"Couldn't find logical worker with id {worker_id}. Continuing with run delete.")

        self._delete_entity(RunEntity, run_id_or_workspace, cleanup_fn=cleanup_run)

    def delete_all(self):
        """
        Delete all run entities from the database.

        This method calls `delete` on each run entity to ensure proper cleanup.
        """
        self._delete_all_by_type(get_all_fn=self.get_all, delete_fn=self.delete, entity_name="runs")

    def set_db_reference(self, db: MerlinDatabase):  # noqa: F821  pylint: disable=undefined-variable
        """
        Set a reference to the main Merlin database object for cross-entity operations.

        This allows the manager to access other entity managers (e.g., logical workers) when
        performing operations like cleanup during deletions.

        Args:
            db (db_scripts.merlin_db.MerlinDatabase): The database object that provides
                access to related entity managers.
        """
        self.db = db
