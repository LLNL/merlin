##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
`StudyManager` module for managing study entities in the Merlin database.

This module defines a manager class responsible for the creation, retrieval,
and deletion of studies. It also ensures appropriate cleanup of associated
run entities when a study is deleted.
"""

from __future__ import annotations

from typing import List

from merlin.db_scripts.data_models import StudyModel
from merlin.db_scripts.entities.study_entity import StudyEntity
from merlin.db_scripts.entity_managers.entity_manager import EntityManager


# Purposefully ignoring this pylint message as each entity will have different parameter requirements
# pylint: disable=arguments-differ,arguments-renamed


class StudyManager(EntityManager[StudyEntity, StudyModel]):
    """
    Manager class for handling study entities.

    The `StudyManager` interacts with the underlying storage backend to create,
    retrieve, and delete study records. It also supports cleanup of related
    run entities during deletion to ensure data integrity.

    Attributes:
        backend (backends.results_backend.ResultsBackend): Backend interface used to persist
            and query study data.
        db (db_scripts.merlin_db.MerlinDatabase): Reference to the full `MerlinDatabase`,
            used for cross-entity operations (e.g., deleting associated runs).

    Methods:
        create: Create a new study if it doesn't already exist.
        get: Retrieve a study by ID or name.
        get_all: Retrieve all study entities.
        delete: Delete a study, with optional cleanup of related runs.
        delete_all: Delete all studies and optionally their runs.
        set_db_reference: Set reference to the MerlinDatabase for cross-entity access.
    """

    def create(self, study_name: str) -> StudyEntity:
        """
        Create a study if it does not already exist.

        If a study with the given name is not found in the database,
        a new study entity is created and persisted.

        Args:
            study_name (str): The name of the study to create.

        Returns:
            The newly created or existing study entity.
        """
        return self._create_entity_if_not_exists(
            entity_class=StudyEntity,
            model_class=StudyModel,
            identifier=study_name,
            log_message_exists=f"Study with name '{study_name}' already has an entry in the database.",
            log_message_create=f"Study with name '{study_name}' does not yet have an entry in the database. Creating one.",
            name=study_name,
        )

    def get(self, study_id_or_name: str) -> StudyEntity:
        """
        Retrieve a study by its ID or name.

        Args:
            study_id_or_name (str): The unique study ID or name to look up.

        Returns:
            The corresponding study entity.

        Raises:
            StudyNotFoundError: If no study matches the given ID or name.
        """
        return self._get_entity(StudyEntity, study_id_or_name)

    def get_all(self) -> List[StudyEntity]:
        """
        Retrieve all study entities stored in the database.

        Returns:
            A list of all available study entities.
        """
        return self._get_all_entities(StudyEntity, "study")

    def delete(self, study_id_or_name: str, remove_associated_runs: bool = True):
        """
        Delete a study and optionally its associated runs.

        If `remove_associated_runs` is True, all runs linked to the study
        will be deleted before the study itself is removed.

        Args:
            study_id_or_name (str): The ID or name of the study to delete.
            remove_associated_runs (bool, optional): Whether to delete runs
                associated with the study. Defaults to True.
        """

        def cleanup_study(study):
            if remove_associated_runs:
                for run_id in study.get_runs():
                    self.db.runs.delete(run_id)

        self._delete_entity(StudyEntity, study_id_or_name, cleanup_fn=cleanup_study)

    def delete_all(self, remove_associated_runs: bool = True):
        """
        Delete all studies in the database, and optionally their associated runs.

        Args:
            remove_associated_runs (bool, optional): Whether to delete runs
                linked to each study. Defaults to True.
        """
        self._delete_all_by_type(
            get_all_fn=self.get_all,
            delete_fn=self.delete,
            entity_name="studies",
            remove_associated_runs=remove_associated_runs,
        )

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
