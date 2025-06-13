##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Module for managing database entities related to studies.

This module defines the `StudyEntity` class, which extends the abstract base class
[`DatabaseEntity`][db_scripts.entities.db_entity.DatabaseEntity], to encapsulate study-specific
operations and behaviors.
"""

import logging

from merlin.db_scripts.data_models import StudyModel
from merlin.db_scripts.entities.db_entity import DatabaseEntity
from merlin.db_scripts.entities.mixins.name import NameMixin
from merlin.db_scripts.entities.mixins.run_management import RunManagementMixin


LOG = logging.getLogger("merlin")


class StudyEntity(DatabaseEntity[StudyModel], RunManagementMixin, NameMixin):
    """
    A class representing a study in the database.

    This class provides methods to interact with and manage a study's data, including
    retrieving, adding, and removing run IDs from the list of runs associated with the
    study, as well as saving or deleting the study itself from the database.

    Attributes:
        entity_info (db_scripts.data_models.StudyModel): An instance of the `StudyModel`
            class containing the study's metadata.
        backend (backends.results_backend.ResultsBackend): An instance of the `ResultsBackend`
            class used to interact with the database.

    Methods:
        __repr__:
            Provide a string representation of the `StudyEntity` instance.

        __str__:
            Provide a human-readable string representation of the `StudyEntity` instance.

        reload_data:
            Reload the latest data for this study from the database.

        get_id:
            Retrieve the unique ID of the study. _Implementation found in
                [`DatabaseEntity.get_id`][db_scripts.entities.db_entity.DatabaseEntity.get_id]._

        get_additional_data:
            Retrieve any additional metadata associated with the study. _Implementation found in
                [`DatabaseEntity.get_additional_data`][db_scripts.entities.db_entity.DatabaseEntity.get_additional_data]._

        get_name:
            Retrieve the name of the study.

        get_runs:
            Retrieve the IDs of the runs associated with this study.

        add_run:
            Add a run ID to the list of runs.

        remove_run:
            Remove a run ID from the list of runs.

        save:
            Save the current state of the study to the database.

        load:
            (classmethod) Load a `StudyEntity` instance from the database by its ID or name.

        delete:
            (classmethod) Delete a study from the database by its ID or name. Optionally, remove all associated runs.
    """

    @classmethod
    def _get_entity_type(cls) -> str:
        return "study"

    def __repr__(self) -> str:
        """
        Provide a string representation of the `StudyEntity` instance.

        Returns:
            A human-readable string representation of the `StudyEntity` instance.
        """
        return (
            f"StudyEntity("
            f"id={self.get_id()}, "
            f"name={self.get_name()}, "
            f"runs={self.get_runs()}, "
            f"additional_data={self.get_additional_data()}, "
            f"backend={self.backend.get_name()})"
        )

    def __str__(self) -> str:
        """
        Provide a string representation of the `StudyEntity` instance.

        Returns:
            A human-readable string representation of the `StudyEntity` instance.
        """
        study_id = self.get_id()
        return (
            f"Study with ID {study_id}\n"
            f"------------{'-' * len(study_id)}\n"
            f"Name: {self.get_name()}\n"
            f"Runs:\n{self.construct_run_string()}"
            f"Additional Data: {self.get_additional_data()}\n\n"
        )
