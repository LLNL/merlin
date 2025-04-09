"""
Module for managing database entities related to studies.

This module defines the `StudyEntity` class, which extends the abstract base class
[`DatabaseEntity`][db_scripts.db_entity.DatabaseEntity], to encapsulate study-specific
operations and behaviors.
"""

import logging

from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.db_entity import DatabaseEntity
from merlin.exceptions import StudyNotFoundError


LOG = logging.getLogger("merlin")


class StudyEntity(DatabaseEntity):
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
                [`DatabaseEntity.get_id`][db_scripts.db_entity.DatabaseEntity.get_id]._

        get_additional_data:
            Retrieve any additional metadata associated with the study. _Implementation found in
                [`DatabaseEntity.get_additional_data`][db_scripts.db_entity.DatabaseEntity.get_additional_data]._

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
            f"Runs: {self.get_runs()}\n"
            f"Additional Data: {self.get_additional_data()}\n\n"
        )

    def reload_data(self):
        """
        Reload the latest data for this study from the database and update the
        [`StudyModel`][db_scripts.db_formats.StudyModel] object.

        Raises:
            (exceptions.StudyNotFoundError): If an entry for this study was not
                found in the database.
        """
        study_id = self.get_id()
        updated_entity_info = self.backend.retrieve(study_id, "study")
        if not updated_entity_info:
            raise StudyNotFoundError(f"Study with id {study_id} not found in the database.")
        self.entity_info = updated_entity_info

    def get_name(self) -> str:
        """
        Get the name associated with this study.

        Returns:
            The name for this study.
        """
        return self.entity_info.name

    def get_runs(self):
        """
        Get every run of this study.

        Returns:
            A list of run ids.
        """
        self.reload_data()
        return self.entity_info.runs

    def add_run(self, run_id: str):
        """
        Add a new run id to the list of runs.

        Args:
            run_id: The id of the run to add.
        """
        self.entity_info.runs.append(run_id)
        self.save()

    def remove_run(self, run_id: str):
        """
        Remove a run id from the list of runs.

        Does *not* delete a [`RunEntity`][db_scripts.run_entity.RunEntity] from the
        database. This will only remove the run's id from the list in this study entity.

        Args:
            run_id: The ID of the run to remove.
        """
        self.reload_data()
        self.entity_info.runs.remove(run_id)
        self.save()

    def save(self):
        """
        Save the current state of this study to the database.
        """
        self.backend.save(self.entity_info)

    @classmethod
    def load(cls, entity_id_or_name: str, backend: ResultsBackend) -> "StudyEntity":
        """
        Load a study from the database by id.

        Args:
            entity_id_or_name: The id or name of the study to load.
            backend: A [`ResultsBackend`][backends.results_backend.ResultsBackend] instance.

        Returns:
            A `StudyEntity` instance.

        Raises:
            (exceptions.StudyNotFoundError): If an entry for study with id `entity_id` was not
                found in the database.
        """
        entity_info = backend.retrieve(entity_id_or_name, "study")
        if entity_info is None:
            raise StudyNotFoundError(f"Study with id or name '{entity_id_or_name}' not found in the database.")

        return cls(entity_info, backend)

    @classmethod
    def delete(cls, entity_id_or_name: str, backend: ResultsBackend):
        """
        Delete a study from the database by id or name.

        By default, this will remove all of the runs associated with the study from the database.

        Args:
            entity_id_or_name: The id or name of the study to delete.
            backend: A [`ResultsBackend`][backends.results_backend.ResultsBackend] instance.
        """
        LOG.info(f"Deleting study with id or name '{entity_id_or_name}' from the database...")
        backend.delete(entity_id_or_name, "study")
        LOG.info(f"Study '{entity_id_or_name}' has been successfully deleted.")
