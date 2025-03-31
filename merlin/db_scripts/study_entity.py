"""
Module for managing database entities related to studies.

This module provides functionality for interacting with studies stored in a database,
including creating, retrieving, updating, and deleting studies and their associated runs.
It defines the `StudyEntity` class, which extends the abstract base class
[`DatabaseEntity`][db_scripts.db_entity.DatabaseEntity], to encapsulate study-specific
operations and behaviors.
"""

import logging
from typing import List

from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.data_models import RunModel
from merlin.db_scripts.db_entity import DatabaseEntity
from merlin.db_scripts.run_entity import RunEntity
from merlin.exceptions import StudyNotFoundError


LOG = logging.getLogger("merlin")


class StudyEntity(DatabaseEntity):
    """
    A class representing a study in the database.

    This class provides methods to interact with and manage a study's data, including
    creating, retrieving, and removing runs associated with the study, as well as saving
    or deleting the study itself from the database.

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

        create_run:
            Create a new run for this study and save it to the database.

        get_run:
            Retrieve a specific run associated with this study by its ID.

        get_all_runs:
            Retrieve all runs associated with this study.

        delete_run:
            Remove a specific run associated with this study by its ID.

        delete_all_runs:
            Remove all runs associated with this study from the database.

        save:
            Save the current state of the study to the database.

        load:
            (classmethod) Load a `StudyEntity` instance from the database by its ID.

        load_by_name:
            (classmethod) Load a `StudyEntity` instance from the database by its name.

        delete:
            (classmethod) Delete a study from the database by its ID. Optionally, remove all associated runs.
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
            f"runs={[run.__str__() for run in self.get_all_runs()]}, "
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
        runs_str = "Runs:\n"
        for run in self.get_all_runs():
            runs_str += f"  - ID: {run.get_id()}\n" f"    Workspace: {run.get_workspace()}\n"
        return (
            f"Study with ID {study_id}\n"
            f"------------{'-' * len(study_id)}\n"
            f"Name: {self.get_name()}\n"
            f"{runs_str}"
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
        updated_entity_info = self.backend.retrieve_study(study_id)
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

    def create_run(self, *args, **kwargs) -> RunEntity:  # pylint: disable=unused-argument
        """
        Create a run for this study. This will create a [`RunEntity`][db_scripts.run_entity.RunEntity]
        instance and link it to this study.

        As a side effect of this method, a new run will be added to the database. Additionally,
        the current status of this study will updated to include this new run.

        Returns:
            A [`RunEntity`][db_scripts.run_entity.RunEntity] instance representing
                the run that was created.
        """
        # Get all valid fields for the RunModel dataclass
        valid_fields = {f.name for f in RunModel.get_class_fields()}

        # Separate valid fields from additional data
        valid_kwargs = {}
        additional_data = {}
        for key, val in kwargs.items():
            if key in valid_fields:
                valid_kwargs[key] = val
            else:
                additional_data[key] = val

        # Create the RunModel object and save it to the backend
        new_run = RunModel(
            study_id=self.get_id(),
            **valid_kwargs,
            additional_data=additional_data,
        )
        run_entity = RunEntity(new_run, self.backend)
        run_entity.save()

        # Add the run ID to the study's list of runs
        self.entity_info.runs.append(new_run.id)
        self.save()  # Save the updated study to the backend

        return run_entity

    def get_run(self, run_id: str) -> RunEntity:
        """
        Given an ID, get the associated run from the database.

        Args:
            run_id: The ID of the run to retrieve.

        Returns:
            A [`RunEntity`][db_scripts.run_entity.RunEntity] instance representing
                the run that was queried.
        """
        return RunEntity.load(run_id, self.backend)

    def get_all_runs(self) -> List[RunEntity]:
        """
        Get every run associated with this study.

        Returns:
            A list of [`RunEntity`][db_scripts.run_entity.RunEntity] instances.
        """
        self.reload_data()
        return [self.get_run(run_id) for run_id in self.entity_info.runs]

    def delete_run(self, run_id: str):
        """
        Given an ID, remove the associated run from the database.

        Args:
            run_id: The ID of the run to remove.
        """
        RunEntity.delete(run_id, self.backend)
        self.entity_info.runs.remove(run_id)
        self.save()

    def delete_all_runs(self):
        """
        Remove every run associated with this study.
        """
        for run_id in self.entity_info.runs:
            self.delete_run(run_id)

    def save(self):
        """
        Save the current state of this study to the database.
        """
        self.backend.save_study(self.entity_info)

    @classmethod
    def load(cls, entity_id: str, backend: ResultsBackend) -> "StudyEntity":
        """
        Load a study from the database by id.

        Args:
            entity_id: The id of the study to load.
            backend: A [`ResultsBackend`][backends.results_backend.ResultsBackend] instance.

        Returns:
            A `StudyEntity` instance.

        Raises:
            (exceptions.StudyNotFoundError): If an entry for study with id `entity_id` was not
                found in the database.
        """
        entity_info = backend.retrieve_study(entity_id)
        if entity_info is None:
            raise StudyNotFoundError(f"Study with id '{entity_id}' not found in the database.")

        return cls(entity_info, backend)

    @classmethod
    def load_by_name(cls, study_name: str, backend: ResultsBackend) -> "StudyEntity":
        """
        Load a study from the database by study name.

        Args:
            study_name: The name of the study to load.
            backend: A [`ResultsBackend`][backends.results_backend.ResultsBackend] instance.

        Returns:
            A `StudyEntity` instance.

        Raises:
            (exceptions.StudyNotFoundError): If an entry for study with name `study_name` was
                not found in the database.
        """
        entity_info = backend.retrieve_study_by_name(study_name)
        if entity_info is None:
            raise StudyNotFoundError(f"Study with name '{study_name}' not found in the database.")

        return cls(entity_info, backend)

    @classmethod
    def delete(cls, entity_id: str, backend: ResultsBackend, remove_associated_runs: bool = True):
        """
        Delete a study from the database by id.

        By default, this will remove all of the runs associated with the study from the database.

        Args:
            entity_id: The name of the study to delete.
            backend: A [`ResultsBackend`][backends.results_backend.ResultsBackend] instance.
            remove_associated_runs: If True, remove all of the runs associated with this study from the db.
        """
        LOG.info(f"Deleting study with id '{entity_id}' from the database...")
        backend.delete_study(entity_id, remove_associated_runs=remove_associated_runs)
        LOG.info(f"Study '{entity_id}' has been successfully deleted.")
