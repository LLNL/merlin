"""
This module contains the functionality necessary to interact with studies
stored in Merlin's database.
"""

import logging
from typing import Dict, List

from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.data_formats import RunInfo, StudyInfo
from merlin.db_scripts.db_run import DatabaseRun
from merlin.exceptions import StudyNotFoundError


LOG = logging.getLogger("merlin")


class DatabaseStudy:
    """
    A class representing a study in the database.

    This class provides methods to interact with and manage a study's data, including
    creating, retrieving, and removing runs associated with the study, as well as saving
    or deleting the study itself from the database.

    Attributes:
        study_info: An instance of the `StudyInfo` class containing the study's metadata.
        backend: An instance of the `ResultsBackend` class used to interact
            with the database.

    Methods:
        __str__:
            Provide a string representation of the `DatabaseStudy` instance.

        get_id:
            Retrieve the unique ID of the study.

        get_name:
            Retrieve the name of the study.

        get_additional_data:
            Retrieve any additional metadata associated with the study.

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

        load (classmethod):
            Load a `DatabaseStudy` instance from the database by its name.

        delete (classmethod):
            Delete a study from the database by its name. Optionally, remove all associated runs.
    """

    def __init__(self, study_info: StudyInfo, backend: ResultsBackend):
        self.study_info: StudyInfo = study_info
        self.backend: ResultsBackend = backend

    def __repr__(self) -> str:
        """
        Provide a string representation of the `DatabaseStudy` instance.

        Returns:
            A human-readable string representation of the `DatabaseStudy` instance.
        """
        return (
            f"DatabaseStudy("
            f"id={self.get_id()}, "
            f"name={self.get_name()}, "
            f"runs={[run.__str__() for run in self.get_all_runs()]}, "
            f"additional_data={self.get_additional_data()}, "
            f"backend={self.backend.get_name()})"
        )

    def __str__(self) -> str:
        """
        Provide a string representation of the `DatabaseStudy` instance.

        Returns:
            A human-readable string representation of the `DatabaseStudy` instance.
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

    def _get_latest_data(self) -> StudyInfo:
        """
        Retrieve the latest dynamic data for the study from the database.

        Returns:
            A [`StudyInfo`][merlin.db_scripts.db_formats.StudyInfo] object
                containing the latest dynamic data.
        """
        return self.backend.retrieve_study(self.get_name())

    def reload_data(self):
        """
        Reload the latest data for this study from the database and update the
        [`StudyInfo`][merlin.db_scripts.db_formats.StudyInfo] object.
        """
        study_name = self.get_name()
        updated_study_info = self.backend.retrieve_study(study_name)
        if not updated_study_info:
            raise StudyNotFoundError(f"Study with name {study_name} not found in the database.")
        self.study_info = updated_study_info

    def get_id(self) -> str:
        """
        Get the ID for this study.

        Returns:
            The ID for this study.
        """
        return self.study_info.id

    def get_name(self) -> str:
        """
        Get the name associated with this study.

        Returns:
            The name for this study.
        """
        return self.study_info.name

    def get_additional_data(self) -> Dict:
        """
        Get any additional data saved to this study.

        Returns:
            Additional data saved to this study.
        """
        return self._get_latest_data().additional_data

    def create_run(self, *args, **kwargs) -> DatabaseRun:  # pylint: disable=unused-argument
        """
        Create a run for this study. This will create a [`DatabaseRun`][merlin.db_scripts.db_run.DatabaseRun]
        instance and link it to this study.

        As a side effect of this method, a new run will be added to the database. Additionally,
        the current status of this study will updated to include this new run.

        Returns:
            A [`DatabaseRun`][merlin.db_scripts.db_run.DatabaseRun] instance representing
                the run that was created.
        """
        # Get all valid fields for the RunInfo dataclass
        valid_fields = {f.name for f in RunInfo.get_class_fields()}

        # Separate valid fields from additional data
        valid_kwargs = {}
        additional_data = {}
        for key, val in kwargs.items():
            if key in valid_fields:
                valid_kwargs[key] = val
            else:
                additional_data[key] = val

        # Create the RunInfo object and save it to the backend
        new_run = RunInfo(
            study_id=self.get_id(),
            **valid_kwargs,
            additional_data=additional_data,
        )
        db_run = DatabaseRun(new_run, self.backend)
        db_run.save()

        # Add the run ID to the study's list of runs
        self.study_info.runs.append(new_run.id)
        self.save()  # Save the updated study to the backend

        return db_run

    def get_run(self, run_id: str) -> DatabaseRun:
        """
        Given an ID, get the associated run from the database.

        Args:
            run_id: The ID of the run to retrieve.

        Returns:
            A [`DatabaseRun`][merlin.db_scripts.db_run.DatabaseRun] instance representing
                the run that was queried.
        """
        return DatabaseRun.load(run_id, self.backend)

    def get_all_runs(self) -> List[DatabaseRun]:
        """
        Get every run associated with this study.

        Returns:
            A list of [`DatabaseRun`][merlin.db_scripts.db_run.DatabaseRun] instances.
        """
        return [self.get_run(run_id) for run_id in self._get_latest_data().runs]

    def delete_run(self, run_id: str):
        """
        Given an ID, remove the associated run from the database.

        Args:
            run_id: The ID of the run to remove.
        """
        DatabaseRun.delete(run_id, self.backend)
        self.study_info.runs.remove(run_id)
        self.save()

    def delete_all_runs(self):
        """
        Remove every run associated with this study.
        """
        for run_id in self.study_info.runs:
            self.delete_run(run_id)

    def save(self):
        """
        Save the current state of this study to the database.
        """
        self.backend.save_study(self.study_info)

    @classmethod
    def load(cls, study_name: str, backend: ResultsBackend) -> "DatabaseStudy":
        """
        Load a study from the database.

        Args:
            study_name: The name of the study to load.
            backend: A [`ResultsBackend`][merlin.backends.results_backend.ResultsBackend] instance.

        Returns:
            A `DatabaseStudy` instance.

        Raises:
            ValueError: If the study can't be retrieved from the database.
        """
        study_info = backend.retrieve_study(study_name)
        if study_info is None:
            raise StudyNotFoundError(f"Study with name '{study_name}' not found in the database.")

        return cls(study_info, backend)

    @classmethod
    def delete(cls, study_name: str, backend: ResultsBackend, remove_associated_runs: bool = True):
        """
        Delete a study from the database.

        By default, this will remove all of the runs associated with the study from the database.

        Args:
            study_name: The name of the study to delete.
            backend: A [`ResultsBackend`][merlin.backends.results_backend.ResultsBackend] instance.
            remove_associated_runs: If True, remove all of the runs associated with this study from the db.
        """
        LOG.info(f"Deleting study '{study_name}' from the database...")
        backend.delete_study(study_name, remove_associated_runs=remove_associated_runs)
        LOG.info(f"Study '{study_name}' has been successfully deleted.")
