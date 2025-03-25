"""
This module contains the functionality necessary to interact with runs
stored in Merlin's database.
"""

import logging
import os
from typing import Dict, List

from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.data_models import RunModel
from merlin.exceptions import RunNotFoundError


LOG = logging.getLogger("merlin")


class DatabaseRun:
    """
    A class representing a run in the database.

    This class provides methods to interact with and manage a run's data, including
    retrieving information about the run, updating its state, and saving or deleting
    it from the database.

    Attributes:
        run_info: An instance of the `RunModel` class containing the run's metadata.
        backend: An instance of the `ResultsBackend` class used to interact
            with the database.
        run_complete: Property to get or set the completion status of the run.

    Methods:
        get_metadata_file:
            Retrieve the path to the metadata file for this run.

        get_metadata_filepath (classmethod):
            Retrieve the path to the metadata file for a given workspace.

        get_id:
            Retrieve the ID of the run.

        get_study_id:
            Retrieve the ID of the study associated with this run.

        get_workspace:
            Retrieve the path to the output workspace for this run.

        get_queues:
            Retrieve the task queues used for this run.

        get_parent:
            Retrieve the ID of the parent run that launched this run (if any).

        get_child:
            Retrieve the ID of the child run launched by this run (if any).

        get_additional_data:
            Retrieve any additional data saved to this run.

        save:
            Save the current state of the run to the database and dump its metadata.

        dump_metadata:
            Dump all metadata for this run to a JSON file.

        load (classmethod):
            Load a `DatabaseRun` instance from the database by its ID.

        load_from_metadata_file (classmethod):
            Load a `DatabaseRun` instance from a metadata file.

        delete (classmethod):
            Delete a run from the database by its ID.
    """

    def __init__(self, run_info: RunModel, backend: ResultsBackend):
        self.run_info: RunModel = run_info
        self.backend: ResultsBackend = backend
        self._metadata_file = self.get_metadata_filepath(self.get_workspace())

    def __repr__(self) -> str:
        """
        Provide a string representation of the `DatabaseRun` instance.

        Returns:
            A human-readable string representation of the `DatabaseRun` instance.
        """
        return (
            f"DatabaseRun("
            f"id={self.get_id()}, "
            f"study_id={self.get_study_id()}, "
            f"workspace={self.get_workspace()}, "
            f"queues={self.get_queues()}, "
            f"workers={self.get_workers()}, "
            f"parent={self.get_parent()}, "
            f"child={self.get_child()}, "
            f"run_complete={self.run_complete}, "
            f"additional_data={self.get_additional_data()}, "
            f"backend={self.backend.get_name()})"
        )

    def __str__(self) -> str:
        """
        Provide a string representation of the `DatabaseRun` instance.

        Returns:
            A human-readable string representation of the `DatabaseRun` instance.
        """
        run_id = self.get_id()
        return (
            f"Run with ID {run_id}\n"
            f"------------{'-' * len(run_id)}\n"
            f"Workspace: {self.get_workspace()}\n"
            f"Study ID: {self.get_study_id()}\n"
            f"Queues: {self.get_queues()}\n"
            f"Workers: {self.get_workers()}\n"
            f"Parent: {self.get_parent()}\n"
            f"Child: {self.get_child()}\n"
            f"Run Complete: {self.run_complete}\n"
            f"Additional Data: {self.get_additional_data()}\n\n"
        )

    def reload_data(self):
        """
        Reload the latest data for this run from the database and update the
        [`RunModel`][merlin.db_scripts.db_formats.RunModel] object.
        """
        updated_run_info = self.backend.retrieve_run(self.run_info.id)
        if not updated_run_info:
            raise RunNotFoundError(f"Run with ID {self.run_info.id} not found in the database.")
        self.run_info = updated_run_info

    @property
    def run_complete(self) -> bool:
        """
        An attribute representing whether this run is complete.

        A "complete" study is a study that has executed all steps.

        Returns:
            True if the study is complete. False, otherwise.
        """
        self.reload_data()
        return self.run_info.run_complete

    @run_complete.setter
    def run_complete(self, value: bool):
        """
        Update the run's completion status.

        Args:
            value: The completion status of the run.
        """
        self.run_info.run_complete = value

    def get_metadata_file(self) -> str:
        """
        Get the path to the metadata file for this run.

        Returns:
            The path to the metadata file for this run
        """
        return self._metadata_file

    @classmethod
    def get_metadata_filepath(cls, workspace: str) -> str:
        """
        Get the path to the metadata file for a given workspace.
        This is needed for the [`load_from_metadata_file`][merlin.db_scripts.db_run.DatabaseRun.load_from_metadata_file]
        method as it can't use the non-classmethod version of this method.

        Args:
            workspace: The workspace directory for the run.

        Returns:
            The path to the metadata file.
        """
        return os.path.join(workspace, "merlin_info", "run_metadata.json")

    def get_id(self) -> str:
        """
        Get the ID for this run.

        Returns:
            The ID for this run.
        """
        return self.run_info.id

    def get_study_id(self) -> str:
        """
        Get the ID for the study associated with this run.

        Returns:
            The ID for the study associated with this run.
        """
        return self.run_info.study_id

    def get_workspace(self) -> str:
        """
        Get the path to the output workspace for this run.

        Returns:
            A string representing the output workspace for this run.
        """
        return self.run_info.workspace

    def get_queues(self) -> List[str]:
        """
        Get the task queues that were used for this run.

        Returns:
            A list of strings representing the queues that were used for this run.
        """
        return self.run_info.queues

    def get_workers(self) -> List[str]:
        """
        Get the workers that were used for this run.

        Returns:
            A list of strings representing the workers that were used for this run.
        """
        return self.run_info.workers

    def get_parent(self) -> str:
        """
        Get the ID of the run that launched this run (if any).

        This will only be set for iterative workflows with greater than 1 iteration.

        Returns:
            The ID of the run that launched this run.
        """
        self.reload_data()
        return self.run_info.parent

    def get_child(self) -> str:
        """
        Get the ID of the run that was launched by this run (if any).

        This will only be set for iterative workflows with greater than 1 iteration.

        Returns:
            The ID of the run that was launched by this run.
        """
        self.reload_data()
        return self.run_info.child

    def get_additional_data(self) -> Dict:
        """
        Get any additional data saved to this run.

        Returns:
            Additional data saved to this run.
        """
        self.reload_data()
        return self.run_info.additional_data

    def save(self):
        """
        Save the current state of this run to the database. This will also re-dump
        the metadata of this run to the output workspace in case something was updated.
        """
        self.backend.save_run(self.run_info)
        self.dump_metadata()

    def dump_metadata(self):
        """
        Dump all of the metadata for this run to a json file.
        """
        self.run_info.dump_to_json_file(self.get_metadata_file())

    @classmethod
    def load(cls, run_id: str, backend: ResultsBackend) -> "DatabaseRun":
        """
        Load a run from the database.

        Args:
            run_id: The ID of the run to load.
            backend: A [`ResultsBackend`][merlin.backends.results_backend.ResultsBackend] instance.

        Returns:
            A `DatabaseRun` instance.
        """
        run_info = backend.retrieve_run(run_id)
        if not run_info:
            raise RunNotFoundError(f"Run with ID {run_id} not found in the database.")

        return cls(run_info, backend)

    @classmethod
    def load_from_metadata_file(cls, metadata_file: str, backend: ResultsBackend) -> "DatabaseRun":
        """
        Load a run from a metadata file.

        Args:
            metadata_file: The path to the metadata file to load from.
            backend: A [`ResultsBackend`][merlin.backends.results_backend.ResultsBackend] instance.

        Returns:
            A `DatabaseRun` instance.
        """
        return cls(RunModel.load_from_json_file(metadata_file), backend)

    @classmethod
    def delete(cls, run_id: str, backend: ResultsBackend):
        """
        Delete a run from the database.

        Args:
            run_id: The ID of the run to delete.
            backend: A [`ResultsBackend`][merlin.backends.results_backend.ResultsBackend] instance.
        """
        LOG.info(f"Deleting run with id '{run_id}' from the database...")
        backend.delete_run(run_id)
        LOG.info(f"Run with id '{run_id}' has been successfully deleted.")
