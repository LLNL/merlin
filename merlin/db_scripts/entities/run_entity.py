##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Module for managing database entities related to runs.

This module provides functionality for interacting with runs stored in a database,
including creating, retrieving, updating, and deleting runs. It defines the `RunEntity`
class, which extends the abstract base class [`DatabaseEntity`][db_scripts.entities.db_entity.DatabaseEntity],
to encapsulate run-specific operations and behaviors.
"""

import logging
import os
from typing import List

from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.data_models import RunModel
from merlin.db_scripts.entities.db_entity import DatabaseEntity
from merlin.db_scripts.entities.mixins.queue_management import QueueManagementMixin
from merlin.exceptions import RunNotFoundError


LOG = logging.getLogger("merlin")


class RunEntity(DatabaseEntity[RunModel], QueueManagementMixin):
    """
    A class representing a run in the database.

    This class provides methods to interact with and manage a run's data, including
    retrieving information about the run, updating its state, and saving or deleting
    it from the database.

    Attributes:
        entity_info (db_scripts.data_models.RunModel): An instance of the `RunModel` class
            containing the run's metadata.
        backend (backends.results_backend.ResultsBackend): An instance of the `ResultsBackend`
            class used to interact with the database.
        run_complete (bool): A property to get or set the completion status of the run.

    Methods:
        __repr__:
            Provide a string representation of the `RunEntity` instance.

        __str__:
            Provide a human-readable string representation of the `RunEntity` instance.

        reload_data:
            Reload the latest data for this run from the database.

        get_id:
            Retrieve the ID of the run. _Implementation found in
                [`DatabaseEntity.get_id`][db_scripts.entities.db_entity.DatabaseEntity.get_id]._

        get_additional_data:
            Retrieve any additional data saved to this run. _Implementation found in
                [`DatabaseEntity.get_additional_data`][db_scripts.entities.db_entity.DatabaseEntity.get_additional_data]._

        get_metadata_file:
            Retrieve the path to the metadata file for this run.

        get_metadata_filepath:
            (classmethod) Retrieve the path to the metadata file for a given workspace.

        get_study_id:
            Retrieve the ID of the study associated with this run.

        get_workspace:
            Retrieve the path to the output workspace for this run.

        get_queues:
            Retrieve the task queues used for this run.

        get_workers:
            Retrieve the workers used for this run.

        add_worker:
            Add a worker to the list of workers used for this run.

        remove_worker:
            Remove a worker from the list of workers used for this run.

        get_parent:
            Retrieve the ID of the parent run that launched this run (if any).

        get_child:
            Retrieve the ID of the child run launched by this run (if any).

        save:
            Save the current state of the run to the database and dump its metadata.

        dump_metadata:
            Dump all metadata for this run to a JSON file.

        load:
            (classmethod) Load a `RunEntity` instance from the database by its ID or workspace.

        delete:
            (classmethod) Delete a run from the database by its ID or workspace.
    """

    def __init__(self, run_info: RunModel, backend: ResultsBackend):
        """
        Initialize a `RunEntity` instance.

        Args:
            run_info (db_scripts.data_models.RunModel): The data model containing
                information about the run.
            backend (backends.results_backend.ResultsBackend): The backend instance used to
                interact with the database.
        """
        super().__init__(run_info, backend)
        self._metadata_file = self.get_metadata_filepath(self.get_workspace())

    @classmethod
    def _get_entity_type(cls) -> str:
        return "run"

    def __repr__(self) -> str:
        """
        Provide a string representation of the `RunEntity` instance.

        Returns:
            A human-readable string representation of the `RunEntity` instance.
        """
        return (
            f"RunEntity("
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
        Provide a string representation of the `RunEntity` instance.

        Returns:
            A human-readable string representation of the `RunEntity` instance.
        """
        from merlin.db_scripts.entities.study_entity import StudyEntity  # pylint: disable=import-outside-toplevel

        run_id = self.get_id()
        study = StudyEntity.load(self.get_study_id(), self.backend)
        study_str = f"  - ID: {study.get_id()}\n    Name: {study.get_name()}"
        return (
            f"Run with ID {run_id}\n"
            f"------------{'-' * len(run_id)}\n"
            f"Workspace: {self.get_workspace()}\n"
            f"Study:\n{study_str}\n"
            f"Queues: {self.get_queues()}\n"
            f"Workers: {self.get_workers()}\n"
            f"Parent: {self.get_parent()}\n"
            f"Child: {self.get_child()}\n"
            f"Run Complete: {self.run_complete}\n"
            f"Additional Data: {self.get_additional_data()}\n\n"
        )

    @property
    def run_complete(self) -> bool:
        """
        An attribute representing whether this run is complete.

        A "complete" study is a study that has executed all steps.

        Returns:
            True if the study is complete. False, otherwise.
        """
        self.reload_data()
        return self.entity_info.run_complete

    @run_complete.setter
    def run_complete(self, value: bool):
        """
        Update the run's completion status.

        Args:
            value: The completion status of the run.
        """
        self.entity_info.run_complete = value

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
        This is needed for the [`load`][db_scripts.entities.run_entity.RunEntity.load]
        method when loading from workspace as it can't use the non-classmethod version
        of this method.

        Args:
            workspace: The workspace directory for the run.

        Returns:
            The path to the metadata file.
        """
        return os.path.join(workspace, "merlin_info", "run_metadata.json")

    def get_study_id(self) -> str:
        """
        Get the ID for the study associated with this run.

        Returns:
            The ID for the study associated with this run.
        """
        return self.entity_info.study_id

    def get_workspace(self) -> str:
        """
        Get the path to the output workspace for this run.

        Returns:
            A string representing the output workspace for this run.
        """
        return self.entity_info.workspace

    def get_workers(self) -> List[str]:
        """
        Get the logical workers that this run is using.

        Returns:
            A list of logical worker ids.
        """
        return self.entity_info.workers

    def add_worker(self, worker_id: str):
        """
        Add a new worker id to the list of workers.

        Args:
            worker_id: The id of the worker to add.
        """
        self.entity_info.workers.append(worker_id)
        self.save()

    def remove_worker(self, worker_id: str):
        """
        Remove a worker id from the list of workers associated with this run.

        Does *not* delete a [`LogicalWorkerEntity`][db_scripts.entities.logical_worker_entity.LogicalWorkerEntity]
        from the database. This will only remove the worker's id from the list in this run.

        Args:
            worker_id: The ID of the worker to remove.
        """
        self.reload_data()
        self.entity_info.workers.remove(worker_id)
        self.save()

    def get_parent(self) -> str:
        """
        Get the ID of the run that launched this run (if any).

        This will only be set for iterative workflows with greater than 1 iteration.

        Returns:
            The ID of the run that launched this run.
        """
        self.reload_data()
        return self.entity_info.parent

    def get_child(self) -> str:
        """
        Get the ID of the run that was launched by this run (if any).

        This will only be set for iterative workflows with greater than 1 iteration.

        Returns:
            The ID of the run that was launched by this run.
        """
        self.reload_data()
        return self.entity_info.child

    def _post_save_hook(self):
        """
        Hook called after saving the run entity.
        For runs, we also need to dump metadata to a file.
        """
        self.dump_metadata()

    def dump_metadata(self):
        """
        Dump all of the metadata for this run to a json file.
        """
        self.entity_info.dump_to_json_file(self.get_metadata_file())

    @classmethod
    def load(cls, entity_identifier: str, backend: ResultsBackend) -> "RunEntity":
        """
        Load a run from the database by id or workspace.

        Args:
            entity_identifier: The ID or workspace of the run to load.
            backend: A [`ResultsBackend`][backends.results_backend.ResultsBackend] instance.

        Returns:
            A `RunEntity` instance.

        Raises:
            (exceptions.RunNotFoundError): If an entry for run was not found in the database.
        """
        if os.path.isdir(entity_identifier) and os.path.exists(entity_identifier):  # Load from workspace
            LOG.debug("Retrieving run from workspace.")
            metadata_file = cls.get_metadata_filepath(entity_identifier)
            entity_info = RunModel.load_from_json_file(metadata_file)
        else:  # Load from ID
            LOG.debug("Retrieving run from backend.")
            entity_info = backend.retrieve(entity_identifier, "run")

        if not entity_info:
            raise RunNotFoundError(f"Run with ID or workspace {entity_identifier} not found in the database.")

        return cls(entity_info, backend)

    @classmethod
    def delete(cls, entity_identifier: str, backend: ResultsBackend):
        """
        Delete a run from the database by id or workpsace.

        Args:
            entity_identifier: The ID or workspace of the run to delete.
            backend: A [`ResultsBackend`][backends.results_backend.ResultsBackend] instance.
        """
        LOG.debug(f"Deleting run with id or workspace '{entity_identifier}' from the database...")
        self = cls.load(entity_identifier, backend)
        backend.delete(self.get_id(), "run")
        LOG.info(f"Run with id or workspace '{entity_identifier}' has been successfully deleted.")
