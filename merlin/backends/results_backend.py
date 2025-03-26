"""
This module contains the base class for all supported
backends in Merlin.
"""

from abc import ABC, abstractmethod
from typing import List

from merlin.db_scripts.data_models import RunModel, StudyModel, WorkerModel


class ResultsBackend(ABC):
    """
    Abstract base class for a results backend, which provides methods to save and retrieve
    information from a backend database.

    This class defines the interface that must be implemented by any concrete backend.

    Attributes:
        backend_name: The name of the backend (e.g., "redis", "postgresql").

    Methods:
        get_name:
            Retrieve the name of the backend.

        get_version:
            Query the backend for the current version.

        save_study:
            Save a [`StudyModel`][db_scripts.data_models.StudyModel] object to the backend database.

        retrieve_study:
            Retrieve a [`StudyModel`][db_scripts.data_models.StudyModel] object from the backend database by its ID.

        retrieve_study_by_name:
            Retrieve a [`StudyModel`][db_scripts.data_models.StudyModel] object from the backend database by its name.

        retrieve_all_studies:
            Retrieve all studies currently stored in the backend database.

        delete_study:
            Delete a study from the backend database by its name. Optionally, remove all associated runs.

        save_run:
            Save a [`RunModel`][db_scripts.data_models.RunModel] object to the backend database.

        retrieve_run:
            Retrieve a [`RunModel`][db_scripts.data_models.RunModel] object from the backend database by its ID.

        retrieve_all_runs:
            Retrieve all runs currently stored in the backend database.

        delete_run:
            Delete a run from the backend database by its ID. This will also remove the run from the associated study's
            list of runs.

        save_worker:
            Save a [`WorkerModel`][db_scripts.data_models.WorkerModel] object to the backend database.

        retrieve_worker:
            Retrieve a [`WorkerModel`][db_scripts.data_models.WorkerModel] object from the backend database by its ID.

        retrieve_worker_by_name:
            Retrieve a [`WorkerModel`][db_scripts.data_models.WorkerModel] object from the backend database by its name.

        retrieve_all_workers:
            Retrieve all workers currently stored in the backend database.

        delete_worker:
            Delete a worker from the backend database by its ID.
    """

    def __init__(self, backend_name: str):
        self.backend_name: str = backend_name

    def get_name(self) -> str:
        """
        Get the name of the backend.

        Returns:
            The name of the backend (e.g. redis).
        """
        return self.backend_name

    @abstractmethod
    def get_version(self) -> str:
        """
        Query the backend for the current version.

        Returns:
            A string representing the current version of the backend.
        """

    @abstractmethod
    def save_study(self, study: StudyModel):
        """
        Given a [`StudyModel`][db_scripts.data_models.StudyModel] object, enter all of
        it's information to the backend database.

        Args:
            study: A [`StudyModel`][db_scripts.data_models.StudyModel] instance.
        """

    @abstractmethod
    def retrieve_study(self, study_id: str) -> StudyModel:
        """
        Given a study's id, retrieve it from the backend database.

        Args:
            study_id: The id of the study to retrieve.

        Returns:
            A [`StudyModel`][db_scripts.data_models.StudyModel] instance.
        """

    @abstractmethod
    def retrieve_study_by_name(self, study_name: str) -> StudyModel:
        """
        Given a study's name, retrieve it from the backend database.

        Args:
            study_name: The name of the study to retrieve.

        Returns:
            A [`StudyModel`][db_scripts.data_models.StudyModel] instance.
        """

    @abstractmethod
    def retrieve_all_studies(self) -> List[StudyModel]:
        """
        Query the backend database for every study that's currently stored.

        Returns:
            A list of [`StudyModel`][db_scripts.data_models.StudyModel] objects.
        """

    @abstractmethod
    def delete_study(self, study_name: str, remove_associated_runs: bool = True):
        """
        Given the name of the study, find it in the database and remove that entry.

        Args:
            study_name: The name of the study to remove from the database.
            remove_associated_runs: If true, remove the runs associated with this study.
        """

    @abstractmethod
    def save_run(self, run: RunModel):
        """
        Given a [`RunModel`][db_scripts.data_models.RunModel] object, enter all of
        it's information to the backend database.

        Args:
            run: A [`RunModel`][db_scripts.data_models.RunModel] instance.
        """

    @abstractmethod
    def retrieve_run(self, run_id: str) -> RunModel:
        """
        Given a run's id, retrieve it from the backend database.

        Args:
            run_id: The ID of the run to retrieve.

        Returns:
            A [`RunModel`][db_scripts.data_models.RunModel] instance.
        """

    @abstractmethod
    def retrieve_all_runs(self) -> List[RunModel]:
        """
        Query the backend database for every run that's currently stored.

        Returns:
            A list of [`RunModel`][db_scripts.data_models.RunModel] objects.
        """

    @abstractmethod
    def delete_run(self, run_id: str):
        """
        Given a run id, find it in the database and remove that entry. This will also
        delete the run id from the list of runs in the associated study's entry.

        Args:
            run_id: The id of the run to delete.
        """

    @abstractmethod
    def save_worker(self, worker: WorkerModel):
        """
        Given a [`WorkerModel`][db_scripts.data_models.WorkerModel] object, enter
        all of it's information to the backend database.

        Args:
            worker: A [`WorkerModel`][db_scripts.data_models.WorkerModel] instance.
        """

    @abstractmethod
    def retrieve_worker(self, worker_id: str) -> WorkerModel:
        """
        Given a worker's id, retrieve it from the backend database.

        Args:
            worker_id: The ID of the worker to retrieve.

        Returns:
            A [`WorkerModel`][db_scripts.data_models.WorkerModel] instance.
        """

    @abstractmethod
    def retrieve_worker_by_name(self, worker_name: str) -> WorkerModel:
        """
        Given a worker's name, retrieve it from the backend database.

        Args:
            worker_name: The name of the worker to retrieve.

        Returns:
            A [`WorkerModel`][db_scripts.data_models.WorkerModel] instance.
        """

    @abstractmethod
    def retrieve_all_workers(self) -> List[WorkerModel]:
        """
        Query the backend database for every worker that's currently stored.

        Returns:
            A list of [`WorkerModel`][db_scripts.data_models.WorkerModel] objects.
        """

    @abstractmethod
    def delete_worker(self, worker_id: str):
        """
        Given a worker id, find it in the database and remove that entry.

        Args:
            worker_id: The id of the worker to delete.
        """