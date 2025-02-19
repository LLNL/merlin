"""
"""
from abc import ABC, abstractmethod
from typing import Any, List

from merlin.db_scripts.data_formats import RunInfo, StudyInfo


class ResultsBackend(ABC):
    """
    Abstract base class for a results backend, which provides methods to save and retrieve 
    information from a backend database.

    This class defines the interface that must be implemented by any concrete backend.

    Attributes:
        backend_name: The name of the backend (e.g., "redis", "postgresql").

    Methods:
        get_name:
            Get the name of the backend.

        save_study:
            Save a [`StudyInfo`][merlin.db_scripts.data_formats.StudyInfo] object to the backend database.

        retrieve_study:
            Retrieve a [`StudyInfo`][merlin.db_scripts.data_formats.StudyInfo] object from the backend database by its name.

        retrieve_all_studies:
            Retrieve all studies currently stored in the backend database.

        delete_study:
            Delete a study from the backend database by its name. Optionally, remove all associated runs.

        save_run:
            Save a [`RunInfo`][merlin.db_scripts.data_formats.RunInfo] object to the backend database.

        retrieve_run:
            Retrieve a [`RunInfo`][merlin.db_scripts.data_formats.RunInfo] object from the backend database by its ID.

        retrieve_all_runs:
            Retrieve all runs currently stored in the backend database.

        delete_run:
            Delete a run from the backend database by its ID. This will also remove the run from the associated study's list of runs.
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

    def get_version(self) -> str:
        """
        Query the backend for the current version.

        Returns:
            A string representing the current version of the backend.
        """
        pass

    @abstractmethod
    def save_study(self, study: StudyInfo):
        """
        Given a [`StudyInfo`][merlin.db_scripts.data_formats.StudyInfo] object, enter all of
        it's information to the backend database.

        Args:
            study_info: A [`StudyInfo`][merlin.db_scripts.data_formats.StudyInfo] instance.
        """
        pass

    @abstractmethod
    def retrieve_study(self, study_name: str) -> StudyInfo:
        """
        Given a study's name, retrieve it from the backend database.

        Args:
            study_name: The name of the study to retrieve.

        Returns:
            A [`StudyInfo`][merlin.db_scripts.data_formats.StudyInfo] instance.
        """
        pass

    @abstractmethod
    def retrieve_all_studies(self) -> List[StudyInfo]:
        """
        Query the backend database for every study that's currently stored.

        Returns:
            A list of [`StudyInfo`][merlin.db_scripts.data_formats.StudyInfo] objects.
        """
        pass

    @abstractmethod
    def delete_study(self, study_name: str, remove_associated_runs: bool = True):
        """
        Given the name of the study, find it in the database and remove that entry.

        Args:
            study_name: The name of the study to remove from the database.
            remove_associated_runs: If true, remove the runs associated with this study.
        """
        pass

    @abstractmethod
    def save_run(self, run: RunInfo):
        """
        Given a RunInfo object, enter all of it's information to the backend database.

        Args:
            study_info: A [`RunInfo`][merlin.db_scripts.data_formats.RunInfo] instance.
        """
        pass

    @abstractmethod
    def retrieve_run(self, run_id: str) -> RunInfo:
        """
        Given a run's id, retrieve it from the backend database.

        Args:
            run_id: The ID of the run to retrieve.
            
        Returns:
            A [`RunInfo`][merlin.db_scripts.data_formats.RunInfo] instance.
        """
        pass

    @abstractmethod
    def retrieve_all_runs(self) -> List[RunInfo]:
        """
        Query the backend database for every study that's currently stored.

        Returns:
            A list of [`RunInfo`][merlin.db_scripts.data_formats.RunInfo] objects.
        """
        pass

    @abstractmethod
    def delete_run(self, run_id: str):
        """
        Given a run id, find it in the database and remove that entry. This will also
        delete the run id from the list of runs in the associated study's entry.

        Args:
            run_id: The id of the run to delete.
        """
        pass
