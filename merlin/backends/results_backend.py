"""
"""
from abc import ABC, abstractmethod
from typing import Any

from merlin.db_scripts.data_formats import RunInfo, StudyInfo


class ResultsBackend(ABC):
    """
    Abstract base class for a results backend, which provides methods to save and retrieve 
    information from a backend database.

    This class defines the interface that must be implemented by any concrete backend.

    Attributes:
        backend_name: The name of the backend (e.g., "redis", "postgresql").

    Methods:
        get_name: Get the name of the backend.
        retrieve_all_studies: Retrieve all studies currently stored in the backend database.
        retrieve_run: Retrieve a [`RunInfo`][merlin.db_scripts.data_formats.RunInfo] object from the backend
            database by its ID.
        retrieve_study: Retrieve a [`StudyInfo`][merlin.db_scripts.data_formats.StudyInfo] object from the
            backend database by its name.
        save_run: Save a [`RunInfo`][merlin.db_scripts.data_formats.RunInfo] object to the backend database.
        save_study: Save a [`StudyInfo`][merlin.db_scripts.data_formats.StudyInfo] object to the backend database.
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
