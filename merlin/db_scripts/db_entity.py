"""
This module defines the `DatabaseEntity` abstract base class, which serves as a common interface 
for interacting with database entities such as studies, runs, and workers. The `DatabaseEntity` 
class provides a standardized structure for managing these entities, including methods for 
saving, deleting, reloading, and retrieving additional metadata.

Classes that inherit from `DatabaseEntity` must implement the abstract methods defined in the base 
class, ensuring consistency across different types of database entities. This abstraction reduces 
code duplication and promotes maintainability by centralizing shared functionality.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List

from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.data_models import BaseDataModel


class DatabaseEntity(ABC):
    """
    Abstract base class for database entities such as runs, studies, and workers.

    This class defines the common interface and behavior for interacting with
    database entities, including saving, deleting, and reloading data.
    """

    def __init__(self, entity_info: BaseDataModel, backend: ResultsBackend):
        self.entity_info: BaseDataModel = entity_info
        self.backend: ResultsBackend = backend

    @abstractmethod
    def __repr__(self) -> str:
        """
        Provide a string representation of the entity.
        """

    @abstractmethod
    def __str__(self) -> str:
        """
        Provide a human-readable string representation of the entity.
        """

    @abstractmethod
    def reload_data(self):
        """
        Reload the latest data for this entity from the database.
        """

    def get_id(self) -> str:
        """
        Get the unique ID for this entity.

        Returns:
            The unique ID for this entity.
        """
        return self.entity_info.id

    def get_additional_data(self) -> Dict:
        """
        Get any additional data saved to this entity.

        Returns:
            A dictionary of additional data.
        """
        self.reload_data()
        return self.entity_info.additional_data

    @abstractmethod
    def save(self):
        """
        Save the current state of this entity to the database.
        """

    @classmethod
    @abstractmethod
    def load(cls, entity_id: str, backend: Any) -> "DatabaseEntity":
        """
        Load an entity from the database by its ID.

        Args:
            entity_id: The ID of the entity to load.
            backend: The backend instance used to interact with the database.

        Returns:
            An instance of the entity.
        """

    @classmethod
    @abstractmethod
    def delete(cls, entity_id: str, backend: Any):
        """
        Delete an entity from the database by its ID.

        Args:
            entity_id: The ID of the entity to delete.
            backend: The backend instance used to interact with the database.
        """
