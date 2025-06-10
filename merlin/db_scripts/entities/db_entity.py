##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module defines the `DatabaseEntity` abstract base class, which serves as a common interface
for interacting with database entities such as studies, runs, and workers. The `DatabaseEntity`
class provides a standardized structure for managing these entities, including methods for
saving, deleting, reloading, and retrieving additional metadata.

Classes that inherit from `DatabaseEntity` must implement the abstract methods defined in the base
class, ensuring consistency across different types of database entities. This abstraction reduces
code duplication and promotes maintainability by centralizing shared functionality.
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Generic, Type, TypeVar

from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.data_models import BaseDataModel
from merlin.exceptions import EntityNotFoundError, RunNotFoundError, StudyNotFoundError, WorkerNotFoundError


# Type variable for entity models
T = TypeVar("T", bound=BaseDataModel)
# Type variable for entity classes
E = TypeVar("E", bound="DatabaseEntity")

LOG = logging.getLogger(__name__)


class DatabaseEntity(Generic[T], ABC):
    """
    Abstract base class for database entities such as runs, studies, and workers.

    This class defines the common interface and behavior for interacting with
    database entities, including saving, deleting, and reloading data.

    Attributes:
        entity_info (T): The data model containing information about the entity.
        backend (backends.results_backend.ResultsBackend): The backend instance used
            to interact with the database.

    Methods:
        __repr__:
            Provide a string representation of the entity.

        __str__:
            Provide a human-readable string representation of the entity.

        reload_data:
            Reload the latest data for this entity from the database.

        get_id:
            Get the unique ID for this entity.

        get_additional_data:
            Get any additional data saved to this entity.

        save:
            Save the current state of this entity to the database.

        _post_save_hook:
            Hook called after saving the entity to the database. Subclasses can override
            this method to add additional behavior.

        load:
            Load an entity from the database by its ID.

        delete:
            Delete an entity from the database by its ID.

        entity_type:
            Get the type of this entity for database operations.

        _get_entity_type:
            Get the entity type for database operations (used internally).
    """

    # Mapping of entity types to their error classes
    _error_classes = {
        "logical_worker": WorkerNotFoundError,
        "physical_worker": WorkerNotFoundError,
        "run": RunNotFoundError,
        "study": StudyNotFoundError,
    }

    def __init__(self, entity_info: T, backend: ResultsBackend):
        """
        Initialize a `DatabaseEntity` instance.

        Args:
            entity_info (T): The data model containing information about the entity.
            backend (backends.results_backend.ResultsBackend): The backend instance used to
                interact with the database.
        """
        self.entity_info: T = entity_info
        self.backend: ResultsBackend = backend

    @abstractmethod
    def __repr__(self) -> str:
        """
        Provide a string representation of the entity.
        """
        raise NotImplementedError("Subclasses of `DatabaseEntity` must implement a `__repr__` method.")

    @abstractmethod
    def __str__(self) -> str:
        """
        Provide a human-readable string representation of the entity.
        """
        raise NotImplementedError("Subclasses of `DatabaseEntity` must implement a `__str__` method.")

    @property
    def entity_type(self) -> str:
        """
        Get the type of this entity for database operations.

        Returns:
            The type name as a string.
        """
        return self._get_entity_type()

    @classmethod
    def _get_entity_type(cls) -> str:
        """
        Get the entity type for database operations.

        Returns:
            The type name as a string.
        """
        # Default implementation based on class name
        # Can be overridden by subclasses if needed
        return cls.__name__.lower().replace("entity", "")

    def reload_data(self):
        """
        Reload the latest data for this entity from the database.

        Raises:
            (exceptions.EntityNotFoundError): If an entry for this entity was not found in the database.
        """
        entity_id = self.get_id()
        updated_entity_info = self.backend.retrieve(entity_id, self.entity_type)
        if not updated_entity_info:
            error_class = self._error_classes.get(self.entity_type, EntityNotFoundError)
            raise error_class(f"{self.entity_type.capitalize()} with ID {entity_id} not found in the database.")
        self.entity_info = updated_entity_info

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

    def save(self):
        """
        Save the current state of this entity to the database.
        """
        self.backend.save(self.entity_info)
        self._post_save_hook()

    def _post_save_hook(self):
        """
        Hook called after saving the entity to the database.
        Subclasses can override to add additional behavior.
        """

    @classmethod
    def load(cls: Type[E], entity_identifier: str, backend: ResultsBackend) -> E:
        """
        Load an entity from the database by its ID.

        Args:
            entity_identifier: The ID of the entity to load.
            backend (backends.results_backend.ResultsBackend): A `ResultsBackend` instance.

        Returns:
            An instance of the entity.

        Raises:
            (exceptions.EntityNotFoundError): If an entry for the entity was not found in the database.
        """
        entity_info = backend.retrieve(entity_identifier, cls._get_entity_type())
        if not entity_info:
            error_class = cls._error_classes.get(cls._get_entity_type(), EntityNotFoundError)
            raise error_class(f"{cls._get_entity_type().capitalize()} with ID {entity_identifier} not found in the database.")

        return cls(entity_info, backend)

    @classmethod
    def delete(cls, entity_identifier: str, backend: ResultsBackend):
        """
        Delete an entity from the database by its ID.

        Args:
            entity_identifier: The ID of the entity to delete.
            backend (backends.results_backend.ResultsBackend): A ResultsBackend instance.
        """
        entity_type = cls._get_entity_type()
        LOG.debug(f"Deleting {entity_type} with ID '{entity_identifier}' from the database...")
        backend.delete(entity_identifier, entity_type)
        LOG.info(f"{entity_type.capitalize()} with ID '{entity_identifier}' has been successfully deleted.")
