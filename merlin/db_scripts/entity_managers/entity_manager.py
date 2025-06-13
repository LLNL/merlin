##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module defines the abstract base class `EntityManager`, which provides a generic framework
for managing database-backed entities in the Merlin system.

`EntityManager` is designed to be subclassed for specific entity types (e.g., studies, runs,
workers), allowing consistent implementation of core CRUD operations (Create, Read, Update, Delete)
across different entity managers. It provides shared helper methods for common operations such as
creating entities if they do not exist, retrieving entities by ID, and deleting entities.
"""

import logging
from abc import ABC, abstractmethod
from typing import Any, Callable, Generic, List, Optional, Type, TypeVar

from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.data_models import BaseDataModel
from merlin.db_scripts.entities.db_entity import DatabaseEntity
from merlin.exceptions import RunNotFoundError, StudyNotFoundError, WorkerNotFoundError


T = TypeVar("T", bound=DatabaseEntity)
M = TypeVar("M", bound=BaseDataModel)

LOG = logging.getLogger("merlin")


class EntityManager(Generic[T, M], ABC):
    """
    Abstract base class for managing database entity lifecycles.

    This class defines a common interface and helper methods for managing entities stored in the
    database, such as studies, runs, and workers. Subclasses must implement methods to create,
    retrieve, and delete these entities using the provided backend.

    Generic Parameters:
        T (DatabaseEntity): The entity class managed by this manager.
        M (BaseDataModel): The data model class corresponding to the entity.

    Attributes:
        backend: The backend interface used to persist and retrieve entity data.

    Methods:
        create: Abstract method to create a new entity.
        get: Abstract method to retrieve a single entity by its identifier.
        get_all: Abstract method to retrieve all entities of this type.
        delete: Abstract method to delete a specific entity by its identifier.
        delete_all: Abstract method to delete all entities of this type.
        _create_entity_if_not_exists: Creates an entity only if it does not already exist in the backend.
        _get_entity: Retrieves an entity instance using its identifier.
        _get_all_entities: Retrieves all entities of a specific type from the backend.
        _delete_entity: Deletes an individual entity, optionally calling a cleanup function before deletion.
        _delete_all_by_type: Deletes all entities of a certain type using the provided getter and deleter functions.
    """

    def __init__(self, backend: ResultsBackend):
        """
        Initialize the EntityManager with a backend.

        Args:
            backend: The backend interface used to persist and retrieve entities.
        """
        self.backend = backend
        self.db = None  # Subclasses can set this by creating a set_db_reference method

    @abstractmethod
    def create(self, *args: Any, **kwargs: Any) -> T:
        """
        Create a new entity.

        This method must be implemented by subclasses to define how a specific entity is created.

        Args:
            *args (Any): Optional positional arguments for create context.
            **kwargs (Any): Optional keyword arguments for create context.

        Returns:
            The newly created entity instance.

        Raises:
            NotImplementedError: If a subclass has not implemented this method.
        """
        raise NotImplementedError("Subclasses of `EntityManager` must implement a `create` method.")

    @abstractmethod
    def get(self, identifier: str) -> T:
        """
        Retrieve a single entity by its identifier.

        Args:
            identifier (str): The unique identifier of the entity.

        Returns:
            The entity instance corresponding to the identifier.

        Raises:
            NotImplementedError: If a subclass has not implemented this method.
        """
        raise NotImplementedError("Subclasses of `EntityManager` must implement a `get` method.")

    @abstractmethod
    def get_all(self) -> List[T]:
        """
        Retrieve all entities managed by this entity manager.

        Returns:
            A list of all entities of the specified type.

        Raises:
            NotImplementedError: If a subclass has not implemented this method.
        """
        raise NotImplementedError("Subclasses of `EntityManager` must implement a `get_all` method.")

    @abstractmethod
    def delete(self, identifier: str, **kwargs: Any):
        """
        Delete a specific entity by its identifier.

        Args:
            identifier (str): The unique identifier of the entity to delete.
            **kwargs (Any): Optional keyword arguments for deletion context (e.g., cleanup flags).

        Raises:
            NotImplementedError: If a subclass has not implemented this method.
        """
        raise NotImplementedError("Subclasses of `EntityManager` must implement a `delete` method.")

    @abstractmethod
    def delete_all(self, **kwargs: Any):
        """
        Delete all entities managed by this entity manager.

        Args:
            **kwargs (Any): Optional keyword arguments for deletion context.

        Raises:
            NotImplementedError: If a subclass has not implemented this method.
        """
        raise NotImplementedError("Subclasses of `EntityManager` must implement a `delete_all` method.")

    def _create_entity_if_not_exists(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        entity_class: Type[T],
        model_class: Type[M],
        identifier: str,
        log_message_exists: str,
        log_message_create: str,
        **model_kwargs: Any,
    ) -> T:
        """
        Helper method to create an entity if it does not already exist.

        Args:
            entity_class (Type[T]): The class used to load and instantiate the entity.
            model_class (Type[M]): The data model class used to create a new entity if needed.
            identifier (str): The unique identifier of the entity.
            log_message_exists (str): Log message to emit if the entity already exists.
            log_message_create (str): Log message to emit when creating a new entity.
            **model_kwargs (Any): Keyword arguments passed to the model class constructor.

        Returns:
            The existing or newly created entity.
        """
        try:
            entity = entity_class.load(identifier, self.backend)
            LOG.info(log_message_exists)
        except (WorkerNotFoundError, StudyNotFoundError, RunNotFoundError):
            LOG.info(log_message_create)
            model = model_class(**model_kwargs)
            entity = entity_class(model, self.backend)
            entity.save()
        return entity

    def _get_entity(self, entity_class: Type[T], identifier: str) -> T:
        """
        Retrieve a single entity instance using its identifier.

        Args:
            entity_class (Type[T]): The class used to load the entity.
            identifier (str): The unique identifier of the entity.

        Returns:
            The loaded entity instance.
        """
        return entity_class.load(identifier, self.backend)

    def _get_all_entities(self, entity_class: Type[T], entity_type: str) -> List[T]:
        """
        Retrieve all entities of a specific type from the backend.

        Args:
            entity_class (Type[T]): The class used to instantiate each entity.
            entity_type (str): The type identifier used by the backend to filter entities.

        Returns:
            A list of all entities of the specified type.
        """
        all_entities = self.backend.retrieve_all(entity_type)
        if not all_entities:
            return []
        return [entity_class(entity_data, self.backend) for entity_data in all_entities]

    def _delete_entity(self, entity_class: Type[T], identifier: str, cleanup_fn: Optional[Callable] = None):
        """
        Delete a single entity, optionally performing cleanup beforehand.

        Args:
            entity_class (Type[T]): The class used to load and delete the entity.
            identifier (str): The unique identifier of the entity.
            cleanup_fn (Optional[Callable]): Optional function to perform cleanup before deletion.
        """
        entity = self._get_entity(entity_class, identifier)
        if cleanup_fn:
            cleanup_fn(entity)
        entity_class.delete(identifier, self.backend)

    def _delete_all_by_type(self, get_all_fn: Callable, delete_fn: Callable, entity_name: str, **delete_kwargs: Any):
        """
        Delete all entities of a specific type using provided getter and deleter functions.

        Args:
            get_all_fn (Callable): Function to retrieve all entities.
            delete_fn (Callable): Function to delete an individual entity by ID.
            entity_name (str): Human-readable name of the entity type, used for logging.
            **delete_kwargs (Any): Additional keyword arguments passed to the delete function.
        """
        all_entities = get_all_fn()
        if all_entities:
            for entity in all_entities:
                delete_fn(entity.get_id(), **delete_kwargs)
        else:
            LOG.warning(f"No {entity_name} found in the database.")
