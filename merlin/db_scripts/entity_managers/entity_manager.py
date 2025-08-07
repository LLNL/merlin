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
from typing import Any, Callable, Dict, Generic, List, Optional, Type, TypeVar

from merlin.backends.filter_support_mixin import FilterSupportMixin
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
        _filter_accessor_map: A dictionary mapping supported filter keys to accessor functions
            for the entity type. Used by filtering logic (e.g., in `get_all`) to dynamically
            retrieve values from entity instances. Subclasses must override this to enable
            filtering support.

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

    _filter_accessor_map: Dict[str, Callable[[T], Any]] = {}

    def __init__(self, backend: ResultsBackend):
        """
        Initialize the EntityManager with a backend.

        Args:
            backend: The backend interface used to persist and retrieve entities.
        """
        self.backend = backend
        self.db = None  # Subclasses can set this by creating a set_db_reference method
        self._entity_type = None  # Subclasses need to set this
        self._entity_class = None  # Subclasses need to set this

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

    def _matches_filters(self, entity: T, filters: Dict) -> bool:
        """
        Determines whether a given entity matches all provided filter criteria.

        This method uses a predefined mapping of filter keys to accessor functions
        (`_filter_accessor_map` which subclasses will need to implement) to retrieve
        values from the entity. It supports both scalar and list-based comparisons.
        For list filters (e.g., "queues"), the filter matches if any expected value
        is present in the entity's corresponding list.

        Args:
            entity: The entity instance to check against the filters.
            filters: A dictionary of filter keys and values used to narrow down the query results.
                    Filter keys must correspond to entries in the `_filter_accessor_map` defined
                    by the subclass. Values are compared against the entity’s corresponding attributes
                    or methods (e.g., {"name": "foo"}, {"queues": ["queue1", "queue2"]}).

        Returns:
            True if the entity matches all filter conditions, False otherwise.
        """
        for key, expected in filters.items():
            # Obtain the correct getter method
            accessor = self._filter_accessor_map.get(key, None)
            if not accessor:
                LOG.warning(f"Could not obtain accessor for filter '{key}'. Skipping this filter.")
                continue

            # Call the getter on the entity to get the actual value
            actual = accessor(entity)

            LOG.debug(f"actual for filter '{key}': {actual}")
            LOG.debug(f"expected for filter '{key}': {expected}")

            # Case where filter is a list
            if isinstance(expected, list):
                # Match if any expected value is in the actual list (e.g., queues)
                if not isinstance(actual, (list, set)) or not any(val in actual for val in expected):
                    return False
            # Case where filter is str or bool
            else:
                if actual != expected:
                    return False
        return True

    def get_all(self, filters: Dict = None) -> List[T]:
        """
        Retrieve all entities managed by this entity manager, optionally filtered by attributes.

        Args:
            filters: A dictionary of filter keys and values used to narrow down the query results.
                 Filter keys must correspond to supported filters defined in the ENTITY_REGISTRY
                 for the given entity type. Values are compared against entity attributes or
                 accessor methods (e.g., {"name": "foo"}, {"queues": ["queue1", "queue2"]}).

        Returns:
            A list of all entities of the specified type matching the filters.
        """
        if isinstance(self.backend, FilterSupportMixin) and filters:
            LOG.debug(f"Using backend filtering with filters: {filters}")
            raw_entities = self.backend.retrieve_all_filtered(self._entity_type, filters)
        else:
            mode = "with in-memory filtering" if filters else "without filters"
            LOG.debug(f"Using full retrieval {mode}.")
            raw_entities = self.backend.retrieve_all(self._entity_type)

        if not raw_entities:
            return []

        entities = [self._entity_class(data, self.backend) for data in raw_entities]

        if filters and not isinstance(self.backend, FilterSupportMixin):
            entities = [entity for entity in entities if self._matches_filters(entity, filters)]
            LOG.info(f"Filtered down to {len(entities)} entities using in-memory filters: {filters}")

        return entities

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
