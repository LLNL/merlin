##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module defines the abstract base class for all data store implementations in Merlin.

This module provides the `StoreBase` class, which outlines the required interface for saving,
retrieving, listing, and deleting entities from a backing data store. All concrete store classes
(e.g., Redis-based stores) must inherit from this class and implement its abstract methods.
"""

from abc import ABC, abstractmethod
from typing import Generic, List, TypeVar

from merlin.db_scripts.data_models import BaseDataModel


T = TypeVar("T", bound=BaseDataModel)


class StoreBase(ABC, Generic[T]):
    """
    Base class for all stores supported in Merlin.

    This class defines the methods that are needed for each store in Merlin.

    Methods:
        save: Save or update an object in the database.
        retrieve: Retrieve an entity from the database by ID.
        retrieve_all: Query the database for all entities of this type.
        delete: Delete an entity from the database by ID.
    """

    @abstractmethod
    def save(self, entity: BaseDataModel):
        """
        Save or update an object in the database.

        Args:
            entity: The object to save.
        """
        raise NotImplementedError("Subclasses of `StoreBase` must implement a `save` method.")

    @abstractmethod
    def retrieve(self, identifier: str) -> BaseDataModel:
        """
        Retrieve an entity from the database by an identifier.

        Args:
            identifier: The identifier (typically ID or name) of the entity to retrieve.

        Returns:
            The entity if found, None otherwise.
        """
        raise NotImplementedError("Subclasses of `StoreBase` must implement a `retrieve` method.")

    @abstractmethod
    def retrieve_all(self) -> List[BaseDataModel]:
        """
        Query the database for all entities of this type.

        Returns:
            A list of entities.
        """
        raise NotImplementedError("Subclasses of `StoreBase` must implement a `retrieve_all` method.")

    @abstractmethod
    def delete(self, identifier: str):
        """
        Delete an entity from the database by an identifier.

        Args:
            identifier: The identifier (typically ID or name) of the entity to delete.
        """
        raise NotImplementedError("Subclasses of `StoreBase` must implement a `delete` method.")
