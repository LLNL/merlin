##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module contains the functionality necessary to interact with everything
stored in Merlin's database.
"""

import logging
from typing import Any, Dict, List

from merlin.backends.backend_factory import backend_factory
from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.entity_managers.entity_manager import EntityManager
from merlin.db_scripts.entity_managers.logical_worker_manager import LogicalWorkerManager
from merlin.db_scripts.entity_managers.physical_worker_manager import PhysicalWorkerManager
from merlin.db_scripts.entity_managers.run_manager import RunManager
from merlin.db_scripts.entity_managers.study_manager import StudyManager
from merlin.exceptions import EntityManagerNotSupportedError


LOG = logging.getLogger("merlin")


class MerlinDatabase:
    """
    High-level interface for accessing Merlin database entities.

    This class provides a unified interface to all entity managers in Merlin.

    Attributes:
        backend (backends.results_backend.ResultsBackend): A `ResultsBackend` instance.
        logical_workers (db_scripts.entity_managers.logical_worker_manager.LogicalWorkerManager):
            A `LogicalWorkerManager` instance.
        physical_workers (db_scripts.entity_managers.physical_worker_manager.PhysicalWorkerManager):
            A `PhysicalWorkerManager` instance.
        runs (db_scripts.entity_managers.run_manager.RunManager): A `RunManager` instance.
        studies (db_scripts.entity_managers.study_manager.StudyManager): A `StudyManager` instance.

    Methods:
        get_db_type: Retrieve the type of the backend being used (e.g., Redis, SQL).
        get_db_version: Retrieve the version of the backend.
        get_connection_string: Retrieve the backend connection string.
        create: Create a new entity of the specified type.
        get: Get an entity by type and identifier.
        get_all:  Get all entities of a specific type.
        delete: Delete an entity by type and identifier.
        delete_all: Delete all entities of a specific type.
        get_everything: Get all entities from all entity managers.
        delete_everything: Delete all entities from all entity managers.
    """

    def __init__(self):
        """
        Initialize a new MerlinDatabase instance.
        """
        from merlin.config.configfile import CONFIG  # pylint: disable=import-outside-toplevel

        self.backend: ResultsBackend = backend_factory.get_backend(CONFIG.results_backend.name.lower())
        self._entity_managers: Dict[str, EntityManager] = {
            "study": StudyManager(self.backend),
            "run": RunManager(self.backend),
            "logical_worker": LogicalWorkerManager(self.backend),
            "physical_worker": PhysicalWorkerManager(self.backend),
        }

        # Set up cross-references for managers that need them
        for manager in self._entity_managers.values():
            if hasattr(manager, "set_db_reference"):
                manager.set_db_reference(self)

    # Provide direct access to entity managers for convenience
    @property
    def studies(self) -> StudyManager:
        """
        Get the study manager.

        Returns:
            A [`StudyManager`][db_scripts.entity_managers.study_manager.StudyManager]
                instance.
        """
        return self._entity_managers["study"]

    @property
    def runs(self) -> RunManager:
        """
        Get the run manager.

        Returns:
            A [`RunManager`][db_scripts.entity_managers.run_manager.RunManager]
                instance.
        """
        return self._entity_managers["run"]

    @property
    def logical_workers(self) -> LogicalWorkerManager:
        """
        Get the logical worker manager.

        Returns:
            A [`LogicalWorkerManager`][db_scripts.entity_managers.logical_worker_manager.LogicalWorkerManager]
                instance.
        """
        return self._entity_managers["logical_worker"]

    @property
    def physical_workers(self) -> PhysicalWorkerManager:
        """
        Get the physical worker manager.

        Returns:
            A [`PhysicalWorkerManager`][db_scripts.entity_managers.physical_worker_manager.PhysicalWorkerManager]
                instance.
        """
        return self._entity_managers["physical_worker"]

    def get_db_type(self) -> str:
        """
        Retrieve the type of backend.

        Returns:
            The type of backend (e.g. redis, sql, etc.).
        """
        return self.backend.get_name()

    def get_db_version(self) -> str:
        """
        Get the version of the backend.

        Returns:
            The version number of the backend.
        """
        return self.backend.get_version()

    def get_connection_string(self) -> str:
        """
        Get the connection string to the backend.

        Returns:
            The connection string to the backend.
        """
        return self.backend.get_connection_string()

    def _validate_entity_type(self, entity_type: str):
        """
        Check to make sure the entity type passed in is supported.

        Args:
            entity_type: The type of entity to validate (study, run, logical_worker, physical_worker).
        """
        if entity_type not in self._entity_managers:
            raise EntityManagerNotSupportedError(f"Entity type not supported: {entity_type}")

    def create(self, entity_type: str, *args, **kwargs) -> Any:
        """
        Create a new entity of the specified type.

        Args:
            entity_type: The type of entity to create (study, run, logical_worker, physical_worker).

        Returns:
            The created entity.

        Raises:
            EntityManagerNotSupportedError: If the entity type is not supported.
        """
        self._validate_entity_type(entity_type)
        return self._entity_managers[entity_type].create(*args, **kwargs)

    def get(self, entity_type: str, *args, **kwargs) -> Any:
        """
        Get an entity by type and identifier.

        Args:
            entity_type: The type of entity to get (study, run, logical_worker, physical_worker).

        Returns:
            The requested entity.

        Raises:
            EntityManagerNotSupportedError: If the entity type is not supported.
        """
        self._validate_entity_type(entity_type)
        return self._entity_managers[entity_type].get(*args, **kwargs)

    def get_all(self, entity_type: str) -> List[Any]:
        """
        Get all entities of a specific type.

        Args:
            entity_type: The type of entities to get (study, run, logical_worker, physical_worker).

        Returns:
            A list of all entities of the specified type.

        Raises:
            EntityManagerNotSupportedError: If the entity type is not supported.
        """
        self._validate_entity_type(entity_type)
        return self._entity_managers[entity_type].get_all()

    def delete(self, entity_type: str, *args, **kwargs) -> None:
        """
        Delete an entity by type and identifier.

        Args:
            entity_type: The type of entity to delete (study, run, logical_worker, physical_worker).

        Raises:
            EntityManagerNotSupportedError: If the entity type is not supported.
        """
        self._validate_entity_type(entity_type)
        self._entity_managers[entity_type].delete(*args, **kwargs)

    def delete_all(self, entity_type: str, **kwargs) -> None:
        """
        Delete all entities of a specific type.

        Args:
            entity_type: The type of entities to delete (study, run, logical_worker, physical_worker).

        Raises:
            EntityManagerNotSupportedError: If the entity type is not supported.
        """
        self._validate_entity_type(entity_type)
        self._entity_managers[entity_type].delete_all(**kwargs)

    def get_everything(self) -> List[Any]:
        """
        Get all entities from all entity managers.

        Returns:
            A dictionary mapping entity types to lists of entities.
        """
        result = []
        for manager in self._entity_managers.values():
            result.extend(manager.get_all())
        return result

    def delete_everything(self, force: bool = False) -> None:
        """
        Delete all entities from all entity managers.

        This method deletes studies last to ensure proper cleanup of dependencies.
        """
        flush_database = False
        if force:
            flush_database = True
        else:
            # Ask the user for confirmation
            valid_inputs = ["y", "n"]
            user_input = input("Are you sure you want to flush the entire database? (y/n): ").strip().lower()
            while user_input not in valid_inputs:
                user_input = input("Invalid input. Use 'y' for 'yes' or 'n' for 'no': ").strip().lower()

            if user_input == "y":
                flush_database = True

        if flush_database:
            LOG.info("Flushing the database...")
            self.backend.flush_database()
            LOG.info("Database successfully flushed.")
        else:
            LOG.info("Database flush cancelled.")
