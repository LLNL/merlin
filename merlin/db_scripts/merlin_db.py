"""
This module contains the functionality necessary to interact with everything
stored in Merlin's database.
"""

import logging
from typing import Callable, List

from merlin.backends.backend_factory import backend_factory
from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.data_models import BaseDataModel, LogicalWorkerModel, PhysicalWorkerModel, RunModel, StudyModel
from merlin.db_scripts.db_entity import DatabaseEntity
from merlin.db_scripts.logical_worker_entity import LogicalWorkerEntity
from merlin.db_scripts.physical_worker_entity import PhysicalWorkerEntity
from merlin.db_scripts.run_entity import RunEntity
from merlin.db_scripts.study_entity import StudyEntity
from merlin.exceptions import RunNotFoundError, StudyNotFoundError, WorkerNotFoundError


LOG = logging.getLogger("merlin")


class MerlinDatabase:
    """
    A class that provides a high-level interface for interacting with database entities.

    This class abstracts the interaction with various database backends (e.g., Redis, SQLAlchemy)
    and provides methods to manage database entities. It ensures that database operations are
    consistent and centralized, allowing for the creation, retrieval, and deletion of entities.

    Attributes:
        backend: An instance of a backend class (e.g., RedisBackend, SQLAlchemyBackend) used
            to interact with the database.

    Methods:
        General:\n
            - [`get_db_type`][db_scripts.merlin_db.MerlinDatabase.get_db_type]: Retrieve the type
                of the backend being used (e.g., Redis, SQL).
            - [`get_db_version`][db_scripts.merlin_db.MerlinDatabase.get_db_version]: Retrieve the
                version of the backend.
            - [`get_connection_string`][db_scripts.merlin_db.MerlinDatabase.get_connection_string]:
                Retrieve the backend connection string.
            - [`get_everything`][db_scripts.merlin_db.MerlinDatabase.get_everything]: Retrieve every
                entity in the database.
            - [`delete_everything`][db_scritps.merlin_db.MerlinDatabase.delete_everything]: Removes
                every entry from the database.

        Study Management:\n
            - [`create_study`][db_scripts.merlin_db.MerlinDatabase.create_study]: Create a new study
                in the database if it does not already exist.
            - [`get_study`][db_scripts.merlin_db.MerlinDatabase.get_study]: Retrieve a specific study
                by its ID or name.
            - [`get_all_studies`][db_scripts.merlin_db.MerlinDatabase.get_all_studies]: Retrieve all
                studies currently stored in the database.
            - [`delete_study`][db_scripts.merlin_db.MerlinDatabase.delete_study]: Remove a specific
                study by its ID or name, with an option to also remove associated runs.
            - [`delete_all_studies`][db_scripts.merlin_db.MerlinDatabase.delete_all_studies]: Remove
                all studies from the database, with an option to also remove associated runs.

        Run Management:\n
            - [`create_run`][db_scripts.merlin_db.MerlinDatabase.create_run]: Create a new run for a
                study. If the study does not exist, it will be created first.
            - [`get_run`][db_scripts.merlin_db.MerlinDatabase.get_run]: Retrieve a specific run by its ID
                or workspace.
            - [`get_all_runs`][db_scripts.merlin_db.MerlinDatabase.get_all_runs]: Retrieve all runs
                currently stored in the database.
            - [`delete_run`][db_scripts.merlin_db.MerlinDatabase.delete_run]: Remove a specific run by
                its ID or workspace.
            - [`delete_all_runs`][db_scripts.merlin_db.MerlinDatabase.delete_all_runs]: Remove all runs
                from the database.

        Logical Worker Management:\n
            - [`create_logical_worker`][db_scripts.merlin_db.MerlinDatabase.create_logical_worker]: Create a
                new logical worker in the database.
            - [`get_logical_worker`][db_scripts.merlin_db.MerlinDatabase.get_logical_worker]: Retrieve a
                specific logical worker by its ID.
            - [`get_all_logical_workers`][db_scripts.merlin_db.MerlinDatabase.get_all_logical_workers]: Retrieve
                all logical workers currently stored in the database.
            - [`delete_logical_worker`][db_scripts.merlin_db.MerlinDatabase.delete_logical_worker]: Remove a
                specific logical worker by its ID.
            - [`delete_all_logical_workers`][db_scripts.merlin_db.MerlinDatabase.delete_all_logical_workers]:
                Remove all logical workers from the database.

        Physical Worker Management:\n
            - [`create_physical_worker`][db_scripts.merlin_db.MerlinDatabase.create_physical_worker]: Create a
                new physical worker in the database.
            - [`get_physical_worker`][db_scripts.merlin_db.MerlinDatabase.get_physical_worker]: Retrieve a
                specific physical worker by its ID or name.
            - [`get_all_physical_workers`][db_scripts.merlin_db.MerlinDatabase.get_all_physical_workers]: Retrieve
                all physical workers currently stored in the database.
            - [`delete_physical_worker`][db_scripts.merlin_db.MerlinDatabase.delete_physical_worker]: Remove a
                specific physical worker by its ID or name.
            - [`delete_all_physical_workers`][db_scripts.merlin_db.MerlinDatabase.delete_all_physical_workers]:
                Remove all physical workers from the database.
    """

    def __init__(self):
        from merlin.config.configfile import CONFIG  # pylint: disable=import-outside-toplevel

        self.backend: ResultsBackend = backend_factory.get_backend(CONFIG.results_backend.name.lower())

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

    def get_connection_string(self):
        """
        Get the connection string to the backend.

        Returns:
            The connection string to the backend.
        """
        return self.backend.get_connection_string()

    def _create_entity_if_not_exists(  # pylint: disable=too-many-arguments
        self,
        entity_class: DatabaseEntity,
        model_class: BaseDataModel,
        identifier: str,
        log_message_exists: str,
        log_message_create: str,
        **model_kwargs,
    ) -> DatabaseEntity:
        """
        Helper method to create an entity if it does not already exist in the database.

        Args:
            entity_class: The class of the entity to create (e.g., `StudyEntity`, `RunEntity`).
            model_class: The class of the model used to initialize the entity (e.g., `StudyModel`, `RunModel`).
            identifier: The identifier used to check if the entity exists.
            backend: The database backend.
            log_message_exists: Log message when the entity already exists.
            log_message_create: Log message when the entity is being created.
            model_kwargs: Additional keyword arguments for the model.

        Returns:
            An instance of the entity class.
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

    def _get_entity(self, entity_class: DatabaseEntity, identifier: str):
        """
        Helper method to retrieve an entity from the database.

        Args:
            entity_class: The class of the entity to retrieve (e.g., `StudyEntity`, `RunEntity`).
            identifier: The identifier used to locate the entity (e.g., ID or name).
            backend: The database backend.

        Returns:
            An instance of the entity class.
        """
        return entity_class.load(identifier, self.backend)

    def _get_all_entities(self, entity_class: DatabaseEntity, entity_type: str) -> List[DatabaseEntity]:
        """
        Helper method to retrieve all entities of a specific type from the database.

        Args:
            entity_class: The class of the entity to instantiate (e.g., `StudyEntity`, `RunEntity`).
            entity_type: The type of entity to retrieve from the backend (e.g., "study", "run").

        Returns:
            A list of instances of the specified entity class.
        """
        all_entities = self.backend.retrieve_all(entity_type)
        if not all_entities:
            return []
        return [entity_class(entity_data, self.backend) for entity_data in all_entities]

    def _delete_entity(self, entity_class, identifier: str, cleanup_fn: Callable = None):
        """
        Helper method to delete an entity from the database.

        Args:
            entity_class: The class of the entity to delete (e.g., `StudyEntity`, `RunEntity`).
            identifier: The identifier used to locate the entity (e.g., ID or name).
            cleanup_fn: A function to perform cleanup operations before deletion (optional).
        """
        entity = self._get_entity(entity_class, identifier)
        if cleanup_fn:
            cleanup_fn(entity)
        entity_class.delete(identifier, self.backend)

    def _delete_all_by_type(self, get_all_fn: Callable, delete_fn: Callable, entity_name: str, **delete_kwargs):
        """
        Helper method to delete all entities of a specific type from the database.

        Args:
            get_all_fn: Function to retrieve all entities (e.g., `self.get_all_studies`).
            delete_fn: Function to delete a single entity (e.g., `self.delete_study`).
            entity_name: Name of the entity type for logging purposes (e.g., "studies", "runs").
            delete_kwargs: Additional keyword arguments to pass to the delete function.
        """
        all_entities = get_all_fn()
        if all_entities:
            for entity in all_entities:
                delete_fn(entity.get_id(), **delete_kwargs)
        else:
            LOG.warning(f"No {entity_name} found in the database.")

    def create_study(self, study_name: str) -> StudyEntity:
        """
        Create [`StudyEntity`][merlin.db_scripts.study_entity.StudyEntity] instance and save
        it to the database, if one does not already exist.

        Args:
            study_name: The name of the study to create.

        Returns:
            A [`StudyEntity`][merlin.db_scripts.study_entity.StudyEntity] instance.
        """
        return self._create_entity_if_not_exists(
            entity_class=StudyEntity,
            model_class=StudyModel,
            identifier=study_name,
            log_message_exists=f"Study with name '{study_name}' already has an entry in the database.",
            log_message_create=f"Study with name '{study_name}' does not yet have an entry in the database. Creating one...",
            name=study_name,
        )

    def get_study(self, study_id_or_name: str) -> StudyEntity:
        """
        Given a study id or name, retrieve the associated study from the database.

        Args:
            study_id_or_name: The id or name of the study to retrieve.

        Returns:
            A [`StudyEntity`][merlin.db_scripts.study_entity.StudyEntity] instance representing
                the study that was queried.
        """
        return self._get_entity(StudyEntity, study_id_or_name)

    def get_all_studies(self) -> List[StudyEntity]:
        """
        Get every study that's currently in the database.

        Returns:
            A list of [`StudyEntity`][merlin.db_scripts.study_entity.StudyEntity] instances.
        """
        return self._get_all_entities(StudyEntity, "study")

    def delete_study(self, study_id_or_name: str, remove_associated_runs: bool = True):
        """
        Given a study id or name, remove the associated study from the database. As a consequence
        of this action, any runs associated with this study will also be removed, unless
        `remove_associated_runs` is set to `False`.

        Args:
            study_id_or_name: The id or name of the study to remove.
            remove_associated_runs: If True, remove all runs associated with the study.
        """

        def cleanup_study(study):
            if remove_associated_runs:
                for run_id in study.get_runs():
                    self.delete_run(run_id)

        self._delete_entity(StudyEntity, study_id_or_name, cleanup_fn=cleanup_study)

    def delete_all_studies(self, remove_associated_runs: bool = True):
        """
        Remove every study in the database.

        Args:
            remove_associated_runs: If True, remove all runs associated with every study we delete.
                Essentially removes all runs as well as all studies.
        """
        self._delete_all_by_type(
            get_all_fn=self.get_all_studies,
            delete_fn=self.delete_study,
            entity_name="studies",
            remove_associated_runs=remove_associated_runs,
        )

    def create_run(self, study_name: str, workspace: str, queues: List[str], **kwargs) -> RunEntity:
        """
        Given a study name, create a run for this study. If the study does not yet exist in
        the database, an entry will be created for it prior to the run being created.

        Args:
            study_name: The name of the study that this run is associated with.
            workspace: The output workspace for the run.
            queues: The task queues for the run.

        Returns:
            A [`RunEntity`][merlin.db_scripts.run_entity.RunEntity] instance.
        """
        # This will only create a new study if one does not already exist
        study_entity = self.create_study(study_name)

        # Get all valid fields for the RunModel dataclass
        valid_fields = {f.name for f in RunModel.get_class_fields()}
        valid_kwargs = {key: val for key, val in kwargs.items() if key in valid_fields}
        additional_data = {key: val for key, val in kwargs.items() if key not in valid_fields}

        # Create the RunModel object and save it to the backend
        new_run = RunModel(
            study_id=study_entity.get_id(),
            workspace=workspace,
            queues=queues,
            **valid_kwargs,
            additional_data=additional_data,
        )
        run_entity = RunEntity(new_run, self.backend)
        run_entity.save()

        # Add the run ID to the study's list of runs
        study_entity.add_run(run_entity.get_id())

        return run_entity

    def get_run(self, run_id_or_workspace: str) -> RunEntity:
        """
        Given a run id or workspace, retrieve the associated run from the database.

        Args:
            run_id_or_workspace: The id or workspace of the run to retrieve.

        Returns:
            A [`RunEntity`][merlin.db_scripts.run_entity.RunEntity] instance representing
                the run that was queried.
        """
        return self._get_entity(RunEntity, run_id_or_workspace)

    def get_all_runs(self) -> List[RunEntity]:
        """
        Get every run that's currently in the database.

        Returns:
            A list of [`RunEntity`][merlin.db_scripts.run_entity.RunEntity] instances.
        """
        return self._get_all_entities(RunEntity, "run")

    def delete_run(self, run_id_or_workspace: str):
        """
        Given a run id or workspace, remove the associated run from the database.

        Args:
            run_id_or_workspace: The id or workspace of the run to remove.
        """

        def cleanup_run(run):
            # Remove the run's id from the study's run list
            try:
                study = self.get_study(run.get_study_id())
                study.remove_run(run.get_id())
            except StudyNotFoundError:  # If the study isn't found then move on
                LOG.warning(f"Couldn't find study with id {run.get_study_id()}. Continuing with run delete.")

            # Remove the run's id from all of its logical worker's runs list
            for worker_id in run.get_workers():
                try:
                    logical_worker = self.get_logical_worker(worker_id)
                    logical_worker.remove_run(run.get_id())
                except WorkerNotFoundError:  # If the logical worker isn't found then move on
                    LOG.warning(f"Couldn't find logical worker with id {worker_id}. Continuing with run delete.")

        self._delete_entity(RunEntity, run_id_or_workspace, cleanup_fn=cleanup_run)

    def delete_all_runs(self):
        """
        Remove every run in the database.
        """
        self._delete_all_by_type(get_all_fn=self.get_all_runs, delete_fn=self.delete_run, entity_name="runs")

    def _resolve_worker_id(self, worker_id: str = None, worker_name: str = None, queues: List[str] = None) -> str:
        """
        Resolve the worker ID based on the provided arguments.

        Args:
            worker_id: The unique ID of the logical worker.
            worker_name: The name of the logical worker.
            queues: The list of queues assigned to the logical worker.

        Returns:
            The resolved worker ID.

        Raises:
            ValueError: If neither `worker_id` is provided nor both `worker_name` and `queues` are provided,
                or if both `worker_id` and `worker_name`/`queues` are provided.
        """
        # Enforce that *either* worker_id is provided *or* worker_name and queues, but not both or neither.
        if worker_id is not None:
            if worker_name is not None or queues is not None:
                raise ValueError("Provide either `worker_id` or (`worker_name` and `queues`), but not both.")
            return worker_id
        if worker_name is None or queues is None:
            raise ValueError("You must provide either `worker_id` or both `worker_name` and `queues`.")

        # Generate the worker_id if worker_name and queues are provided
        return LogicalWorkerModel.generate_id(worker_name, queues)

    def create_logical_worker(self, name: str, queues: List[str]) -> LogicalWorkerEntity:
        """
        Create a new logical worker in the database (if one doesn't exist) and return a
        [`LogicalWorkerEntity`][merlin.db_scripts.logical_worker_entity.LogicalWorkerEntity]
        instance.

        Args:
            name: The name of the worker.

        Returns:
            A [`LogicalWorkerEntity`][merlin.db_scripts.logical_worker_entity.LogicalWorkerEntity]
                instance representing the newly created worker.
        """
        logical_worker_id = self._resolve_worker_id(worker_name=name, queues=queues)
        log_message_create = (
            f"Logical worker with name '{name}' and queues '{queues}' does not yet have "
            "an entry in the database. Creating one..."
        )
        log_message_exists = f"Logical worker with name '{name}' and queues '{queues}' already has an entry in the database."
        return self._create_entity_if_not_exists(
            entity_class=LogicalWorkerEntity,
            model_class=LogicalWorkerModel,
            identifier=logical_worker_id,
            log_message_exists=log_message_exists,
            log_message_create=log_message_create,
            name=name,
            queues=queues,
        )

    def get_logical_worker(
        self, worker_id: str = None, worker_name: str = None, queues: List[str] = None
    ) -> LogicalWorkerEntity:
        """
        Retrieve a logical worker by either its ID or by its name and queues.

        Expected Usages:
            ```python
            get_logical_worker(worker_id=worker_id)
            get_logical_worker(worker_name=worker_name, queues=queues)
            ```

        Args:
            worker_id: The unique ID of the logical worker.
            worker_name: The name of the logical worker.
            queues: The list of queues assigned to the logical worker.

        Returns:
            A [`LogicalWorkerEntity`][merlin.db_scripts.logical_worker_entity.LogicalWorkerEntity] instance.

        Raises:
            ValueError: If neither `worker_id` is provided nor both `worker_name` and `queues` are provided,
                or if both `worker_id` and `worker_name`/`queues` are provided.
        """
        worker_id = self._resolve_worker_id(worker_id=worker_id, worker_name=worker_name, queues=queues)
        return self._get_entity(LogicalWorkerEntity, worker_id)

    def get_all_logical_workers(self) -> List[LogicalWorkerEntity]:
        """
        Get every logical worker that's currently in the database.

        Returns:
            A list of
                [`LogicalWorkerEntity`][merlin.db_scripts.logical_worker_entity.LogicalWorkerEntity]
                instances.
        """
        return self._get_all_entities(LogicalWorkerEntity, "logical_worker")

    def delete_logical_worker(self, worker_id: str = None, worker_name: str = None, queues: List[str] = None):
        """
        Delete a logical worker from the database by either its ID or by its name and queues.

        Expected Usages:
            ```python
            delete_logical_worker(worker_id=worker_id)
            delete_logical_worker(worker_name=worker_name, queues=queues)
            ```

        Args:
            worker_id: The unique ID of the logical worker.
            worker_name: The name of the logical worker.
            queues: The list of queues assigned to the logical worker.

        Raises:
            ValueError: If neither `worker_id` is provided nor both `worker_name` and `queues` are provided,
                or if both `worker_id` and `worker_name`/`queues` are provided.
        """
        logical_worker = self.get_logical_worker(worker_id=worker_id, worker_name=worker_name, queues=queues)

        def cleanup_logical_worker(worker):
            runs_using_worker = worker.get_runs()
            for run_id in runs_using_worker:
                try:
                    run = self.get_run(run_id)
                    run.remove_worker(worker.get_id())
                except RunNotFoundError:  # If the run isn't found then move on
                    LOG.warning(f"Couldn't find run with id {run_id}. Continuing with logical worker delete.")

        self._delete_entity(LogicalWorkerEntity, logical_worker.get_id(), cleanup_fn=cleanup_logical_worker)

    def delete_all_logical_workers(self):
        """
        Remove every logical worker in the database.
        """
        self._delete_all_by_type(
            get_all_fn=self.get_all_logical_workers, delete_fn=self.delete_logical_worker, entity_name="logical workers"
        )

    def create_physical_worker(self, name: str, **kwargs) -> PhysicalWorkerEntity:
        """
        Create a new physical worker in the database (if one doesn't exist) and return a
        [`PhysicalWorkerEntity`][merlin.db_scripts.physical_worker_entity.PhysicalWorkerEntity]
        instance.

        Args:
            name: The name of the worker.

        Returns:
            A [`PhysicalWorkerEntity`][merlin.db_scripts.physical_worker_entity.PhysicalWorkerEntity]
                instance representing the newly created worker.
        """
        log_message_create = (
            f"Physical worker with name '{name}' does not yet have an " "entry in the database. Creating one...",
        )
        return self._create_entity_if_not_exists(
            entity_class=PhysicalWorkerEntity,
            model_class=PhysicalWorkerModel,
            identifier=name,
            log_message_exists=f"Physical worker with name '{name}' already has an entry in the database.",
            log_message_create=log_message_create,
            name=name,
            **kwargs,
        )

    def get_physical_worker(self, worker_id_or_name: str) -> PhysicalWorkerEntity:
        """
        Given a physical worker id or name, retrieve the associated worker from the database.

        Args:
            worker_id_or_name: The id or name of the physical worker to retrieve.

        Returns:
            A [`PhysicalWorkerEntity`][merlin.db_scripts.physical_worker_entitiy.PhysicalWorkerEntity]
                instance representing the physical worker that was queried.
        """
        return self._get_entity(PhysicalWorkerEntity, worker_id_or_name)

    def get_all_physical_workers(self) -> List[PhysicalWorkerEntity]:
        """
        Get every physical worker that's currently in the database.

        Returns:
            A list of
                [`PhysicalWorkerEntity`][merlin.db_scripts.physical_worker_entity.PhysicalWorkerEntity]
                instances.
        """
        return self._get_all_entities(PhysicalWorkerEntity, "physical_worker")

    def delete_physical_worker(self, worker_id_or_name: str):
        """
        Given a phsyical worker id or name, remove the associated worker from the database.

        Args:
            worker_id_or_name: The id or name of the physical worker to remove.
        """

        def cleanup_physical_worker(worker):
            logical_worker_id = worker.get_logical_worker_id()
            try:
                logical_worker = self.get_logical_worker(worker_id=logical_worker_id)
                logical_worker.remove_physical_worker(worker.get_id())
            except WorkerNotFoundError:  # If logical worker isn't found move on
                LOG.warning(
                    f"Couldn't find logical worker with id {logical_worker_id}. Continuing with physical worker delete."
                )

        self._delete_entity(PhysicalWorkerEntity, worker_id_or_name, cleanup_fn=cleanup_physical_worker)

    def delete_all_physical_workers(self):
        """
        Remove every physical worker in the database.
        """
        self._delete_all_by_type(
            get_all_fn=self.get_all_physical_workers, delete_fn=self.delete_physical_worker, entity_name="physical workers"
        )

    def get_everything(self) -> List[DatabaseEntity]:
        """
        Retrieve all entities from the database.

        This method aggregates and returns a list containing all logical workers, physical workers,
        runs, and studies stored in the database. Each entity is represented by its corresponding
        class type.

        Returns:
            A list of entities retrieved from the database, including:\n
            - [`LogicalWorkerEntity`][merlin.db_scripts.logical_worker_entity.LogicalWorkerEntity]:
                Represents logical workers.
            - [`PhysicalWorkerEntity`][merlin.db_scripts.physical_worker_entity.PhysicalWorkerEntity]:
                Represents physical workers.
            - [`RunEntity`][merlin.db_scripts.run_entity.RunEntity]: Represents runs.
            - [`StudyEntity`][merlin.db_scripts.study_entity.StudyEntity]: Represents studies.
        """
        return list(
            self.get_all_logical_workers() + self.get_all_physical_workers() + self.get_all_runs() + self.get_all_studies()
        )

    def delete_everything(self, force: bool = False):
        """
        Flush the entire database. This will ask users for a confirmation unless
        `force` is set to True.

        Args:
            force: If True, ignore confirmation from the user.
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
