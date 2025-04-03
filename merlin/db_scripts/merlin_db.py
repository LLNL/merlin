"""
This module contains the functionality necessary to interact with everything
stored in Merlin's database.
"""

import logging
from typing import List

from merlin.backends.backend_factory import backend_factory
from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.data_models import LogicalWorkerModel, RunModel, StudyModel
from merlin.db_scripts.logical_worker_entity import LogicalWorkerEntity
from merlin.db_scripts.run_entity import RunEntity
from merlin.db_scripts.study_entity import StudyEntity
from merlin.exceptions import StudyNotFoundError, WorkerNotFoundError


LOG = logging.getLogger("merlin")


# TODO I think we should make this the default way to interact with backends to abstract it a bit
# - Can have abstract ResultsBackend class
# - Can have RedisBackend, SQLAlchemyBackend, etc. classes to extend ResultsBackend
# - Instead of using CONFIG.results_backend in the init for this class we could insted take in
#     an instance of the ResultsBackend class
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
            - [`delete_everything`][db_scritps.merlin_db.MerlinDatabase.delete_everything]: Removes
                every entry from the database.

        Study Management:\n
            - [`create_study`][db_scripts.merlin_db.MerlinDatabase.create_study]: Create a new study
                in the database if it does not already exist.
            - [`get_study`][db_scripts.merlin_db.MerlinDatabase.get_study]: Retrieve a specific study
                by its ID.
            - [`get_study_by_name`][db_scripts.merlin_db.MerlinDatabase.get_study_by_name]: Retrieve
                a specific study by its name.
            - [`get_all_studies`][db_scripts.merlin_db.MerlinDatabase.get_all_studies]: Retrieve all
                studies currently stored in the database.
            - [`delete_study`][db_scripts.merlin_db.MerlinDatabase.delete_study]: Remove a specific
                study by its ID, with an option to also remove associated runs.
            - [`delete_study_by_name`][db_scripts.merlin_db.MerlinDatabase.delete_study_by_name]: Remove
                a specific study by its name, with an option to also remove associated runs.
            - [`delete_all_studies`][db_scripts.merlin_db.MerlinDatabase.delete_all_studies]: Remove
                all studies from the database, with an option to also remove associated runs.

        Run Management:\n
            - [`create_run`][db_scripts.merlin_db.MerlinDatabase.create_run]: Create a new run for a
                study. If the study does not exist, it will be created first.
            - [`get_run`][db_scripts.merlin_db.MerlinDatabase.get_run]: Retrieve a specific run by its ID.
            - [`get_run_by_workspace`][db_scripts.merlin_db.MerlinDatabase.get_run_by_workspace]: Retrieve
                a specific run by its workspace.
            - [`get_all_runs`][db_scripts.merlin_db.MerlinDatabase.get_all_runs]: Retrieve all runs
                currently stored in the database.
            - [`delete_run`][db_scripts.merlin_db.MerlinDatabase.delete_run]: Remove a specific run by
                its ID.
            - [`delete_run_by_workspace`][db_scripts.merlin_db.MerlinDatabase.delete_run_by_workspace]:
                Remove a specific run by its workspace.
            - [`delete_all_runs`][db_scripts.merlin_db.MerlinDatabase.delete_all_runs]: Remove all runs
                from the database.

        Worker Management:\n
            - [`create_worker`][db_scripts.merlin_db.MerlinDatabase.create_worker]: Create a new worker
                in the database.
            - [`get_worker`][db_scripts.merlin_db.MerlinDatabase.get_worker]: Retrieve a specific worker
                by its ID.
            - [`get_worker_by_name`][db_scripts.merlin_db.MerlinDatabase.get_worker_by_name]: Retrieve a
                specific worker by its name.
            - [`get_all_workers`][db_scripts.merlin_db.MerlinDatabase.get_all_workers]: Retrieve all workers
                currently stored in the database.
            - [`delete_worker`][db_scripts.merlin_db.MerlinDatabase.delete_worker]: Remove a specific worker
                by its ID.
            - [`delete_worker_by_name`][db_scripts.merlin_db.MerlinDatabase.delete_worker_by_name]: Remove
                a specific worker by its name.
            - [`delete_all_workers`][db_scripts.merlin_db.MerlinDatabase.delete_all_workers]: Remove all
                workers from the database.
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

    def create_study(self, study_name: str) -> StudyEntity:
        """
        Create [`StudyEntity`][merlin.db_scripts.study_entity.StudyEntity] instance and save
        it to the database, if one does not already exist.

        Args:
            study_name: The name of the study to create.

        Returns:
            A [`StudyEntity`][merlin.db_scripts.study_entity.StudyEntity] instance.
        """
        try:
            study_entity = self.get_study(study_name)
            LOG.info(f"Study with name '{study_name}' already has an entry in the database.")
        except StudyNotFoundError:
            LOG.info(f"Study with name '{study_name}' does not yet have an entry in the database. Creating one...")
            study_info = StudyModel(name=study_name)
            study_entity = StudyEntity(study_info, self.backend)
            study_entity.save()

        return study_entity

    def get_study(self, study_id_or_name: str) -> StudyEntity:
        """
        Given a study id or name, retrieve the associated study from the database.

        Args:
            study_id_or_name: The id or name of the study to retrieve.

        Returns:
            A [`StudyEntity`][merlin.db_scripts.study_entity.StudyEntity] instance representing
                the study that was queried.
        """
        return StudyEntity.load(study_id_or_name, self.backend)

    def get_all_studies(self) -> List[StudyEntity]:
        """
        Get every study that's currently in the database.

        Returns:
            A list of [`StudyEntity`][merlin.db_scripts.study_entity.StudyEntity] instances.
        """
        all_studies = self.backend.retrieve_all("study")
        if not all_studies:
            return []
        return [StudyEntity(study, self.backend) for study in all_studies]

    def delete_study(self, study_id_or_name: str, remove_associated_runs: bool = True):
        """
        Given a study id or name, remove the associated study from the database. As a consequence
        of this action, any runs associated with this study will also be removed, unless
        `remove_associated_runs` is set to `False`.

        Args:
            study_id_or_name: The id or name of the study to remove.
            remove_associated_runs: If True, remove all runs associated with the study.
        """
        study = self.get_study(study_id_or_name)
        if remove_associated_runs:
            for run_id in study.get_runs():
                self.delete_run(run_id)
        StudyEntity.delete(study_id_or_name, self.backend)

    def delete_all_studies(self, remove_associated_runs: bool = True):
        """
        Remove every study in the database.

        Args:
            remove_associated_runs: If True, remove all runs associated with every study we delete.
                Essentially removes all runs as well as all studies.
        """
        all_studies = self.get_all_studies()
        if all_studies:
            # TODO should display studies to user and ask them if it's still ok to delete them
            # - can add a -f flag to ignore this prompt (like purge)
            for study in all_studies:
                self.delete_study(study.get_id(), remove_associated_runs=remove_associated_runs)
        else:
            LOG.warning("No studies found in the database.")

    def create_run(self, study_name: str, workspace: str, queues: List[str], *args, **kwargs) -> RunEntity:
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
        try:
            study_entity = self.get_study(study_name)
        except StudyNotFoundError:
            study_entity = self.create_study(study_name)

        # Get all valid fields for the RunModel dataclass
        valid_fields = {f.name for f in RunModel.get_class_fields()}

        # Separate valid fields from additional data
        valid_kwargs = {}
        additional_data = {}
        for key, val in kwargs.items():
            if key in valid_fields:
                valid_kwargs[key] = val
            else:
                additional_data[key] = val

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
        return RunEntity.load(run_id_or_workspace, self.backend)

    def get_all_runs(self) -> List[RunEntity]:
        """
        Get every run that's currently in the database.

        Returns:
            A list of [`RunEntity`][merlin.db_scripts.run_entity.RunEntity] instances.
        """
        all_runs = self.backend.retrieve_all("run")
        if not all_runs:
            return []
        return [RunEntity(run, self.backend) for run in all_runs]

    def delete_run(self, run_id_or_workspace: str):
        """
        Given a run id or workspace, remove the associated run from the database.

        Args:
            run_id_or_workspace: The id or workspace of the run to remove.
        """
        run = self.get_run(run_id_or_workspace)

        # Remove the run's id from the study's run list
        study = self.get_study(run.get_study_id())
        study.remove_run(run.get_id())

        # Remove the run's id from all of its' logical worker's runs list
        for worker_id in run.get_workers():
            logical_worker = self.get_logical_worker(worker_id)
            logical_worker.remove_run(run.get_id())

        # Delete the actual run entry
        RunEntity.delete(run_id_or_workspace, self.backend)

    def delete_all_runs(self):
        """
        Remove every run in the database.
        """
        all_runs = self.get_all_runs()
        if all_runs:
            # TODO should display runs to user and ask them if it's still ok to delete them
            # - can add a -f flag to ignore this prompt (like purge)
            for run in all_runs:
                self.delete_run(run.get_id())
        else:
            LOG.warning("No runs found in the database.")

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
        try:
            logical_worker_id = LogicalWorkerModel.generate_id(name, queues)
            logical_worker = LogicalWorkerEntity.load(logical_worker_id, self.backend)
            LOG.info(f"Worker with name '{name}' and queues '{queues}' already has an entry in the database.")
        except WorkerNotFoundError:
            LOG.info(
                f"Logical worker with name '{name}' and queues '{queues}' does not yet have an entry in the "
                "database. Creating one..."
            )
            logical_worker_info = LogicalWorkerModel(name=name, queues=queues)
            logical_worker = LogicalWorkerEntity(logical_worker_info, self.backend)
            logical_worker.save()
        return logical_worker
    
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
        elif worker_name is None or queues is None:
            raise ValueError("You must provide either `worker_id` or both `worker_name` and `queues`.")

        # Generate the worker_id if worker_name and queues are provided
        return LogicalWorkerModel.generate_id(worker_name, queues)

    def get_logical_worker(self, worker_id: str = None, worker_name: str = None, queues: List[str] = None):
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
        return LogicalWorkerEntity.load(worker_id, self.backend)
    
    def get_all_logical_workers(self) -> List[LogicalWorkerEntity]:
        """
        Get every logical worker that's currently in the database.

        Returns:
            A list of
                [`LogicalWorkerEntity`][merlin.db_scripts.logical_worker_entity.LogicalWorkerEntity]
                instances.
        """
        all_logical_workers = self.backend.retrieve_all("logical_worker")
        if not all_logical_workers:
            return []
        return [LogicalWorkerEntity(logical_worker, self.backend) for logical_worker in all_logical_workers]
    
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
        worker_id = self._resolve_worker_id(worker_id=worker_id, worker_name=worker_name, queues=queues)

        # Remove the worker from the list of workers in the associated runs
        logical_worker = LogicalWorkerEntity.load(worker_id, self.backend)
        runs_using_worker = logical_worker.get_runs()
        for run_id in runs_using_worker:
            run = self.get_run(run_id)
            run.remove_worker(worker_id)

        # Delete the actual logical worker entry
        LogicalWorkerEntity.delete(worker_id, self.backend)

    def delete_all_logical_workers(self):
        """
        Remove every logical worker in the database.
        """
        all_logical_workers = self.get_all_logical_workers()
        if all_logical_workers:
            for worker in all_logical_workers:
                self.delete_logical_worker(worker.get_id())
        else:
            LOG.warning("No logical workers found in the database.")

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
