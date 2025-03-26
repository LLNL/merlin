"""
This module contains the functionality necessary to interact with everything
stored in Merlin's database.
"""

import logging
from typing import List

from merlin.backends.backend_factory import backend_factory
from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.data_models import StudyModel, WorkerModel
from merlin.db_scripts.db_run import DatabaseRun
from merlin.db_scripts.db_study import DatabaseStudy
from merlin.db_scripts.db_worker import DatabaseWorker
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

    def create_study(self, study_name: str) -> DatabaseStudy:
        """
        Create [`DatabaseStudy`][merlin.db_scripts.db_study.DatabaseStudy] instance and save
        it to the database, if one does not already exist.

        Args:
            study_name: The name of the study to create.

        Returns:
            A [`DatabaseStudy`][merlin.db_scripts.db_study.DatabaseStudy] instance.
        """
        try:
            db_study = self.get_study_by_name(study_name)
            LOG.info(f"Study with name '{study_name}' already has an entry in the database.")
        except StudyNotFoundError:
            LOG.info(f"Study with name '{study_name}' does not yet have an entry in the database. Creating one...")
            study_info = StudyModel(name=study_name)
            db_study = DatabaseStudy(study_info, self.backend)
            db_study.save()

        return db_study

    def get_study(self, study_id: str) -> DatabaseStudy:
        """
        Given a study id, retrieve the associated study from the database.

        Args:
            study_id: The id of the study to retrieve.

        Returns:
            A [`DatabaseStudy`][merlin.db_scripts.db_study.DatabaseStudy] instance representing
                the study that was queried.
        """
        return DatabaseStudy.load(study_id, self.backend)

    def get_study_by_name(self, study_name: str) -> DatabaseRun:
        """
        Given a study name, retrieve the associated study from the database.

        Args:
            study_name: The name of the study to retrieve.

        Returns:
            A [`DatabaseStudy`][merlin.db_scripts.db_study.DatabaseStudy] instance representing
                the study that was queried.
        """
        return DatabaseStudy.load_by_name(study_name, self.backend)

    def get_all_studies(self) -> List[DatabaseStudy]:
        """
        Get every study that's currently in the database.

        Returns:
            A list of [`DatabaseStudy`][merlin.db_scripts.db_study.DatabaseStudy] instances.
        """
        all_studies = self.backend.retrieve_all_studies()
        if not all_studies:
            return []
        return [DatabaseStudy(study, self.backend) for study in all_studies]

    def delete_study(self, study_id: str, remove_associated_runs: bool = True):
        """
        Given a study id, remove the associated study from the database. As a consequence
        of this action, any runs associated with this study will also be removed, unless
        `remove_associated_runs` is set to `False`.

        Args:
            study_id: The id of the study to remove.
            remove_associated_runs: If True, remove all runs associated with the study.
        """
        DatabaseStudy.delete(study_id, self.backend, remove_associated_runs=remove_associated_runs)

    def delete_study_by_name(self, study_name: str, remove_associated_runs: bool = True):
        """
        Given a study name, remove the associated study from the database. As a consequence
        of this action, any runs associated with this study will also be removed, unless
        `remove_associated_runs` is set to `False`.

        Args:
            study_name: The name of the study to remove.
            remove_associated_runs: If True, remove all runs associated with the study.
        """
        study = self.get_study_by_name(study_name)
        self.delete_study(study.get_id(), remove_associated_runs=remove_associated_runs)

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

    def create_run(self, study_name: str, workspace: str, queues: List[str], *args, **kwargs) -> DatabaseRun:
        """
        Given a study name, create a run for this study. If the study does not yet exist in
        the database, an entry will be created for it prior to the run being created.

        Args:
            study_name: The name of the study that this run is associated with.
            workspace: The output workspace for the run.
            queues: The task queues for the run.

        Returns:
            A [`DatabaseRun`][merlin.db_scripts.db_run.DatabaseRun] instance.
        """
        try:
            db_study = self.get_study_by_name(study_name)
        except StudyNotFoundError:
            db_study = self.create_study(study_name)

        return db_study.create_run(workspace=workspace, queues=queues, *args, **kwargs)

    def get_run(self, run_id: str) -> DatabaseRun:
        """
        Given a run id, retrieve the associated run from the database.

        Args:
            run_id: The name of the run to retrieve.

        Returns:
            A [`DatabaseRun`][merlin.db_scripts.db_run.DatabaseRun] instance representing
                the run that was queried.
        """
        return DatabaseRun.load(run_id, self.backend)

    def get_run_by_workspace(self, workspace: str) -> DatabaseRun:
        """
        Given an output workspace for a run, find the run metadata file and load
        the run from it.

        Args:
            workspace: The output workspace for a run.

        Returns:
            A [`DatabaseRun`][merlin.db_scripts.db_run.DatabaseRun] instance representing
                the run that was queried.
        """
        run_metadata_filepath = DatabaseRun.get_metadata_filepath(workspace)
        return DatabaseRun.load_from_metadata_file(run_metadata_filepath, self.backend)

    def get_all_runs(self) -> List[DatabaseRun]:
        """
        Get every run that's currently in the database.

        Returns:
            A list of [`DatabaseRun`][merlin.db_scripts.db_run.DatabaseRun] instances.
        """
        all_runs = self.backend.retrieve_all_runs()
        if not all_runs:
            return []
        return [DatabaseRun(run, self.backend) for run in all_runs]

    def delete_run(self, run_id: str):
        """
        Given a run id, remove the associated run from the database.

        Args:
            run_id: The id of the run to remove.
        """
        DatabaseRun.delete(run_id, self.backend)

    def delete_run_by_workspace(self, run_workspace: str):
        """
        Given a run workspace, remove the associated run from the database.

        Args:
            run_workspace: The workspace of the run to remove.
        """
        run = self.get_run_by_workspace(run_workspace)
        self.delete_run(run.get_id())

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

    def create_worker(self, name: str) -> DatabaseWorker:
        """
        Create a new worker in the database and return a
        [`DatabaseWorker`][merlin.db_scripts.db_worker.DatabaseWorker] instance.

        Args:
            name: The name of the worker.

        Returns:
            A [`DatabaseWorker`][merlin.db_scripts.db_worker.DatabaseWorker] instance
                representing the newly created worker.
        """
        try:
            db_worker = self.get_worker_by_name(name)
            LOG.info(f"Worker with name '{name}' already has an entry in the database.")
        except WorkerNotFoundError:
            LOG.info(f"Worker with name '{name}' does not yet have an entry in the database. Creating one...")
            worker_info = WorkerModel(name=name)
            db_worker = DatabaseWorker(worker_info, self.backend)
            db_worker.save()
        return db_worker

    def get_worker(self, worker_id: str) -> DatabaseWorker:
        """
        Given a worker id, retrieve the associated worker from the database.

        Args:
            worker_id: The name of the worker to retrieve.

        Returns:
            A [`DatabaseWorker`][merlin.db_scripts.db_worker.DatabaseWorker] instance representing
                the worker that was queried.
        """
        return DatabaseWorker.load(worker_id, self.backend)

    def get_worker_by_name(self, worker_name: str) -> DatabaseWorker:
        """
        Given a worker name, retrieve the associated worker from the database.

        Args:
            worker_name: The name of the worker to retrieve.

        Returns:
            A [`DatabaseWorker`][merlin.db_scripts.db_worker.DatabaseWorker] instance representing
                the worker that was queried.
        """
        return DatabaseWorker.load_by_name(worker_name, self.backend)

    def get_all_workers(self) -> List[DatabaseWorker]:
        """
        Get every worker that's currently in the database.

        Returns:
            A list of [`DatabaseWorker`][merlin.db_scripts.db_worker.DatabaseWorker] instances.
        """
        all_workers = self.backend.retrieve_all_workers()
        if not all_workers:
            return []
        return [DatabaseWorker(worker, self.backend) for worker in all_workers]

    def delete_worker(self, worker_id: str):
        """
        Given a worker id, remove the associated worker from the database.

        Args:
            worker_id: The id of the worker to remove.
        """
        DatabaseWorker.delete(worker_id, self.backend)

    def delete_worker_by_name(self, worker_name: str):
        """
        Given a worker name, remove the associated worker from the database.

        Args:
            worker_name: The name of the worker to remove.
        """
        worker = self.get_worker_by_name(worker_name)
        self.delete_worker(worker.get_id())

    def delete_all_workers(self):
        """
        Remove every worker in the database.
        """
        # TODO how do we want to handle this in runs? do we delete those entries as well?
        # - if so will we need a 'runs' entry in WorkerModel?
        all_workers = self.get_all_workers()
        if all_workers:
            for worker in all_workers:
                self.delete_worker(worker.get_id())
        else:
            LOG.warning("No workers found in the database.")
