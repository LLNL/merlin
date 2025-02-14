"""
"""
import logging
from typing import List

from merlin.backends.backend_factory import backend_factory
from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.data_formats import RunInfo, StudyInfo
from merlin.db_scripts.db_study import DatabaseRun, DatabaseStudy

LOG = logging.getLogger("merlin")


# TODO I think we should make this the default way to interact with backends to abstract it a bit
# - Can have abstract ResultsBackend class
# - Can have RedisBackend, SQLAlchemyBackend, etc. classes to extend ResultsBackend
# - Instead of using CONFIG.results_backend in the init for this class we could insted take in
#     an instance of the ResultsBackend class
class MerlinDatabase:
    """
    A class that provides a high-level interface for interacting with the database backend.

    This class abstracts the interaction with different types of backend implementations 
    (e.g., Redis, SQLAlchemy) and provides methods to manage studies and their associated 
    runs in the database.

    Attributes:
        backend: An instance of a backend class (e.g., RedisBackend, SQLAlchemyBackend) used
            to interact with the database.

    Methods:
        create_study:
            Create a new study in the database if it does not already exist.

        get_study:
            Retrieve a specific study from the database by its name.

        get_all_studies:
            Retrieve all studies currently stored in the database.

        delete_study:
            Remove a specific study from the database by its name. Optionally, remove associated runs.

        delete_all_studies:
            Remove all studies from the database. Optionally, remove associated runs.

        create_run:
            Create a new run for a study. If the study does not exist, it will be created first.

        get_run:
            Retrieve a specific run from the database by its ID.

        get_all_runs:
            Retrieve all runs currently stored in the database.

        delete_run:
            Remove a specific run from the database by its ID.

        delete_all_runs:
            Remove all runs currently stored in the database.
    """

    def __init__(self):
        from merlin.config.configfile import CONFIG
        self.backend: ResultsBackend = backend_factory.get_backend(CONFIG.results_backend.name.lower())

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
            db_study = self.get_study(study_name)
            LOG.info(f"Study with name '{study_name}' already has an entry in the database.")
        except ValueError:  # TODO create a StudyNotFoundError
            LOG.info(f"Study with name '{study_name}' does not yet have an entry in the database. Creating one...")
            study_info = StudyInfo(name=study_name)
            db_study = DatabaseStudy(study_info, self.backend)
            db_study.save()

        return db_study

    def get_study(self, study_name: str) -> DatabaseStudy:
        """
        Given a study name, retrieve the associated study from the database.

        Args:
            study_name: The name of the study to retrieve.

        Returns:
            A [`DatabaseStudy`][merlin.db_scripts.db_study.DatabaseStudy] instance representing
                the study that was queried.
        """
        return DatabaseStudy.load(study_name, self.backend)

    def get_all_studies(self) -> List[DatabaseStudy]:
        """
        Get every study that's currently in the database.

        Returns:
            A list of [`DatabaseStudy`][merlin.db_scripts.db_study.DatabaseStudy] instances.
        """
        all_studies = self.backend.retrieve_all_studies()
        return [DatabaseStudy(study, self.backend) for study in all_studies]

    def delete_study(self, study_name: str, remove_associated_runs: bool = True):
        """
        Given a study name, remove the associated study from the database. As a consequence
        of this action, any runs associated with this study will also be removed, unless
        `remove_associated_runs` is set to `False`.

        Args:
            study_name: The name of the study to remove.
            remove_associated_runs: If True, remove all runs associated with the study.
        """
        DatabaseStudy.delete(study_name, self.backend, remove_associated_runs=remove_associated_runs)

    def delete_all_studies(self, remove_associated_runs: bool = True):
        """
        Remove every study in the database.

        Args:
            remove_associated_runs: If True, remove all runs associated with every study we delete.
                Essentially removes all runs as well as all studies.
        """
        all_studies = self.backend.retrieve_all_studies()
        # TODO should display studies to user and ask them if it's still ok to delete them
        # - can add a -f flag to ignore this prompt (like purge)
        for study in all_studies:
            self.delete_study(study.name, remove_associated_runs=remove_associated_runs)

    def create_run(self, study_name: str, workspace: str, queues: List[str], *args, **kwargs):
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
            db_study = self.get_study(study_name)
        except ValueError:
            db_study = self.create_study(study_name)
        
        return db_study.create_run(workspace=workspace, queues=queues, *args, **kwargs)

    def get_run(self, run_id: str):
        """
        Given a run id, retrieve the associated run from the database.

        Args:
            run_id: The name of the run to retrieve.

        Returns:
            A [`DatabaseRun`][merlin.db_scripts.db_run.DatabaseRun] instance representing
                the run that was queried.
        """
        return DatabaseRun.load(run_id, self.backend)

    def get_all_runs(self) -> List[DatabaseRun]:
        """
        Get every run that's currently in the database.

        Returns:
            A list of [`DatabaseRun`][merlin.db_scripts.db_run.DatabaseRun] instances.
        """
        all_runs = self.backend.retrieve_all_runs()
        return [DatabaseRun(run, self.backend) for run in all_runs]

    def delete_run(self, run_id: str):
        """
        Given a run id, remove the associated run from the database.

        Args:
            run_id: The id of the run to remove.
        """
        DatabaseRun.delete(run_id, self.backend)

    def delete_all_runs(self):
        """
        Remove every run in the database.
        """
        all_runs = self.backend.retrieve_all_runs()
        # TODO should display runs to user and ask them if it's still ok to delete them
        # - can add a -f flag to ignore this prompt (like purge)
        for run in all_runs:
            self.delete_run(run.id)

    
