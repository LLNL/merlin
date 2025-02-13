"""
"""
import logging
from typing import List

from merlin.backends.backend_factory import backend_factory
from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.data_formats import StudyInfo
from merlin.db_scripts.db_study import DatabaseStudy

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
        create_study: Create a new study in the database if it does not already exist.
        get_all_studies: Retrieve all studies currently stored in the database.
        get_study: Retrieve a specific study from the database by its name.
        remove_all_studies: Remove all studies from the database.
        remove_study: Remove a specific study from the database by its ID.
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

    def remove_study(self, id: int):  # TODO not sure if id is an int or str
        """
        Given an ID, remove the associated study from the database. As a consequence
        of this action, any study runs associated with this study will also be removed.

        Args:
            id: The ID of the study to remove.
        """
        pass

    def remove_all_studies(self):
        """
        Remove every study in the database.

        TODO is this essentially clearing the db? What other info will be in the db?
        """
        pass
