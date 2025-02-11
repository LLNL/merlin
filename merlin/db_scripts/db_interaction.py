"""
"""
from merlin.db_scripts.db_study import DatabaseStudy


class MerlinDatabase:
    """

    TODO I think we should make this the default way to interact with backends to abstract it a bit
    - Can have abstract ResultsBackend class
    - Can have RedisBackend, SQLAlchemyBackend, etc. classes to extend ResultsBackend
    - Instead of using CONFIG.results_backend in the init for this class we could insted take in
      an instance of the ResultsBackend class
    """

    def __init__(self):
        pass

    def get_study(self, id: int) -> DatabaseStudy:  # TODO not sure if id is an int or str
        """
        Given an ID, get the associated study from the database.

        Args:
            id: The ID of the study to retrieve.

        Returns:
            A [`DatabaseStudy`][merlin.db_scripts.db_study.DatabaseStudy] object representing
                the study that was queried.
        """
        pass

    def get_all_studies(self) -> List[DatabaseStudy]:
        """
        Get every study that's currently in the database.

        Returns:
            A list of [`DatabaseStudy`][merlin.db_scripts.db_study.DatabaseStudy] objects.
        """
        pass

    def remove_study(self, id: int):  # TODO not sure if id is an int or str
        """
        Given an ID, remove the associated study from the database. As a consequence
        of this action, any study runs associated with this study will also be removed.

        Args:
            id: The ID of the study to remove.

        TODO do we want to remove runs? Should they be able to exist by themselves?
        """
        pass

    def remove_all_studies(self):
        """
        Remove every study in the database.

        TODO is this essentially clearing the db? What other info will be in the db?
        """
        pass
