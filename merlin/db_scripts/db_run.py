"""
"""
from typing import List

from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.data_formats import RunInfo


class DatabaseRun:
    """
    A class representing a run in the database.

    This class provides methods to interact with and manage a run's data, including 
    retrieving information about the run, updating its state, and saving or deleting 
    it from the database.

    Attributes:
        run_info: An instance of the `RunInfo` class containing the run's metadata.
        backend: An instance of the `ResultsBackend` class used to interact 
            with the database.
        run_complete: Property to get or set the completion status of the run.

    Methods:
        delete: Class method to delete a run from the database by its ID.
        get_id: Retrieve the ID of the run.
        get_study_id: Retrieve the ID of the study associated with this run.
        get_workspace: Retrieve the path to the output workspace for this run.
        get_queues: Retrieve the task queues used for this run.
        get_parent: Retrieve the ID of the parent run that launched this run (if any).
        get_child: Retrieve the ID of the child run launched by this run (if any).
        load: Class method to load a run from the database by its ID.
        save: Save the current state of the run to the database.
    """

    def __init__(self, run_info: RunInfo, backend: ResultsBackend):
        self.run_info: RunInfo = run_info
        self.backend: ResultsBackend = backend

    @property
    def run_complete(self) -> bool:
        """
        An attribute representing whether this run is complete.

        A "complete" study is a study that has executed all steps.

        Returns:
            True if the study is complete. False, otherwise.
        """
        return self.run_info.run_complete

    @run_complete.setter
    def run_complete(self, value: bool):
        """
        Update the run's completion status.

        Args:
            value: The completion status of the run.
        """
        self.run_info.run_complete = value
        self.save()

    def get_id(self) -> str:
        """
        Get the ID for this run.

        Returns:
            The ID for this run.
        """
        return self.run_info.id

    def get_study_id(self) -> str:
        """
        Get the ID for the study associated with this run.

        Returns:
            The ID for the study associated with this run.
        """
        return self.run_info.study_id
    
    def get_workspace(self) -> str:
        """
        Get the path to the output workspace for this run.

        Returns:
            A string representing the output workspace for this run.
        """
        return self.run_info.workspace

    def get_queues(self) -> List[str]:
        """
        Get the task queues that were used for this run.

        Returns:
            A list of strings representing the queues that were used for this run.
        """
        return self.run_info.queues

    def get_parent(self) -> str:
        """
        Get the ID of the run that launched this run (if any).

        This will only be set for iterative workflows with greater than 1 iteration.

        Returns:
            The ID of the run that launched this run.
        """
        return self.run_info.parent

    def get_child(self) -> str:
        """
        Get the ID of the run that was launched by this run (if any).

        This will only be set for iterative workflows with greater than 1 iteration.

        Returns:
            The ID of the run that was launched by this run.
        """
        return self.run_info.child

    def save(self):
        """
        Save the current state of this run to the database.
        """
        self.backend.save_run(self.run_info)

    @classmethod
    def load(cls, run_id: str, backend: ResultsBackend) -> "DatabaseRun":
        """
        Load a run from the database.

        Args:
            run_id: The ID of the run to load.
            backend: A [`ResultsBackend`][merlin.backends.results_backend.ResultsBackend] instance.

        Returns:
            A `DatabaseRun` instance.
        """
        run_data = backend.get(run_id)
        if not run_data:
            raise ValueError(f"Run with ID {run_id} not found in the database.")
        
        run_info = RunInfo.from_dict(run_data)
        return cls(run_info, backend)

    @classmethod
    def delete(cls, run_id: str, backend: ResultsBackend):
        """
        Delete a run from the database.

        Args:
            run_id: The ID of the run to delete.
            backend: A [`ResultsBackend`][merlin.backends.results_backend.ResultsBackend] instance.
        """
        # TODO make sure this deletes everything for the run
        self.backend.delete(run_id)
    