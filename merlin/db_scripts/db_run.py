"""
"""
from dataclasses import dataclass, field, asdict
from typing import Dict, List
import uuid

from merlin.backends.results_backend import ResultsBackend


@dataclass
class RunInfo:
    """
    A dataclass to store all of the information for a run.

    Attributes:
        id: The unique ID for the run.
        study_id: The unique ID of the study this run is associated with.
        workspace: The path to the output workspace.
        queues: The task queues used for this run.
        parent: The ID of the parent run (if any).
        child: The ID of the child run (if any).
        run_complete: Wether the run is complete.
        parameters: The parameters used in this run.
        samples: The samples used in this run.
        additional_data: For any extra data not explicitly defined.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    study_id: str
    workspace: str = None
    queues: List[str] = field(default_factory=list)
    parent: str = None 
    child: str = None
    run_complete: bool = False
    parameters: Dict = field(default_factory=dict)  # TODO NOT YET IMPLEMENTED
    samples: Dict = field(default_factory=dict)  # TODO NOT YET IMPLEMENTED
    additional_data: Dict = field(default_factory=dict)

    def to_dict(self) -> Dict:
        """
        Convert the run data to a dictionary for storage in the database.
        """
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> "RunInfo":
        """
        Create a `RunInfo` instance from a dictionary.
        """
        return cls(
            id=data.get("id"),
            study_id=data.get("study_id"),
            workspace=data.get("workspace"),
            queues=data.get("queues", ["merlin"]),
            parent=data.get("parent", None),
            child=data.get("child", None),
            run_complete=data.get("run_complete", False),
            parameters=data.get("parameters", {})
            samples=data.get("samples", {})
            additional_data=data.get("additional_data", {})
        )


class DatabaseRun:
    """
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
        # TODO flush out logic in backend class to set this (might require more work here)
        self.backend.set(self.get_id(), self.run_info)

        # # Assuming the parent study's name is required to save the run
        # study_name = self._data.get("study_name")
        # if study_name:
        #     self.backend.set(f"{study_name}:{self.id}", self._data)

        # run_data = self.run_info.to_dict()
        # self.backend.set(f"run:{self.run_info.run_id}", run_data)

    @classmethod
    def load(cls, run_id: str, backend: ResultsBackend) -> "DatabaseRun":
        """
        Load a run from the database.

        Args:
            run_id: The ID of the run to load.
            backend: A [`ResultsBackend`][merlin.backends.results_backend.ResultsBackend] object.

        Returns:
            A `DatabaseRun` instance.
        """
        # TODO 

        run_data = self.backend.get(run_id)
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
            backend: A [`ResultsBackend`][merlin.backends.results_backend.ResultsBackend] object.
        """
        # TODO make sure this deletes everything for the run
        self.backend.delete(run_id)
    