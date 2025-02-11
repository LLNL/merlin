"""
"""
from dataclasses import dataclass, field, asdict

from merlin.db_scripts.db_run import DatabaseRun, RunInfo

@dataclass
class StudyInfo:
    """
    A dataclass to store all of the information for a run.

    Attributes:
        id: The unique ID for the study.
        name: The name of the study.
        runs: A list of runs associated with this study.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = None
    runs: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        """
        Convert the study data to a dictionary for storage in the database.
        """
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> "StudyInfo":
        """
        Create a `StudyInfo` instance from a dictionary.
        """
        return cls(
            id=data.get("id"),
            name=data.get("name"),
            runs=data.get("runs", []),
        )


class DatabaseStudy:
    """
    """

    def __init__(self, study_info: StudyInfo, backend: ResultsBackend):
        self.study_info = study_info
        self.backend = backend

    def get_id() -> str:
        """
        Get the ID for this study.

        Returns:
            The ID for this study.
        """
        return self.study_info.id

    def get_name() -> str:
        """
        Get the name associated with this study.

        Returns:
            The name for this study.
        """
        return self.study_info.name

    def create_run(self):  # TODO not sure if we want to return the ID of the run here?
        """
        Create a run for this study. This will create a [`DatabaseRun`][merlin.db_scripts.db_run.DatabaseRun]
        object and link it to this study.
        """
        new_run = RunInfo()

        pass

    def get_run(self, id: str) -> DatabaseRun:
        """
        Given an ID, get the associated run from the database.

        Args:
            id: The ID of the run to retrieve.

        Returns:
            A [`DatabaseRun`][merlin.db_scripts.db_run.DatabaseRun] object representing
                the study that was queried.
        """
        return DatabaseRun.load(id, self.backend)

    def get_all_runs(self):
        """
        Get every run associated with this study.

        Returns:
            A list of [`DatabaseRun`][merlin.db_scripts.db_run.DatabaseRun] objects.
        """
        return [self.get_run(run_id) for run_id in self.study_info.runs]

    def remove_run(self, id: str):
        """
        Given an ID, remove the associated run from the database.

        Args:
            id: The ID of the run to remove.
        """
        DatabaseRun.delete(id, self.backend)
        self.study_info.runs.remove(id)

    def remove_all_runs(self):
        """
        Remove every run associated with this study.
        """
        for run_id in self.study_info.runs:
            self.remove_run(run_id)

    def save(self):
        """
        """
        