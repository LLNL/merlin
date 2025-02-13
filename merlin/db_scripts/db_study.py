"""
"""
from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.data_formats import RunInfo, StudyInfo
from merlin.db_scripts.db_run import DatabaseRun


class DatabaseStudy:
    """
    A class representing a study in the database.

    This class provides methods to interact with and manage a study's data, including 
    creating, retrieving, and removing runs associated with the study, as well as saving 
    or deleting the study itself from the database.

    Attributes:
        study_info: An instance of the `StudyInfo` class containing the study's metadata.
        backend: An instance of the `ResultsBackend` class used to interact 
            with the database.

    Methods:
        create_run: Create a new run for this study and save it to the database.
        delete: Class method to delete a study from the database.
        get_all_runs: Retrieve all runs associated with this study.
        get_id: Retrieve the ID of the study.
        get_name: Retrieve the name of the study.
        get_run: Retrieve a specific run associated with this study by its ID.
        load: Class method to load a study from the database by its name.
        remove_all_runs: Remove all runs associated with this study from the database.
        remove_run: Remove a specific run associated with this study by its ID.
        save: Save the current state of the study to the database.
    """

    def __init__(self, study_info: StudyInfo, backend: ResultsBackend):
        self.study_info: StudyInfo = study_info
        self.backend: ResultsBackend = backend

    def get_id(self) -> str:
        """
        Get the ID for this study.

        Returns:
            The ID for this study.
        """
        return self.study_info.id

    def get_name(self) -> str:
        """
        Get the name associated with this study.

        Returns:
            The name for this study.
        """
        return self.study_info.name

    def create_run(self, *args, **kwargs) -> DatabaseRun:
        """
        Create a run for this study. This will create a [`DatabaseRun`][merlin.db_scripts.db_run.DatabaseRun]
        instance and link it to this study.

        As a side effect of this method, a new run will be added to the database. Additionally,
        the current status of this study will updated to include this new run.

        Returns:
            A [`DatabaseRun`][merlin.db_scripts.db_run.DatabaseRun] instance representing
                the run that was created.
        """
        # Get all valid fields for the RunInfo dataclass
        valid_fields = {f.name for f in RunInfo.fields()}

        # Separate valid fields from additional data
        valid_kwargs = {}
        additional_data = {}
        for key, val in kwargs.items():
            if key in valid_fields:
                valid_kwargs[key] = val
            else:
                additional_data[key] = val

        # Create the RunInfo object and save it to the backend
        new_run = RunInfo(
            study_id=self.get_id(),
            **valid_kwargs,
            additional_data=additional_data,
        )
        db_run = DatabaseRun(new_run, self.backend)
        db_run.save()

        # Add the run ID to the study's list of runs
        self.study_info.runs.append(new_run.id)
        self.save()  # Save the updated study to the backend

        return db_run


    def get_run(self, run_id: str) -> DatabaseRun:
        """
        Given an ID, get the associated run from the database.

        Args:
            run_id: The ID of the run to retrieve.

        Returns:
            A [`DatabaseRun`][merlin.db_scripts.db_run.DatabaseRun] instance representing
                the run that was queried.
        """
        return DatabaseRun.load(run_id, self.backend)

    def get_all_runs(self):
        """
        Get every run associated with this study.

        Returns:
            A list of [`DatabaseRun`][merlin.db_scripts.db_run.DatabaseRun] instances.
        """
        return [self.get_run(run_id) for run_id in self.study_info.runs]

    def remove_run(self, run_id: str):
        """
        Given an ID, remove the associated run from the database.

        Args:
            run_id: The ID of the run to remove.
        """
        DatabaseRun.delete(run_id, self.backend)
        self.study_info.runs.remove(run_id)

    def remove_all_runs(self):
        """
        Remove every run associated with this study.
        """
        for run_id in self.study_info.runs:
            self.remove_run(run_id)

    def save(self):
        """
        Save the current state of this study to the database.
        """
        self.backend.save_study(self.study_info)

    @classmethod
    def load(cls, study_name: str, backend: ResultsBackend) -> "DatabaseStudy":
        """
        Load a study from the database.

        Args:
            study_name: The name of the study to load.
            backend: A [`ResultsBackend`][merlin.backends.results_backend.ResultsBackend] instance.

        Returns:
            A `DatabaseStudy` instance.

        Raises:
            ValueError: If the study can't be retrieved from the database.
        """
        study_info = backend.retrieve_study(study_name)
        if study_info is None:
            raise ValueError(f"Study with name '{study_name}' not found in the database.")
        
        return cls(study_info, backend)

    @classmethod
    def delete(cls, study_id: str, backend: ResultsBackend, remove_all_runs: bool = True):
        """
        Delete a study from the database.

        By default, this will remove all of the runs associated with the study from the database.

        Args:
            study_id: The ID of the study to load.
            backend: A [`ResultsBackend`][merlin.backends.results_backend.ResultsBackend] instance.
            remove_all_runs: If True, remove all of the runs associated with this study from the db.
        """
        # Remove all runs associated with this study if necessary
        if remove_all_runs:
            current_study = cls.load(study_id, backend)
            current_study.remove_all_runs()
        
        # Remove the actual study entry
        backend.delete(study_id)
