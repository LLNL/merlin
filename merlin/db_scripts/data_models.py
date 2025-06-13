##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module houses dataclasses that define the format of the data
that's stored in Merlin's database.
"""

import hashlib
import json
import logging
import os
import uuid
from abc import ABC, abstractmethod
from dataclasses import Field, asdict, dataclass, field
from dataclasses import fields as dataclass_fields
from datetime import datetime
from typing import Dict, List, Set, Tuple, Type, TypeVar

from filelock import FileLock

from merlin.common.enums import WorkerStatus


LOG = logging.getLogger("merlin")
T = TypeVar("T", bound="BaseDataModel")


@dataclass
class BaseDataModel(ABC):
    """
    A base class for dataclasses that provides common serialization, deserialization, and
    update functionality, with support for additional data.

    This class is designed to be extended by other dataclasses and includes methods for
    converting instances to and from dictionaries or JSON, managing fields, and updating
    field values with validation.

    Attributes:
        additional_data: A dictionary to store any extra data not explicitly defined
            as fields in the dataclass.
        fields_allowed_to_be_updated: A list of field names that are allowed to be updated.
            Must be defined in subclasses.

    Methods:
        to_dict:
            Convert the dataclass instance to a dictionary.

        to_json:
            Serialize the dataclass instance to a JSON string.

        from_dict (classmethod):
            Create an instance of the dataclass from a dictionary.

        from_json (classmethod):
            Create an instance of the dataclass from a JSON string.

        dump_to_json_file:
            Dump the data of this dataclass to a JSON file.

        load_from_json_file (classmethod):
            Load the data stored in a JSON file to this dataclass.

        fields:
            Retrieve the fields associated with this dataclass instance or class.

        fields (classmethod):
            Retrieve the fields associated with the dataclass class itself.

        update_fields:
            Update the fields of the dataclass based on a given dictionary of updates.
    """

    additional_data: Dict = field(default_factory=dict)

    def to_dict(self) -> Dict:
        """
        Convert the dataclass to a dictionary.

        Returns:
            The dataclass as a dictionary.
        """
        return asdict(self)

    def to_json(self) -> str:
        """
        Serialize the dataclass to a JSON string.

        Returns:
            The dataclass as a JSON string.
        """
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls: Type[T], data: Dict) -> T:
        """
        Create an instance of the dataclass from a dictionary.

        Args:
            data: A dictionary to turn into an instance of this dataclass.

        Returns:
            An instance of the dataclass that called this.
        """
        return cls(**data)

    @classmethod
    def from_json(cls: Type[T], json_str: str) -> T:
        """
        Create an instance of the dataclass from a JSON string.

        Args:
            json_str: A JSON string to turn into an instance of this dataclass.

        Returns:
            An instance of the dataclass that called this.
        """
        data = json.loads(json_str)
        return cls.from_dict(data)

    def dump_to_json_file(self, filepath: str):
        """
        Dump the data of this dataclass to a JSON file.

        Args:
            filepath: The path to the JSON file where the data will be written.

        Raises:
            ValueError: If the `filepath` is not provided or is invalid.
        """
        if not filepath:
            raise ValueError("A valid file path must be provided.")

        # Ensure the directory for the file exists
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        # Create a lock file alongside the target JSON file
        lock_file = f"{filepath}.lock"
        with FileLock(lock_file):  # pylint: disable=abstract-class-instantiated
            # Write the data to the JSON file
            temp_filepath = f"{filepath}.tmp"  # Use a temporary file for atomic writes
            with open(temp_filepath, "w") as json_file:
                json.dump(self.to_dict(), json_file, indent=4)

            # Replace the temporary file with the target file
            os.replace(temp_filepath, filepath)

        LOG.debug(f"Data successfully dumped to {filepath}.")

    @classmethod
    def load_from_json_file(cls: Type[T], filepath: str) -> T:
        """
        Load the data stored in a JSON file to this dataclass.

        Args:
            filepath: The path to the JSON file where the data is located.

        Raises:
            ValueError: If the `filepath` is not provided or is invalid.
        """
        if not filepath or not os.path.exists(filepath):
            raise ValueError("A valid file path must be provided.")

        # Create a lock file alongside the target JSON file
        lock_file = f"{filepath}.lock"
        with FileLock(lock_file):  # pylint: disable=abstract-class-instantiated
            with open(filepath, "r") as json_file:
                # Parse the JSON data into a dictionary
                data = json.load(json_file)

        # Use from_dict to create an instance of the dataclass
        return cls.from_dict(data)

    def get_instance_fields(self) -> Tuple[Field]:
        """
        Get the fields associated with this instance. Added this method so that the dataclass.fields
        doesn't have to be imported each time you want this info.

        Returns:
            A tuple of dataclass.Field objects representing the fields in this data class.
        """
        return dataclass_fields(self)

    @classmethod
    def get_class_fields(cls) -> Tuple[Field]:
        """
        Get the fields associated with this object. Added this method so that the dataclass.fields
        doesn't have to be imported each time you want this info.

        Returns:
            A tuple of dataclass.Field objects representing the fields in this data class.
        """
        return dataclass_fields(cls)

    @property
    @abstractmethod
    def fields_allowed_to_be_updated(self) -> List[str]:
        """
        A property to be overridden in subclasses to define which fields are allowed to be updated.

        Returns:
            A list of fields that are allowed to be updated in this class.
        """

    def update_fields(self, updates: Dict):
        """
        Given a dictionary of updates to be made to this data class, loop through the updates
        applying them when valid.

        Args:
            updates: A dictionary of updates to be made to this data class.
        """
        # Iterate through the updates
        for field_name, new_value in updates.items():
            if field_name == "id":
                continue

            if hasattr(self, field_name):
                if getattr(self, field_name) == new_value:  # Not an update so skip
                    continue

                if field_name in self.fields_allowed_to_be_updated:
                    # Update the allowed field
                    setattr(self, field_name, new_value)
                else:
                    # Log a warning for unauthorized updates
                    LOG.warning(f"Field '{field_name}' is not allowed to be updated. Ignoring the change.")
            else:
                # Log a warning if the field doesn't exist explicitly
                LOG.warning(
                    f"Field '{field_name}' does not explicitly exist in the object. Adding it to the 'additional_data' field."
                )
                self.additional_data[field_name] = new_value


@dataclass
class StudyModel(BaseDataModel):
    """
    A dataclass to store all of the information for a study.

    Attributes:
        additional_data (Dict): For any extra data not explicitly defined.
        fields_allowed_to_be_updated (List[str]): A list of field names that are
            allowed to be updated.
        id (str): The unique ID for the study.
        name (str): The name of the study.
        runs (List[str]): A list of runs associated with this study.
    """

    id: str = field(default_factory=lambda: str(uuid.uuid4()))  # pylint: disable=invalid-name
    name: str = None
    runs: List[str] = field(default_factory=list)

    @property
    def fields_allowed_to_be_updated(self) -> List[str]:
        """
        Define the fields that are allowed to be updated for a `StudyModel` object.

        Returns:
            A list of fields that are allowed to be updated in this class.
        """
        return ["runs"]


@dataclass
class RunModel(BaseDataModel):  # pylint: disable=too-many-instance-attributes
    """
    A dataclass to store all of the information for a run.

    Attributes:
        additional_data (Dict): For any extra data not explicitly defined.
        child (str): The ID of the child run (if any).
        fields_allowed_to_be_updated (List[str]): A list of field names that are allowed
            to be updated.
        id (str): The unique ID for the run.
        parameters (Dict): The parameters used in this run.
        parent (str): The ID of the parent run (if any).
        queues (List[str]): The task queues used for this run.
        run_complete (bool): Wether the run is complete.
        samples (Dict): The samples used in this run.
        steps (List[str]): A list of unique step IDs that are executed in this run.
            Each ID will correspond to a `StepInfo` entry.
        study_id (str): The unique ID of the study this run is associated with.
            Corresponds with a `StudyModel` entry.
        workers (List[str]): A list of worker ids executing tasks for this run. Each ID
            will correspond with a `LogicalWorkerModel` entry.
        workspace (str): The path to the output workspace.
    """

    id: str = field(default_factory=lambda: str(uuid.uuid4()))  # pylint: disable=invalid-name
    study_id: str = None
    workspace: str = None
    steps: List[str] = field(default_factory=list)  # TODO NOT YET IMPLEMENTED
    queues: List[str] = field(default_factory=list)
    workers: List[str] = field(default_factory=list)
    parent: str = None  # TODO NOT YET IMPLEMENTED; do we even have a good way that this and `child` can be set?
    child: str = None  # TODO NOT YET IMPLEMENTED
    run_complete: bool = False
    parameters: Dict = field(default_factory=dict)  # TODO NOT YET IMPLEMENTED
    samples: Dict = field(default_factory=dict)  # TODO NOT YET IMPLEMENTED

    @property
    def fields_allowed_to_be_updated(self) -> List[str]:
        """
        Define the fields that are allowed to be updated for a `RunModel` object.

        Returns:
            A list of fields that are allowed to be updated in this class.
        """
        return ["parent", "child", "run_complete", "additional_data", "workers"]


@dataclass
class LogicalWorkerModel(BaseDataModel):
    """
    Represents a high-level definition of a Celery worker, as defined by the user.

    Logical workers are abstract representations of workers that define their behavior
    and configuration, such as the queues they listen to and their name. They are unique
    based on their name and queues, and do not correspond directly to any running process.
    Instead, they serve as templates or logical definitions from which physical workers
    are created.

    Note:
        Logical workers are abstract and do not represent actual running processes. They are
        used to define worker behavior and configuration at a high level, while physical workers
        represent the actual running instances of these logical definitions.

    Attributes:
        additional_data (Dict): For any extra data not explicitly defined.
        fields_allowed_to_be_updated (List[str]): A list of field names that are
            allowed to be updated.
        id (str): A unique identifier for the logical worker. Defaults to a UUID string.
        name (str): The name of the logical worker.
        physical_workers (List[str]): A list of unique IDs of the physical worker instances
            created from this logical instance. Corresponds with
            [`PhyiscalWorkerModel`][db_scripts.data_models.PhysicalWorkerModel] entries.
        queues (List[str]): A list of task queues the worker is listening to.
        runs (List[str]): A list of unique IDs of the runs using this worker.
            Corresponds with [`RunModel`][db_scripts.data_models.RunModel] entries.
    """

    name: str = None
    queues: Set[str] = field(default_factory=set)
    id: str = None  # pylint: disable=invalid-name
    runs: List[str] = field(default_factory=list)
    physical_workers: List[str] = field(default_factory=list)

    def __post_init__(self):
        """
        Generate and save a UUID based on the values of `name` and `queues`, to help ensure that
        each logical worker is unique to these values.

        Raises:
            TypeError: When name or queues are not provided to the constructor.
        """
        if self.name is None or not self.queues:
            raise TypeError("The `name` and `queues` arguments of LogicalWorkerModel are required.")

        generated_id = self.generate_id(self.name, self.queues)
        if self.id != generated_id:
            if self.id is not None:
                LOG.warning(f"ID '{self.id}' for LogicalWorkerModel was provided but it will be overwritten.")
            self.id = generated_id

    @classmethod
    def generate_id(cls, name: str, queues: List[str]) -> uuid.UUID:
        """
        Generate a UUID based on the values of `name` and `queues`.

        Args:
            name: The name of the logical worker.
            queues: The queues that the logical worker is assigned.

        Returns:
            A UUID based on the values of `name` and `queues`.
        """
        unique_string = f"{name}:{','.join(sorted(queues))}"
        hex_string = hashlib.md5(unique_string.encode("UTF-8")).hexdigest()
        return str(uuid.UUID(hex=hex_string))

    @property
    def fields_allowed_to_be_updated(self) -> List[str]:
        """
        Define the fields that are allowed to be updated for a `LogicalWorkerModel` object.

        Returns:
            A list of fields that are allowed to be updated in this class.
        """
        return ["runs", "physical_workers"]


@dataclass
class PhysicalWorkerModel(BaseDataModel):  # pylint: disable=too-many-instance-attributes
    """
    Represents a running instance of a Celery worker, created from a logical worker definition.

    Physical workers are the actual implementations of logical workers, running as processes on a host machine.
    They are responsible for executing tasks defined in the queues specified by their corresponding logical worker.
    Each physical worker is uniquely identified and includes runtime-specific details such as its PID, status, and
    heartbeat timestamp.

    Attributes:
        additional_data (Dict): For any extra data not explicitly defined.
        args (Dict): A dictionary of arguments used to configure the worker.
        fields_allowed_to_be_updated (List[str]): A list of field names that are
            allowed to be updated.
        heartbeat_timestamp (datetime): The last time the worker sent a heartbeat signal.
        host (str): The hostname or IP address of the machine running the worker.
        id (str): A unique identifier for the physical worker. Defaults to a UUID string.
        latest_start_time (datetime): The timestamp when the worker process was last started.
        launch_cmd (str): The command used to launch the worker process.
        logical_worker_id (str): The ID of the logical worker that this was created from.
        name (str): The name of the physical worker.
        pid (str): The process ID (PID) of the worker process.
        restart_count (int): The number of times this worker has been restarted.
        status (WorkerStatus): The current status of the worker (e.g., running, stopped).
    """

    id: str = field(default_factory=lambda: str(uuid.uuid4()))  # pylint: disable=invalid-name
    logical_worker_id: str = None
    name: str = None  # Will be of the form celery@worker_name.hostname
    launch_cmd: str = None
    args: Dict = field(default_factory=dict)
    pid: str = None
    status: WorkerStatus = WorkerStatus.STOPPED
    heartbeat_timestamp: datetime = field(default_factory=datetime.now)
    latest_start_time: datetime = field(default_factory=datetime.now)
    host: str = None
    restart_count: int = 0

    @property
    def fields_allowed_to_be_updated(self) -> List[str]:
        """
        Define the fields that are allowed to be updated for a `PhysicalWorkerModel` object.

        Returns:
            A list of fields that are allowed to be updated in this class.
        """
        return [
            "launch_cmd",
            "args",
            "pid",
            "status",
            "heartbeat_timestamp",
            "latest_start_time",
            "restart_count",
        ]


# TODO create a StepInfo class to store information about a step
# - Can probably link this to status
# - Each step should have entries for parameters/samples but only those that are actually used in the step
