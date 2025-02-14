"""
"""
import json
import logging
import uuid
from abc import ABC, abstractmethod
from dataclasses import Field, dataclass, field, fields, asdict
from typing import Dict, List, Tuple, Type, TypeVar


LOG = logging.getLogger("merlin")
T = TypeVar("T", bound="BaseDataClass")


@dataclass
class BaseDataClass(ABC):
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

    def fields(self) -> Tuple[Field]:
        """
        Get the fields associated with this instance. Added this method so that the dataclass.fields
        doesn't have to be imported each time you want this info.

        Returns:
            A tuple of dataclass.Field objects representing the fields in this data class.
        """
        return fields(self)

    @classmethod
    def fields(cls) -> Tuple[Field]:
        """
        Get the fields associated with this object. Added this method so that the dataclass.fields
        doesn't have to be imported each time you want this info.

        Returns:
            A tuple of dataclass.Field objects representing the fields in this data class.
        """
        return fields(cls)

    @property
    @abstractmethod
    def fields_allowed_to_be_updated(self) -> List[str]:
        """
        A property to be overridden in subclasses to define which fields are allowed to be updated.

        Returns:
            A list of fields that are allowed to be updated in this class.
        """
        pass

    def update_fields(self, updates: Dict):
        """
        Given a dictionary of updates to be made to this data class, loop through the updates
        applying them when valid.

        Args:
            updates: A dictionary of updates to be made to this data class.
        """
        # Iterate through the updates
        for field_name, new_value in updates.items():
            if field_name not in self.fields_allowed_to_be_updated and getattr(self, field_name) != updates[field_name]:
                # Log a warning for unauthorized updates
                LOG.warning(f"Field '{field_name}' is not allowed to be updated. Ignoring the change.")
            elif hasattr(self, field_name):
                # Update the allowed field
                setattr(self, field_name, new_value)
            else:
                # Log a warning if the field doesn't exist explicitly
                LOG.warning(
                    f"Field '{field_name}' does not explicitly exist in the object. Adding it to the 'additional_data' field."
                )
                self.additional_data[field_name] = new_value


@dataclass
class StudyInfo(BaseDataClass):
    """
    A dataclass to store all of the information for a study.

    Attributes:
        fields_allowed_to_be_updated: A list of field names that are allowed to be updated.
        id: The unique ID for the study.
        name: The name of the study.
        runs: A list of runs associated with this study.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = None
    runs: List[str] = field(default_factory=list)

    @property
    def fields_allowed_to_be_updated(self) -> List[str]:
        """
        Define the fields that are allowed to be updated for a `StudyInfo` object.

        Returns:
            A list of fields that are allowed to be updated in this class.
        """
        return ["runs"]


@dataclass
class RunInfo(BaseDataClass):
    """
    A dataclass to store all of the information for a run.

    Attributes:
        additional_data: For any extra data not explicitly defined.
        child: The ID of the child run (if any).
        fields_allowed_to_be_updated: A list of field names that are allowed to be updated.
        id: The unique ID for the run.
        parameters: The parameters used in this run.
        parent: The ID of the parent run (if any).
        queues: The task queues used for this run.
        run_complete: Wether the run is complete.
        samples: The samples used in this run.
        study_id: The unique ID of the study this run is associated with.
        workspace: The path to the output workspace.
    """
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    study_id: str = None
    workspace: str = None
    queues: List[str] = field(default_factory=list)
    parent: str = None
    child: str = None
    run_complete: bool = False
    parameters: Dict = field(default_factory=dict)  # TODO NOT YET IMPLEMENTED
    samples: Dict = field(default_factory=dict)  # TODO NOT YET IMPLEMENTED

    @property
    def fields_allowed_to_be_updated(self) -> List[str]:
        """
        Define the fields that are allowed to be updated for a `RunInfo` object.

        Returns:
            A list of fields that are allowed to be updated in this class.
        """
        return ["parent", "child", "run_complete", "additional_data"]
