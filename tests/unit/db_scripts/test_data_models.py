import json
import os
from dataclasses import dataclass
from datetime import datetime
from unittest.mock import patch

import pytest
from _pytest.capture import CaptureFixture
from pytest_mock import MockerFixture

from merlin.common.enums import WorkerStatus
from merlin.db_scripts.data_models import BaseDataModel, LogicalWorkerModel, PhysicalWorkerModel, RunModel, StudyModel
from tests.fixture_types import FixtureCallable, FixtureStr


# Create a concrete subclass of BaseDataModel for testing
@pytest.fixture
def concrete_base_model_class() -> BaseDataModel:
    """
    Creates a concrete subclass of `BaseDataModel` for testing.

    Returns:
        A concrete subclass of `BaseDataModel`.
    """

    @dataclass
    class ConcreteBaseModel(BaseDataModel):
        field1: str = "default"
        field2: int = 0

        @property
        def fields_allowed_to_be_updated(self):
            return ["field1"]

    return ConcreteBaseModel


@pytest.fixture(scope="session")
def db_scripts_testing_dir(create_testing_dir: FixtureCallable, temp_output_dir: FixtureStr) -> FixtureStr:
    """
    Fixture to create a temporary output directory for tests of database scripts.

    Args:
        create_testing_dir: A fixture which returns a function that creates the testing directory.
        temp_output_dir: The path to the temporary ouptut directory we'll be using for this test run.

    Returns:
        The path to the temporary testing directory for database script tests.
    """
    return create_testing_dir(temp_output_dir, "db_scripts_testing")


class TestBaseDataModel:
    """Tests for the BaseDataModel abstract base class."""

    def test_to_dict(self, concrete_base_model_class: BaseDataModel):
        """
        Test conversion to dictionary.

        Args:
            concrete_base_model_class: A concrete subclass of `BaseDataModel`.
        """
        model = concrete_base_model_class(field1="test", field2=42)
        model_dict = model.to_dict()

        assert model_dict["field1"] == "test"
        assert model_dict["field2"] == 42
        assert "additional_data" in model_dict

    def test_to_json(self, concrete_base_model_class: BaseDataModel):
        """
        Test conversion to JSON.

        Args:
            concrete_base_model_class: A concrete subclass of `BaseDataModel`.
        """
        model = concrete_base_model_class(field1="test", field2=42)
        json_str = model.to_json()

        # Parse back to verify the structure
        parsed = json.loads(json_str)
        assert parsed["field1"] == "test"
        assert parsed["field2"] == 42

    def test_from_dict(self, concrete_base_model_class: BaseDataModel):
        """
        Test creation from dictionary.

        Args:
            concrete_base_model_class: A concrete subclass of `BaseDataModel`.
        """
        data = {"field1": "from_dict", "field2": 99, "additional_data": {"extra": "data"}}
        model = concrete_base_model_class.from_dict(data)

        assert model.field1 == "from_dict"
        assert model.field2 == 99
        assert model.additional_data == {"extra": "data"}

    def test_from_json(self, concrete_base_model_class: BaseDataModel):
        """
        Test creation from JSON string.

        Args:
            concrete_base_model_class: A concrete subclass of `BaseDataModel`.
        """
        json_str = '{"field1": "from_json", "field2": 77, "additional_data": {"json": "data"}}'
        model = concrete_base_model_class.from_json(json_str)

        assert model.field1 == "from_json"
        assert model.field2 == 77
        assert model.additional_data == {"json": "data"}

    def test_dump_to_json_file(
        self, mocker: MockerFixture, concrete_base_model_class: BaseDataModel, db_scripts_testing_dir: FixtureStr
    ):
        """
        Test writing to a JSON file.

        Args:
            mocker: A built-in fixture from the pytest-mock library to create a Mock object.
            concrete_base_model_class: A concrete subclass of `BaseDataModel`.
            db_scripts_testing_dir: The path to the temporary testing directory for database script tests.
        """
        model = concrete_base_model_class(field1="file_test", field2=123)
        filepath = os.path.join(db_scripts_testing_dir, "base_data_model_test_dump_to_json.json")

        # Mock the FileLock
        with mocker.patch("merlin.db_scripts.data_models.FileLock"):
            model.dump_to_json_file(filepath)

            # Verify the file exists and contains expected content
            assert os.path.exists(filepath)
            with open(filepath, "r") as f:
                data = json.load(f)
                assert data["field1"] == "file_test"
                assert data["field2"] == 123

    def test_dump_to_json_file_invalid_path(self, concrete_base_model_class: BaseDataModel):
        """
        Test error handling for invalid filepath.

        Args:
            concrete_base_model_class: A concrete subclass of `BaseDataModel`.
        """
        model = concrete_base_model_class()

        with pytest.raises(ValueError):
            model.dump_to_json_file("")

    def test_load_from_json_file(self, concrete_base_model_class: BaseDataModel, db_scripts_testing_dir: FixtureStr):
        """
        Test loading from a JSON file.

        Args:
            concrete_base_model_class: A concrete subclass of `BaseDataModel`.
            db_scripts_testing_dir: The path to the temporary testing directory for database script tests.
        """
        # Create a test file
        test_data = {"field1": "loaded", "field2": 321, "additional_data": {}}
        filepath = os.path.join(db_scripts_testing_dir, "base_data_model_test_load_from_json_file.json")

        with open(filepath, "w") as f:
            json.dump(test_data, f)

        # Mock the FileLock
        with patch("merlin.db_scripts.data_models.FileLock"):
            model = concrete_base_model_class.load_from_json_file(filepath)

            assert model.field1 == "loaded"
            assert model.field2 == 321

    def test_load_from_json_file_not_exists(self, concrete_base_model_class: BaseDataModel):
        """
        Test error handling when file does not exist.

        Args:
            concrete_base_model_class: A concrete subclass of `BaseDataModel`.
        """
        with pytest.raises(ValueError):
            concrete_base_model_class.load_from_json_file("/nonexistent/file.json")

    def test_get_instance_fields(self, concrete_base_model_class: BaseDataModel):
        """
        Test retrieving instance fields.

        Args:
            concrete_base_model_class: A concrete subclass of `BaseDataModel`.
        """
        model = concrete_base_model_class()
        fields = model.get_instance_fields()

        field_names = [f.name for f in fields]
        assert "field1" in field_names
        assert "field2" in field_names
        assert "additional_data" in field_names

    def test_get_class_fields(self, concrete_base_model_class: BaseDataModel):
        """
        Test retrieving class fields.

        Args:
            concrete_base_model_class: A concrete subclass of `BaseDataModel`.
        """
        fields = concrete_base_model_class.get_class_fields()

        field_names = [f.name for f in fields]
        assert "field1" in field_names
        assert "field2" in field_names
        assert "additional_data" in field_names

    def test_update_fields_allowed(self, concrete_base_model_class: BaseDataModel):
        """
        Test updating allowed fields.

        Args:
            concrete_base_model_class: A concrete subclass of `BaseDataModel`.
        """
        model = concrete_base_model_class(field1="original", field2=100)

        model.update_fields({"field1": "updated"})
        assert model.field1 == "updated"
        assert model.field2 == 100

    def test_update_fields_not_allowed(self, caplog: CaptureFixture, concrete_base_model_class: BaseDataModel):
        """
        Test attempting to update fields that are not allowed.

        Args:
            caplog: A built-in fixture from the pytest library to capture logs.
            concrete_base_model_class: A concrete subclass of `BaseDataModel`.
        """
        model = concrete_base_model_class(field1="original", field2=100)

        # Patch the logger to check for warnings
        model.update_fields({"field2": 999})

        # Field should not be updated
        assert model.field2 == 100

        # Warning should be logged
        assert "not allowed to be updated" in caplog.text

    def test_update_fields_nonexistent(self, caplog: CaptureFixture, concrete_base_model_class: BaseDataModel):
        """
        Test updating fields that don't exist on the model.

        Args:
            caplog: A built-in fixture from the pytest library to capture logs.
            concrete_base_model_class: A concrete subclass of `BaseDataModel`.
        """
        model = concrete_base_model_class()
        model.update_fields({"nonexistent_field": "value"})

        # Should be added to additional_data
        assert model.additional_data["nonexistent_field"] == "value"

        # Warning should be logged
        assert "does not explicitly exist" in caplog.text

    def test_update_fields_ignore_id(self, concrete_base_model_class: BaseDataModel):
        """
        Test that ID field cannot be updated.

        Args:
            concrete_base_model_class: A concrete subclass of `BaseDataModel`.
        """
        model = concrete_base_model_class()
        original_id = model.id if hasattr(model, "id") else None

        model.update_fields({"id": "new_id"})

        # ID shouldn't be updated, even if it's in allowed fields
        if hasattr(model, "id"):
            assert model.id == original_id


class TestStudyModel:
    """Tests for the StudyModel class."""

    def test_default_initialization(self):
        """Test default initialization of StudyModel."""
        study = StudyModel()

        assert isinstance(study.id, str)
        assert study.name is None
        assert study.runs == []
        assert study.additional_data == {}

    def test_custom_initialization(self):
        """Test initialization with custom values."""
        study = StudyModel(id="custom-id", name="Test Study", runs=["run1", "run2"], additional_data={"key": "value"})

        assert study.id == "custom-id"
        assert study.name == "Test Study"
        assert study.runs == ["run1", "run2"]
        assert study.additional_data == {"key": "value"}

    def test_allowed_fields_update(self, caplog: CaptureFixture):
        """
        Test updating allowed fields.

        Args:
            caplog: A built-in fixture from the pytest library to capture logs.
        """
        study = StudyModel(name="Original Study", runs=["run1"])

        # Test allowed field
        study.update_fields({"runs": ["run1", "run2"]})
        assert study.runs == ["run1", "run2"]

        # Test not allowed field
        study.update_fields({"name": "Updated Study"})
        assert study.name == "Original Study"  # Should be the same as before
        assert "not allowed to be updated" in caplog.text  # Warning should be logged

    def test_fields_allowed_to_be_updated(self):
        """Test that fields_allowed_to_be_updated returns expected values."""
        study = StudyModel()
        assert study.fields_allowed_to_be_updated == ["runs"]


class TestRunModel:
    """Tests for the RunModel class."""

    def test_default_initialization(self):
        """Test default initialization of RunModel."""
        run = RunModel()

        assert isinstance(run.id, str)
        assert run.study_id is None
        assert run.workspace is None
        assert run.steps == []
        assert run.queues == []
        assert run.workers == []
        assert run.parent is None
        assert run.child is None
        assert run.run_complete is False
        assert run.parameters == {}
        assert run.samples == {}
        assert run.additional_data == {}

    def test_custom_initialization(self):
        """Test initialization with custom values."""
        run = RunModel(
            id="run-id",
            study_id="study-id",
            workspace="/path/to/workspace",
            steps=["step1", "step2"],
            queues=["queue1", "queue2"],
            workers=["worker1"],
            parent="parent-run",
            child="child-run",
            run_complete=True,
            parameters={"param1": "value1"},
            samples={"sample1": "data1"},
            additional_data={"meta": "data"},
        )

        assert run.id == "run-id"
        assert run.study_id == "study-id"
        assert run.workspace == "/path/to/workspace"
        assert run.steps == ["step1", "step2"]
        assert run.queues == ["queue1", "queue2"]
        assert run.workers == ["worker1"]
        assert run.parent == "parent-run"
        assert run.child == "child-run"
        assert run.run_complete is True
        assert run.parameters == {"param1": "value1"}
        assert run.samples == {"sample1": "data1"}
        assert run.additional_data == {"meta": "data"}

    def test_allowed_fields_update(self, caplog: CaptureFixture):
        """
        Test updating allowed fields.

        Args:
            caplog: A built-in fixture from the pytest library to capture logs.
        """
        run = RunModel(workers=["worker1"], study_id="study-id")

        # Test allowed field
        run.update_fields({"workers": ["worker1", "worker2"]})
        assert run.workers == ["worker1", "worker2"]

        # Test not allowed field
        run.update_fields({"study_id": "new-study-id"})
        assert run.study_id == "study-id"  # Should be the same as before
        assert "not allowed to be updated" in caplog.text  # Warning should be logged

    def test_fields_allowed_to_be_updated(self):
        """Test that fields_allowed_to_be_updated returns expected values."""
        run = RunModel()
        assert set(run.fields_allowed_to_be_updated) == {"parent", "child", "run_complete", "additional_data", "workers"}


class TestLogicalWorkerModel:
    """Tests for the LogicalWorkerModel class."""

    def test_initialization_requires_name_and_queues(self):
        """Test that initialization requires name and queues."""
        # Missing name
        with pytest.raises(TypeError):
            LogicalWorkerModel(queues={"queue1"})

        # Missing queues
        with pytest.raises(TypeError):
            LogicalWorkerModel(name="worker1")

        # Empty queues
        with pytest.raises(TypeError):
            LogicalWorkerModel(name="worker1", queues={})

        # Valid initialization
        worker = LogicalWorkerModel(name="worker1", queues={"queue1"})
        assert worker.name == "worker1"
        assert worker.queues == {"queue1"}

    def test_id_generation(self):
        """Test that ID is generated consistently based on name and queues."""
        worker1 = LogicalWorkerModel(name="worker1", queues={"queue1", "queue2"})
        worker2 = LogicalWorkerModel(name="worker1", queues={"queue2", "queue1"})

        # Should generate the same ID regardless of queue order
        assert worker1.id == worker2.id

        # Different name should generate different ID
        worker3 = LogicalWorkerModel(name="different", queues={"queue1", "queue2"})
        assert worker1.id != worker3.id

        # Different queue should generate different ID
        worker4 = LogicalWorkerModel(name="worker1", queues={"queue1", "queue3"})
        assert worker1.id != worker4.id

    def test_id_overwrite_warning(self, caplog: CaptureFixture):
        """
        Test warning when provided ID is overwritten.

        Args:
            caplog: A built-in fixture from the pytest library to capture logs.
        """
        worker = LogicalWorkerModel(name="worker1", queues={"queue1"}, id="provided-id")

        # ID should be overwritten
        assert worker.id != "provided-id"

        # Warning should be logged
        assert "will be overwritten" in caplog.text

    def test_allowed_fields_update(self, caplog: CaptureFixture):
        """
        Test updating allowed fields.

        Args:
            caplog: A built-in fixture from the pytest library to capture logs.
        """
        worker = LogicalWorkerModel(name="worker1", queues={"queue1"})

        # Test allowed field
        worker.update_fields({"runs": ["run1", "run2"]})
        assert worker.runs == ["run1", "run2"]

        worker.update_fields({"physical_workers": ["pw1", "pw2"]})
        assert worker.physical_workers == ["pw1", "pw2"]

        # Test not allowed field
        worker.update_fields({"name": "new-name"})

        # Field should not be updated
        assert worker.name == "worker1"

        # Warning should be logged
        assert "not allowed to be updated" in caplog.text

    def test_generate_id_class_method(self):
        """Test the class method for ID generation."""
        id1 = LogicalWorkerModel.generate_id("worker1", ["queue1", "queue2"])
        id2 = LogicalWorkerModel.generate_id("worker1", ["queue2", "queue1"])

        # Same inputs should produce same ID
        assert id1 == id2

        # Different inputs should produce different IDs
        id3 = LogicalWorkerModel.generate_id("worker2", ["queue1", "queue2"])
        assert id1 != id3

    def test_fields_allowed_to_be_updated(self):
        """Test that fields_allowed_to_be_updated returns expected values."""
        worker = LogicalWorkerModel(name="worker1", queues={"queue1"})
        assert worker.fields_allowed_to_be_updated == ["runs", "physical_workers"]


class TestPhysicalWorkerModel:
    """Tests for the PhysicalWorkerModel class."""

    def test_default_initialization(self):
        """Test default initialization of PhysicalWorkerModel."""
        worker = PhysicalWorkerModel()

        assert isinstance(worker.id, str)
        assert worker.logical_worker_id is None
        assert worker.name is None
        assert worker.launch_cmd is None
        assert worker.args == {}
        assert worker.pid is None
        assert worker.status == WorkerStatus.STOPPED
        assert isinstance(worker.heartbeat_timestamp, datetime)
        assert isinstance(worker.latest_start_time, datetime)
        assert worker.host is None
        assert worker.restart_count == 0
        assert worker.additional_data == {}

    def test_custom_initialization(self):
        """Test initialization with custom values."""
        current_time = datetime.now()
        worker = PhysicalWorkerModel(
            id="physical-id",
            logical_worker_id="logical-id",
            name="celery@worker1.host",
            launch_cmd="celery worker",
            args={"arg1": "value1"},
            pid="12345",
            status=WorkerStatus.RUNNING,
            heartbeat_timestamp=current_time,
            latest_start_time=current_time,
            host="hostname",
            restart_count=3,
            additional_data={"meta": "data"},
        )

        assert worker.id == "physical-id"
        assert worker.logical_worker_id == "logical-id"
        assert worker.name == "celery@worker1.host"
        assert worker.launch_cmd == "celery worker"
        assert worker.args == {"arg1": "value1"}
        assert worker.pid == "12345"
        assert worker.status == WorkerStatus.RUNNING
        assert worker.heartbeat_timestamp == current_time
        assert worker.latest_start_time == current_time
        assert worker.host == "hostname"
        assert worker.restart_count == 3
        assert worker.additional_data == {"meta": "data"}

    def test_allowed_fields_update(self, caplog: CaptureFixture):
        """
        Test updating allowed fields.

        Args:
            caplog: A built-in fixture from the pytest library to capture logs.
        """
        worker = PhysicalWorkerModel(pid="12345", name="celery@worker1.host")

        # Test allowed field
        worker.update_fields({"pid": "67890"})
        assert worker.pid == "67890"

        worker.update_fields({"status": WorkerStatus.RUNNING})
        assert worker.status == WorkerStatus.RUNNING

        # Test not allowed field
        worker.update_fields({"name": "celery@new.host"})

        # Field should not be updated
        assert worker.name == "celery@worker1.host"

        # Warning should be logged
        assert "not allowed to be updated" in caplog.text

    def test_fields_allowed_to_be_updated(self):
        """Test that fields_allowed_to_be_updated returns expected values."""
        worker = PhysicalWorkerModel()
        allowed_fields = worker.fields_allowed_to_be_updated

        # Check each expected field is in the list
        expected_fields = ["launch_cmd", "args", "pid", "status", "heartbeat_timestamp", "latest_start_time", "restart_count"]
        for field in expected_fields:
            assert field in allowed_fields

        # Check field count matches
        assert len(allowed_fields) == len(expected_fields)
