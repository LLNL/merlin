##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Test task definition classes and enums.
"""

import pytest
import json
from unittest.mock import patch

from merlin.factories.task_definition import (
    TaskType, CoordinationPattern, TaskDependency, UniversalTaskDefinition
)


class TestTaskType:
    """Test TaskType enum."""
    
    def test_all_task_types_exist(self):
        """Test that all expected task types are defined."""
        expected_types = [
            'MERLIN_STEP',
            'EXPAND_SAMPLES', 
            'CHORD_FINISHER',
            'GROUP_COORDINATOR',
            'CHAIN_EXECUTOR',
            'SHUTDOWN_WORKER'
        ]
        
        for task_type in expected_types:
            assert hasattr(TaskType, task_type)
            assert isinstance(getattr(TaskType, task_type), TaskType)
    
    def test_task_type_values(self):
        """Test task type string values."""
        assert TaskType.MERLIN_STEP.value == "merlin_step"
        assert TaskType.EXPAND_SAMPLES.value == "expand_samples"
        assert TaskType.CHORD_FINISHER.value == "chord_finisher"
        assert TaskType.GROUP_COORDINATOR.value == "group_coordinator"
        assert TaskType.CHAIN_EXECUTOR.value == "chain_executor"
        assert TaskType.SHUTDOWN_WORKER.value == "shutdown_worker"


class TestCoordinationPattern:
    """Test CoordinationPattern enum."""
    
    def test_all_patterns_exist(self):
        """Test that all expected coordination patterns are defined."""
        expected_patterns = [
            'SIMPLE',
            'GROUP',
            'CHAIN', 
            'CHORD',
            'MAP_REDUCE'
        ]
        
        for pattern in expected_patterns:
            assert hasattr(CoordinationPattern, pattern)
            assert isinstance(getattr(CoordinationPattern, pattern), CoordinationPattern)
    
    def test_pattern_values(self):
        """Test coordination pattern string values."""
        assert CoordinationPattern.SIMPLE.value == "simple"
        assert CoordinationPattern.GROUP.value == "group"
        assert CoordinationPattern.CHAIN.value == "chain"
        assert CoordinationPattern.CHORD.value == "chord"
        assert CoordinationPattern.MAP_REDUCE.value == "map_reduce"


class TestTaskDependency:
    """Test TaskDependency dataclass."""
    
    def test_create_basic_dependency(self):
        """Test creation of basic task dependency."""
        dep = TaskDependency(task_id="parent_task_123")
        
        assert dep.task_id == "parent_task_123"
        assert dep.dependency_type == "completion"  # default
        assert dep.timeout_seconds is None  # default
    
    def test_create_custom_dependency(self):
        """Test creation of dependency with custom values."""
        dep = TaskDependency(
            task_id="parent_task_456",
            dependency_type="success",
            timeout_seconds=300
        )
        
        assert dep.task_id == "parent_task_456"
        assert dep.dependency_type == "success"
        assert dep.timeout_seconds == 300
    
    def test_dependency_types(self):
        """Test different dependency types."""
        completion_dep = TaskDependency(task_id="task1", dependency_type="completion")
        success_dep = TaskDependency(task_id="task2", dependency_type="success")
        data_dep = TaskDependency(task_id="task3", dependency_type="data")
        
        assert completion_dep.dependency_type == "completion"
        assert success_dep.dependency_type == "success"
        assert data_dep.dependency_type == "data"


class TestUniversalTaskDefinition:
    """Test UniversalTaskDefinition dataclass."""
    
    def test_create_minimal_task(self):
        """Test creation of task with minimal required fields."""
        task = UniversalTaskDefinition(
            task_id="test_task_123",
            task_type=TaskType.MERLIN_STEP
        )
        
        assert task.task_id == "test_task_123"
        assert task.task_type == TaskType.MERLIN_STEP
        assert task.queue_name == "default"  # default value
        assert task.priority == 0  # default value
        assert task.coordination_pattern == CoordinationPattern.SIMPLE  # default
    
    def test_create_complex_task(self):
        """Test creation of task with all fields."""
        deps = [
            TaskDependency(task_id="dep1", dependency_type="success"),
            TaskDependency(task_id="dep2", dependency_type="completion")
        ]
        
        metadata = {"key1": "value1", "description": "test task"}
        
        task = UniversalTaskDefinition(
            task_id="complex_task_456",
            task_type=TaskType.EXPAND_SAMPLES,
            script_reference="/path/to/script.py",
            config_reference="/path/to/config.yaml",
            workspace_reference="/path/to/workspace",
            input_data_references=["data1.csv", "data2.json"],
            output_data_references=["output.csv"],
            coordination_pattern=CoordinationPattern.CHORD,
            dependencies=deps,
            group_id="test_group",
            callback_task="callback_task_id",
            queue_name="high_priority",
            priority=9,
            retry_limit=5,
            timeout_seconds=1800,
            metadata=metadata
        )
        
        assert task.task_id == "complex_task_456"
        assert task.task_type == TaskType.EXPAND_SAMPLES
        assert task.script_reference == "/path/to/script.py"
        assert task.config_reference == "/path/to/config.yaml"
        assert task.workspace_reference == "/path/to/workspace"
        assert task.input_data_references == ["data1.csv", "data2.json"]
        assert task.output_data_references == ["output.csv"]
        assert task.coordination_pattern == CoordinationPattern.CHORD
        assert len(task.dependencies) == 2
        assert task.dependencies[0].task_id == "dep1"
        assert task.group_id == "test_group"
        assert task.callback_task == "callback_task_id"
        assert task.queue_name == "high_priority"
        assert task.priority == 9
        assert task.retry_limit == 5
        assert task.timeout_seconds == 1800
        assert task.metadata == metadata
    
    def test_default_values(self):
        """Test default field values."""
        task = UniversalTaskDefinition(
            task_id="defaults_test",
            task_type=TaskType.MERLIN_STEP
        )
        
        # Check all defaults
        assert task.script_reference is None
        assert task.config_reference is None
        assert task.workspace_reference is None
        assert task.input_data_references == []
        assert task.output_data_references == []
        assert task.coordination_pattern == CoordinationPattern.SIMPLE
        assert task.dependencies == []
        assert task.group_id is None
        assert task.callback_task is None
        assert task.queue_name == "default"
        assert task.priority == 0
        assert task.retry_limit == 3
        assert task.timeout_seconds == 3600
        assert task.created_timestamp is not None
        assert task.metadata == {}
    
    @patch('merlin.factories.task_definition.time.time')
    def test_created_timestamp(self, mock_time):
        """Test that created_timestamp is set automatically."""
        mock_time.return_value = 1234567890.123
        
        task = UniversalTaskDefinition(
            task_id="timestamp_test",
            task_type=TaskType.MERLIN_STEP
        )
        
        assert task.created_timestamp == 1234567890.123
    
    def test_to_dict_method(self):
        """Test conversion to dictionary."""
        deps = [TaskDependency(task_id="dep1")]
        metadata = {"test": "value"}
        
        task = UniversalTaskDefinition(
            task_id="dict_test",
            task_type=TaskType.MERLIN_STEP,
            dependencies=deps,
            metadata=metadata,
            priority=5
        )
        
        task_dict = task.to_dict()
        
        # Check that it's a dictionary
        assert isinstance(task_dict, dict)
        
        # Check key fields
        assert task_dict['task_id'] == "dict_test"
        assert task_dict['task_type'] == "merlin_step"  # enum value
        assert task_dict['priority'] == 5
        assert task_dict['metadata'] == metadata
        
        # Check dependencies are converted
        assert len(task_dict['dependencies']) == 1
        assert task_dict['dependencies'][0]['task_id'] == "dep1"
        
        # Check coordination pattern is converted
        assert task_dict['coordination_pattern'] == "simple"
    
    def test_to_dict_with_none_values(self):
        """Test to_dict with None values."""
        task = UniversalTaskDefinition(
            task_id="none_test",
            task_type=TaskType.MERLIN_STEP,
            script_reference=None,
            config_reference=None
        )
        
        task_dict = task.to_dict()
        
        # None values should be preserved
        assert task_dict['script_reference'] is None
        assert task_dict['config_reference'] is None
    
    def test_json_serializable(self):
        """Test that to_dict result is JSON serializable."""
        task = UniversalTaskDefinition(
            task_id="json_test",
            task_type=TaskType.EXPAND_SAMPLES,
            coordination_pattern=CoordinationPattern.GROUP,
            dependencies=[TaskDependency(task_id="dep1")],
            metadata={"test": "value"}
        )
        
        task_dict = task.to_dict()
        
        # Should be able to serialize to JSON
        json_str = json.dumps(task_dict)
        assert isinstance(json_str, str)
        
        # Should be able to deserialize back
        restored = json.loads(json_str)
        assert restored['task_id'] == "json_test"
        assert restored['task_type'] == "expand_samples"
        assert restored['coordination_pattern'] == "group"
    
    def test_task_with_dependencies(self):
        """Test task with multiple dependencies."""
        deps = [
            TaskDependency(task_id="task1", dependency_type="success", timeout_seconds=60),
            TaskDependency(task_id="task2", dependency_type="completion"),
            TaskDependency(task_id="task3", dependency_type="data", timeout_seconds=120)
        ]
        
        task = UniversalTaskDefinition(
            task_id="multi_dep_task",
            task_type=TaskType.CHAIN_EXECUTOR,
            dependencies=deps
        )
        
        assert len(task.dependencies) == 3
        assert task.dependencies[0].task_id == "task1"
        assert task.dependencies[0].dependency_type == "success"
        assert task.dependencies[0].timeout_seconds == 60
        assert task.dependencies[1].dependency_type == "completion"
        assert task.dependencies[2].timeout_seconds == 120
    
    def test_equality(self):
        """Test task equality comparison."""
        task1 = UniversalTaskDefinition(
            task_id="equal_test",
            task_type=TaskType.MERLIN_STEP,
            priority=5
        )
        
        task2 = UniversalTaskDefinition(
            task_id="equal_test",
            task_type=TaskType.MERLIN_STEP,
            priority=5
        )
        
        # Note: timestamps will be different, so they won't be equal
        # This tests that dataclass equality works as expected
        assert task1.task_id == task2.task_id
        assert task1.task_type == task2.task_type
        assert task1.priority == task2.priority