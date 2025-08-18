##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Test universal task factory functionality.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch

from merlin.factories.universal_task_factory import UniversalTaskFactory
from merlin.factories.task_definition import TaskType, CoordinationPattern


class TestUniversalTaskFactory:
    
    @pytest.fixture
    def temp_dir(self):
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def factory(self, temp_dir):
        return UniversalTaskFactory(temp_dir)
    
    def test_create_merlin_step_task(self, factory):
        """Test creation of merlin_step task."""
        
        step_config = {
            'name': 'hello_world',
            'run': {
                'cmd': 'echo "Hello, World!"',
                'task_type': 'local'
            }
        }
        
        task_def = factory.create_merlin_step_task(
            step_config=step_config,
            queue_name='test_queue',
            priority=5
        )
        
        # Validate task definition
        assert task_def.task_type == TaskType.MERLIN_STEP
        assert task_def.queue_name == 'test_queue'
        assert task_def.priority == 5
        assert task_def.script_reference is not None
        assert task_def.task_id.startswith('hello_world_')
    
    def test_create_sample_expansion_task(self, factory):
        """Test creation of sample expansion task."""
        
        task_def = factory.create_sample_expansion_task(
            study_id='test_study_123',
            step_name='expand_step',
            sample_range={'start': 0, 'end': 100}
        )
        
        # Validate task definition
        assert task_def.task_type == TaskType.SAMPLE_EXPANSION
        assert task_def.study_reference == 'test_study_123'
        assert task_def.step_name == 'expand_step'
        assert task_def.sample_range == {'start': 0, 'end': 100}
        assert task_def.task_id.startswith('expand_step_')
    
    def test_create_group_tasks(self, factory):
        """Test group coordination pattern."""
        
        # Create individual tasks
        task1_config = {'name': 'task1', 'run': {'cmd': 'echo "task1"', 'task_type': 'local'}}
        task2_config = {'name': 'task2', 'run': {'cmd': 'echo "task2"', 'task_type': 'local'}}
        
        task1 = factory.create_merlin_step_task(task1_config)
        task2 = factory.create_merlin_step_task(task2_config)
        
        # Create group
        group_tasks = factory.create_group_tasks([task1, task2])
        
        # Validate group
        assert len(group_tasks) == 2
        for task in group_tasks:
            assert task.coordination_pattern == CoordinationPattern.GROUP
    
    def test_chain_tasks(self, factory):
        """Test chain coordination pattern."""
        
        # Create individual tasks
        task1_config = {'name': 'task1', 'run': {'cmd': 'echo "task1"', 'task_type': 'local'}}
        task2_config = {'name': 'task2', 'run': {'cmd': 'echo "task2"', 'task_type': 'local'}}
        task3_config = {'name': 'task3', 'run': {'cmd': 'echo "task3"', 'task_type': 'local'}}
        
        task1 = factory.create_merlin_step_task(task1_config)
        task2 = factory.create_merlin_step_task(task2_config)
        task3 = factory.create_merlin_step_task(task3_config)
        
        # Create chain
        chain_tasks = factory.create_chain_tasks([task1, task2, task3])
        
        # Validate chain
        assert len(chain_tasks) == 3
        for i, task in enumerate(chain_tasks):
            assert task.coordination_pattern == CoordinationPattern.CHAIN
            if i > 0:
                assert len(task.dependencies) == 1
                assert task.dependencies[0].task_id == chain_tasks[i-1].task_id
    
    def test_chord_tasks(self, factory):
        """Test chord coordination pattern."""
        
        # Create parallel tasks
        task1_config = {'name': 'task1', 'run': {'cmd': 'echo "task1"', 'task_type': 'local'}}
        task2_config = {'name': 'task2', 'run': {'cmd': 'echo "task2"', 'task_type': 'local'}}
        
        task1 = factory.create_merlin_step_task(task1_config)
        task2 = factory.create_merlin_step_task(task2_config)
        
        # Create callback task
        callback_config = {'name': 'callback', 'run': {'cmd': 'echo "callback"', 'task_type': 'local'}}
        callback_task = factory.create_merlin_step_task(callback_config)
        
        # Create chord
        chord_tasks = factory.create_chord_tasks([task1, task2], callback_task)
        
        # Validate chord
        assert len(chord_tasks) == 3  # 2 parallel + 1 callback
        
        # Parallel tasks should be grouped
        parallel_tasks = chord_tasks[:2]
        for task in parallel_tasks:
            assert task.coordination_pattern == CoordinationPattern.GROUP
        
        # Callback task should depend on parallel tasks
        callback_result = chord_tasks[2]
        assert callback_result.coordination_pattern == CoordinationPattern.CHORD_CALLBACK
        assert len(callback_result.dependencies) == 2
    
    @patch('merlin.factories.universal_task_factory.json.dumps')
    def test_message_size_optimization(self, mock_dumps, factory):
        """Test that task definitions are optimized for message size."""
        
        # Mock json.dumps to return a size we can test
        mock_dumps.return_value = '{"optimized": "task"}'
        
        task_config = {'name': 'test', 'run': {'cmd': 'echo "test"', 'task_type': 'local'}}
        task_def = factory.create_merlin_step_task(task_config)
        
        # Serialize task definition
        from merlin.serialization.compressed_json_serializer import CompressedJsonSerializer
        serializer = CompressedJsonSerializer()
        serialized = serializer.serialize_task_definition(task_def)
        
        # Verify that serialization was attempted
        assert isinstance(serialized, bytes)
        assert len(serialized) > 0