##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Test signature adapter functionality.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock

from merlin.adapters.signature_adapters import (
    SignatureAdapter, CelerySignatureAdapter, KafkaSignatureAdapter
)
from merlin.factories.task_definition import UniversalTaskDefinition, TaskType, CoordinationPattern


class TestCelerySignatureAdapter:
    
    @pytest.fixture
    def mock_task_registry(self):
        """Mock task registry with Celery task functions."""
        mock_task = Mock()
        mock_task.s.return_value.set.return_value = Mock()
        
        return {
            'merlin_step': mock_task,
            'expand_samples': mock_task,
            'chord_finisher': mock_task
        }
    
    @pytest.fixture
    def adapter(self, mock_task_registry):
        return CelerySignatureAdapter(mock_task_registry)
    
    @pytest.fixture
    def simple_task(self):
        return UniversalTaskDefinition(
            task_id='test_task_123',
            task_type=TaskType.MERLIN_STEP,
            script_reference='test_script.py',
            queue_name='test_queue',
            priority=5
        )
    
    def test_create_signature_merlin_step(self, adapter, simple_task, mock_task_registry):
        """Test creation of Celery signature for merlin_step task."""
        
        mock_task = mock_task_registry['merlin_step']
        mock_signature = Mock()
        mock_task.s.return_value.set.return_value = mock_signature
        
        result = adapter.create_signature(simple_task)
        
        # Verify task function was called with correct parameters
        mock_task.s.assert_called_once_with(
            task_id='test_task_123',
            script_reference='test_script.py',
            config_reference=None,
            workspace_reference=None
        )
        
        # Verify signature was configured correctly
        mock_task.s.return_value.set.assert_called_once_with(
            queue='test_queue',
            priority=5,
            retry=3,  # default retry_limit
            time_limit=3600  # default timeout_seconds
        )
        
        assert result == mock_signature
    
    def test_create_signature_other_task_type(self, adapter, mock_task_registry):
        """Test creation of signature for non-merlin_step task."""
        
        task = UniversalTaskDefinition(
            task_id='expand_task_456',
            task_type=TaskType.EXPAND_SAMPLES,
            queue_name='expansion_queue',
            priority=3
        )
        
        mock_task = mock_task_registry['expand_samples']
        mock_signature = Mock()
        mock_task.s.return_value.set.return_value = mock_signature
        
        result = adapter.create_signature(task)
        
        # Should call with task definition dict for other task types
        mock_task.s.assert_called_once_with(
            task_definition=task.to_dict()
        )
        
        mock_task.s.return_value.set.assert_called_once_with(
            queue='expansion_queue',
            priority=3
        )
    
    def test_submit_task(self, adapter):
        """Test single task submission."""
        
        mock_signature = Mock()
        mock_result = Mock()
        mock_result.id = 'task_result_123'
        mock_signature.apply_async.return_value = mock_result
        
        result_id = adapter.submit_task(mock_signature)
        
        mock_signature.apply_async.assert_called_once()
        assert result_id == 'task_result_123'
    
    @patch('merlin.adapters.signature_adapters.group')
    def test_submit_group(self, mock_group_class, adapter):
        """Test group task submission."""
        
        signatures = [Mock(), Mock(), Mock()]
        mock_group_instance = Mock()
        mock_group_class.return_value = mock_group_instance
        
        mock_result = Mock()
        mock_result.id = 'group_result_456'
        mock_group_instance.apply_async.return_value = mock_result
        
        result_id = adapter.submit_group(signatures)
        
        mock_group_class.assert_called_once_with(signatures)
        mock_group_instance.apply_async.assert_called_once()
        assert result_id == 'group_result_456'
    
    @patch('merlin.adapters.signature_adapters.chain')
    def test_submit_chain(self, mock_chain_class, adapter):
        """Test chain task submission."""
        
        signatures = [Mock(), Mock(), Mock()]
        mock_chain_instance = Mock()
        mock_chain_class.return_value = mock_chain_instance
        
        mock_result = Mock()
        mock_result.id = 'chain_result_789'
        mock_chain_instance.apply_async.return_value = mock_result
        
        result_id = adapter.submit_chain(signatures)
        
        mock_chain_class.assert_called_once_with(signatures)
        mock_chain_instance.apply_async.assert_called_once()
        assert result_id == 'chain_result_789'
    
    @patch('merlin.adapters.signature_adapters.chord')
    def test_submit_chord(self, mock_chord_class, adapter):
        """Test chord task submission."""
        
        parallel_signatures = [Mock(), Mock()]
        callback_signature = Mock()
        
        mock_chord_instance = Mock()
        mock_chord_class.return_value = mock_chord_instance
        
        mock_job = Mock()
        mock_chord_instance.return_value = mock_job
        
        mock_result = Mock()
        mock_result.id = 'chord_result_101'
        mock_job.apply_async.return_value = mock_result
        
        result_id = adapter.submit_chord(parallel_signatures, callback_signature)
        
        mock_chord_class.assert_called_once_with(parallel_signatures)
        mock_chord_instance.assert_called_once_with(callback_signature)
        mock_job.apply_async.assert_called_once()
        assert result_id == 'chord_result_101'
    
    def test_get_task_function(self, adapter, mock_task_registry):
        """Test retrieval of task function by type."""
        
        task_func = adapter._get_task_function('merlin_step')
        assert task_func == mock_task_registry['merlin_step']
        
        # Test non-existent task type
        task_func = adapter._get_task_function('nonexistent')
        assert task_func is None


class TestKafkaSignatureAdapter:
    
    @pytest.fixture
    def mock_producer(self):
        producer = Mock()
        # Mock send method to return a future
        mock_future = Mock()
        mock_result = Mock()
        mock_result.topic = 'test_topic'
        mock_result.partition = 0
        mock_result.offset = 123
        mock_future.get.return_value = mock_result
        producer.send.return_value = mock_future
        return producer
    
    @pytest.fixture
    def mock_topic_manager(self):
        manager = Mock()
        manager.get_topic_for_queue.return_value = 'test_topic'
        return manager
    
    @pytest.fixture
    def adapter(self, mock_producer, mock_topic_manager):
        return KafkaSignatureAdapter(mock_producer, mock_topic_manager)
    
    @pytest.fixture
    def simple_task(self):
        return UniversalTaskDefinition(
            task_id='kafka_task_123',
            task_type=TaskType.MERLIN_STEP,
            queue_name='kafka_queue',
            group_id='test_group'
        )
    
    def test_create_signature(self, adapter, simple_task, mock_topic_manager):
        """Test creation of Kafka signature."""
        
        signature = adapter.create_signature(simple_task)
        
        # Verify signature structure
        assert isinstance(signature, dict)
        assert 'task_definition' in signature
        assert 'topic' in signature
        assert 'partition_key' in signature
        
        assert signature['task_definition'] == simple_task.to_dict()
        assert signature['topic'] == 'test_topic'
        assert signature['partition_key'] == 'test_group'
        
        # Verify topic manager was called
        mock_topic_manager.get_topic_for_queue.assert_called_once_with('kafka_queue')
    
    def test_create_signature_no_group_id(self, adapter, mock_topic_manager):
        """Test signature creation when no group_id is provided."""
        
        task = UniversalTaskDefinition(
            task_id='kafka_task_456',
            task_type=TaskType.MERLIN_STEP,
            queue_name='kafka_queue'
        )
        
        signature = adapter.create_signature(task)
        
        # Should use task_id as partition key when no group_id
        assert signature['partition_key'] == 'kafka_task_456'
    
    def test_submit_task(self, adapter, simple_task, mock_producer):
        """Test single task submission to Kafka."""
        
        signature = adapter.create_signature(simple_task)
        result_id = adapter.submit_task(signature)
        
        # Verify producer was called correctly
        mock_producer.send.assert_called_once_with(
            'test_topic',
            value=simple_task.to_dict(),
            key='test_group'
        )
        
        # Verify result ID format
        assert result_id == 'test_topic:0:123'
    
    def test_submit_group(self, adapter, mock_producer):
        """Test group submission to Kafka."""
        
        # Create signatures for group
        task1 = UniversalTaskDefinition(task_id='task1', task_type=TaskType.MERLIN_STEP, group_id='group1')
        task2 = UniversalTaskDefinition(task_id='task2', task_type=TaskType.MERLIN_STEP, group_id='group1')
        
        signatures = [
            adapter.create_signature(task1),
            adapter.create_signature(task2)
        ]
        
        result_id = adapter.submit_group(signatures)
        
        # Should submit both tasks
        assert mock_producer.send.call_count == 2
        assert result_id == 'group1'
    
    def test_submit_chain(self, adapter, mock_producer):
        """Test chain submission to Kafka."""
        
        # Create signatures for chain
        task1 = UniversalTaskDefinition(task_id='chain1', task_type=TaskType.MERLIN_STEP, group_id='chain_group')
        task2 = UniversalTaskDefinition(task_id='chain2', task_type=TaskType.MERLIN_STEP, group_id='chain_group')
        
        signatures = [
            adapter.create_signature(task1),
            adapter.create_signature(task2)
        ]
        
        result_id = adapter.submit_chain(signatures)
        
        # Should submit all tasks
        assert mock_producer.send.call_count == 2
        assert result_id == 'chain_group'
    
    def test_submit_chord(self, adapter, mock_producer):
        """Test chord submission to Kafka."""
        
        # Create parallel signatures
        task1 = UniversalTaskDefinition(task_id='parallel1', task_type=TaskType.MERLIN_STEP, group_id='chord_group')
        task2 = UniversalTaskDefinition(task_id='parallel2', task_type=TaskType.MERLIN_STEP, group_id='chord_group')
        
        parallel_signatures = [
            adapter.create_signature(task1),
            adapter.create_signature(task2)
        ]
        
        # Create callback signature
        callback_task = UniversalTaskDefinition(task_id='callback', task_type=TaskType.CHORD_FINISHER, group_id='chord_group')
        callback_signature = adapter.create_signature(callback_task)
        
        result_id = adapter.submit_chord(parallel_signatures, callback_signature)
        
        # Should submit all tasks (2 parallel + 1 callback)
        assert mock_producer.send.call_count == 3
        assert result_id == 'chord_group'


class TestSignatureAdapterInterface:
    """Test the abstract base class interface."""
    
    def test_abstract_methods(self):
        """Test that SignatureAdapter cannot be instantiated directly."""
        
        with pytest.raises(TypeError):
            SignatureAdapter()
    
    def test_required_methods(self):
        """Test that abstract methods are properly defined."""
        
        required_methods = [
            'create_signature',
            'submit_task', 
            'submit_group',
            'submit_chain',
            'submit_chord'
        ]
        
        for method_name in required_methods:
            assert hasattr(SignatureAdapter, method_name)
            assert getattr(SignatureAdapter, method_name).__isabstractmethod__