##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Unit tests for KafkaWorker implementation.

Tests the KafkaWorker class to ensure it properly consumes Kafka messages
and executes Merlin tasks using the task registry.
"""

import json
import unittest
from unittest.mock import MagicMock, patch, Mock
from typing import Dict, Any

from merlin.task_servers.implementations.kafka_task_consumer import KafkaTaskConsumer


class TestKafkaWorker(unittest.TestCase):
    """Test cases for KafkaWorker implementation."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock Kafka consumer to avoid requiring actual Kafka
        self.mock_kafka_patcher = patch('kafka.KafkaConsumer')
        self.mock_kafka_consumer_class = self.mock_kafka_patcher.start()
        self.mock_consumer = MagicMock()
        self.mock_kafka_consumer_class.return_value = self.mock_consumer
        
        # Test configuration
        self.config = {
            'kafka': {
                'bootstrap_servers': ['localhost:9092']
            },
            'queues': ['test_queue']
        }
        
        # Create KafkaTaskConsumer instance
        self.kafka_worker = KafkaTaskConsumer(self.config)
        
    def tearDown(self):
        """Clean up test fixtures."""
        self.mock_kafka_patcher.stop()
        
    def test_initialization(self):
        """Test KafkaWorker initializes properly."""
        self.assertEqual(self.kafka_worker.config, self.config)
        self.assertFalse(self.kafka_worker.running)
        self.assertIsNone(self.kafka_worker.consumer)
        
    def test_initialization_with_consumer(self):
        """Test consumer is created during initialization."""
        # Call initialize to create consumer
        self.kafka_worker._initialize_consumer()
        
        # Verify consumer was created
        self.mock_kafka_consumer_class.assert_called_once()
        self.assertEqual(self.kafka_worker.consumer, self.mock_consumer)
        
    def test_consumer_configuration(self):
        """Test consumer is configured with correct parameters."""
        self.kafka_worker._initialize_consumer()
        
        # Check consumer was called with expected config
        call_args = self.mock_kafka_consumer_class.call_args
        self.assertIn('bootstrap_servers', call_args[1])
        self.assertEqual(call_args[1]['bootstrap_servers'], ['localhost:9092'])
        
    def test_topic_subscription(self):
        """Test topics are subscribed correctly."""
        self.kafka_worker._initialize_consumer()
        
        # Verify consumer subscribed to correct topics
        expected_topics = ['merlin_tasks_test_queue']
        self.mock_consumer.subscribe.assert_called_once_with(expected_topics)
        
    @patch('time.sleep')
    def test_start_worker(self, mock_sleep):
        """Test starting the worker."""
        # Mock consumer messages
        mock_message = MagicMock()
        mock_message.value = json.dumps({
            'task_type': 'merlin_step',
            'parameters': {'param1': 'value1'},
            'task_id': 'test_task_123'
        }).encode()
        
        self.mock_consumer.__iter__.return_value = [mock_message]
        
        # Mock task registry
        with patch('merlin.execution.task_registry.task_registry') as mock_registry:
            mock_task_func = MagicMock(return_value='success')
            mock_registry.get.return_value = mock_task_func
            
            # Start worker (will run one iteration due to mocking)
            self.kafka_worker.running = True
            self.kafka_worker._initialize_consumer()
            
            # Process one message then stop
            self.kafka_worker._process_message(mock_message)
            
            # Verify task registry was called
            mock_registry.get.assert_called_once_with('merlin_step')
            mock_task_func.assert_called_once()
            
    def test_message_processing(self):
        """Test processing individual messages."""
        # Create test message
        message_data = {
            'task_type': 'merlin_step',
            'parameters': {'step_name': 'test_step'},
            'task_id': 'test_123'
        }
        
        mock_message = MagicMock()
        mock_message.value = json.dumps(message_data).encode()
        
        # Mock task registry and execution
        with patch('merlin.execution.task_registry.task_registry') as mock_registry:
            mock_task_func = MagicMock(return_value='OK')
            mock_registry.get.return_value = mock_task_func
            
            # Process the message
            self.kafka_worker._process_message(mock_message)
            
            # Verify task function was called with correct parameters
            mock_registry.get.assert_called_once_with('merlin_step')
            mock_task_func.assert_called_once_with(**message_data['parameters'])
            
    def test_message_processing_invalid_json(self):
        """Test processing message with invalid JSON."""
        mock_message = MagicMock()
        mock_message.value = b"invalid json"
        
        # Should handle gracefully without crashing
        try:
            self.kafka_worker._process_message(mock_message)
        except Exception as e:
            self.fail(f"Processing invalid JSON should not crash: {e}")
            
    def test_message_processing_missing_task_type(self):
        """Test processing message without task_type."""
        mock_message = MagicMock()
        mock_message.value = json.dumps({'parameters': {}}).encode()
        
        # Should handle gracefully
        try:
            self.kafka_worker._process_message(mock_message)
        except Exception as e:
            self.fail(f"Processing message without task_type should not crash: {e}")
            
    def test_message_processing_unknown_task(self):
        """Test processing message with unknown task type."""
        mock_message = MagicMock()
        mock_message.value = json.dumps({
            'task_type': 'unknown_task',
            'parameters': {}
        }).encode()
        
        with patch('merlin.execution.task_registry.task_registry') as mock_registry:
            mock_registry.get.return_value = None
            
            # Should handle unknown task gracefully
            try:
                self.kafka_worker._process_message(mock_message)
            except Exception as e:
                self.fail(f"Processing unknown task should not crash: {e}")
                
    def test_stop_worker(self):
        """Test stopping the worker."""
        self.kafka_worker.running = True
        self.kafka_worker.stop()
        
        self.assertFalse(self.kafka_worker.running)
        
    def test_signal_handler(self):
        """Test signal handler stops worker."""
        self.kafka_worker.running = True
        self.kafka_worker._signal_handler(15, None)  # SIGTERM
        
        self.assertFalse(self.kafka_worker.running)
        
    def test_consumer_cleanup(self):
        """Test consumer is cleaned up properly."""
        self.kafka_worker._initialize_consumer()
        self.kafka_worker.consumer = self.mock_consumer
        
        # Stop should close consumer
        self.kafka_worker.stop()
        self.mock_consumer.close.assert_called_once()
        
    def test_different_message_types(self):
        """Test processing different types of Kafka messages."""
        # Test condense status message
        condense_message = {
            'type': 'condense_status',
            'workspace': '/path/to/workspace',
            'condensed_workspace': '/path/condensed'
        }
        
        mock_message = MagicMock()
        mock_message.value = json.dumps(condense_message).encode()
        
        # Should handle different message types
        try:
            self.kafka_worker._process_message(mock_message)
        except Exception as e:
            self.fail(f"Processing condense message should not crash: {e}")
            
    def test_control_messages(self):
        """Test processing control messages."""
        # Test stop workers control message
        control_message = {
            'type': 'control',
            'action': 'stop_workers'
        }
        
        mock_message = MagicMock()
        mock_message.value = json.dumps(control_message).encode()
        
        self.kafka_worker.running = True
        self.kafka_worker._process_message(mock_message)
        
        # Should stop worker when receiving stop command
        self.assertFalse(self.kafka_worker.running)
        
    def test_configuration_validation(self):
        """Test configuration validation."""
        # Test with missing kafka config
        invalid_config = {'queues': ['test']}
        
        try:
            worker = KafkaTaskConsumer(invalid_config)
            worker._initialize_consumer()
        except Exception:
            pass  # Expected to fail gracefully
            
    def test_topic_generation(self):
        """Test correct topic names are generated."""
        config = {
            'kafka': {'bootstrap_servers': ['localhost:9092']},
            'queues': ['queue1', 'queue2']
        }
        
        worker = KafkaTaskConsumer(config)
        worker._initialize_consumer()
        
        expected_topics = ['merlin_tasks_queue1', 'merlin_tasks_queue2']
        self.mock_consumer.subscribe.assert_called_once_with(expected_topics)
        
    def test_error_handling_during_execution(self):
        """Test error handling when task execution fails."""
        message_data = {
            'task_type': 'merlin_step',
            'parameters': {'param': 'value'}
        }
        
        mock_message = MagicMock()
        mock_message.value = json.dumps(message_data).encode()
        
        with patch('merlin.execution.task_registry.task_registry') as mock_registry:
            # Mock task function that raises exception
            mock_task_func = MagicMock(side_effect=Exception("Task failed"))
            mock_registry.get.return_value = mock_task_func
            
            # Should handle task execution errors gracefully
            try:
                self.kafka_worker._process_message(mock_message)
            except Exception as e:
                self.fail(f"Task execution errors should be handled gracefully: {e}")
                
    def test_graceful_shutdown_sequence(self):
        """Test complete graceful shutdown sequence."""
        # Initialize everything
        self.kafka_worker._initialize_consumer()
        self.kafka_worker.consumer = self.mock_consumer
        self.kafka_worker.running = True
        
        # Perform graceful shutdown
        self.kafka_worker.stop()
        
        # Verify shutdown sequence
        self.assertFalse(self.kafka_worker.running)
        self.mock_consumer.close.assert_called_once()
        
    def test_multiple_queue_handling(self):
        """Test handling multiple queues correctly."""
        multi_queue_config = {
            'kafka': {'bootstrap_servers': ['localhost:9092']},
            'queues': ['high_priority', 'normal', 'low_priority']
        }
        
        worker = KafkaTaskConsumer(multi_queue_config)
        worker._initialize_consumer()
        
        expected_topics = [
            'merlin_tasks_high_priority',
            'merlin_tasks_normal', 
            'merlin_tasks_low_priority'
        ]
        self.mock_consumer.subscribe.assert_called_once_with(expected_topics)


if __name__ == '__main__':
    unittest.main()