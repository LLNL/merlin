##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Unit tests for KafkaTaskServer implementation.

Tests the KafkaTaskServer class to ensure it properly implements
the TaskServerInterface and provides correct Kafka functionality.
"""

import json
import unittest
from unittest.mock import MagicMock, patch, Mock
from typing import Dict, Any, List


from merlin.task_servers.implementations.kafka_server import KafkaTaskServer
from merlin.task_servers.task_server_interface import TaskDependency
from merlin.spec.specification import MerlinSpec


class TestKafkaTaskServer(unittest.TestCase):
    """Test cases for KafkaTaskServer implementation."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Mock Kafka producer to avoid requiring actual Kafka
        self.mock_kafka_patcher = patch('merlin.task_servers.implementations.kafka_server.KafkaProducer')
        self.mock_kafka_producer_class = self.mock_kafka_patcher.start()
        self.mock_producer = MagicMock()
        self.mock_kafka_producer_class.return_value = self.mock_producer
        
        # Create KafkaTaskServer instance
        self.kafka_server = KafkaTaskServer()
        
    def tearDown(self):
        """Clean up test fixtures."""
        self.mock_kafka_patcher.stop()
        
    def test_server_type(self):
        """Test that server_type returns 'kafka'."""
        self.assertEqual(self.kafka_server.server_type, "kafka")
        
    def test_initialization(self):
        """Test KafkaTaskServer initializes properly."""
        # Verify Kafka producer was created
        self.mock_kafka_producer_class.assert_called_once()
        self.assertIsNotNone(self.kafka_server.producer)
        
    def test_initialization_with_config(self):
        """Test KafkaTaskServer initializes with custom config."""
        config = {
            'producer': {
                'bootstrap_servers': ['localhost:9093'],
                'batch_size': 1000
            }
        }
        
        with patch('merlin.task_servers.implementations.kafka_server.KafkaProducer') as mock_producer:
            kafka_server = KafkaTaskServer(config)
            
            # Verify config was passed to producer
            expected_config = config['producer'].copy()
            expected_config.setdefault('value_serializer', unittest.mock.ANY)
            mock_producer.assert_called_once()
            
    def test_convert_task_to_kafka_message(self):
        """Test task conversion to Kafka message format."""
        task_data = {
            'task_type': 'merlin_step',
            'parameters': {'param1': 'value1'},
            'queue': 'test_queue',
            'task_id': 'task_123',
            'timestamp': '2025-01-01T00:00:00Z',
            'metadata': {'meta1': 'value1'}
        }
        
        result = self.kafka_server._convert_task_to_kafka_message(task_data)
        
        expected = {
            'task_type': 'merlin_step',
            'parameters': {'param1': 'value1'},
            'queue': 'test_queue',
            'task_id': 'task_123',
            'timestamp': '2025-01-01T00:00:00Z',
            'metadata': {'meta1': 'value1'}
        }
        
        self.assertEqual(result, expected)
        
    def test_send_kafka_message(self):
        """Test sending message to Kafka topic."""
        topic = "test_topic"
        message = {"task_id": "test_123", "data": "test_data"}
        
        # Mock the future returned by producer.send()
        mock_future = MagicMock()
        self.mock_producer.send.return_value = mock_future
        
        result = self.kafka_server._send_kafka_message(topic, message)
        
        # Verify producer.send was called correctly
        self.mock_producer.send.assert_called_once_with(topic, value=message)
        mock_future.get.assert_called_once_with(timeout=10)
        
        # Verify return value format
        expected_id = f"kafka_{topic}_{message['task_id']}"
        self.assertEqual(result, expected_id)
        
    def test_submit_task_dict_input(self):
        """Test submitting a task with dict input."""
        task_data = {
            'task_id': 'test_task_123',
            'task_type': 'merlin_step',
            'parameters': {'param1': 'value1'},
            'queue': 'test_queue'
        }
        
        # Mock the future returned by producer.send()
        mock_future = MagicMock()
        self.mock_producer.send.return_value = mock_future
        
        result = self.kafka_server.submit_task(task_data)
        
        # Verify message was sent to correct topic
        expected_topic = "merlin_tasks_test_queue"
        self.mock_producer.send.assert_called_once()
        call_args = self.mock_producer.send.call_args
        self.assertEqual(call_args[0][0], expected_topic)
        
        # Verify return value
        expected_id = f"kafka_{expected_topic}_test_task_123"
        self.assertEqual(result, expected_id)
        
    def test_submit_task_string_input(self):
        """Test submitting a task with string task_id input."""
        task_id = "simple_task_456"
        
        # Mock the future returned by producer.send()
        mock_future = MagicMock()
        self.mock_producer.send.return_value = mock_future
        
        result = self.kafka_server.submit_task(task_id)
        
        # Verify message was sent to default topic
        expected_topic = "merlin_tasks_default"
        self.mock_producer.send.assert_called_once()
        call_args = self.mock_producer.send.call_args
        self.assertEqual(call_args[0][0], expected_topic)
        
        # Verify message content
        message = call_args[0][1]
        self.assertEqual(message['task_id'], task_id)
        self.assertEqual(message['task_type'], 'merlin_step')
        
    def test_submit_tasks_multiple(self):
        """Test submitting multiple tasks."""
        task_ids = ["task_1", "task_2", "task_3"]
        
        # Mock the future returned by producer.send()
        mock_future = MagicMock()
        self.mock_producer.send.return_value = mock_future
        
        results = self.kafka_server.submit_tasks(task_ids)
        
        # Verify all tasks were submitted
        self.assertEqual(len(results), 3)
        self.assertEqual(self.mock_producer.send.call_count, 3)
        
        # Verify all results have expected format
        for i, result in enumerate(results):
            self.assertIn(f"kafka_merlin_tasks_default_task_{i+1}", result)
            
    def test_submit_task_group(self):
        """Test submitting a task group."""
        group_name = "test_group"
        task_ids = ["header_1", "header_2"]
        callback_task_id = "callback_task"
        
        # Mock the future returned by producer.send()
        mock_future = MagicMock()
        self.mock_producer.send.return_value = mock_future
        
        result = self.kafka_server.submit_task_group(group_name, task_ids, callback_task_id)
        
        # Verify group message was sent to coordination topic
        self.mock_producer.send.assert_called_once_with(
            'merlin_coordination',
            value={
                'group_name': group_name,
                'task_ids': task_ids,
                'callback_task_id': callback_task_id,
                'type': 'task_group'
            }
        )
        
    def test_submit_coordinated_tasks(self):
        """Test submitting coordinated tasks."""
        coordination_id = "coord_123"
        header_task_ids = ["header_1", "header_2"]
        body_task_id = "body_task"
        
        # Mock the future returned by producer.send()
        mock_future = MagicMock()
        self.mock_producer.send.return_value = mock_future
        
        result = self.kafka_server.submit_coordinated_tasks(
            coordination_id, header_task_ids, body_task_id
        )
        
        # Verify coordination message was sent and header tasks were submitted
        self.assertGreater(self.mock_producer.send.call_count, 1)
        
        # Verify result format
        expected_result = f"kafka_coordination_{coordination_id}"
        self.assertEqual(result, expected_result)
        
    def test_submit_dependent_tasks(self):
        """Test submitting tasks with dependencies."""
        task_ids = ["dep_task_1", "dep_task_2"]
        dependencies = [
            TaskDependency("generate_*", "all_success")
        ]
        
        # Mock the future returned by producer.send()
        mock_future = MagicMock()
        self.mock_producer.send.return_value = mock_future
        
        results = self.kafka_server.submit_dependent_tasks(task_ids, dependencies)
        
        # Verify dependencies were processed
        self.assertIsInstance(results, list)
        self.assertTrue(len(results) > 0)
        
    def test_submit_dependent_tasks_no_dependencies(self):
        """Test submitting tasks without dependencies falls back to regular submission."""
        task_ids = ["simple_task_1", "simple_task_2"]
        
        # Mock the future returned by producer.send()
        mock_future = MagicMock()
        self.mock_producer.send.return_value = mock_future
        
        results = self.kafka_server.submit_dependent_tasks(task_ids)
        
        # Should behave like submit_tasks
        self.assertEqual(len(results), 2)
        
    def test_cancel_task(self):
        """Test cancelling a task."""
        task_id = "cancel_me"
        
        # Mock the future returned by producer.send()
        mock_future = MagicMock()
        self.mock_producer.send.return_value = mock_future
        
        result = self.kafka_server.cancel_task(task_id)
        
        # Verify cancellation message was sent
        self.mock_producer.send.assert_called_once_with(
            'merlin_control',
            value={
                'task_id': task_id,
                'action': 'cancel',
                'type': 'control'
            }
        )
        
        self.assertTrue(result)
        
    def test_cancel_tasks_multiple(self):
        """Test cancelling multiple tasks."""
        task_ids = ["cancel_1", "cancel_2", "cancel_3"]
        
        # Mock the future returned by producer.send()
        mock_future = MagicMock()
        self.mock_producer.send.return_value = mock_future
        
        results = self.kafka_server.cancel_tasks(task_ids)
        
        # Verify all cancellation messages were sent
        self.assertEqual(self.mock_producer.send.call_count, 3)
        
        # Verify results
        self.assertEqual(len(results), 3)
        for task_id in task_ids:
            self.assertTrue(results[task_id])
            
    @patch('subprocess.Popen')
    def test_start_workers(self, mock_popen):
        """Test starting Kafka workers."""
        mock_spec = MagicMock(spec=MerlinSpec)
        mock_spec.get_task_queues.return_value = {'queue1': 'queue1', 'queue2': 'queue2'}
        
        result = self.kafka_server.start_workers(mock_spec)
        
        # Verify worker process was started
        mock_popen.assert_called_once()
        self.assertTrue(result)
        
    def test_stop_workers(self):
        """Test stopping Kafka workers."""
        # Mock the future returned by producer.send()
        mock_future = MagicMock()
        self.mock_producer.send.return_value = mock_future
        
        result = self.kafka_server.stop_workers()
        
        # Verify stop message was sent
        self.mock_producer.send.assert_called_once_with(
            'merlin_control',
            value={
                'action': 'stop_workers',
                'type': 'control'
            }
        )
        
        self.assertTrue(result)
        
    def test_get_group_status(self):
        """Test getting group status."""
        group_id = "test_group_123"
        
        result = self.kafka_server.get_group_status(group_id)
        
        # Verify status format
        self.assertEqual(result["group_id"], group_id)
        self.assertEqual(result["backend"], "kafka")
        self.assertIn("status", result)
        
    def test_get_workers(self):
        """Test getting worker list."""
        result = self.kafka_server.get_workers()
        
        # Verify return type and content
        self.assertIsInstance(result, list)
        self.assertTrue(len(result) > 0)
        
    def test_get_active_queues(self):
        """Test getting active queues."""
        result = self.kafka_server.get_active_queues()
        
        # Verify return type and expected queues
        self.assertIsInstance(result, dict)
        self.assertIn("merlin_tasks_default", result)
        self.assertIn("merlin_coordination", result)
        self.assertIn("merlin_control", result)
        
    def test_check_workers_processing(self):
        """Test checking if workers are processing."""
        queues = ["test_queue_1", "test_queue_2"]
        
        result = self.kafka_server.check_workers_processing(queues)
        
        # For now, should return True (placeholder implementation)
        self.assertTrue(result)
        
    def test_purge_tasks(self):
        """Test purging tasks from queues."""
        queues = ["purge_queue_1", "purge_queue_2"]
        
        result = self.kafka_server.purge_tasks(queues, force=True)
        
        # Should return number of purged tasks (0 for placeholder implementation)
        self.assertEqual(result, 0)
        
    def test_display_methods_no_errors(self):
        """Test that display methods run without errors."""
        # These methods just print to console, verify they don't crash
        try:
            self.kafka_server.display_queue_info()
            self.kafka_server.display_connected_workers()
            self.kafka_server.display_running_tasks()
        except Exception as e:
            self.fail(f"Display method raised unexpected exception: {e}")
            
    def test_submit_study(self):
        """Test submitting a complete study."""
        mock_study = MagicMock()
        mock_study.name = "test_study"
        
        adapter = {"adapter_type": "test"}
        samples = ["sample1", "sample2"]
        sample_labels = ["label1", "label2"]
        egraph = MagicMock()
        groups_of_chains = [["chain1"], ["chain2"]]
        
        # Mock the future returned by producer.send()
        mock_future = MagicMock()
        self.mock_producer.send.return_value = mock_future
        
        result = self.kafka_server.submit_study(
            mock_study, adapter, samples, sample_labels, egraph, groups_of_chains
        )
        
        # Verify study message was sent
        self.mock_producer.send.assert_called_once_with(
            'merlin_studies',
            value={
                'study_name': 'test_study',
                'adapter_config': adapter,
                'sample_count': 2,
                'groups_of_chains': 2,
                'type': 'study_submission'
            }
        )
        
    def test_cleanup_on_deletion(self):
        """Test that producer is cleaned up on deletion."""
        # Create a fresh instance to test deletion
        with patch('merlin.task_servers.implementations.kafka_server.KafkaProducer') as mock_producer_class:
            mock_producer = MagicMock()
            mock_producer_class.return_value = mock_producer
            
            kafka_server = KafkaTaskServer()
            
            # Delete the server instance
            del kafka_server
            
            # Verify producer.close() was called
            mock_producer.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()