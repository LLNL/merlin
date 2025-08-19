##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Unit tests for TaskRegistry.

Tests the backend-agnostic task registry system that provides task function
resolution for non-Celery backends like Kafka.
"""

import unittest
from unittest.mock import MagicMock, patch, Mock
from typing import Callable

from merlin.execution.task_registry import TaskRegistry, task_registry


class TestTaskRegistry(unittest.TestCase):
    """Test cases for TaskRegistry."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.registry = TaskRegistry()
        
    def test_initialization(self):
        """Test TaskRegistry initializes properly."""
        registry = TaskRegistry()
        self.assertEqual(registry._tasks, {})
        self.assertFalse(registry._registered)
        
    def test_register_task(self):
        """Test registering a task function."""
        def test_task():
            return "test_result"
            
        self.registry.register("test_task", test_task)
        
        self.assertIn("test_task", self.registry._tasks)
        self.assertEqual(self.registry._tasks["test_task"], test_task)
        
    def test_register_duplicate_task(self):
        """Test registering duplicate task name."""
        def task1():
            return "task1"
            
        def task2():
            return "task2"
            
        self.registry.register("duplicate", task1)
        
        with patch('merlin.execution.task_registry.LOG') as mock_log:
            self.registry.register("duplicate", task2)
            
            # Should warn about overwriting
            mock_log.warning.assert_called_once()
            
        # Should have new function
        self.assertEqual(self.registry._tasks["duplicate"], task2)
        
    def test_get_task_before_registration(self):
        """Test getting task triggers lazy registration."""
        with patch.object(self.registry, '_register_tasks_on_demand') as mock_register:
            self.registry.get("test_task")
            
            # Should trigger lazy registration
            mock_register.assert_called_once()
            
    def test_get_existing_task(self):
        """Test getting existing registered task."""
        def test_task():
            return "result"
            
        self.registry.register("existing_task", test_task)
        self.registry._registered = True  # Skip lazy registration
        
        result_func = self.registry.get("existing_task")
        
        self.assertEqual(result_func, test_task)
        self.assertEqual(result_func(), "result")
        
    def test_get_nonexistent_task(self):
        """Test getting nonexistent task returns None."""
        self.registry._registered = True  # Skip lazy registration
        
        result = self.registry.get("nonexistent")
        
        self.assertIsNone(result)
        
    def test_list_tasks(self):
        """Test listing registered tasks."""
        def task1():
            pass
        def task2():
            pass
            
        self.registry.register("task1", task1)
        self.registry.register("task2", task2)
        self.registry._registered = True  # Skip lazy registration
        
        task_list = self.registry.list_tasks()
        
        self.assertIn("task1", task_list)
        self.assertIn("task2", task_list)
        self.assertEqual(len(task_list), 2)
        
    def test_list_tasks_triggers_registration(self):
        """Test list_tasks triggers lazy registration."""
        with patch.object(self.registry, '_register_tasks_on_demand') as mock_register:
            self.registry.list_tasks()
            
            mock_register.assert_called_once()
            
    def test_task_decorator(self):
        """Test task decorator for registration."""
        @self.registry.task("decorated_task")
        def my_task():
            return "decorated_result"
            
        # Function should be registered
        self.assertIn("decorated_task", self.registry._tasks)
        self.assertEqual(self.registry._tasks["decorated_task"], my_task)
        
        # Original function should be returned
        self.assertEqual(my_task(), "decorated_result")
        
    def test_unregister_task(self):
        """Test unregistering a task."""
        def test_task():
            pass
            
        self.registry.register("removable", test_task)
        self.assertIn("removable", self.registry._tasks)
        
        with patch('merlin.execution.task_registry.LOG') as mock_log:
            self.registry.unregister("removable")
            
            mock_log.debug.assert_called_once()
            
        self.assertNotIn("removable", self.registry._tasks)
        
    def test_unregister_nonexistent_task(self):
        """Test unregistering nonexistent task."""
        # Should not raise exception
        self.registry.unregister("nonexistent")
        
    @patch('merlin.execution.task_registry.GenericStepExecutor')
    @patch('merlin.execution.task_registry.Step')
    @patch('merlin.execution.task_registry.SampleIndex')
    @patch('merlin.execution.task_registry.ReturnCode')
    def test_register_tasks_on_demand(self, mock_return_code, mock_sample_index, mock_step, mock_executor):
        """Test lazy registration of default tasks."""
        # Mock the executor and its methods
        mock_executor_instance = MagicMock()
        mock_executor.return_value = mock_executor_instance
        mock_executor_instance.execute_step.return_value.return_code = "OK"
        
        # Call the registration method
        self.registry._register_tasks_on_demand()
        
        # Should be marked as registered
        self.assertTrue(self.registry._registered)
        
        # Should have registered merlin_step task
        self.assertIn("merlin_step", self.registry._tasks)
        
        # Should have registered chordfinisher task
        self.assertIn("chordfinisher", self.registry._tasks)
        
        # Test the registered chordfinisher function
        chord_func = self.registry._tasks["chordfinisher"]
        result = chord_func()
        self.assertEqual(result, "SYNC")
        
    def test_register_tasks_on_demand_with_import_error(self):
        """Test lazy registration handles import errors gracefully."""
        with patch('merlin.execution.task_registry.LOG') as mock_log:
            # Patch imports to raise ImportError
            with patch('builtins.__import__', side_effect=ImportError("Module not found")):
                self.registry._register_tasks_on_demand()
                
            # Should log warning but continue
            mock_log.warning.assert_called()
            
        # Should still register simple tasks that don't require imports
        self.assertIn("chordfinisher", self.registry._tasks)
        self.assertTrue(self.registry._registered)
        
    def test_register_tasks_on_demand_called_once(self):
        """Test lazy registration is only called once."""
        with patch.object(self.registry, '_register_tasks_on_demand', wraps=self.registry._register_tasks_on_demand) as mock_register:
            # Call get multiple times
            self.registry.get("test1")
            self.registry.get("test2")
            self.registry.list_tasks()
            
            # Should only be called once
            mock_register.assert_called_once()
            
    def test_global_task_registry_instance(self):
        """Test global task_registry instance."""
        # Global instance should exist
        self.assertIsNotNone(task_registry)
        self.assertIsInstance(task_registry, TaskRegistry)
        
        # Should have lazy registration capability
        self.assertFalse(task_registry._registered)
        
    def test_registered_merlin_step_function(self):
        """Test that registered merlin_step function works."""
        # Trigger registration
        task_registry.get("merlin_step")
        
        # Should have merlin_step registered
        merlin_step_func = task_registry.get("merlin_step")
        self.assertIsNotNone(merlin_step_func)
        self.assertTrue(callable(merlin_step_func))
        
    def test_registered_chordfinisher_function(self):
        """Test that registered chordfinisher function works."""
        # Trigger registration
        task_registry.get("chordfinisher")
        
        # Test chordfinisher function
        chord_func = task_registry.get("chordfinisher")
        self.assertIsNotNone(chord_func)
        
        result = chord_func()
        self.assertEqual(result, "SYNC")
        
    def test_task_registry_thread_safety(self):
        """Test task registry handles concurrent access."""
        import threading
        results = []
        
        def register_task(name):
            def task_func():
                return f"result_{name}"
            self.registry.register(name, task_func)
            results.append(name)
            
        # Create multiple threads registering tasks
        threads = []
        for i in range(10):
            thread = threading.Thread(target=register_task, args=(f"task_{i}",))
            threads.append(thread)
            
        # Start all threads
        for thread in threads:
            thread.start()
            
        # Wait for completion
        for thread in threads:
            thread.join()
            
        # All tasks should be registered
        self.assertEqual(len(results), 10)
        for i in range(10):
            self.assertIn(f"task_{i}", self.registry._tasks)
            
    def test_task_execution_with_parameters(self):
        """Test registered tasks can be called with parameters."""
        def parameterized_task(param1, param2=None, **kwargs):
            return {"param1": param1, "param2": param2, "kwargs": kwargs}
            
        self.registry.register("param_task", parameterized_task)
        
        task_func = self.registry.get("param_task")
        result = task_func("value1", param2="value2", extra="extra_value")
        
        expected = {
            "param1": "value1",
            "param2": "value2", 
            "kwargs": {"extra": "extra_value"}
        }
        self.assertEqual(result, expected)
        
    def test_task_registry_clear_and_reregister(self):
        """Test clearing and re-registering tasks."""
        # Register initial tasks
        self.registry.register("task1", lambda: "result1")
        self.registry.register("task2", lambda: "result2")
        
        # Clear tasks
        self.registry._tasks.clear()
        self.registry._registered = False
        
        # Re-register
        self.registry.register("new_task", lambda: "new_result")
        
        # Only new task should exist
        task_list = self.registry.list_tasks()
        self.assertNotIn("task1", task_list)
        self.assertNotIn("task2", task_list)
        self.assertIn("new_task", task_list)


if __name__ == '__main__':
    unittest.main()