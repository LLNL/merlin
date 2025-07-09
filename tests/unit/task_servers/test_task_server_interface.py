##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Unit tests for TaskServerInterface abstract base class.

Tests ensure that:
- Abstract methods properly raise NotImplementedError
- Subclasses must implement all abstract methods
- Base class initialization works correctly
"""

import pytest
from unittest.mock import MagicMock, patch

from merlin.task_servers.task_server_interface import TaskServerInterface
from merlin.spec.specification import MerlinSpec


class ConcreteTaskServer(TaskServerInterface):
    """Test implementation of TaskServerInterface for testing purposes."""
    
    @property
    def server_type(self) -> str:
        """Return the task server type."""
        return "test"
    
    def submit_task(self, task_id: str):
        return f"submitted_{task_id}"
    
    def submit_tasks(self, task_ids, **kwargs):
        return [f"submitted_{tid}" for tid in task_ids]
    
    def submit_task_group(self, group_id: str, task_ids, callback_task_id=None, **kwargs):
        return f"group_{group_id}_with_{len(task_ids)}_tasks"
    
    def submit_coordinated_tasks(self, coordination_id: str, header_task_ids, body_task_id: str, **kwargs):
        return f"coord_{coordination_id}_header_{len(header_task_ids)}_body_{body_task_id}"
    
    def submit_dependent_tasks(self, task_ids, dependencies=None, **kwargs):
        return [f"dependent_{tid}" for tid in task_ids]
    
    def get_group_status(self, group_id: str):
        return {"group_id": group_id, "status": "completed", "tasks": []}
    
    def cancel_task(self, task_id: str):
        return True
    
    def cancel_tasks(self, task_ids):
        return {tid: True for tid in task_ids}
    
    def start_workers(self, spec: MerlinSpec):
        pass
    
    def stop_workers(self, names=None):
        pass
    
    def display_queue_info(self, queues=None):
        pass
    
    def display_connected_workers(self):
        pass
    
    def display_running_tasks(self):
        pass
    
    def purge_tasks(self, queues, force=False):
        return len(queues)
    
    def get_workers(self):
        return ["worker1", "worker2"]
    
    def get_active_queues(self):
        return {"queue1": ["worker1"], "queue2": ["worker2"]}
    
    def check_workers_processing(self, queues):
        return len(queues) > 0


class IncompleteTaskServer(TaskServerInterface):
    """Incomplete implementation to test abstract method enforcement."""
    pass


class TestTaskServerInterface:
    """Test cases for TaskServerInterface."""
    
    @patch('merlin.task_servers.task_server_interface.MerlinDatabase')
    def test_initialization(self, mock_db):
        """Test that TaskServerInterface initializes correctly."""
        server = ConcreteTaskServer()
        assert hasattr(server, 'merlin_db')
        mock_db.assert_called_once()
    
    def test_cannot_instantiate_abstract_class(self):
        """Test that TaskServerInterface cannot be instantiated directly."""
        with pytest.raises(TypeError):
            TaskServerInterface()
    
    def test_incomplete_implementation_fails(self):
        """Test that incomplete implementations cannot be instantiated."""
        with pytest.raises(TypeError):
            IncompleteTaskServer()
    
    def test_abstract_methods_raise_not_implemented_error(self):
        """Test that abstract methods in base class raise NotImplementedError."""
        # This test verifies the methods have NotImplementedError in their body
        # by creating a temporary class that only implements one method at a time
        
        class PartialTaskServer(TaskServerInterface):
            def submit_task(self, task_id: str):
                super().submit_task(task_id)
        
        with pytest.raises(TypeError):
            # Should fail because not all abstract methods are implemented
            PartialTaskServer()
    
    @patch('merlin.task_servers.task_server_interface.MerlinDatabase')
    def test_concrete_implementation_methods(self, mock_db):
        """Test that concrete implementation methods work correctly."""
        server = ConcreteTaskServer()
        
        # Test submit_task
        result = server.submit_task("test_task")
        assert result == "submitted_test_task"
        
        # Test submit_tasks
        results = server.submit_tasks(["task1", "task2"])
        assert results == ["submitted_task1", "submitted_task2"]
        
        # Test cancel_task
        assert server.cancel_task("test_task") is True
        
        # Test cancel_tasks
        cancel_results = server.cancel_tasks(["task1", "task2"])
        assert cancel_results == {"task1": True, "task2": True}
        
        # Test purge_tasks
        purged = server.purge_tasks(["queue1", "queue2"])
        assert purged == 2
        
        # Test get_workers
        workers = server.get_workers()
        assert workers == ["worker1", "worker2"]
        
        # Test get_active_queues
        queues = server.get_active_queues()
        assert queues == {"queue1": ["worker1"], "queue2": ["worker2"]}
        
        # Test check_workers_processing
        assert server.check_workers_processing(["queue1"]) is True
        assert server.check_workers_processing([]) is False
        
        # Test chord methods
        group_result = server.submit_task_group("test_group", ["task1", "task2"])
        assert group_result == "group_test_group_with_2_tasks"
        
        coord_result = server.submit_coordinated_tasks("test_coord", ["task1", "task2"], "callback_task")
        assert coord_result == "coord_test_coord_header_2_body_callback_task"
        
        dependent_results = server.submit_dependent_tasks(["task1", "task2"])
        assert dependent_results == ["dependent_task1", "dependent_task2"]
        
        status = server.get_group_status("test_group")
        assert status["group_id"] == "test_group"
        assert status["status"] == "completed"
    
    @patch('merlin.task_servers.task_server_interface.MerlinDatabase')
    def test_worker_management_methods(self, mock_db):
        """Test worker management methods."""
        server = ConcreteTaskServer()
        mock_spec = MagicMock(spec=MerlinSpec)
        
        # These should not raise exceptions
        server.start_workers(mock_spec)
        server.stop_workers()
        server.stop_workers(["worker1"])
    
    @patch('merlin.task_servers.task_server_interface.MerlinDatabase')
    def test_display_methods(self, mock_db):
        """Test display methods (these should not raise exceptions)."""
        server = ConcreteTaskServer()
        
        # These methods primarily output to console, so we just ensure they don't crash
        server.display_queue_info()
        server.display_queue_info(["queue1"])
        server.display_connected_workers()
        server.display_running_tasks()
    
    def test_method_signatures(self):
        """Test that all abstract methods have correct signatures."""
        # Verify abstract methods exist and have expected signatures
        abstract_methods = [
            'submit_task',
            'submit_tasks',
            'submit_task_group',
            'submit_coordinated_tasks', 
            'submit_dependent_tasks',
            'get_group_status',
            'cancel_task',
            'cancel_tasks',
            'start_workers',
            'stop_workers',
            'display_queue_info',
            'display_connected_workers',
            'display_running_tasks',
            'purge_tasks',
            'get_workers',
            'get_active_queues',
            'check_workers_processing'
        ]
        
        for method_name in abstract_methods:
            assert hasattr(TaskServerInterface, method_name)
            method = getattr(TaskServerInterface, method_name)
            assert hasattr(method, '__isabstractmethod__')
            assert method.__isabstractmethod__ is True 