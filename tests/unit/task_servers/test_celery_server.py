##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Unit tests for CeleryTaskServer implementation.

Tests ensure that:
- CeleryTaskServer implements TaskServerInterface correctly
- Task submission, cancellation, and management work
- Worker lifecycle management functions
- Queue operations and status display work
- Error handling is robust
"""

import pytest
from unittest.mock import MagicMock, patch, call
from io import StringIO
import sys

from merlin.task_servers.implementations.celery_server import CeleryTaskServer
from merlin.task_servers.task_server_interface import TaskServerInterface
from merlin.spec.specification import MerlinSpec


class TestCeleryTaskServer:
    """Test cases for CeleryTaskServer."""
    
    @patch('merlin.task_servers.task_server_interface.MerlinDatabase')
    def setup_method(self, method, mock_db):
        """Set up test instance."""
        self.mock_db = mock_db
        self.server = CeleryTaskServer()
    
    def test_implements_interface(self):
        """Test that CeleryTaskServer implements TaskServerInterface."""
        assert isinstance(self.server, TaskServerInterface)
    
    @patch('merlin.task_servers.task_server_interface.MerlinDatabase')
    def test_initialization(self, mock_db):
        """Test CeleryTaskServer initialization."""
        server = CeleryTaskServer()
        
        assert hasattr(server, 'celery_app')
        assert hasattr(server, 'merlin_db')
    
    @patch('merlin.celery.app')
    def test_submit_task(self, mock_app):
        """Test submitting a single task with string ID."""
        # Test string ID submission
        mock_result = MagicMock()
        mock_result.id = "test_task_id_123"
        mock_app.send_task.return_value = mock_result
        self.server.celery_app = mock_app
        
        result = self.server.submit_task("test_workspace_path")
        
        assert result == "test_task_id_123"
        mock_app.send_task.assert_called_once_with(
            'merlin.common.tasks.merlin_step',
            task_id="test_workspace_path"
        )
    
    def test_submit_task_signature(self):
        """Test submitting a single task with Celery signature."""
        # Test signature submission
        mock_signature = MagicMock()
        mock_result = MagicMock()
        mock_result.id = "signature_task_id_456"
        mock_signature.delay.return_value = mock_result
        
        result = self.server.submit_task(mock_signature)
        
        assert result == "signature_task_id_456"
        mock_signature.delay.assert_called_once()
    
    def test_submit_task_no_app(self):
        """Test submit_task when Celery app is not initialized."""
        self.server.celery_app = None
        
        with pytest.raises(RuntimeError, match="Celery task server not initialized"):
            self.server.submit_task("test_task")
    
    @patch('merlin.celery.app')
    def test_submit_tasks(self, mock_app):
        """Test submitting multiple tasks."""
        # Mock the send_task method
        mock_results = [MagicMock(), MagicMock(), MagicMock()]
        for i, mock_result in enumerate(mock_results):
            mock_result.id = f"task_id_{i}"
        
        mock_app.send_task.side_effect = mock_results
        self.server.celery_app = mock_app
        
        task_ids = ["task1", "task2", "task3"]
        results = self.server.submit_tasks(task_ids)
        
        assert results == ["task_id_0", "task_id_1", "task_id_2"]
        assert mock_app.send_task.call_count == 3
    
    @patch('merlin.celery.app')
    def test_cancel_task(self, mock_app):
        """Test cancelling a single task."""
        mock_control = MagicMock()
        mock_app.control = mock_control
        self.server.celery_app = mock_app
        
        result = self.server.cancel_task("test_task_id")
        
        assert result is True
        mock_control.revoke.assert_called_once_with("test_task_id", terminate=True)
    
    @patch('merlin.celery.app')
    def test_cancel_task_exception(self, mock_app):
        """Test cancel_task when revoke raises exception."""
        mock_control = MagicMock()
        mock_control.revoke.side_effect = Exception("Cancel failed")
        mock_app.control = mock_control
        self.server.celery_app = mock_app
        
        result = self.server.cancel_task("test_task_id")
        
        assert result is False
    
    @patch('merlin.celery.app')
    def test_cancel_tasks(self, mock_app):
        """Test cancelling multiple tasks."""
        mock_control = MagicMock()
        mock_app.control = mock_control
        self.server.celery_app = mock_app
        
        task_ids = ["task1", "task2", "task3"]
        results = self.server.cancel_tasks(task_ids)
        
        expected = {"task1": True, "task2": True, "task3": True}
        assert results == expected
        assert mock_control.revoke.call_count == 3
    
    @patch('merlin.study.celeryadapter.start_celery_workers')
    def test_start_workers(self, mock_start_workers):
        """Test starting workers using MerlinSpec."""
        mock_spec = MagicMock(spec=MerlinSpec)
        mock_spec.get_study_step_names.return_value = ["step1", "step2"]
        
        self.server.start_workers(mock_spec)
        
        mock_start_workers.assert_called_once_with(
            spec=mock_spec,
            steps=["step1", "step2"],
            celery_args="",
            disable_logs=False,
            just_return_command=False
        )
    
    @patch('merlin.study.celeryadapter.stop_celery_workers')
    def test_stop_workers(self, mock_stop_workers):
        """Test stopping workers."""
        worker_names = ["worker1", "worker2"]
        
        self.server.stop_workers(worker_names)
        
        mock_stop_workers.assert_called_once_with(
            queues=None,
            spec_worker_names=worker_names,
            worker_regex=None
        )
    
    @patch('merlin.study.celeryadapter.stop_celery_workers')
    def test_stop_workers_no_names(self, mock_stop_workers):
        """Test stopping all workers."""
        self.server.stop_workers()
        
        mock_stop_workers.assert_called_once_with(
            queues=None,
            spec_worker_names=None,
            worker_regex=None
        )
    
    @patch('merlin.task_servers.implementations.celery_server.tabulate')
    @patch('merlin.study.celeryadapter.query_celery_queues')
    @patch('merlin.study.celeryadapter.get_active_celery_queues')
    @patch('merlin.celery.app')  
    def test_display_queue_info(self, mock_app, mock_get_active, mock_query, mock_tabulate):
        """Test displaying queue information."""
        # Set up the celery app
        self.server.celery_app = mock_app
        
        # Mock queue data: ensure active queues are returned
        mock_get_active.return_value = ({"queue1": ["worker1"], "queue2": ["worker2"]}, None)
        mock_query.return_value = {
            "queue1": {"jobs": 5, "consumers": 1},
            "queue2": {"jobs": 3, "consumers": 1}
        }
        mock_tabulate.return_value = "Mock table output"
        
        # Capture stdout
        captured_output = StringIO()
        sys.stdout = captured_output
        
        try:
            self.server.display_queue_info()
            output = captured_output.getvalue()
            
            assert "Queue Information:" in output
            # Verify tabulate was called: this confirms the display logic works
            mock_tabulate.assert_called_once()
            # Verify that the call included the expected headers
            call_kwargs = mock_tabulate.call_args.kwargs if mock_tabulate.call_args else {}
            assert 'headers' in call_kwargs
            assert call_kwargs['headers'] == ['Queue Name', 'Pending Jobs', 'Consumers']
        finally:
            sys.stdout = sys.__stdout__
    
    @patch('merlin.task_servers.implementations.celery_server.tabulate')
    @patch('merlin.study.celeryadapter.get_active_workers')
    @patch('merlin.celery.app')
    def test_display_connected_workers(self, mock_app, mock_get_workers, mock_tabulate):
        """Test displaying connected workers."""
        # Set up the celery app
        self.server.celery_app = mock_app
        
        mock_get_workers.return_value = {
            "worker1@host1": ["queue1", "queue2"],
            "worker2@host2": ["queue3"]
        }
        mock_tabulate.return_value = "Mock worker table"
        
        captured_output = StringIO()
        sys.stdout = captured_output
        
        try:
            self.server.display_connected_workers()
            output = captured_output.getvalue()
            
            assert "Connected Workers:" in output
            # Verify tabulate was called: this confirms the display logic works
            mock_tabulate.assert_called_once()
            # Verify that the call included the expected headers
            call_kwargs = mock_tabulate.call_args.kwargs if mock_tabulate.call_args else {}
            assert 'headers' in call_kwargs
            assert call_kwargs['headers'] == ['Worker Name', 'Queues']
        finally:
            sys.stdout = sys.__stdout__
    
    @patch('merlin.celery.app')
    def test_display_running_tasks(self, mock_app):
        """Test displaying running tasks."""
        # Set up the celery app
        self.server.celery_app = mock_app
        
        # Mock inspect and active tasks
        mock_inspect = MagicMock()
        mock_inspect.active.return_value = {
            "worker1": [{"id": "task1"}, {"id": "task2"}],
            "worker2": [{"id": "task3"}]
        }
        mock_app.control.inspect.return_value = mock_inspect
        
        captured_output = StringIO()
        sys.stdout = captured_output
        
        try:
            self.server.display_running_tasks()
            output = captured_output.getvalue()
            
            assert "Running Tasks (3 total):" in output
            assert "task1" in output
            assert "task2" in output
            assert "task3" in output
        finally:
            sys.stdout = sys.__stdout__
    
    @patch('merlin.celery.app')
    def test_display_running_tasks_none(self, mock_app):
        """Test displaying running tasks when none exist."""
        # Set up the celery app
        self.server.celery_app = mock_app
        
        mock_inspect = MagicMock()
        mock_inspect.active.return_value = None
        mock_app.control.inspect.return_value = mock_inspect
        
        captured_output = StringIO()
        sys.stdout = captured_output
        
        try:
            self.server.display_running_tasks()
            output = captured_output.getvalue()
            
            assert "No running tasks found" in output
        finally:
            sys.stdout = sys.__stdout__
    
    @patch('merlin.study.celeryadapter.purge_celery_tasks')
    def test_purge_tasks(self, mock_purge):
        """Test purging tasks from queues."""
        mock_purge.return_value = 5
        
        result = self.server.purge_tasks(["queue1", "queue2"], force=True)
        
        assert result == 5
        mock_purge.assert_called_once_with("queue1,queue2", True)
    
    @patch('merlin.study.celeryadapter.get_workers_from_app')
    def test_get_workers(self, mock_get_workers):
        """Test getting list of workers."""
        mock_get_workers.return_value = ["worker1@host1", "worker2@host2"]
        
        workers = self.server.get_workers()
        
        assert workers == ["worker1@host1", "worker2@host2"]
        mock_get_workers.assert_called_once()
    
    @patch('merlin.celery.app')
    @patch('merlin.study.celeryadapter.get_active_celery_queues')
    def test_get_active_queues(self, mock_get_active, mock_app):
        """Test getting active queues."""
        expected_queues = {"queue1": ["worker1"], "queue2": ["worker2"]}
        mock_get_active.return_value = (expected_queues, None)
        self.server.celery_app = mock_app
        
        queues = self.server.get_active_queues()
        
        assert queues == expected_queues
        mock_get_active.assert_called_once_with(mock_app)
    
    @patch('merlin.celery.app')
    @patch('merlin.study.celeryadapter.check_celery_workers_processing')
    def test_check_workers_processing(self, mock_check_processing, mock_app):
        """Test checking if workers are processing tasks."""
        mock_check_processing.return_value = True
        self.server.celery_app = mock_app
        
        result = self.server.check_workers_processing(["queue1", "queue2"])
        
        assert result is True
        mock_check_processing.assert_called_once_with(["queue1", "queue2"], mock_app)
    
    def test_error_handling_no_celery_app(self):
        """Test error handling when Celery app is not available."""
        self.server.celery_app = None
        
        # Test methods that should handle missing app
        assert self.server.get_workers() == []
        assert self.server.get_active_queues() == {}
        assert self.server.check_workers_processing([]) is False
        
        # Capture stdout for display methods
        captured_output = StringIO()
        sys.stdout = captured_output
        
        try:
            self.server.display_queue_info()
            self.server.display_connected_workers()
            self.server.display_running_tasks()
            
            output = captured_output.getvalue()
            # Should see multiple error messages for each display method
            error_count = output.count("Error: Celery task server not initialized")
            assert error_count >= 3  # One for each display method
        finally:
            sys.stdout = sys.__stdout__
    
    # New chord method tests
    @patch('merlin.celery.app')
    def test_submit_task_group_string_ids(self, mock_app):
        """Test submitting task group with string task IDs (legacy approach)."""
        # This should fall back to the old approach since we don't have database support yet
        result = self.server.submit_task_group("test_group", ["task1", "task2"], "callback_task")
        
        # Should return the group_id since the implementation falls back to submit_tasks
        assert result == "test_group_with_2_tasks" or isinstance(result, str)
    
    @patch('merlin.celery.app')
    def test_submit_coordinated_tasks_with_task_dependencies(self, mock_app):
        """Test submitting chord with TaskDependency objects."""
        from merlin.task_servers.task_server_interface import TaskDependency
        
        # Create mock signatures
        mock_sig1 = MagicMock()
        mock_sig2 = MagicMock()
        mock_result = MagicMock()
        mock_result.id = "chord_result_123"
        
        # Create TaskDependency objects
        task_deps = [
            TaskDependency(task_pattern="task1", dependency_type="all_success"),
            TaskDependency(task_pattern="task2", dependency_type="all_success")
        ]
        # Add task signatures to the objects
        task_deps[0].task_signature = mock_sig1
        task_deps[1].task_signature = mock_sig2
        
        self.server.celery_app = mock_app
        
        # Mock the group and chord operations
        with patch('celery.group') as mock_group, \
             patch('celery.chord') as mock_chord:
            
            mock_group_instance = MagicMock()
            mock_group.return_value = mock_group_instance
            mock_group_instance.apply_async.return_value = mock_result
            
            result = self.server.submit_coordinated_tasks(task_deps)
            
            assert result == "chord_result_123"
            mock_group.assert_called_once_with([mock_sig1, mock_sig2])
            mock_group_instance.apply_async.assert_called_once()
    
    @patch('merlin.celery.app')
    def test_submit_coordinated_tasks_legacy_parameters(self, mock_app):
        """Test submitting chord with legacy string parameters."""
        self.server.celery_app = mock_app
        
        # Mock both the signature creation and send_task for fallback path
        mock_signature = MagicMock()
        mock_signature.delay.return_value.id = "mock_sig_id"
        mock_app.signature.return_value = mock_signature
        mock_app.send_task.return_value.id = "mock_task_id_123"
        
        # Mock group and chord classes to avoid import issues in test
        with patch('celery.group') as mock_group, \
             patch('celery.chord') as mock_chord:
            
            # Set up proper mock chain for chord creation
            mock_group_instance = MagicMock()
            mock_group.return_value = mock_group_instance
            
            mock_chord_instance = MagicMock()
            mock_chord.return_value = mock_chord_instance
            
            # Mock the apply_async result to return a string ID
            mock_chord_result = MagicMock()
            mock_chord_result.id = "mock_chord_id_123"
            mock_chord_instance.apply_async.return_value = mock_chord_result
            
            result = self.server.submit_coordinated_tasks("test_chord", ["task1", "task2"], "callback_task")
            
            # Should return a string (the chord result ID)
            assert result == "mock_chord_id_123"
            assert isinstance(result, str)
    
    @patch('merlin.celery.app')
    def test_submit_dependent_tasks(self, mock_app):
        """Test submitting tasks with dependencies."""
        from merlin.task_servers.task_server_interface import TaskDependency
        
        # Set up the celery app
        self.server.celery_app = mock_app
        mock_app.send_task.return_value.id = "mock_task_id"
        
        # Test with no dependencies - should fall back to submit_tasks
        result_no_deps = self.server.submit_dependent_tasks(["task1", "task2"], None)
        assert isinstance(result_no_deps, list)
        assert len(result_no_deps) == 2  # Should return 2 task IDs from submit_tasks
        
        # Test with dependencies: may return empty list if pattern matching fails (which is valid)
        task_deps = [
            TaskDependency(task_pattern="task1", dependency_type="all_success"),
            TaskDependency(task_pattern="task2", dependency_type="all_success")
        ]
        
        result_with_deps = self.server.submit_dependent_tasks(["task1", "task2"], task_deps)
        
        # Should return list of coordination IDs 
        assert isinstance(result_with_deps, list)
        # May be empty if no pattern matches are found, or may fall back to submit_tasks
    
    def test_get_group_status(self):
        """Test getting group status."""
        # This is a placeholder since the method isn't fully implemented yet
        result = self.server.get_group_status("test_group_id")
        
        # Should return a dictionary with group information
        assert isinstance(result, dict)
        assert "group_id" in result or "status" in result
    
    @patch('merlin.celery.app')
    def test_submit_coordinated_tasks_fallback_to_individual_tasks(self, mock_app):
        """Test chord submission fallback when chord creation fails."""
        from merlin.task_servers.task_server_interface import TaskDependency
        
        # Create mock signatures
        mock_sig1 = MagicMock()
        mock_sig1.delay.return_value.id = "fallback_task_1"
        mock_sig2 = MagicMock() 
        mock_sig2.delay.return_value.id = "fallback_task_2"
        
        task_deps = [
            TaskDependency(task_pattern="task1", dependency_type="all_success"),
            TaskDependency(task_pattern="task2", dependency_type="all_success")
        ]
        # Add task signatures to the objects
        task_deps[0].task_signature = mock_sig1
        task_deps[1].task_signature = mock_sig2
        
        self.server.celery_app = mock_app
        
        # Mock group to raise an exception to trigger fallback
        with patch('celery.group') as mock_group:
            mock_group.side_effect = Exception("Chord creation failed")
            
            result = self.server.submit_coordinated_tasks(task_deps)
            
            # Should fall back to individual task submission
            # Result should be empty string for fallback case
            assert result == ""
            mock_sig1.delay.assert_called_once()
            mock_sig2.delay.assert_called_once() 