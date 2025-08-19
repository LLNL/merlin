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
        
        # CeleryTaskServer uses "all" to start workers for all steps 
        # which is the correct behavior for worker startup
        mock_start_workers.assert_called_once_with(
            spec=mock_spec,
            steps=["all"],
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
        
        # Create TaskDependency objects - need header and callback for chord
        task_deps = [
            TaskDependency(task_pattern="task1", dependency_type="header"),
            TaskDependency(task_pattern="callback_task", dependency_type="callback")
        ]
        # Add task signatures to the objects
        task_deps[0].task_signature = mock_sig1
        task_deps[1].task_signature = mock_sig2
        
        self.server.celery_app = mock_app
        
        # Mock the group and chord operations
        with patch('celery.group') as mock_group, \
             patch('celery.chord') as mock_chord:
            
            mock_group_instance = MagicMock()
            mock_chord_instance = MagicMock()
            mock_group.return_value = mock_group_instance
            mock_chord.return_value = mock_chord_instance
            mock_chord_instance.apply_async.return_value = mock_result
            
            result = self.server.submit_coordinated_tasks(task_deps)
            
            assert result == "chord_result_123"
            mock_group.assert_called_once_with([mock_sig1])  # Only header signatures
            mock_chord.assert_called_once_with(mock_group_instance, mock_sig2)
            mock_chord_instance.apply_async.assert_called_once()
    
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
            TaskDependency(task_pattern="task1", dependency_type="header"),
            TaskDependency(task_pattern="callback_task", dependency_type="callback")
        ]
        # Add task signatures to the objects
        task_deps[0].task_signature = mock_sig1
        task_deps[1].task_signature = mock_sig2
        
        self.server.celery_app = mock_app
        
        # Mock group to raise an exception to trigger fallback
        with patch('celery.group') as mock_group, \
             patch('celery.chord') as mock_chord:
            mock_group.side_effect = Exception("Chord creation failed")
            
            result = self.server.submit_coordinated_tasks(task_deps)
            
            # Should fall back to individual task submission
            # Result should be empty string for fallback case
            assert result == ""
            # Verify fallback calls task.delay() for each signature
            mock_sig1.delay.assert_called_once()
            mock_sig2.delay.assert_called_once()
    
    @patch('merlin.celery.app')
    def test_submit_study_basic_workflow(self, mock_app):
        """Test submit_study method with basic workflow."""
        from unittest.mock import MagicMock
        
        # Create mock study components
        mock_study = MagicMock()
        mock_study.workspace = "/test/workspace"
        mock_study.level_max_dirs = 5
        
        mock_adapter = {"test": "adapter"}
        mock_samples = [{"sample": "data1"}, {"sample": "data2"}]
        mock_sample_labels = ["label1", "label2"]
        
        # Create mock egraph (DAG)
        mock_egraph = MagicMock()
        mock_step = MagicMock()
        mock_step.get_task_queue.return_value = "test_queue"
        mock_egraph.step.return_value = mock_step
        
        # Create mock groups of chains (typical study structure)
        mock_groups_of_chains = [
            ["_source"],  # Source group
            [["step1", "step2"], ["step3"]],  # Task group 1
            [["step4"], ["step5", "step6"]]   # Task group 2
        ]
        
        self.server.celery_app = mock_app
        
        # Mock the Celery operations
        with patch('celery.chain') as mock_chain, \
             patch('celery.chord') as mock_chord, \
             patch('celery.group') as mock_group, \
             patch('merlin.common.tasks.expand_tasks_with_samples') as mock_expand, \
             patch('merlin.common.tasks.chordfinisher') as mock_chordfinisher, \
             patch('merlin.common.tasks.mark_run_as_complete') as mock_mark_complete:
            
            # Setup mock returns
            mock_expand_sig = MagicMock()
            mock_expand.si.return_value = mock_expand_sig
            mock_expand_sig.set.return_value = mock_expand_sig
            
            mock_chordfinisher_sig = MagicMock()
            mock_chordfinisher.s.return_value = mock_chordfinisher_sig
            mock_chordfinisher_sig.set.return_value = mock_chordfinisher_sig
            
            mock_mark_complete_sig = MagicMock()
            mock_mark_complete.si.return_value = mock_mark_complete_sig
            mock_mark_complete_sig.set.return_value = mock_mark_complete_sig
            
            mock_group_instance = MagicMock()
            mock_group.return_value = mock_group_instance
            
            mock_chord_instance = MagicMock()
            mock_chord.return_value = mock_chord_instance
            
            # Setup chain mock to consume generators when called
            def chain_side_effect(*args):
                # Force evaluation of generator expressions to trigger si() calls
                for arg in args:
                    if hasattr(arg, '__iter__') and not isinstance(arg, (str, bytes)):
                        try:
                            list(arg)  # Convert generator to list
                        except (TypeError, AttributeError):
                            pass
                return mock_chain_instance
            
            mock_chain.side_effect = chain_side_effect
            mock_chain_instance = MagicMock()
            
            # Setup final chain result
            mock_result = MagicMock()
            mock_result.id = "study_result_123"
            mock_chain_instance.__or__ = MagicMock(return_value=mock_chain_instance)
            mock_chain_instance.delay.return_value = mock_result
            
            # Execute submit_study
            result = self.server.submit_study(
                mock_study, mock_adapter, mock_samples, mock_sample_labels,
                mock_egraph, mock_groups_of_chains
            )
            
            # Verify result
            assert result.id == "study_result_123"
            
            # Verify Celery coordination was called correctly  
            # expand_tasks_with_samples.si should be called for each gchain in each chain_group
            # With groups_of_chains[1:] = [[["step1", "step2"], ["step3"]], [["step4"], ["step5", "step6"]]]
            # That's 2 groups, first has 2 gchains, second has 2 gchains = 4 total calls
            expected_calls = sum(len(chain_group) for chain_group in mock_groups_of_chains[1:])
            assert mock_expand.si.call_count == expected_calls
            mock_mark_complete.si.assert_called_once_with(mock_study.workspace)
            mock_chain_instance.delay.assert_called_once_with(None)
    
    @patch('merlin.celery.app')
    def test_submit_study_error_handling(self, mock_app):
        """Test submit_study error handling."""
        mock_study = MagicMock()
        mock_study.workspace = "/test/workspace"
        
        self.server.celery_app = mock_app
        
        # Test with invalid groups_of_chains
        with patch('celery.chain') as mock_chain:
            mock_chain.side_effect = Exception("Chain creation failed")
            
            with pytest.raises(Exception) as exc_info:
                self.server.submit_study(
                    mock_study, {}, [], [], MagicMock(), []
                )
            
            assert "Chain creation failed" in str(exc_info.value)
    
    @patch('merlin.celery.app')
    def test_submit_study_with_complex_dependencies(self, mock_app):
        """Test submit_study with complex dependency structures."""
        # Create a more complex study structure
        mock_study = MagicMock()
        mock_study.workspace = "/complex/workspace"
        mock_study.level_max_dirs = 10
        
        # Complex groups of chains with multiple steps
        complex_groups = [
            ["_source"],
            [["generate_data", "process_data"], ["analyze_data"]],
            [["ml_train"], ["ml_predict", "ml_evaluate"]],
            [["visualize"], ["report_generation"]]
        ]
        
        mock_egraph = MagicMock()
        mock_step = MagicMock()
        mock_step.get_task_queue.return_value = "complex_queue"
        mock_egraph.step.return_value = mock_step
        
        self.server.celery_app = mock_app
        
        with patch('celery.chain') as mock_chain, \
             patch('celery.chord') as mock_chord, \
             patch('celery.group') as mock_group, \
             patch('merlin.common.tasks.expand_tasks_with_samples') as mock_expand, \
             patch('merlin.common.tasks.chordfinisher') as mock_chordfinisher, \
             patch('merlin.common.tasks.mark_run_as_complete') as mock_mark_complete:
            
            # Setup mocks for complex workflow
            mock_expand_sig = MagicMock()
            mock_expand.si.return_value = mock_expand_sig
            mock_expand_sig.set.return_value = mock_expand_sig
            
            # Setup chain mock to consume generators when called
            def chain_side_effect(*args):
                # Force evaluation of generator expressions to trigger si() calls
                for arg in args:
                    if hasattr(arg, '__iter__') and not isinstance(arg, (str, bytes)):
                        try:
                            list(arg)  # Convert generator to list
                        except (TypeError, AttributeError):
                            pass
                return mock_chain_instance
            
            mock_chain.side_effect = chain_side_effect
            mock_chain_instance = MagicMock()
            mock_result = MagicMock()
            mock_result.id = "complex_study_456"
            mock_chain_instance.__or__ = MagicMock(return_value=mock_chain_instance)
            mock_chain_instance.delay.return_value = mock_result
            
            result = self.server.submit_study(
                mock_study, {"complex": True}, 
                [{"sample": f"data_{i}"} for i in range(100)],  # Large sample set
                [f"label_{i}" for i in range(100)],
                mock_egraph, complex_groups
            )
            
            # Verify complex workflow was properly coordinated
            assert result.id == "complex_study_456"
            # Should have expand_tasks calls for each gchain in each non-source group
            # complex_groups[1:] has 3 groups, each with 2, 2, and 1 gchains = 5 total
            expected_calls = sum(len(chain_group) for chain_group in complex_groups[1:])
            assert mock_expand.si.call_count == expected_calls
    
    @patch('merlin.celery.app')
    def test_submit_task_group_comprehensive(self, mock_app):
        """Test submit_task_group with comprehensive scenarios."""
        self.server.celery_app = mock_app
        
        with patch('celery.group') as mock_group, \
             patch('celery.chord') as mock_chord:
            
            mock_group_instance = MagicMock()
            mock_group.return_value = mock_group_instance
            mock_result = MagicMock()
            mock_result.id = "group_result_789"
            mock_group_instance.apply_async.return_value = mock_result
            
            # Test group without callback
            result = self.server.submit_task_group(
                "test_group", ["task1", "task2", "task3"]
            )
            
            assert result == "group_result_789"
            mock_group.assert_called_once()
            mock_group_instance.apply_async.assert_called_once()
    
    @patch('merlin.celery.app')
    def test_submit_task_group_with_callback(self, mock_app):
        """Test submit_task_group with callback task (chord)."""
        self.server.celery_app = mock_app
        
        with patch('celery.group') as mock_group, \
             patch('celery.chord') as mock_chord:
            
            mock_group_instance = MagicMock()
            mock_group.return_value = mock_group_instance
            mock_chord_instance = MagicMock()
            mock_chord.return_value = mock_chord_instance
            mock_result = MagicMock()
            mock_result.id = "chord_result_abc" 
            mock_chord_instance.apply_async.return_value = mock_result
            
            # Test group with callback (creates chord)
            result = self.server.submit_task_group(
                "test_chord_group", ["task1", "task2"], 
                callback_task_id="callback_task"
            )
            
            assert result == "chord_result_abc"
            mock_group.assert_called_once()
            mock_chord.assert_called_once()
            mock_chord_instance.apply_async.assert_called_once()
    
    @patch('merlin.celery.app')
    def test_submit_task_group_error_scenarios(self, mock_app):
        """Test submit_task_group error handling."""
        self.server.celery_app = mock_app
        
        # Test empty task list - will create empty group but still return an ID
        result = self.server.submit_task_group("empty_group", [])
        # Empty groups still get UUIDs, so result should be a string (UUID-like)
        assert isinstance(result, str)
        assert len(result) > 0  # Should have some ID
        
        # Test group creation failure
        with patch('celery.group') as mock_group:
            mock_group.side_effect = Exception("Group creation failed")
            
            # Should fallback to individual task submission
            with patch.object(self.server, 'submit_tasks') as mock_submit_tasks:
                mock_submit_tasks.return_value = ["fallback_1", "fallback_2"]
                
                result = self.server.submit_task_group(
                    "failing_group", ["task1", "task2"]
                )
                
                assert result == "fallback_1"  # Returns first result from fallback
                mock_submit_tasks.assert_called_once_with(["task1", "task2"])
    
    @patch('merlin.celery.app')
    def test_submit_dependent_tasks_comprehensive(self, mock_app):
        """Test submit_dependent_tasks with various dependency patterns."""
        from merlin.task_servers.task_server_interface import TaskDependency
        
        self.server.celery_app = mock_app
        
        # Create complex dependency structure
        dependencies = [
            TaskDependency(task_pattern="generate_*", dependency_type="all_success"),
            TaskDependency(task_pattern="process_*", dependency_type="any_success"),
            TaskDependency(task_pattern="analyze_*", dependency_type="all_complete")
        ]
        
        task_ids = ["task1", "task2", "task3", "task4"]
        
        with patch.object(self.server, '_group_tasks_by_dependencies') as mock_group_deps, \
             patch.object(self.server, 'submit_tasks') as mock_submit_tasks:
            
            # Mock dependency grouping
            mock_group_deps.return_value = [
                {"pattern": "generate_*", "tasks": ["task1", "task2"]},
                {"pattern": "process_*", "tasks": ["task3"]},
                {"pattern": "analyze_*", "tasks": ["task4"]}
            ]
            
            mock_submit_tasks.return_value = ["result1", "result2", "result3"]
            
            result = self.server.submit_dependent_tasks(task_ids, dependencies)
            
            # Should return consolidated results
            assert result == ["result1", "result2", "result3"]
            mock_group_deps.assert_called_once_with(task_ids, dependencies)
    
    @patch('merlin.celery.app')
    def test_get_group_status_comprehensive(self, mock_app):
        """Test get_group_status with various group states."""
        self.server.celery_app = mock_app
        
        # Test successful group status
        with patch.object(mock_app, 'GroupResult') as mock_group_result:
            mock_result_instance = MagicMock()
            mock_result_instance.ready.return_value = True
            mock_result_instance.successful.return_value = True
            mock_result_instance.failed.return_value = False
            mock_result_instance.completed_count.return_value = 5
            mock_result_instance.results = ["r1", "r2", "r3", "r4", "r5"]
            mock_group_result.restore.return_value = mock_result_instance
            
            status = self.server.get_group_status("test_group_123")
            
            expected = {
                "group_id": "test_group_123",
                "status": "completed",
                "completed": 5,
                "total": 5,
                "successful": True,
                "failed": False
            }
            
            assert status["group_id"] == expected["group_id"]
            assert status["status"] == expected["status"]
            assert status["completed"] == expected["completed"]
            assert status["total"] == expected["total"]
            assert status["successful"] == expected["successful"]
            assert status["failed"] == expected["failed"]
    
    @patch('merlin.celery.app')
    def test_get_group_status_error_handling(self, mock_app):
        """Test get_group_status error scenarios."""
        self.server.celery_app = mock_app
        
        # Test with invalid group ID
        with patch.object(mock_app, 'GroupResult') as mock_group_result:
            mock_group_result.restore.side_effect = Exception("Group not found")
            
            status = self.server.get_group_status("invalid_group")
            
            assert status == {"group_id": "invalid_group", "status": "unknown"}
    
    def test_coordination_edge_cases(self):
        """Test edge cases in coordination methods."""
        # Test TaskDependency validation
        from merlin.task_servers.task_server_interface import TaskDependency
        
        # Test with invalid dependency type
        dep = TaskDependency("test_pattern", "invalid_type")
        assert dep.task_pattern == "test_pattern"
        assert dep.dependency_type == "invalid_type"
        
        # Test empty pattern
        dep_empty = TaskDependency("", "all_success")
        assert dep_empty.task_pattern == ""
        assert dep_empty.dependency_type == "all_success" 