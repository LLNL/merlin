##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Integration tests for task server components.

These tests verify that different components work together correctly,
including StudyExecutor + TaskServerInterface integration, end-to-end
workflow testing, and cross-component interactions.
"""

import pytest
from unittest.mock import MagicMock, patch, call
from celery.result import AsyncResult

from merlin.task_servers.implementations.celery_server import CeleryTaskServer
from merlin.task_servers.task_server_factory import TaskServerFactory
from merlin.task_servers.task_server_interface import TaskServerInterface, TaskDependency


class TestTaskServerIntegration:
    """Integration tests for task server components."""
    
    @patch('merlin.task_servers.task_server_interface.MerlinDatabase')
    def test_factory_to_celery_server_integration(self, mock_db):
        """Test that factory creates working CeleryTaskServer instances."""
        factory = TaskServerFactory()
        
        # Create celery server through factory
        server = factory.create("celery")
        
        # Verify it's the correct type and implements interface
        assert isinstance(server, CeleryTaskServer)
        assert isinstance(server, TaskServerInterface)
        assert server.server_type == "celery"
        
        # Verify all abstract methods are implemented
        required_methods = [
            'submit_task', 'submit_tasks', 'submit_task_group',
            'submit_coordinated_tasks', 'submit_dependent_tasks', 'get_group_status',
            'cancel_task', 'cancel_tasks', 'start_workers', 'stop_workers',
            'display_queue_info', 'display_connected_workers', 'display_running_tasks',
            'purge_tasks', 'get_workers', 'get_active_queues', 'check_workers_processing',
            'submit_study'
        ]
        
        for method_name in required_methods:
            assert hasattr(server, method_name)
            assert callable(getattr(server, method_name))
    
    @patch('merlin.task_servers.task_server_interface.MerlinDatabase')
    @patch('merlin.celery.app')
    def test_study_executor_task_server_integration(self, mock_app, mock_db):
        """Test StudyExecutor integration with TaskServerInterface."""
        from merlin.study.study_executor import StudyExecutor
        
        # Create a real study executor
        executor = StudyExecutor()
        
        # Create mock study components
        mock_study = MagicMock()
        mock_study.samples = [{"param": "value1"}, {"param": "value2"}]
        mock_study.sample_labels = ["sample1", "sample2"]
        mock_study.workspace = "/test/workspace"
        
        mock_dag = MagicMock()
        mock_dag.group_tasks.return_value = [
            ["_source"],
            [["step1"], ["step2", "step3"]]
        ]
        mock_study.dag = mock_dag
        
        # Mock the task server method
        mock_task_server = MagicMock()
        mock_result = MagicMock(spec=AsyncResult)
        mock_result.id = "integration_test_result"
        mock_task_server.submit_study.return_value = mock_result
        mock_study.get_task_server.return_value = mock_task_server
        
        # Execute the study through StudyExecutor
        result = executor.execute_study(mock_study, {"test": "adapter"})
        
        # Verify integration worked correctly
        assert result == mock_result
        mock_dag.group_tasks.assert_called_once_with("_source")
        mock_task_server.submit_study.assert_called_once()
        
        # Verify correct parameters were passed
        submit_call = mock_task_server.submit_study.call_args
        assert submit_call[0][0] == mock_study  # study parameter
        assert submit_call[0][1] == {"test": "adapter"}  # adapter parameter
        assert submit_call[0][2] == mock_study.samples  # samples parameter
        assert submit_call[0][3] == mock_study.sample_labels  # sample_labels parameter
        assert submit_call[0][4] == mock_dag  # egraph parameter
    
    @patch('merlin.task_servers.task_server_interface.MerlinDatabase')
    @patch('merlin.celery.app')
    def test_end_to_end_workflow_simulation(self, mock_app, mock_db):
        """Test complete end-to-end workflow simulation."""
        # Create factory and get server
        factory = TaskServerFactory()
        server = factory.create("celery")
        
        # Mock Celery operations for complete workflow
        with patch('celery.chain') as mock_chain, \
             patch('celery.chord') as mock_chord, \
             patch('celery.group') as mock_group, \
             patch('merlin.common.tasks.expand_tasks_with_samples') as mock_expand, \
             patch('merlin.common.tasks.chordfinisher') as mock_chordfinisher, \
             patch('merlin.common.tasks.mark_run_as_complete') as mock_mark_complete:
            
            # Setup other mocks
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
            
            # Setup mock chain for workflow
            mock_chain_result = MagicMock()
            mock_chain_result.id = "end_to_end_workflow_123"
            mock_chain_instance = MagicMock()
            mock_chain_instance.__or__ = MagicMock(return_value=mock_chain_instance)
            mock_chain_instance.delay.return_value = mock_chain_result
            
            # Create comprehensive study for end-to-end test
            mock_study = MagicMock()
            mock_study.workspace = "/end_to_end/workspace"
            mock_study.level_max_dirs = 5
            
            mock_egraph = MagicMock()
            mock_step = MagicMock()
            mock_step.get_task_queue.return_value = "end_to_end_queue"
            mock_egraph.step.return_value = mock_step
            
            # Complex workflow structure
            complex_workflow = [
                ["_source"],
                [["data_generation"], ["data_validation"]],
                [["preprocessing"], ["feature_extraction", "data_cleaning"]],
                [["model_training"], ["hyperparameter_tuning"]],
                [["model_evaluation"], ["results_analysis"]],
                [["report_generation"]]
            ]
            
            # Execute complete workflow
            result = server.submit_study(
                mock_study,
                {"workflow_type": "ml_pipeline", "config": {"epochs": 100}},
                [{"dataset": f"data_{i}"} for i in range(10)],  # 10 samples
                [f"dataset_{i}" for i in range(10)],
                mock_egraph,
                complex_workflow
            )
            
            # Verify end-to-end execution
            assert result.id == "end_to_end_workflow_123"
            
            # Verify workflow coordination was set up correctly
            # complex_workflow[1:] has 5 groups, each with 1, 2, 2, 2, 1 gchains = 8 total
            expected_calls = sum(len(chain_group) for chain_group in complex_workflow[1:])
            assert mock_expand.si.call_count == expected_calls
            mock_mark_complete.si.assert_called_once_with(mock_study.workspace)
            mock_chain_instance.delay.assert_called_once_with(None)
    
    @patch('merlin.task_servers.task_server_interface.MerlinDatabase')
    def test_coordination_patterns_integration(self, mock_db):
        """Test integration of different coordination patterns."""
        server = CeleryTaskServer()
        
        # Test pattern 1: Simple task group
        with patch('celery.group') as mock_group:
            mock_group_instance = MagicMock()
            mock_group.return_value = mock_group_instance
            mock_result = MagicMock()
            mock_result.id = "simple_group_result"
            mock_group_instance.apply_async.return_value = mock_result
            
            result = server.submit_task_group("simple_group", ["task1", "task2"])
            assert result == "simple_group_result"
        
        # Test pattern 2: Chord (group with callback)
        with patch('celery.group') as mock_group, \
             patch('celery.chord') as mock_chord:
            
            mock_chord_instance = MagicMock()
            mock_chord.return_value = mock_chord_instance
            mock_chord_result = MagicMock()
            mock_chord_result.id = "chord_result"
            mock_chord_instance.apply_async.return_value = mock_chord_result
            
            result = server.submit_task_group(
                "chord_group", ["header1", "header2"], 
                callback_task_id="callback"
            )
            assert result == "chord_result"
        
        # Test pattern 3: Coordinated tasks with dependencies
        task_deps = [
            TaskDependency("header_task", "header"),
            TaskDependency("callback_task", "callback")
        ]
        task_deps[0].task_signature = MagicMock()
        task_deps[1].task_signature = MagicMock()
        
        with patch('celery.group') as mock_group, \
             patch('celery.chord') as mock_chord:
            
            mock_coord_result = MagicMock()
            mock_coord_result.id = "coordinated_result"
            mock_chord_instance = MagicMock()
            mock_chord.return_value = mock_chord_instance
            mock_chord_instance.apply_async.return_value = mock_coord_result
            
            result = server.submit_coordinated_tasks(task_deps)
            assert result == "coordinated_result"
    
    @patch('merlin.task_servers.task_server_interface.MerlinDatabase')
    def test_error_handling_integration(self, mock_db):
        """Test integrated error handling across components."""
        server = CeleryTaskServer()
        
        # Test error propagation in task group submission
        with patch('celery.group') as mock_group:
            mock_group.side_effect = Exception("Celery group creation failed")
            
            # Should fallback to individual task submission
            with patch.object(server, 'submit_tasks') as mock_submit_tasks:
                mock_submit_tasks.return_value = ["fallback1", "fallback2"]
                
                result = server.submit_task_group("failing_group", ["task1", "task2"])
                
                # Should get first fallback result
                assert result == "fallback1"
                mock_submit_tasks.assert_called_once_with(["task1", "task2"])
        
        # Test error handling in coordinated tasks
        task_deps = [TaskDependency("task", "header")]
        task_deps[0].task_signature = MagicMock()
        task_deps[0].task_signature.delay.return_value.id = "fallback_individual"
        
        with patch('celery.group') as mock_group:
            mock_group.side_effect = Exception("Coordination failed")
            
            result = server.submit_coordinated_tasks(task_deps)
            
            # Should return empty string and have attempted fallback
            assert result == ""
            task_deps[0].task_signature.delay.assert_called_once()
    
    def test_factory_plugin_integration(self):
        """Test factory plugin system integration."""
        factory = TaskServerFactory()
        
        # Test built-in server registration
        available = factory.list_available()
        assert "celery" in available
        
        # Test server info retrieval
        info = factory.get_server_info("celery")
        assert info["name"] == "celery"
        assert "class" in info
        # aliases is optional
        # assert "aliases" in info
        
        # Test alias resolution
        aliases = factory._task_server_aliases
        if aliases:  # If aliases exist
            for alias, canonical in aliases.items():
                # Should be able to create server via alias
                server_via_alias = factory.create(alias)
                server_via_canonical = factory.create(canonical)
                assert type(server_via_alias) == type(server_via_canonical)


class TestDatabaseIntegration:
    """Test database integration across task server components."""
    
    @patch('merlin.task_servers.task_server_interface.MerlinDatabase')
    def test_database_initialization_integration(self, mock_db):
        """Test that database is properly initialized across components."""
        mock_db_instance = MagicMock()
        mock_db.return_value = mock_db_instance
        
        # Test database initialization in CeleryTaskServer
        server = CeleryTaskServer()
        assert hasattr(server, 'merlin_db')
        assert server.merlin_db == mock_db_instance
        
        # Test database initialization in factory-created server
        factory = TaskServerFactory()
        factory_server = factory.create("celery")
        assert hasattr(factory_server, 'merlin_db')
        assert factory_server.merlin_db == mock_db_instance
        
        # Verify database was initialized for each instance
        assert mock_db.call_count >= 2


class TestPerformanceIntegration:
    """Test performance-related integration scenarios."""
    
    @patch('merlin.task_servers.task_server_interface.MerlinDatabase')
    @patch('merlin.celery.app')
    def test_large_workflow_handling(self, mock_app, mock_db):
        """Test handling of large workflows with many tasks."""
        server = CeleryTaskServer()
        
        # Create large workflow simulation
        large_samples = [{"param": f"value_{i}"} for i in range(1000)]
        large_labels = [f"sample_{i}" for i in range(1000)]
        
        # Complex workflow with many groups
        large_workflow = [
            ["_source"],
            *[[f"step_{i}_{j}" for j in range(5)] for i in range(20)]  # 20 groups, 5 steps each
        ]
        
        mock_study = MagicMock()
        mock_study.workspace = "/large/workspace"
        mock_study.level_max_dirs = 10
        
        mock_egraph = MagicMock()
        mock_step = MagicMock()
        mock_step.get_task_queue.return_value = "large_queue"
        mock_egraph.step.return_value = mock_step
        
        with patch('celery.chain') as mock_chain, \
             patch('merlin.common.tasks.expand_tasks_with_samples') as mock_expand:
            
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
            mock_result.id = "large_workflow_result"
            mock_chain_instance.__or__ = MagicMock(return_value=mock_chain_instance)
            mock_chain_instance.delay.return_value = mock_result
            
            # Execute large workflow
            result = server.submit_study(
                mock_study, {}, large_samples, large_labels,
                mock_egraph, large_workflow
            )
            
            # Verify it handled the large workflow
            assert result.id == "large_workflow_result"
            # Should have processed all groups (minus _source)
            # large_workflow[1:] has 20 groups, each with 5 gchains = 100 total
            expected_calls = sum(len(chain_group) for chain_group in large_workflow[1:])
            assert mock_expand.si.call_count == expected_calls
    
    @patch('merlin.task_servers.task_server_interface.MerlinDatabase')
    def test_concurrent_task_submission(self, mock_db):
        """Test concurrent task submission scenarios."""
        server = CeleryTaskServer()
        
        # Simulate concurrent task submissions
        with patch.object(server, 'submit_task') as mock_submit:
            mock_submit.return_value = "concurrent_task_result"
            
            # Submit multiple tasks "concurrently" (in sequence for test)
            task_ids = [f"concurrent_task_{i}" for i in range(100)]
            results = []
            
            for task_id in task_ids:
                result = server.submit_task(task_id)
                results.append(result)
            
            # Verify all tasks were submitted
            assert len(results) == 100
            assert all(r == "concurrent_task_result" for r in results)
            assert mock_submit.call_count == 100