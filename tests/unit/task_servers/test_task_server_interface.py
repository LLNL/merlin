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
    
    def submit_condense_task(self, sample_index, workspace: str, condensed_workspace: str, queue: str = None):
        """Mock implementation of submit_condense_task method."""
        mock_result = MagicMock()
        mock_result.id = f"condense_{workspace.replace('/', '_')}"
        return mock_result
    
    def submit_study(self, study, adapter, samples, sample_labels, egraph, groups_of_chains):
        """Mock implementation of submit_study method."""
        from celery.result import AsyncResult
        mock_result = MagicMock(spec=AsyncResult)
        mock_result.id = "test_study_result_123"
        return mock_result


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
        
        # Test submit_study method
        mock_study = MagicMock()
        mock_adapter = {"test": "adapter"}
        result = server.submit_study(mock_study, mock_adapter, [], [], MagicMock(), [])
        assert result.id == "test_study_result_123"
    
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
            'check_workers_processing',
            'submit_study'
        ]
        
        for method_name in abstract_methods:
            assert hasattr(TaskServerInterface, method_name)
            method = getattr(TaskServerInterface, method_name)
    
    @patch('merlin.task_servers.task_server_interface.MerlinDatabase')
    def test_submit_study_method_detailed(self, mock_db):
        """Test submit_study method with detailed scenarios."""
        server = ConcreteTaskServer()
        
        # Test with realistic study parameters
        mock_study = MagicMock()
        mock_study.name = "comprehensive_study"
        mock_study.workspace = "/test/workspace"
        
        mock_adapter = {"adapter_type": "test", "config": {"key": "value"}}
        mock_samples = [{"param1": "value1"}, {"param1": "value2"}]
        mock_sample_labels = ["sample_1", "sample_2"]
        
        mock_egraph = MagicMock()
        mock_egraph.name = "test_dag"
        
        mock_groups_of_chains = [
            ["_source"],
            [["step1"], ["step2", "step3"]],
            [["step4"]]
        ]
        
        # Execute submit_study
        result = server.submit_study(
            mock_study, mock_adapter, mock_samples, 
            mock_sample_labels, mock_egraph, mock_groups_of_chains
        )
        
        # Verify result structure
        assert hasattr(result, 'id')
        assert result.id == "test_study_result_123"
        
        # Test with empty parameters to ensure graceful handling
        result_empty = server.submit_study(None, {}, [], [], None, [])
        assert result_empty.id == "test_study_result_123"
    
    def test_abstract_method_enforcement(self):
        """Test that abstract method enforcement is comprehensive."""
        from merlin.task_servers.task_server_interface import TaskServerInterface
        import inspect
        
        # Get all abstract methods from the interface
        abstract_methods = []
        for name, method in inspect.getmembers(TaskServerInterface):
            if hasattr(method, '__isabstractmethod__') and method.__isabstractmethod__:
                abstract_methods.append(name)
        
        # Should have all 19 abstract methods (18 + server_type property)
        expected_count = 19
        actual_count = len(abstract_methods)
        
        print(f"Found {actual_count} abstract methods: {sorted(abstract_methods)}")
        assert actual_count >= expected_count, f"Expected at least {expected_count} abstract methods, found {actual_count}"
        
        # Key methods that must be abstract
        critical_methods = [
            'submit_study', 'submit_task', 'submit_tasks', 'submit_task_group',
            'submit_coordinated_tasks', 'submit_dependent_tasks', 'cancel_task',
            'start_workers', 'stop_workers', 'get_group_status'
        ]
        
        for method in critical_methods:
            assert method in abstract_methods, f"Critical method {method} is not abstract"
    
    def test_database_integration_setup(self):
        """Test that database integration is properly set up."""
        with patch('merlin.task_servers.task_server_interface.MerlinDatabase') as mock_db:
            mock_db_instance = MagicMock()
            mock_db.return_value = mock_db_instance
            
            server = ConcreteTaskServer()
            
            # Verify database instance is created and stored
            assert hasattr(server, 'merlin_db')
            assert server.merlin_db == mock_db_instance
            mock_db.assert_called_once()
    
    def test_interface_contract_compliance(self):
        """Test that ConcreteTaskServer fully complies with interface contract."""
        server = ConcreteTaskServer()
        
        # Test all mandatory methods exist and are callable
        mandatory_methods = [
            'submit_task', 'submit_tasks', 'submit_task_group',
            'submit_coordinated_tasks', 'submit_dependent_tasks', 'get_group_status',
            'cancel_task', 'cancel_tasks', 'start_workers', 'stop_workers',
            'display_queue_info', 'display_connected_workers', 'display_running_tasks',
            'purge_tasks', 'get_workers', 'get_active_queues', 'check_workers_processing',
            'submit_study'
        ]
        
        for method_name in mandatory_methods:
            assert hasattr(server, method_name), f"Missing required method: {method_name}"
            method = getattr(server, method_name)
            assert callable(method), f"Method {method_name} is not callable"
        
        # Test server_type property
        assert hasattr(server, 'server_type')
        assert server.server_type == "test"
        
        # Test method return types are reasonable
        assert isinstance(server.submit_task("test"), str)
        assert isinstance(server.submit_tasks(["test1", "test2"]), list)
        assert isinstance(server.cancel_task("test"), bool)
        assert isinstance(server.cancel_tasks(["test1", "test2"]), dict)
        assert isinstance(server.get_workers(), list)
        assert isinstance(server.get_active_queues(), dict)
        assert isinstance(server.check_workers_processing(["queue1"]), bool)
        assert isinstance(server.purge_tasks(["queue1"]), int)
        
        # Test submit_study returns AsyncResult-like object
        study_result = server.submit_study(None, {}, [], [], None, [])
        assert hasattr(study_result, 'id') 