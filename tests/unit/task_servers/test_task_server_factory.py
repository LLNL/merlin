##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Unit tests for TaskServerFactory.

Tests ensure that:
- Factory can register and create task servers
- Built-in task servers are auto-registered 
- Error handling for invalid task servers
- Plugin discovery functionality
- Legacy method compatibility
"""

import pytest
from unittest.mock import MagicMock, patch

from merlin.task_servers.task_server_factory import TaskServerFactory, task_server_factory
from merlin.task_servers.task_server_interface import TaskServerInterface
from merlin.exceptions import MerlinInvalidTaskServerError


class MockTaskServer(TaskServerInterface):
    """Mock task server for testing."""
    
    def __init__(self):
        super().__init__()
        self.initialized = True
    
    @property
    def server_type(self) -> str:
        """Return the task server type."""
        return "mock"
    
    def submit_task(self, task_id: str):
        return f"mock_submitted_{task_id}"
    
    def submit_tasks(self, task_ids, **kwargs):
        return [f"mock_submitted_{tid}" for tid in task_ids]
    
    def submit_task_group(self, group_id: str, task_ids, callback_task_id=None, **kwargs):
        return f"mock_group_{group_id}_with_{len(task_ids)}_tasks"
    
    def submit_coordinated_tasks(self, coordination_id: str, header_task_ids, body_task_id: str, **kwargs):
        return f"mock_coord_{coordination_id}_header_{len(header_task_ids)}_body_{body_task_id}"
    
    def submit_dependent_tasks(self, task_ids, dependencies=None, **kwargs):
        return [f"mock_dependent_{tid}" for tid in task_ids]
    
    def get_group_status(self, group_id: str):
        return {"group_id": group_id, "status": "completed", "total": 1}
    
    def cancel_task(self, task_id: str):
        return True
    
    def cancel_tasks(self, task_ids):
        return {tid: True for tid in task_ids}
    
    def start_workers(self, spec):
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
        return ["mock_worker"]
    
    def get_active_queues(self):
        return {"mock_queue": ["mock_worker"]}
    
    def check_workers_processing(self, queues):
        return True
    
    def submit_condense_task(self, sample_index, workspace: str, condensed_workspace: str, queue: str = None):
        """Mock implementation of submit_condense_task method."""
        mock_result = MagicMock()
        mock_result.id = f"mock_condense_{workspace.replace('/', '_')}"
        return mock_result
    
    def submit_study(self, study, adapter, samples, sample_labels, egraph, groups_of_chains):
        """Mock implementation of submit_study method."""
        from celery.result import AsyncResult
        mock_result = MagicMock(spec=AsyncResult)
        mock_result.id = "mock_study_result_123"
        return mock_result


class InvalidTaskServer:
    """Invalid task server that doesn't implement TaskServerInterface."""
    pass


class TestTaskServerFactory:
    """Test cases for TaskServerFactory."""
    
    def setup_method(self):
        """Set up a fresh factory for each test."""
        self.factory = TaskServerFactory()
    
    def test_factory_initialization(self):
        """Test that factory initializes with built-in servers."""
        available = self.factory.list_available()
        
        # Should have at least celery
        assert "celery" in available
        
        # Should have aliases
        assert "redis" in self.factory._task_server_aliases
        assert "rabbitmq" in self.factory._task_server_aliases
    
    def test_register_valid_task_server(self):
        """Test registering a valid task server."""
        self.factory.register("mock", MockTaskServer)
        
        available = self.factory.list_available()
        assert "mock" in available
        
        # Should be able to create it
        server = self.factory.create("mock")
        assert isinstance(server, MockTaskServer)
        assert server.initialized is True
    
    def test_register_with_aliases(self):
        """Test registering a task server with aliases."""
        self.factory.register("mock", MockTaskServer, aliases=["test", "dummy"])
        
        # All names should work
        server1 = self.factory.create("mock")
        server2 = self.factory.create("test")
        server3 = self.factory.create("dummy")
        
        assert isinstance(server1, MockTaskServer)
        assert isinstance(server2, MockTaskServer)
        assert isinstance(server3, MockTaskServer)
    
    def test_register_invalid_task_server(self):
        """Test that registering invalid task server raises TypeError."""
        with pytest.raises(TypeError):
            self.factory.register("invalid", InvalidTaskServer)
    
    def test_create_nonexistent_task_server(self):
        """Test that creating nonexistent task server raises error."""
        with pytest.raises(MerlinInvalidTaskServerError) as exc_info:
            self.factory.create("nonexistent")
        
        assert "not supported by Merlin" in str(exc_info.value)
        assert "Available task servers:" in str(exc_info.value)
    
    def test_create_with_config(self):
        """Test creating task server with configuration."""
        self.factory.register("mock", MockTaskServer)
        
        config = {"broker": "redis://localhost", "backend": "redis://localhost"}
        server = self.factory.create("mock", config)
        
        assert isinstance(server, MockTaskServer)
    
    def test_alias_resolution(self):
        """Test that aliases resolve to canonical names."""
        # Test built-in aliases
        if "celery" in self.factory.list_available():
            celery_server = self.factory.create("celery")
            redis_server = self.factory.create("redis")  # Should resolve to celery
            
            # Both should be the same type
            assert type(celery_server) == type(redis_server)
    
    def test_get_server_info(self):
        """Test getting server information."""
        self.factory.register("mock", MockTaskServer)
        
        info = self.factory.get_server_info("mock")
        
        assert info["name"] == "mock"
        assert info["class"] == "MockTaskServer"
        assert "module" in info
        assert "description" in info
    
    def test_get_server_info_invalid(self):
        """Test getting info for invalid server."""
        with pytest.raises(MerlinInvalidTaskServerError):
            self.factory.get_server_info("nonexistent")
    
    @patch('importlib.metadata.entry_points')
    def test_plugin_discovery_with_entry_points(self, mock_entry_points):
        """Test plugin discovery using importlib.metadata entry_points."""
        # Mock entry point
        mock_entry_point = MagicMock()
        mock_entry_point.name = "test_plugin"
        mock_entry_point.load.return_value = MockTaskServer
        
        # Mock entry_points object
        mock_eps = MagicMock()
        mock_eps.select.return_value = [mock_entry_point]
        mock_eps.get.return_value = [mock_entry_point]
        mock_entry_points.return_value = mock_eps
        
        # Trigger discovery
        self.factory._discover_plugins()
        
        # Should have registered the plugin
        assert "test_plugin" in self.factory.list_available()
    
    @patch('importlib.metadata.entry_points')
    def test_plugin_discovery_with_errors(self, mock_entry_points):
        """Test plugin discovery handles errors gracefully."""
        # Mock entry point that fails to load
        mock_entry_point = MagicMock()
        mock_entry_point.name = "broken_plugin"
        mock_entry_point.load.side_effect = ImportError("Plugin broken")
        
        # Mock entry_points object
        mock_eps = MagicMock()
        mock_eps.select.return_value = [mock_entry_point]
        mock_eps.get.return_value = [mock_entry_point]
        mock_entry_points.return_value = mock_eps
        
        # Should not raise exception
        self.factory._discover_plugins()
        
        # Broken plugin should not be registered
        assert "broken_plugin" not in self.factory.list_available()
    
    def test_legacy_methods(self):
        """Test legacy method compatibility."""
        self.factory.register("mock", MockTaskServer)
        
        # Test legacy list method
        available = self.factory.get_supported_task_servers()
        assert "mock" in available
        
        # Test legacy create method
        server = self.factory.get_task_server("mock")
        assert isinstance(server, MockTaskServer)
        
        # Test legacy register method
        class AnotherMockServer(MockTaskServer):
            pass
        
        self.factory.register_task_server("another", AnotherMockServer)
        assert "another" in self.factory.list_available()
    
    def test_global_factory_instance(self):
        """Test that global factory instance works."""
        # Should be able to use global instance
        available = task_server_factory.list_available()
        assert isinstance(available, list)
        assert len(available) > 0
    
    @patch('importlib.import_module')
    @patch('pkgutil.iter_modules')
    def test_builtin_discovery(self, mock_iter_modules, mock_import_module):
        """Test built-in implementation discovery."""
        # Mock module scanning - need to mock the implementations module path
        with patch('merlin.task_servers.implementations') as mock_implementations:
            mock_implementations.__path__ = ['/fake/path']
            mock_iter_modules.return_value = [
                (None, "test_server", None)
            ]
            
            # Mock module with TaskServer class
            mock_module = MagicMock()
            mock_module.TestTaskServer = MockTaskServer
            mock_import_module.return_value = mock_module
            
            # Mock dir() to return our class
            with patch('builtins.dir', return_value=['TestTaskServer']):
                self.factory._discover_plugins()
            
            # Should discover and register
            assert "test" in self.factory.list_available()
    
    def test_create_initialization_error(self):
        """Test handling of task server initialization errors."""
        class FailingTaskServer(TaskServerInterface):
            def __init__(self):
                raise Exception("Initialization failed")
            
            @property
            def server_type(self) -> str:
                return "failing"
            
            # Dummy implementations (won't be called)
            def submit_task(self, task_id): pass
            def submit_tasks(self, task_ids, **kwargs): pass
            def submit_task_group(self, group_id, task_ids, callback_task_id=None, **kwargs): pass
            def submit_coordinated_tasks(self, coordination_id, header_task_ids, body_task_id, **kwargs): pass
            def submit_dependent_tasks(self, task_ids, dependencies=None, **kwargs): pass
            def get_group_status(self, group_id): pass
            def cancel_task(self, task_id): pass
            def cancel_tasks(self, task_ids): pass
            def start_workers(self, spec): pass
            def stop_workers(self, names=None): pass
            def display_queue_info(self, queues=None): pass
            def display_connected_workers(self): pass
            def display_running_tasks(self): pass
            def purge_tasks(self, queues, force=False): pass
            def get_workers(self): pass
            def get_active_queues(self): pass
            def check_workers_processing(self, queues): pass
            def submit_condense_task(self, sample_index, workspace: str, condensed_workspace: str, queue: str = None): pass
            def submit_study(self, study, adapter, samples, sample_labels, egraph, groups_of_chains): pass
        
        self.factory.register("failing", FailingTaskServer)
        
        with pytest.raises(MerlinInvalidTaskServerError) as exc_info:
            self.factory.create("failing")
        
        assert "Failed to create" in str(exc_info.value)
        assert "Initialization failed" in str(exc_info.value) 