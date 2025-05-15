"""
Tests for database-related mixin classes.
"""
import pytest
from unittest.mock import MagicMock, patch
from typing import List, NamedTuple
from dataclasses import dataclass

from merlin.db_scripts.mixins.name import NameMixin
from merlin.db_scripts.mixins.queue_management import QueueManagementMixin
from merlin.db_scripts.mixins.run_management import RunManagementMixin


# Test fixtures and helper classes

@dataclass
class EntityInfo:
    """Simple data class to mimic `entity_info` objects"""
    name: str = ""
    queues: List[str] = None
    runs: List[str] = None
    
    def __post_init__(self):
        if self.queues is None:
            self.queues = []
        if self.runs is None:
            self.runs = []

class TestClass(NameMixin, QueueManagementMixin, RunManagementMixin):
    """A test class that uses all the mixins"""
    
    def __init__(self):
        self.entity_info = EntityInfo()
        self.backend = "mock_backend"
        self.reload_data_called = 0
        self.save_called = 0
    
    def reload_data(self):
        self.reload_data_called += 1
    
    def save(self):
        self.save_called += 1


class TestNameMixin:
    """Tests for the `NameMixin` class."""
    
    def test_get_name(self):
        """Test that `get_name` returns the correct name"""
        test_obj = TestClass()
        test_obj.entity_info.name = "Test Entity"
        
        assert test_obj.get_name() == "Test Entity"
    
    def test_get_name_empty(self):
        """Test that `get_name` returns an empty string when name is empty"""
        test_obj = TestClass()
        test_obj.entity_info.name = ""
        
        assert test_obj.get_name() == ""


class TestQueueManagementMixin:
    """Tests for the `QueueManagementMixin` class."""
    
    def test_get_queues_empty(self):
        """Test that `get_queues` returns an empty list when no queues are assigned"""
        test_obj = TestClass()
        
        assert test_obj.get_queues() == []
    
    def test_get_queues_with_values(self):
        """Test that `get_queues` returns the correct list of queues"""
        test_obj = TestClass()
        test_obj.entity_info.queues = ["queue1", "queue2", "queue3"]
        
        queues = test_obj.get_queues()
        assert len(queues) == 3
        assert "queue1" in queues
        assert "queue2" in queues
        assert "queue3" in queues


class TestRunManagementMixin:
    """Tests for the `RunManagementMixin` class."""
    
    def test_get_runs_empty(self):
        """Test that `get_runs` returns an empty list when no runs are present"""
        test_obj = TestClass()
        
        runs = test_obj.get_runs()
        assert test_obj.reload_data_called == 1
        assert runs == []
    
    def test_get_runs_with_values(self):
        """Test that `get_runs` returns the correct list of runs"""
        test_obj = TestClass()
        test_obj.entity_info.runs = ["run1", "run2", "run3"]
        
        runs = test_obj.get_runs()
        assert test_obj.reload_data_called == 1
        assert len(runs) == 3
        assert "run1" in runs
        assert "run2" in runs
        assert "run3" in runs
    
    def test_add_run(self):
        """Test that `add_run` correctly adds a run ID to the list"""
        test_obj = TestClass()
        test_obj.entity_info.runs = ["run1"]
        
        test_obj.add_run("run2")
        assert test_obj.save_called == 1
        assert len(test_obj.entity_info.runs) == 2
        assert "run1" in test_obj.entity_info.runs
        assert "run2" in test_obj.entity_info.runs
    
    def test_remove_run(self):
        """Test that `remove_run` correctly removes a run ID from the list"""
        test_obj = TestClass()
        test_obj.entity_info.runs = ["run1", "run2", "run3"]
        
        test_obj.remove_run("run2")
        assert test_obj.reload_data_called == 1
        assert test_obj.save_called == 1
        assert len(test_obj.entity_info.runs) == 2
        assert "run1" in test_obj.entity_info.runs
        assert "run3" in test_obj.entity_info.runs
        assert "run2" not in test_obj.entity_info.runs
    
    def test_remove_run_not_in_list(self):
        """Test that `remove_run` raises `ValueError` when run ID is not in the list"""
        test_obj = TestClass()
        test_obj.entity_info.runs = ["run1", "run3"]
        
        with pytest.raises(ValueError):
            test_obj.remove_run("run2")
    
    def test_construct_run_string_empty(self):
        """Test `construct_run_string` when no runs are present"""
        test_obj = TestClass()
        test_obj.entity_info.runs = []
        
        run_str = test_obj.construct_run_string()
        assert run_str == "  No runs found.\n"
    
    @patch('merlin.db_scripts.entities.run_entity.RunEntity')
    def test_construct_run_string_with_runs(self, mock_run_entity: MagicMock):
        """
        Test `construct_run_string` with runs.
        
        Args:
            mock_run_entity: A mocked version of the `RunEntity` class.
        """
        # Setup the mock
        mock_run1 = MagicMock()
        mock_run1.get_id.return_value = "run1"
        mock_run1.get_workspace.return_value = "workspace-run1"
        
        mock_run2 = MagicMock()
        mock_run2.get_id.return_value = "run2"
        mock_run2.get_workspace.return_value = "workspace-run2"
        
        mock_run_entity.load.side_effect = lambda run_id, backend: {
            "run1": mock_run1,
            "run2": mock_run2
        }[run_id]
        
        # Test the function
        test_obj = TestClass()
        test_obj.entity_info.runs = ["run1", "run2"]
        
        expected_str = "  - ID: run1\n    Workspace: workspace-run1\n" + \
                      "  - ID: run2\n    Workspace: workspace-run2\n"
        
        with patch('merlin.db_scripts.entities.run_entity.RunEntity', mock_run_entity):
            run_str = test_obj.construct_run_string()
            assert run_str == expected_str
    
    @patch('merlin.db_scripts.entities.run_entity.RunEntity')
    def test_construct_run_string_with_error(self, mock_run_entity: MagicMock):
        """
        Test `construct_run_string` when an error occurs loading a run
        
        Args:
            mock_run_entity: A mocked version of the `RunEntity` class.
        """
        # Setup the mock
        mock_run1 = MagicMock()
        mock_run1.get_id.return_value = "run1"
        mock_run1.get_workspace.return_value = "workspace-run1"
        
        def mock_load(run_id, backend):
            if run_id == "run1":
                return mock_run1
            else:
                raise Exception("Error loading run")
        
        mock_run_entity.load.side_effect = mock_load
        
        # Test the function
        test_obj = TestClass()
        test_obj.entity_info.runs = ["run1", "run2"]
        
        expected_str = "  - ID: run1\n    Workspace: workspace-run1\n" + \
                      "  - ID: run2 (Error loading run)\n"
        
        with patch('merlin.db_scripts.entities.run_entity.RunEntity', mock_run_entity):
            run_str = test_obj.construct_run_string()
            assert run_str == expected_str
