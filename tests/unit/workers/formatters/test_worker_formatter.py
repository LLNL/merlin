##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `merlin/workers/formatters/worker_formatter.py` module.
"""

from typing import Dict, List
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from merlin.common.enums import WorkerStatus
from merlin.db_scripts.entities.logical_worker_entity import LogicalWorkerEntity
from merlin.db_scripts.merlin_db import MerlinDatabase
from merlin.workers.formatters.worker_formatter import WorkerFormatter


class DummyWorkerFormatter(WorkerFormatter):
    """Dummy implementation of WorkerFormatter for testing."""
    
    def __init__(self):
        super().__init__()
        self.formatted_data = None
        self.display_called = False
        
    def format_and_display(self, logical_workers: List, filters: Dict, merlin_db: MerlinDatabase):
        """Mock implementation that stores the call parameters."""
        self.formatted_data = {
            "logical_workers": logical_workers,
            "filters": filters,
            "merlin_db": merlin_db
        }
        self.display_called = True
        return f"Formatted {len(logical_workers)} workers with filters {filters}"


def test_abstract_formatter_cannot_be_instantiated():
    """Test that attempting to instantiate the abstract base class raises a TypeError."""
    with pytest.raises(TypeError):
        WorkerFormatter()


def test_unimplemented_method_raises_not_implemented():
    """Test that calling abstract methods on a subclass without implementation raises NotImplementedError."""
    
    class IncompleteFormatter(WorkerFormatter):
        pass
    
    # Should raise TypeError due to unimplemented abstract method
    with pytest.raises(TypeError):
        IncompleteFormatter()


class TestWorkerFormatter:
    """Tests for the WorkerFormatter abstract base class and its concrete implementations."""

    @pytest.fixture
    def formatter(self, mocker: MockerFixture) -> DummyWorkerFormatter:
        """
        Create a DummyWorkerFormatter instance for testing.
        
        Args:
            mocker: Pytest mocker fixture.
            
        Returns:
            DummyWorkerFormatter instance with mocked console.
        """
        # Mock the console to avoid actual Rich console creation
        mock_console = MagicMock()
        mocker.patch("merlin.workers.formatters.worker_formatter.Console", return_value=mock_console)
        return DummyWorkerFormatter()

    @pytest.fixture
    def mock_logical_workers(self) -> List[MagicMock]:
        """
        Create mock logical worker entities for testing.
        
        Returns:
            List of mock logical worker entities.
        """
        worker1 = MagicMock(spec=LogicalWorkerEntity)
        worker1.get_name.return_value = "worker1"
        worker1.get_queues.return_value = ["queue1", "queue2"]
        worker1.get_physical_workers.return_value = ["phys1", "phys2"]
        
        worker2 = MagicMock(spec=LogicalWorkerEntity)
        worker2.get_name.return_value = "worker2"
        worker2.get_queues.return_value = ["queue3"]
        worker2.get_physical_workers.return_value = []
        
        return [worker1, worker2]

    @pytest.fixture
    def mock_physical_workers(self) -> List[MagicMock]:
        """
        Create mock physical worker entities for testing.
        
        Returns:
            List of mock physical worker entities.
        """
        worker1 = MagicMock()
        worker1.get_status.return_value = WorkerStatus.RUNNING
        
        worker2 = MagicMock()
        worker2.get_status.return_value = WorkerStatus.STOPPED
        
        worker3 = MagicMock()
        worker3.get_status.return_value = WorkerStatus.STALLED
        
        return [worker1, worker2, worker3]

    @pytest.fixture
    def mock_db(self) -> MagicMock:
        """
        Create a mock MerlinDatabase for testing.

        Returns:
            Mock MerlinDatabase instance.
        """
        return MagicMock(spec=MerlinDatabase)

    def test_console_initialization(self, formatter: DummyWorkerFormatter):
        """
        Test that WorkerFormatter initializes with a Rich Console.

        Args:
            formatter: DummyWorkerFormatter instance for testing.
        """
        assert hasattr(formatter, 'console')
        assert formatter.console is not None

    def test_format_and_display_abstract_method_implemented(
        self,
        formatter: DummyWorkerFormatter,
        mock_logical_workers: List[MagicMock],
        mock_db: MagicMock
    ):
        """
        Test that concrete implementation can override format_and_display.
        
        Args:
            formatter: DummyWorkerFormatter instance for testing.
            mock_logical_workers: List of mock logical worker entities.
            mock_db: Mock MerlinDatabase instance.
        """
        filters = {"queues": ["test_queue"]}
        result = formatter.format_and_display(mock_logical_workers, filters, mock_db)
        
        assert formatter.display_called is True
        assert "Formatted 2 workers" in result
        assert formatter.formatted_data["logical_workers"] == mock_logical_workers
        assert formatter.formatted_data["filters"] == filters
        assert formatter.formatted_data["merlin_db"] == mock_db

    def test_get_worker_statistics_basic_functionality(
        self,
        formatter: DummyWorkerFormatter,
        mock_logical_workers: List[MagicMock],
        mock_physical_workers: List[MagicMock],
        mock_db: MagicMock
    ):
        """
        Test that get_worker_statistics computes basic statistics correctly.
        
        Args:
            formatter: DummyWorkerFormatter instance for testing.
            mock_logical_workers: List of mock logical worker entities.
            mock_physical_workers: List of mock physical worker entities.
            mock_db: Mock MerlinDatabase instance.
        """
        # Setup database to return physical workers
        mock_db.get.side_effect = mock_physical_workers
        
        stats = formatter.get_worker_statistics(mock_logical_workers, mock_db)
        
        # Verify basic counts
        assert stats["total_logical"] == 2
        assert stats["logical_with_instances"] == 1  # Only worker1 has physical workers
        assert stats["logical_without_instances"] == 1  # worker2 has no physical workers
        assert stats["total_physical"] == 2  # worker1 has 2 physical workers

    def test_get_worker_statistics_status_counts(
        self,
        formatter: DummyWorkerFormatter,
        mock_logical_workers: List[MagicMock],
        mock_physical_workers: List[MagicMock],
        mock_db: MagicMock
    ):
        """
        Test that get_worker_statistics counts worker statuses correctly.
        
        Args:
            formatter: DummyWorkerFormatter instance for testing.
            mock_logical_workers: List of mock logical worker entities.
            mock_physical_workers: List of mock physical worker entities.
            mock_db: Mock MerlinDatabase instance.
        """
        # Setup database to return physical workers for first logical worker only
        mock_db.get.side_effect = mock_physical_workers[:2]  # First 2 physical workers
        
        stats = formatter.get_worker_statistics(mock_logical_workers, mock_db)
        
        # Verify status counts
        assert stats["physical_running"] == 1
        assert stats["physical_stopped"] == 1
        assert stats["physical_stalled"] == 0
        assert stats["physical_rebooting"] == 0

    def test_get_worker_statistics_with_empty_logical_workers(
        self,
        formatter: DummyWorkerFormatter,
        mock_db: MagicMock
    ):
        """
        Test get_worker_statistics with empty logical workers list.
        
        Args:
            formatter: DummyWorkerFormatter instance for testing.
            mock_db: Mock MerlinDatabase instance.
        """
        stats = formatter.get_worker_statistics([], mock_db)
        
        # All counts should be zero
        assert stats["total_logical"] == 0
        assert stats["logical_with_instances"] == 0
        assert stats["logical_without_instances"] == 0
        assert stats["total_physical"] == 0
        assert stats["physical_running"] == 0
        assert stats["physical_stopped"] == 0
        assert stats["physical_stalled"] == 0
        assert stats["physical_rebooting"] == 0

    def test_get_worker_statistics_with_all_status_types(
        self,
        formatter: DummyWorkerFormatter,
        mock_db: MagicMock
    ):
        """
        Test get_worker_statistics counts all worker status types correctly.
        
        Args:
            formatter: DummyWorkerFormatter instance for testing.
            mock_db: Mock MerlinDatabase instance.
        """        
        # Create logical worker with multiple physical workers of different statuses
        logical_worker = MagicMock(spec=LogicalWorkerEntity)
        logical_worker.get_physical_workers.return_value = ["p1", "p2", "p3", "p4"]
        
        # Create physical workers with all status types
        physical_workers = []
        statuses = [WorkerStatus.RUNNING, WorkerStatus.STOPPED, WorkerStatus.STALLED, WorkerStatus.REBOOTING]
        for i, status in enumerate(statuses):
            worker = MagicMock()
            worker.get_status.return_value = status
            physical_workers.append(worker)
        
        mock_db.get.side_effect = physical_workers
        
        stats = formatter.get_worker_statistics([logical_worker], mock_db)
        
        # Verify all status types are counted
        assert stats["total_logical"] == 1
        assert stats["logical_with_instances"] == 1
        assert stats["logical_without_instances"] == 0
        assert stats["total_physical"] == 4
        assert stats["physical_running"] == 1
        assert stats["physical_stopped"] == 1
        assert stats["physical_stalled"] == 1
        assert stats["physical_rebooting"] == 1

    def test_get_worker_statistics_database_interaction(
        self,
        formatter: DummyWorkerFormatter,
        mock_logical_workers: List[MagicMock],
        mock_physical_workers: List[MagicMock],
        mock_db: MagicMock
    ):
        """
        Test that get_worker_statistics interacts with database correctly.
        
        Args:
            formatter: DummyWorkerFormatter instance for testing.
            mock_logical_workers: List of mock logical worker entities.
            mock_physical_workers: List of mock physical worker entities.
            mock_db: Mock MerlinDatabase instance.
        """
        mock_db.get.side_effect = mock_physical_workers
        
        formatter.get_worker_statistics(mock_logical_workers, mock_db)
        
        # Verify database was called for each physical worker ID
        expected_calls = [
            ("physical_worker", "phys1"),
            ("physical_worker", "phys2")
        ]
        actual_calls = [call[0] for call in mock_db.get.call_args_list]
        
        for expected_call in expected_calls:
            assert expected_call in actual_calls

    def test_get_worker_statistics_handles_mixed_scenarios(
        self,
        formatter: DummyWorkerFormatter,
        mock_db: MagicMock
    ):
        """
        Test get_worker_statistics with mixed scenarios (some workers with/without instances).
        
        Args:
            formatter: DummyWorkerFormatter instance for testing.
            mock_db: Mock MerlinDatabase instance.
        """        
        # Create logical workers: one with multiple instances, one without, one with single instance
        logical_workers = []
        
        # Worker 1: Has 2 physical workers
        worker1 = MagicMock(spec=LogicalWorkerEntity)
        worker1.get_physical_workers.return_value = ["p1", "p2"]
        logical_workers.append(worker1)
        
        # Worker 2: No physical workers
        worker2 = MagicMock(spec=LogicalWorkerEntity)
        worker2.get_physical_workers.return_value = []
        logical_workers.append(worker2)
        
        # Worker 3: Has 1 physical worker
        worker3 = MagicMock(spec=LogicalWorkerEntity)
        worker3.get_physical_workers.return_value = ["p3"]
        logical_workers.append(worker3)
        
        # Create physical workers
        physical_workers = [
            MagicMock(get_status=lambda: WorkerStatus.RUNNING),
            MagicMock(get_status=lambda: WorkerStatus.STOPPED),
            MagicMock(get_status=lambda: WorkerStatus.STALLED),
        ]
        
        mock_db.get.side_effect = physical_workers
        
        stats = formatter.get_worker_statistics(logical_workers, mock_db)
        
        assert stats["total_logical"] == 3
        assert stats["logical_with_instances"] == 2  # worker1 and worker3
        assert stats["logical_without_instances"] == 1  # worker2
        assert stats["total_physical"] == 3
        assert stats["physical_running"] == 1
        assert stats["physical_stopped"] == 1
        assert stats["physical_stalled"] == 1
        assert stats["physical_rebooting"] == 0