##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `merlin/workers/formatters/json_formatter.py` module.
"""

import json
from datetime import datetime, timedelta
from typing import Dict, List
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from merlin.common.enums import WorkerStatus
from merlin.db_scripts.entities.logical_worker_entity import LogicalWorkerEntity
from merlin.db_scripts.entities.physical_worker_entity import PhysicalWorkerEntity
from merlin.db_scripts.merlin_db import MerlinDatabase
from merlin.workers.formatters.json_formatter import JSONWorkerFormatter


class TestJSONWorkerFormatter:
    """Tests for the JSONWorkerFormatter class."""

    @pytest.fixture
    def formatter(self, mocker: MockerFixture) -> JSONWorkerFormatter:
        """
        Create a JSONWorkerFormatter instance for testing.

        Args:
            mocker: Pytest mocker fixture.

        Returns:
            JSONWorkerFormatter instance with mocked console.
        """
        # Mock the console from the base class
        mock_console = MagicMock()
        mocker.patch("merlin.workers.formatters.worker_formatter.Console", return_value=mock_console)
        return JSONWorkerFormatter()

    @pytest.fixture
    def mock_logical_workers(self) -> List[MagicMock]:
        """
        Create mock logical worker entities for testing.

        Returns:
            List of mock logical worker entities.
        """
        worker1 = MagicMock(spec=LogicalWorkerEntity)
        worker1.get_name.return_value = "logical_worker1"
        worker1.get_queues.return_value = ["queue1", "queue2", "custom_queue"]
        worker1.get_physical_workers.return_value = ["phys1", "phys2"]

        worker2 = MagicMock(spec=LogicalWorkerEntity)
        worker2.get_name.return_value = "logical_worker2"
        worker2.get_queues.return_value = ["queue3"]
        worker2.get_physical_workers.return_value = []  # No physical workers

        worker3 = MagicMock(spec=LogicalWorkerEntity)
        worker3.get_name.return_value = "logical_worker3"
        worker3.get_queues.return_value = ["queue4"]
        worker3.get_physical_workers.return_value = ["phys3"]

        return [worker1, worker2, worker3]

    @pytest.fixture
    def mock_physical_workers(self) -> List[MagicMock]:
        """
        Create mock physical worker entities for testing.

        Returns:
            List of mock physical worker entities.
        """
        # Current time for consistent testing
        now = datetime.now()

        worker1 = MagicMock(spec=PhysicalWorkerEntity)
        worker1.get_id.return_value = "phys1"
        worker1.get_name.return_value = "physical_worker1"
        worker1.get_host.return_value = "host1.example.com"
        worker1.get_pid.return_value = 12345
        worker1.get_status.return_value = WorkerStatus.RUNNING
        worker1.get_restart_count.return_value = 0
        worker1.get_latest_start_time.return_value = now - timedelta(hours=2)
        worker1.get_heartbeat_timestamp.return_value = now - timedelta(minutes=1)

        worker2 = MagicMock(spec=PhysicalWorkerEntity)
        worker2.get_id.return_value = "phys2"
        worker2.get_name.return_value = "physical_worker2"
        worker2.get_host.return_value = "host2.example.com"
        worker2.get_pid.return_value = 54321
        worker2.get_status.return_value = WorkerStatus.STOPPED
        worker2.get_restart_count.return_value = 3
        worker2.get_latest_start_time.return_value = None
        worker2.get_heartbeat_timestamp.return_value = None

        worker3 = MagicMock(spec=PhysicalWorkerEntity)
        worker3.get_id.return_value = "phys3"
        worker3.get_name.return_value = "physical_worker3"
        worker3.get_host.return_value = "host3.example.com"
        worker3.get_pid.return_value = 99999
        worker3.get_status.return_value = WorkerStatus.STALLED
        worker3.get_restart_count.return_value = 1
        worker3.get_latest_start_time.return_value = now - timedelta(hours=1)
        worker3.get_heartbeat_timestamp.return_value = now - timedelta(minutes=30)

        return [worker1, worker2, worker3]

    @pytest.fixture
    def mock_db(self) -> MagicMock:
        """
        Create a mock MerlinDatabase for testing.

        Returns:
            Mock MerlinDatabase instance.
        """
        return MagicMock(spec=MerlinDatabase)

    @pytest.fixture
    def sample_stats(self) -> Dict[str, int]:
        """
        Create sample worker statistics for testing.

        Returns:
            Dictionary containing sample worker statistics.
        """
        return {
            "total_logical": 3,
            "logical_with_instances": 2,
            "logical_without_instances": 1,
            "total_physical": 3,
            "physical_running": 1,
            "physical_stopped": 1,
            "physical_stalled": 1,
            "physical_rebooting": 0,
        }

    def test_format_and_display_basic_structure(
        self,
        formatter: JSONWorkerFormatter,
        mock_logical_workers: List[MagicMock],
        mock_physical_workers: List[MagicMock],
        mock_db: MagicMock,
        sample_stats: Dict[str, int],
    ):
        """
        Test that format_and_display outputs valid JSON with correct basic structure.

        Args:
            formatter: JSONWorkerFormatter instance for testing.
            mock_logical_workers: List of mock logical worker entities.
            mock_physical_workers: List of mock physical worker entities.
            mock_db: Mock MerlinDatabase instance.
            sample_stats: Sample worker statistics dictionary.
        """
        # Setup database to return physical workers
        mock_db.get.side_effect = mock_physical_workers

        # Mock get_worker_statistics method
        formatter.get_worker_statistics = MagicMock(return_value=sample_stats)

        filters = {"queues": ["queue1"], "name": ["worker1"]}
        formatter.format_and_display(mock_logical_workers, filters, mock_db)

        # Get the JSON output from the mocked console.print call
        formatter.console.print.assert_called_once()
        json_output = formatter.console.print.call_args[0][0]
        data = json.loads(json_output)

        # Verify top-level structure
        assert "filters" in data
        assert "timestamp" in data
        assert "logical_workers" in data
        assert "summary" in data

        # Verify filters are preserved
        assert data["filters"] == filters

        # Verify timestamp is ISO format
        datetime.fromisoformat(data["timestamp"])  # Should not raise exception

        # Verify summary matches expected stats
        assert data["summary"] == sample_stats

        # Verify logical workers array exists
        assert isinstance(data["logical_workers"], list)

    def test_format_and_display_with_filters(
        self,
        formatter: JSONWorkerFormatter,
        mock_logical_workers: List[MagicMock],
        mock_physical_workers: List[MagicMock],
        mock_db: MagicMock,
        sample_stats: Dict[str, int],
    ):
        """
        Test JSON output includes filters correctly.

        Args:
            formatter: JSONWorkerFormatter instance for testing.
            mock_logical_workers: List of mock logical worker entities.
            mock_physical_workers: List of mock physical worker entities.
            mock_db: Mock MerlinDatabase instance.
            sample_stats: Sample worker statistics dictionary.
        """
        mock_db.get.side_effect = mock_physical_workers
        formatter.get_worker_statistics = MagicMock(return_value=sample_stats)

        filters = {"queues": ["queue1", "queue2"], "name": ["worker_a", "worker_b"]}
        formatter.format_and_display(mock_logical_workers, filters, mock_db)

        # Get the JSON output from the mocked console.print call
        formatter.console.print.assert_called_once()
        json_output = formatter.console.print.call_args[0][0]
        data = json.loads(json_output)

        assert data["filters"]["queues"] == ["queue1", "queue2"]
        assert data["filters"]["name"] == ["worker_a", "worker_b"]

    def test_format_and_display_with_empty_filters(
        self,
        formatter: JSONWorkerFormatter,
        mock_logical_workers: List[MagicMock],
        mock_physical_workers: List[MagicMock],
        mock_db: MagicMock,
        sample_stats: Dict[str, int],
    ):
        """
        Test JSON output with empty filters.

        Args:
            formatter: JSONWorkerFormatter instance for testing.
            mock_logical_workers: List of mock logical worker entities.
            mock_physical_workers: List of mock physical worker entities.
            mock_db: Mock MerlinDatabase instance.
            sample_stats: Sample worker statistics dictionary.
        """
        mock_db.get.side_effect = mock_physical_workers
        formatter.get_worker_statistics = MagicMock(return_value=sample_stats)

        filters = {}
        formatter.format_and_display(mock_logical_workers, filters, mock_db)

        # Get the JSON output from the mocked console.print call
        formatter.console.print.assert_called_once()
        json_output = formatter.console.print.call_args[0][0]
        data = json.loads(json_output)

        assert data["filters"] == {}
