##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `merlin/workers/formatters/rich_formatter.py` module.
"""

from datetime import datetime, timedelta
from typing import List, Union
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture
from rich.text import Text

from merlin.common.enums import WorkerStatus
from merlin.db_scripts.entities.logical_worker_entity import LogicalWorkerEntity
from merlin.db_scripts.entities.physical_worker_entity import PhysicalWorkerEntity
from merlin.db_scripts.merlin_db import MerlinDatabase
from merlin.workers.formatters.rich_formatter import (
    ColumnConfig,
    LayoutConfig,
    LayoutSize,
    ResponsiveLayoutManager,
    RichWorkerFormatter,
)


class TestLayoutSize:
    """Tests for the LayoutSize enum."""

    def test_layout_size_values(self):
        """Test that LayoutSize enum has expected values."""
        assert LayoutSize.COMPACT.value == "compact"
        assert LayoutSize.NARROW.value == "narrow"
        assert LayoutSize.MEDIUM.value == "medium"
        assert LayoutSize.WIDE.value == "wide"


class TestColumnConfig:
    """Tests for the ColumnConfig dataclass."""

    def test_column_config_defaults(self):
        """Test that ColumnConfig has correct default values."""
        config = ColumnConfig("test_key", "Test Title")

        assert config.key == "test_key"
        assert config.title == "Test Title"
        assert config.style == "white"
        assert config.width is None
        assert config.max_width is None
        assert config.justify == "left"
        assert config.no_wrap is False
        assert config.formatter is None

    def test_column_config_custom_values(self):
        """Test that ColumnConfig accepts custom values."""

        def formatter(x):
            return str(x).upper()

        config = ColumnConfig(
            key="custom_key",
            title="Custom Title",
            style="bold red",
            width=10,
            max_width=20,
            justify="center",
            no_wrap=True,
            formatter=formatter,
        )

        assert config.key == "custom_key"
        assert config.title == "Custom Title"
        assert config.style == "bold red"
        assert config.width == 10
        assert config.max_width == 20
        assert config.justify == "center"
        assert config.no_wrap is True
        assert config.formatter == formatter


class TestLayoutConfig:
    """Tests for the LayoutConfig dataclass."""

    def test_layout_config_defaults(self):
        """Test that LayoutConfig has correct default values."""
        config = LayoutConfig(LayoutSize.MEDIUM)

        assert config.size == LayoutSize.MEDIUM
        assert config.show_summary_panels is True
        assert config.panels_horizontal is True
        assert config.physical_worker_columns is None
        assert config.logical_worker_columns is None
        assert config.use_compact_view is False


class TestResponsiveLayoutManager:
    """Tests for the ResponsiveLayoutManager class."""

    @pytest.fixture
    def layout_manager(self) -> ResponsiveLayoutManager:
        """
        Create a ResponsiveLayoutManager instance for testing.

        Returns:
            ResponsiveLayoutManager instance.
        """
        return ResponsiveLayoutManager()

    @pytest.mark.parametrize(
        "width, expected_size",
        [
            (30, LayoutSize.COMPACT),  # Compact
            (59, LayoutSize.COMPACT),
            (60, LayoutSize.NARROW),  # Narrow
            (70, LayoutSize.NARROW),
            (79, LayoutSize.NARROW),
            (80, LayoutSize.MEDIUM),  # Medium
            (100, LayoutSize.MEDIUM),
            (119, LayoutSize.MEDIUM),
            (120, LayoutSize.WIDE),  # Wide
            (150, LayoutSize.WIDE),
            (200, LayoutSize.WIDE),
        ],
    )
    def test_get_layout_size(self, layout_manager: ResponsiveLayoutManager, width: int, expected_size: LayoutSize):
        """
        Test that get_layout_size returns the correct layout size.

        Args:
            layout_manager: ResponsiveLayoutManager instance.
            width: Terminal width to test.
            expected_size: Expected LayoutSize result.
        """
        assert layout_manager.get_layout_size(width) == expected_size

    def test_get_layout_config_returns_correct_config(self, layout_manager: ResponsiveLayoutManager):
        """
        Test that get_layout_config returns the correct LayoutConfig.

        Args:
            layout_manager: ResponsiveLayoutManager instance.
        """
        config = layout_manager.get_layout_config(100)
        assert config.size == LayoutSize.MEDIUM
        assert isinstance(config, LayoutConfig)

    @pytest.mark.parametrize(
        "status, expected_icon, expected_text",
        [
            (WorkerStatus.RUNNING, "✓", "RUNNING"),
            (WorkerStatus.STOPPED, "✗", "STOPPED"),
            (WorkerStatus.STALLED, "⚠", "STALLED"),
            (WorkerStatus.REBOOTING, "↻", "REBOOTING"),
        ],
    )
    def test_format_status(
        self, layout_manager: ResponsiveLayoutManager, status: WorkerStatus, expected_icon: str, expected_text: str
    ):
        """
        Test that various worker statuses are formatted correctly.

        Args:
            layout_manager: ResponsiveLayoutManager instance.
            status: WorkerStatus to format.
            expected_icon: Expected icon in the formatted output.
            expected_text: Expected text in the formatted output.
        """
        formatted = layout_manager._format_status(status)
        assert isinstance(formatted, Text)
        assert expected_icon in str(formatted)
        assert expected_text in str(formatted)


class TestRichWorkerFormatter:
    """Tests for the RichWorkerFormatter class."""

    @pytest.fixture
    def formatter(self, mocker: MockerFixture) -> RichWorkerFormatter:
        """
        Create a RichWorkerFormatter instance for testing.

        Args:
            mocker: Pytest mocker fixture.

        Returns:
            RichWorkerFormatter instance.
        """
        # Mock the console to control its behavior
        mock_console = MagicMock()
        mock_console.size.width = 120  # Default to wide layout
        mocker.patch("merlin.workers.formatters.worker_formatter.Console", return_value=mock_console)
        return RichWorkerFormatter()

    @pytest.fixture
    def mock_logical_workers(self) -> List[MagicMock]:
        """
        Create mock logical worker entities for testing.

        Returns:
            List of mock LogicalWorkerEntity instances.
        """
        worker1 = MagicMock(spec=LogicalWorkerEntity)
        worker1.get_name.return_value = "logical_worker1"
        worker1.get_queues.return_value = ["queue1", "queue2"]
        worker1.get_physical_workers.return_value = ["phys1", "phys2"]

        worker2 = MagicMock(spec=LogicalWorkerEntity)
        worker2.get_name.return_value = "logical_worker2"
        worker2.get_queues.return_value = ["queue3"]
        worker2.get_physical_workers.return_value = []  # No physical workers

        return [worker1, worker2]

    @pytest.fixture
    def mock_physical_workers(self) -> List[MagicMock]:
        """
        Create mock physical worker entities for testing.

        Returns:
            List of mock PhysicalWorkerEntity instances.
        """
        worker1 = MagicMock(spec=PhysicalWorkerEntity)
        worker1.get_id.return_value = "phys1"
        worker1.get_name.return_value = "physical_worker1"
        worker1.get_host.return_value = "host1"
        worker1.get_pid.return_value = 12345
        worker1.get_status.return_value = WorkerStatus.RUNNING
        worker1.get_restart_count.return_value = 0
        worker1.get_latest_start_time.return_value = datetime.now() - timedelta(hours=2)
        worker1.get_heartbeat_timestamp.return_value = datetime.now() - timedelta(minutes=1)

        worker2 = MagicMock(spec=PhysicalWorkerEntity)
        worker2.get_id.return_value = "phys2"
        worker2.get_name.return_value = "physical_worker2"
        worker2.get_host.return_value = "host2"
        worker2.get_pid.return_value = 54321
        worker2.get_status.return_value = WorkerStatus.STOPPED
        worker2.get_restart_count.return_value = 2
        worker2.get_latest_start_time.return_value = None
        worker2.get_heartbeat_timestamp.return_value = None

        return [worker1, worker2]

    @pytest.fixture
    def mock_db(self) -> MagicMock:
        """
        Create a mock MerlinDatabase for testing.

        Returns:
            Mock MerlinDatabase instance.
        """
        return MagicMock(spec=MerlinDatabase)

    def test_get_queues_str_removes_merlin_prefix(self, formatter: RichWorkerFormatter):
        """
        Test that _get_queues_str removes [merlin]_ prefix correctly.

        Args:
            formatter: RichWorkerFormatter instance.
        """
        queues = ["[merlin]_queue1", "[merlin]_queue2", "custom_queue"]
        result = formatter._get_queues_str(queues)
        assert result == "custom_queue, queue1, queue2"

    def test_get_queues_str_sorts_queues(self, formatter: RichWorkerFormatter):
        """
        Test that _get_queues_str sorts queue names.

        Args:
            formatter: RichWorkerFormatter instance.
        """
        queues = ["[merlin]_zebra", "[merlin]_alpha", "[merlin]_beta"]
        result = formatter._get_queues_str(queues)
        assert result == "alpha, beta, zebra"

    @pytest.mark.parametrize(
        "status, expected_icon, expected_color",
        [
            (WorkerStatus.RUNNING, "✓", "green"),
            (WorkerStatus.STOPPED, "✗", "red"),
            (WorkerStatus.STALLED, "⚠", "yellow"),
            (WorkerStatus.REBOOTING, "↻", "cyan"),
            ("unknown", "?", "white"),
        ],
    )
    def test_format_status(
        self, formatter: RichWorkerFormatter, status: Union[WorkerStatus, str], expected_icon: str, expected_color: str
    ):
        """
        Test that _format_status returns Rich Text with icons.

        Args:
            formatter: RichWorkerFormatter instance.
            status: WorkerStatus or string to format.
            expected_icon: Expected icon in the formatted output.
            expected_color: Expected color in the formatted output.
        """
        formatted = formatter._format_status(status)
        assert isinstance(formatted, Text)
        assert expected_icon in str(formatted)
        assert expected_color in formatted.style

        if isinstance(status, WorkerStatus):
            assert status.name in str(formatted)
        else:
            assert status in str(formatted)

    @pytest.mark.parametrize(
        "duration, expected",
        [
            (timedelta(days=2, hours=5), "2d 5h 0m"),
            (timedelta(days=1, hours=1, minutes=1), "1d 1h 1m"),
            (timedelta(hours=3, minutes=30), "3h 30m"),
            (timedelta(minutes=45), "45m"),
            (timedelta(seconds=30), "30s"),
        ],
    )
    def test_format_time_duration(self, formatter: RichWorkerFormatter, duration: timedelta, expected: str):
        """
        Test time duration formatting.

        Args:
            formatter: RichWorkerFormatter instance.
            duration: timedelta to format.
            expected: Expected formatted string.
        """
        result = formatter._format_time_duration(duration)
        assert result == expected

    @pytest.mark.parametrize(
        "timestamp, expected",
        [
            (datetime.now() - timedelta(seconds=30), "Just now"),
            (datetime.now() - timedelta(minutes=10), "10m ago"),
            (datetime.now() - timedelta(hours=2), "2h ago"),
            (None, "-"),
        ],
    )
    def test_format_last_heartbeat(self, formatter: RichWorkerFormatter, timestamp: datetime, expected: str):
        """
        Test heartbeat formatting for very recent heartbeats.

        Args:
            formatter: RichWorkerFormatter instance.
            timestamp: datetime of the last heartbeat.
            expected: Expected formatted string.
        """
        result = formatter._format_last_heartbeat(timestamp)
        assert isinstance(result, Text)
        assert expected in str(result)

    @pytest.mark.parametrize(
        "status, timestamp, expected",
        [
            (WorkerStatus.RUNNING, datetime.now() - timedelta(hours=1, minutes=30), "1h 30m"),
            (WorkerStatus.RUNNING, None, "-"),
        ],
    )
    def test_format_uptime_or_downtime_running_worker(
        self, formatter: RichWorkerFormatter, status: WorkerStatus, timestamp: timedelta, expected: str
    ):
        """
        Test uptime or downtime formatting.

        Args:
            formatter: RichWorkerFormatter instance.
            status: WorkerStatus of the worker.
            timestamp: datetime of the latest start time.
            expected: Expected formatted string.
        """
        mock_worker = MagicMock()
        mock_worker.get_status.return_value = status
        mock_worker.get_latest_start_time.return_value = timestamp

        result = formatter._format_uptime_or_downtime(mock_worker)
        assert result == expected

    @pytest.mark.parametrize(
        "status, timestamp, expected",
        [
            (WorkerStatus.STOPPED, datetime.now() - timedelta(minutes=15), "down 15m"),
            (WorkerStatus.STOPPED, None, "stopped"),
        ],
    )
    def test_format_uptime_or_downtime_stopped_worker(
        self, formatter: RichWorkerFormatter, status: WorkerStatus, timestamp: timedelta, expected: str
    ):
        """
        Test uptime or downtime formatting.

        Args:
            formatter: RichWorkerFormatter instance.
            status: WorkerStatus of the worker.
            timestamp: datetime of the stop time.
            expected: Expected formatted string.
        """
        mock_worker = MagicMock()
        mock_worker.get_status.return_value = status
        mock_worker.get_stop_time.return_value = timestamp

        result = formatter._format_uptime_or_downtime(mock_worker)
        assert result == expected

    def test_get_physical_worker_data(
        self,
        formatter: RichWorkerFormatter,
        mock_logical_workers: List[MagicMock],
        mock_physical_workers: List[MagicMock],
        mock_db: MagicMock,
    ):
        """
        Test extraction of physical worker data for table display.

        Args:
            formatter: RichWorkerFormatter instance.
            mock_logical_workers: List of mock LogicalWorkerEntity instances.
            mock_physical_workers: List of mock PhysicalWorkerEntity instances.
            mock_db: Mock MerlinDatabase instance.
        """
        # Setup database to return physical workers
        mock_db.get.side_effect = mock_physical_workers

        data = formatter._get_physical_worker_data(mock_logical_workers, mock_db)

        assert len(data) == 2  # 2 physical workers for first logical, 0 for second logical
        assert data[0]["worker"] == "logical_worker1"
        assert data[0]["host"] == "host1"
        assert data[0]["pid"] == "12345"
        assert data[0]["status"] == WorkerStatus.RUNNING

    def test_get_logical_workers_without_instances_data(
        self, formatter: RichWorkerFormatter, mock_logical_workers: List[MagicMock]
    ):
        """
        Test extraction of logical workers without physical instances.

        Args:
            formatter: RichWorkerFormatter instance.
            mock_logical_workers: List of mock LogicalWorkerEntity instances.
        """
        data = formatter._get_logical_workers_without_instances_data(mock_logical_workers)

        # Only logical_worker2 has no physical workers
        assert len(data) == 1
        assert data[0]["worker"] == "logical_worker2"
        assert data[0]["queues"] == "queue3"
        assert isinstance(data[0]["status"], Text)
        assert "NO INSTANCES" in str(data[0]["status"])

    def test_sort_physical_workers(self, formatter: RichWorkerFormatter):
        """
        Test that physical workers are sorted correctly.

        Args:
            formatter: RichWorkerFormatter instance.
        """
        data = [
            {"_sort_status": "STOPPED", "worker": "worker2", "instance": "inst2"},
            {"_sort_status": "RUNNING", "worker": "worker1", "instance": "inst1"},
            {"_sort_status": "RUNNING", "worker": "worker1", "instance": "inst2"},
            {"_sort_status": "STOPPED", "worker": "worker1", "instance": "inst1"},
        ]

        sorted_data = formatter._sort_physical_workers(data)

        # Running workers should come first, then sorted by worker and instance name
        assert sorted_data[0]["_sort_status"] == "RUNNING"
        assert sorted_data[1]["_sort_status"] == "RUNNING"
        assert sorted_data[2]["_sort_status"] == "STOPPED"
        assert sorted_data[3]["_sort_status"] == "STOPPED"

    def test_build_summary_panels_with_filters(self, formatter: RichWorkerFormatter):
        """
        Test building summary panels with filters applied.

        Args:
            formatter: RichWorkerFormatter instance.
        """
        stats = {
            "total_logical": 5,
            "logical_with_instances": 3,
            "logical_without_instances": 2,
            "total_physical": 8,
            "physical_running": 6,
            "physical_stopped": 2,
            "physical_stalled": 0,
            "physical_rebooting": 0,
        }
        filters = {"queues": ["queue1", "queue2"], "name": ["worker1", "worker2"]}

        panels = formatter._build_summary_panels(stats, filters)

        assert len(panels) == 3  # Filter, Logical, Physical panels
        # Check that filter panel contains expected content
        filter_panel_content = panels[0]
        assert "queue1, queue2" in filter_panel_content.renderable
        assert "worker1, worker2" in filter_panel_content.renderable

    def test_build_summary_panels_no_filters(self, formatter: RichWorkerFormatter):
        """
        Test building summary panels without filters.

        Args:
            formatter: RichWorkerFormatter instance.
        """
        stats = {
            "total_logical": 3,
            "logical_with_instances": 2,
            "logical_without_instances": 1,
            "total_physical": 0,
            "physical_running": 0,
            "physical_stopped": 0,
            "physical_stalled": 0,
            "physical_rebooting": 0,
        }
        filters = {}

        panels = formatter._build_summary_panels(stats, filters)

        # No filter panel, only logical panel (no physical since total is 0)
        assert len(panels) == 1

    def test_build_compact_view(
        self,
        formatter: RichWorkerFormatter,
        mock_logical_workers: List[MagicMock],
        mock_physical_workers: List[MagicMock],
        mock_db: MagicMock,
    ):
        """
        Test building compact view for narrow terminals.

        Args:
            formatter: RichWorkerFormatter instance.
            mock_logical_workers: List of mock LogicalWorkerEntity instances.
            mock_physical_workers: List of mock PhysicalWorkerEntity instances.
            mock_db: Mock MerlinDatabase instance.
        """
        mock_db.get.side_effect = mock_physical_workers

        compact_view = formatter._build_compact_view(mock_logical_workers, mock_db)

        assert "logical_worker1" in compact_view
        assert "logical_worker2" in compact_view
        assert "NO INSTANCES" in compact_view
        assert "host1" in compact_view

    def test_build_responsive_table(self, formatter: RichWorkerFormatter):
        """
        Test building a responsive table with column configuration.

        Args:
            formatter: RichWorkerFormatter instance.
        """
        columns = [
            ColumnConfig(key="name", title="Name", style="bold white"),
            ColumnConfig(key="status", title="Status", style="green", formatter=lambda x: f"[{x}]"),
        ]
        data = [{"name": "worker1", "status": "running"}, {"name": "worker2", "status": "stopped"}]

        table = formatter._build_responsive_table("Test Table", columns, data)

        assert table.title == "Test Table"
        assert len(table.columns) == 2

    def test_format_and_display_compact_view(
        self, mocker: MockerFixture, mock_logical_workers: List[MagicMock], mock_db: MagicMock
    ):
        """
        Test format_and_display uses compact view for narrow terminals.

        Args:
            mocker: Pytest mocker fixture.
            mock_logical_workers: List of mock LogicalWorkerEntity instances.
            mock_db: Mock MerlinDatabase instance.
        """
        # Create formatter with narrow console
        mock_console = MagicMock()
        mock_console.size.width = 40  # Compact layout
        mocker.patch("merlin.workers.formatters.worker_formatter.Console", return_value=mock_console)
        formatter = RichWorkerFormatter()

        # Mock get_worker_statistics
        stats = {
            "total_logical": 2,
            "total_physical": 2,
            "logical_with_instances": 1,
            "logical_without_instances": 1,
            "physical_running": 1,
            "physical_stopped": 1,
            "physical_stalled": 0,
            "physical_rebooting": 0,
        }
        mocker.patch.object(formatter, "get_worker_statistics", return_value=stats)

        filters = {}
        formatter.format_and_display(mock_logical_workers, filters, mock_db)

        # Should call _display_compact_view instead of normal tables
        assert mock_console.print.called

    def test_format_and_display_normal_view(
        self,
        mocker: MockerFixture,
        mock_logical_workers: List[MagicMock],
        mock_physical_workers: List[MagicMock],
        mock_db: MagicMock,
    ):
        """
        Test format_and_display uses normal view for wide terminals.

        Args:
            mocker: Pytest mocker fixture.
            mock_logical_workers: List of mock LogicalWorkerEntity instances.
            mock_physical_workers: List of mock PhysicalWorkerEntity instances.
            mock_db: Mock MerlinDatabase instance.
        """
        # Create formatter with wide console
        mock_console = MagicMock()
        mock_console.size.width = 150  # Wide layout
        mocker.patch("merlin.workers.formatters.worker_formatter.Console", return_value=mock_console)
        formatter = RichWorkerFormatter()

        # Mock database to return physical workers
        mock_db.get.side_effect = mock_physical_workers

        # Mock get_worker_statistics
        stats = {
            "total_logical": 2,
            "total_physical": 2,
            "logical_with_instances": 1,
            "logical_without_instances": 1,
            "physical_running": 1,
            "physical_stopped": 1,
            "physical_stalled": 0,
            "physical_rebooting": 0,
        }
        mocker.patch.object(formatter, "get_worker_statistics", return_value=stats)

        filters = {"queues": ["queue1"]}
        formatter.format_and_display(mock_logical_workers, filters, mock_db)

        # Should display summary panels and tables
        assert mock_console.print.called
        # Should be called multiple times (empty lines, panels, tables)
        assert mock_console.print.call_count > 3

    def test_display_summary_panels_horizontal(self, mocker: MockerFixture, formatter: RichWorkerFormatter):
        """
        Test that summary panels are displayed horizontally when configured.

        Args:
            mocker: Pytest mocker fixture.
            formatter: RichWorkerFormatter instance.
        """
        mock_columns = mocker.patch("merlin.workers.formatters.rich_formatter.Columns")
        stats = {
            "total_logical": 1,
            "logical_with_instances": 1,
            "logical_without_instances": 0,
            "total_physical": 1,
            "physical_running": 0,
            "physical_stopped": 1,
            "physical_stalled": 0,
            "physical_rebooting": 0,
        }
        filters = {}
        layout_config = LayoutConfig(LayoutSize.WIDE, panels_horizontal=True)

        formatter._display_summary_panels(stats, filters, layout_config)

        # Should create Columns object for horizontal layout
        mock_columns.assert_called_once()

    def test_display_summary_panels_vertical(self, mocker: MockerFixture, formatter: RichWorkerFormatter):
        """
        Test that summary panels are displayed vertically when configured.

        Args:
            mocker: Pytest mocker fixture.
            formatter: RichWorkerFormatter instance.
        """
        stats = {
            "total_logical": 1,
            "logical_with_instances": 0,
            "logical_without_instances": 1,
            "total_physical": 0,
            "physical_running": 0,
            "physical_stopped": 0,
            "physical_stalled": 0,
            "physical_rebooting": 0,
        }
        filters = {}
        layout_config = LayoutConfig(LayoutSize.NARROW, panels_horizontal=False)

        # Mock console.print to track calls
        mock_print = mocker.patch.object(formatter.console, "print")

        formatter._display_summary_panels(stats, filters, layout_config)

        # Should print each panel individually (not using Columns)
        assert mock_print.called

    def test_display_compact_view(self, mocker: MockerFixture, formatter: RichWorkerFormatter):
        """
        Test display of compact view.

        Args:
            mocker: Pytest mocker fixture.
            formatter: RichWorkerFormatter instance.
        """
        compact_view = "Test compact view content"
        filters = {"queues": ["queue1"]}
        stats = {
            "total_logical": 1,
            "logical_with_instances": 1,
            "logical_without_instances": 0,
            "total_physical": 2,
            "physical_running": 1,
            "physical_stopped": 1,
            "physical_stalled": 0,
            "physical_rebooting": 0,
        }

        mock_print = mocker.patch.object(formatter.console, "print")

        formatter._display_compact_view(compact_view, filters, stats)

        # Should print title, filters, summary, and compact view
        assert mock_print.call_count == 4

        # Check that filter information was included in one of the print calls
        print_args = [str(call[0][0]) for call in mock_print.call_args_list]
        filter_found = any("queue1" in arg for arg in print_args)
        assert filter_found
