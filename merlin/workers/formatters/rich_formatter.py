##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Rich-based worker formatter with responsive layout for Merlin.

This module provides classes and utilities to format and display
logical and physical worker information in a terminal using Rich.
It adapts automatically to different terminal widths, providing
compact views for narrow screens and detailed tables and panels
for wider screens. Key features include:

- Responsive layouts: Adjusts tables and panels based on terminal width.
- Summary panels: Displays aggregated statistics for logical and
  physical workers, including running, stopped, stalled, and rebooting counts.
- Compact view: Condensed text output for narrow terminals where full tables
  would not fit.
- Rich styling: Uses colors, icons, and text formatting to make worker
  statuses and information visually distinct.

Classes:
    LayoutSize: Enum defining terminal width categories for responsive layouts.
    ColumnConfig: Dataclass defining display properties for individual table columns.
    LayoutConfig: Dataclass defining full layout configuration for a given size.
    ResponsiveLayoutManager: Selects and provides layout configurations based
        on terminal width.
    RichWorkerFormatter: Formats and renders worker information using Rich
        components with responsive layouts.

This module depends on the Merlin database interface and worker entities
([`LogicalWorkerEntity`][db_scripts.entities.logical_worker_entity.LogicalWorkerEntity]
and [`PhysicalWorkerEntity`][db_scripts.entities.physical_worker_entity.PhysicalWorkerEntity])
to retrieve and display worker information.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from rich.columns import Columns
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from merlin.common.enums import WorkerStatus
from merlin.db_scripts.entities.logical_worker_entity import LogicalWorkerEntity
from merlin.db_scripts.entities.physical_worker_entity import PhysicalWorkerEntity
from merlin.db_scripts.merlin_db import MerlinDatabase
from merlin.workers.formatters.worker_formatter import WorkerFormatter


class LayoutSize(Enum):
    """
    Enumeration of terminal width categories for responsive layouts.

    This enum is used to adapt table and panel rendering based on
    the current terminal width, enabling responsive designs that
    remain readable across narrow and wide displays.

    Attributes:
        COMPACT (str): Very small terminals (< 60 characters wide).
        NARROW (str): Narrow terminals (60-79 characters wide).
        MEDIUM (str): Standard terminals (80-119 characters wide).
        WIDE (str): Wide terminals (>= 120 characters wide).
    """

    COMPACT = "compact"  # < 60 chars
    NARROW = "narrow"  # 60-79 chars
    MEDIUM = "medium"  # 80-119 chars
    WIDE = "wide"  # >= 120 chars


@dataclass
class ColumnConfig:  # pylint: disable=too-many-instance-attributes
    """
    Configuration for an individual table column.

    Defines how a column should be displayed within a table,
    including its label, alignment, width constraints, and
    optional formatting logic.

    Attributes:
        key (str): The field or attribute name mapped to this column.
        title (str): The display name of the column header.
        style (str): Text style to apply (e.g., "white", "bold red").
        width (Optional[int]): Fixed width of the column, if specified.
        max_width (Optional[int]): Maximum allowed width before truncation or wrapping.
        justify (str): Text alignment ("left", "center", or "right").
        no_wrap (bool): Whether to prevent text wrapping in this column.
        formatter (Optional[Callable[[Any], Any]]): Optional callable to transform cell
            values before display.
    """

    key: str
    title: str
    style: str = "white"
    width: Optional[int] = None
    max_width: Optional[int] = None
    justify: str = "left"
    no_wrap: bool = False
    formatter: Optional[Callable[[Any], Any]] = None


@dataclass
class LayoutConfig:
    """
    Configuration for rendering worker information at a given layout size.

    Determines which elements (tables, panels) are displayed and how
    they are arranged, based on the current terminal width category.

    Attributes:
        size (workers.formatters.rich_formatter.LayoutSize): The layout
            size category.
        show_summary_panels (bool): Whether to display summary panels.
        panels_horizontal (bool): If True, display panels side-by-side
            (horizontal), otherwise stack them vertically.
        physical_worker_columns (List[workers.formatters.rich_formatter.ColumnConfig]):
            Column configurations for physical worker tables.
        logical_worker_columns (List[workers.formatters.rich_formatter.ColumnConfig]):
            Column configurations for logical worker tables.
        use_compact_view (bool): Whether to enable a simplified, space-saving
            view of worker information.
    """

    size: LayoutSize
    show_summary_panels: bool = True
    panels_horizontal: bool = True
    physical_worker_columns: List[ColumnConfig] = None
    logical_worker_columns: List[ColumnConfig] = None
    use_compact_view: bool = False


class ResponsiveLayoutManager:
    """
    Manage and provide responsive layout configurations for terminal output.

    This class adapts the display of worker information (both physical and logical)
    to different terminal widths. It selects column visibility, ordering, and
    formatting rules depending on the width category (compact, narrow, medium, wide).

    Attributes:
        layouts (Dict[workers.formatters.rich_formatter.LayoutSize, workers.formatters.rich_formatter.LayoutConfig]):
            A mapping of layout sizes to their respective configurations, including
            column definitions and display options.

    Methods:
        get_layout_size: Determine the appropriate layout size category based on terminal width.
        get_layout_config: Retrieve the full layout configuration object for a given terminal width.
        _format_status: Format a worker status into a styled Rich Text object with icons.
    """

    def __init__(self):
        """
        Initialize the responsive layout manager.

        Predefines layout configurations for all supported terminal width categories
        (compact, narrow, medium, wide). Each configuration specifies which columns
        should be shown for physical and logical worker tables, whether summary panels
        are displayed, and how they are arranged.
        """
        self.layouts = {
            LayoutSize.COMPACT: LayoutConfig(
                size=LayoutSize.COMPACT,
                show_summary_panels=False,
                use_compact_view=True,
                physical_worker_columns=[],
                logical_worker_columns=[],
            ),
            LayoutSize.NARROW: LayoutConfig(
                size=LayoutSize.NARROW,
                show_summary_panels=True,
                panels_horizontal=False,
                physical_worker_columns=[
                    ColumnConfig(key="worker", title="Worker", style="bold cyan", max_width=12),
                    ColumnConfig(key="host", title="Host", style="blue", max_width=10),
                    ColumnConfig(key="pid", title="PID", style="yellow", width=8, justify="right"),
                    ColumnConfig(key="status", title="Status", style="bold", width=10, formatter=self._format_status),
                ],
                logical_worker_columns=[
                    ColumnConfig(key="worker", title="Worker", style="bold white", max_width=20),
                    ColumnConfig(key="status", title="Status", style="bold red", width=12),
                ],
            ),
            LayoutSize.MEDIUM: LayoutConfig(
                size=LayoutSize.MEDIUM,
                show_summary_panels=True,
                panels_horizontal=True,
                physical_worker_columns=[
                    ColumnConfig(key="worker", title="Worker", style="bold cyan", max_width=15),
                    ColumnConfig(key="instance", title="Instance", style="bold magenta", max_width=25),
                    ColumnConfig(key="host", title="Host", style="blue", max_width=12),
                    ColumnConfig(key="pid", title="PID", style="yellow", width=8, justify="right"),
                    ColumnConfig(key="status", title="Status", style="bold", width=10, formatter=self._format_status),
                    ColumnConfig(key="runtime", title="Runtime", style="cyan", width=8),
                ],
                logical_worker_columns=[
                    ColumnConfig(key="worker", title="Worker Name", style="bold white", max_width=25),
                    ColumnConfig(key="queues", title="Queues", style="green", max_width=30, no_wrap=True),
                    ColumnConfig(key="status", title="Status", style="bold red", width=12),
                ],
            ),
            LayoutSize.WIDE: LayoutConfig(
                size=LayoutSize.WIDE,
                show_summary_panels=True,
                panels_horizontal=True,
                physical_worker_columns=[
                    ColumnConfig(key="worker", title="Logical Worker", style="bold cyan", max_width=15),
                    ColumnConfig(key="queues", title="Queues", style="green", max_width=20, no_wrap=True),
                    ColumnConfig(key="instance", title="Instance Name", style="bold magenta", max_width=30),
                    ColumnConfig(key="host", title="Host", style="blue", max_width=12),
                    ColumnConfig(key="pid", title="PID", style="yellow", width=8, justify="right"),
                    ColumnConfig(key="status", title="Status", style="bold", width=10, formatter=self._format_status),
                    ColumnConfig(key="runtime", title="Runtime", style="cyan", width=8),
                    ColumnConfig(key="heartbeat", title="Heartbeat", style="bright_blue", width=10),
                    ColumnConfig(key="restarts", title="Restarts", style="red", width=8, justify="right"),
                ],
                logical_worker_columns=[
                    ColumnConfig(key="worker", title="Worker Name", style="bold white", max_width=25),
                    ColumnConfig(key="queues", title="Queues", style="green", max_width=30, no_wrap=True),
                    ColumnConfig(key="status", title="Status", style="bold red", width=12),
                ],
            ),
        }

    def get_layout_size(self, width: int) -> LayoutSize:
        """
        Determine the layout size category for a given terminal width.

        Args:
            width (int): The terminal width in characters.

        Returns:
            The corresponding layout size category (COMPACT, NARROW, MEDIUM, or WIDE).
        """
        if width < 60:
            return LayoutSize.COMPACT
        if width < 80:
            return LayoutSize.NARROW
        if width < 120:
            return LayoutSize.MEDIUM
        return LayoutSize.WIDE

    def get_layout_config(self, width: int) -> LayoutConfig:
        """
        Retrieve the layout configuration for a given terminal width.

        Args:
            width (int): The terminal width in characters.

        Returns:
            The full layout configuration for the determined size,
                including column definitions and panel options.
        """
        size = self.get_layout_size(width)
        return self.layouts[size]

    def _format_status(self, status: WorkerStatus) -> Text:
        """
        Format a worker status value into a Rich `Text` object.

        Adds an icon and applies color styling to make worker statuses
        visually distinguishable in tables.

        Args:
            status (common.enums.WorkerStatus): The worker status value.

        Returns:
            A Rich Text object containing the styled status with an icon.
        """
        status_str = str(status).replace("WorkerStatus.", "")

        status_config = {
            "RUNNING": ("✓", "bold green"),
            "STALLED": ("⚠", "bold yellow"),
            "STOPPED": ("✗", "bold red"),
            "REBOOTING": ("↻", "bold cyan"),
        }

        icon, color = status_config.get(status_str.upper(), ("?", "white"))
        return Text(f"{icon} {status_str}", style=color)


class RichWorkerFormatter(WorkerFormatter):
    """
    Format and display worker information using Rich with responsive layouts.

    This class provides a Rich-based implementation of a worker formatter that
    adapts to terminal width. It uses responsive tables, panels, and compact
    views to display logical and physical worker information in a clear,
    visually rich way. Layouts are selected automatically based on terminal
    width using a [`ResponsiveLayoutManager`][workers.formatters.rich_formatter.ResponsiveLayoutManager].

    Attributes:
        console (rich.console.Console): A Rich Console object used for displaying
            output to the terminal.
        layout_manager (workers.formatters.rich_formatter.ResponsiveLayoutManager):
            The layout manager responsible for selecting column and panel
            configurations based on terminal width.

    Methods:
        format_and_display: Format and display worker information with responsive Rich components.
        get_worker_statistics: Compute summary statistics for logical and physical workers.
        _display_compact_view: Display a simplified worker summary for very narrow terminals.
        _display_summary_panels: Render summary panels (logical, physical, filters)
            depending on layout settings.
        _build_responsive_table: Construct a Rich table using the given column configuration and data.
        _get_physical_worker_data: Extract detailed physical worker data for table display.
        _get_logical_workers_without_instances_data: Extract logical workers that have no
            physical instances.
        _sort_physical_workers: Sort physical worker data by status (running first), then by
            worker and instance name.
        _format_status: Format a worker status with icons and Rich color highlighting.
        _format_uptime_or_downtime: Return uptime for running workers or downtime for stopped
            workers in human-readable format.
        _format_time_duration: Format a time duration into a human-readable string (e.g., "2h 15m").
        _format_last_heartbeat: Format last heartbeat timestamp with color coding based on recency.
        _build_summary_panels: Build summary panels showing filters, logical workers, and
            physical workers.
        _build_compact_view: Build a compact text-only view of worker status for very narrowterminals.
    """

    def __init__(self):
        """
        Initialize the Rich-based worker formatter.

        This constructor sets up the responsive layout manager
        that determines how worker data will be displayed based
        on the terminal width.
        """
        super().__init__()
        self.layout_manager = ResponsiveLayoutManager()

    def format_and_display(
        self,
        logical_workers: List[LogicalWorkerEntity],
        filters: Dict,
        merlin_db: MerlinDatabase,
    ):
        """
        Format and display worker information using Rich components.

        This method generates a responsive console output of worker
        statistics, tables, and summary panels. The output adapts
        to the current terminal width and user-defined filters.

        Args:
            logical_workers (List[db_scripts.entities.logical_worker_entity.LogicalWorkerEntity]):
                A list of logical worker objects to display and summarize.
            filters (Dict): Active filters applied to the display
                (e.g., by worker type or status).
            merlin_db (db_scripts.merlin_db.MerlinDatabase): Reference to
                the Merlin database, used to query worker-related data.
        """
        # Calculate statistics
        stats = self.get_worker_statistics(logical_workers, merlin_db)
        console_width = self.console.size.width
        layout_config = self.layout_manager.get_layout_config(console_width)

        self.console.print()  # Empty line

        # Handle compact view for very narrow terminals
        if layout_config.use_compact_view:
            compact_view = self._build_compact_view(logical_workers, merlin_db)
            self._display_compact_view(compact_view, filters, stats)
            return

        # Display summary panels
        if layout_config.show_summary_panels:
            self._display_summary_panels(stats, filters, layout_config)
            self.console.print()  # Empty line

        # Show physical workers table if any exist
        if stats["total_physical"] > 0:
            physical_table = self._build_responsive_table(
                "[bold magenta]Physical Worker Instances[/bold magenta]",
                layout_config.physical_worker_columns,
                self._get_physical_worker_data(logical_workers, merlin_db),
                self._sort_physical_workers,
            )
            self.console.print(physical_table)
            self.console.print()  # Empty line

        # Show logical workers without instances if any exist
        if stats["logical_without_instances"] > 0:
            no_instances_table = self._build_responsive_table(
                "[bold cyan]Logical Workers Without Instances[/bold cyan]",
                layout_config.logical_worker_columns,
                self._get_logical_workers_without_instances_data(logical_workers),
                lambda data: sorted(data, key=lambda x: x["worker"]),
            )
            self.console.print(no_instances_table)

    def _get_queues_str(self, queues: List[str]) -> str:
        """
        Given a list of queue names, remove the '[merlin]_' prefix and
        combine them into a comma-delimited string.

        Args:
            queues (List[str]): The list of queue names to combine.

        Returns:
            A comma-delimited string of queues without the '[merlin]_' prefix.
        """
        return ", ".join(q[len("[merlin]_") :] if q.startswith("[merlin]_") else q for q in sorted(queues))

    def _display_compact_view(
        self,
        compact_view: str,
        filters: Dict,
        stats: Dict[str, int],
    ):
        """
        Display a compact view of worker information for narrow terminals.

        This fallback view is used when the terminal width is too small
        to render full tables or summary panels. It prints a concise
        summary of worker status, applied filters, and worker details.

        Args:
            compact_view (str): Multi-line string representing the compact worker view.
            filters (Dict): Active filters applied to the display.
            stats (Dict[str, int]): Aggregated worker statistics (e.g., running counts).
        """
        self.console.print("[bold cyan]Worker Status[/bold cyan]")
        if filters:
            filter_text = " | ".join([f"{k}: {','.join(v)}" for k, v in filters.items()])
            self.console.print(f"[dim]Filters: {filter_text}[/dim]")

        self.console.print(
            f"[bold]Summary:[/bold] {stats['physical_running']}/{stats['total_physical']} "
            f"running, {stats['logical_without_instances']} logical workers without instances\n"
        )

        self.console.print(compact_view)

    def _display_summary_panels(self, stats: Dict[str, int], filters: Dict, layout_config: LayoutConfig):
        """
        Display summary panels based on the given layout configuration.

        This method renders one or more Rich panels summarizing worker statistics.
        The panels are built dynamically from the provided stats and filters, and
        displayed either horizontally in columns or vertically in sequence,
        depending on the layout configuration.

        Args:
            stats (Dict[str, int]):
                A dictionary of aggregated worker statistics to summarize.
            filters (Dict):
                A dictionary of filter criteria used to generate the summary content.
            layout_config (workers.formatters.rich_formatter.LayoutConfig):
                An object defining layout preferences (e.g., whether to
                arrange panels horizontally).
        """
        summary_panels = self._build_summary_panels(stats, filters)

        if layout_config.panels_horizontal:
            self.console.print(Columns(summary_panels, equal=True))
        else:
            for panel in summary_panels:
                self.console.print(panel)

    def _build_responsive_table(
        self, title: str, columns: List[ColumnConfig], data: List[Dict], sort_func: Callable = None
    ) -> Table:
        """
        Build and return a Rich table with responsive column configuration.

        This method creates a table with headers, applies styles and sizing rules
        based on column configuration, and optionally sorts the data before
        rendering. Each row of the table is constructed by extracting values
        from the input data and applying any specified formatters.

        Args:
            title (str):
                Title displayed above the table.
            columns (List[workers.formatters.rich_formatter.ColumnConfig]):
                A list of column configuration objects defining headers,
                widths, alignment, and formatting functions.
            data (List[Dict]):
                List of dictionaries containing the row data to display.
            sort_func (Optional[Callable]):
                A function to sort the data before rendering. Defaults to None.

        Returns:
            A fully constructed Rich Table object ready for display.
        """
        table = Table(
            show_header=True,
            header_style="bold white",
            title=title,
        )

        # Add columns based on configuration
        for col in columns:
            table.add_column(
                col.title, style=col.style, width=col.width, max_width=col.max_width, justify=col.justify, no_wrap=col.no_wrap
            )

        # Sort data if sort function provided
        if sort_func:
            data = sort_func(data)

        # Add rows
        for row_data in data:
            row_values = []
            for col in columns:
                value = row_data.get(col.key, "-")
                if col.formatter:
                    value = col.formatter(value)
                row_values.append(value)
            table.add_row(*row_values)

        return table

    def _get_physical_worker_data(self, logical_workers: List[LogicalWorkerEntity], merlin_db) -> List[Dict]:
        """
        Extract and format physical worker data for table display.

        This method retrieves all physical workers associated with the given
        logical workers, queries the database for their details, and formats
        the data into a list of dictionaries suitable for table rendering.
        Each entry includes identifiers, host, status, runtime, heartbeat,
        and restart count.

        Args:
            logical_workers (List[db_scripts.entities.logical_worker_entity.LogicalWorkerEntity]):
                A list of logical worker entities, each referencing one or more
                associated physical workers.
            merlin_db (db_scripts.merlin_db.MerlinDatabase):
                The database interface used to fetch physical worker entities.

        Returns:
            A list of dictionaries, where each dictionary contains:\n
                - worker (str): Logical worker name.
                - queues (str): Comma-separated queue names without "[merlin]_".
                - instance (str): Instance/worker name.
                - host (str): Hostname where the worker is running.
                - pid (str): Process ID of the worker, or "-" if unavailable.
                - status (common.enums.WorkerStatus): Raw worker status object (used for formatting).
                - runtime (str): Formatted uptime/downtime string.
                - heartbeat (str): Last heartbeat timestamp or "-".
                - restarts (str): Number of times the worker has restarted.
                - _sort_status (str): String representation of the status
                used for sorting.
        """
        data = []

        for logical_worker in logical_workers:
            worker_name = logical_worker.get_name()
            queues_str = self._get_queues_str(logical_worker.get_queues())

            physical_worker_ids = logical_worker.get_physical_workers()
            physical_workers = [merlin_db.get("physical_worker", pid) for pid in physical_worker_ids]

            for physical_worker in physical_workers:
                status = physical_worker.get_status()
                status_str = str(status).replace("WorkerStatus.", "")

                # Only show heartbeat for running workers
                heartbeat_text = "-"
                if status_str == "RUNNING":
                    heartbeat_text = str(self._format_last_heartbeat(physical_worker.get_heartbeat_timestamp()))

                instance_name = physical_worker.get_name() or "-"

                data.append(
                    {
                        "worker": worker_name,
                        "queues": queues_str,
                        "instance": instance_name,
                        "host": physical_worker.get_host() or "-",
                        "pid": str(physical_worker.get_pid()) if physical_worker.get_pid() else "-",
                        "status": status,  # Raw status for formatter
                        "runtime": self._format_uptime_or_downtime(physical_worker),
                        "heartbeat": heartbeat_text,
                        "restarts": str(physical_worker.get_restart_count()),
                        "_sort_status": status_str,  # For sorting
                    }
                )

        return data

    def _get_logical_workers_without_instances_data(self, logical_workers: List[LogicalWorkerEntity]) -> List[Dict]:
        """
        Extract logical workers that have no associated physical instances.

        This method identifies all logical workers that currently do not have
        any physical worker instances. It formats their data into a list of
        dictionaries suitable for table rendering, including a status field
        indicating "NO INSTANCES".

        Args:
            logical_workers (List[db_scripts.entities.logical_worker_entity.LogicalWorkerEntity]):
                A list of logical worker entities to check.

        Returns:
            A list of dictionaries with the following keys:\n
                - worker (str): Name of the logical worker.
                - queues (str): Comma-separated list of queues the worker is associated with.
                - status (Text): Rich-formatted status indicating no instances.
        """
        data = []

        for logical_worker in logical_workers:
            physical_worker_ids = logical_worker.get_physical_workers()
            if not physical_worker_ids:
                queues_str = self._get_queues_str(logical_worker.get_queues())

                data.append(
                    {
                        "worker": logical_worker.get_name(),
                        "queues": queues_str,
                        "status": Text("NO INSTANCES", style="bold red"),
                    }
                )

        return data

    def _sort_physical_workers(self, data: List[Dict]) -> List[Dict]:
        """
        Sort a list of physical worker data dictionaries for display.

        Sorting prioritizes workers that are currently running first,
        followed by sorting alphabetically by logical worker name and
        then by physical instance name.

        Args:
            data (List[Dict]): List of dictionaries containing physical
                worker data, including a '_sort_status' key.

        Returns:
            Sorted list of physical worker dictionaries.
        """
        return sorted(data, key=lambda row: (0 if row["_sort_status"] == "RUNNING" else 1, row["worker"], row["instance"]))

    def _format_status(self, status: WorkerStatus) -> Text:
        """
        Format a worker status into a Rich Text object with an icon and color.

        Converts the raw status into a human-readable string with an
        associated Unicode icon and color highlighting for easier visualization.

        Args:
            status (common.enums.WorkerStatus): Raw worker status object.

        Returns:
            Rich Text object combining an icon and colored status string.
                Status mapping:\n
                    - "RUNNING": ✓ green
                    - "STALLED": ⚠ yellow
                    - "STOPPED": ✗ red
                    - "REBOOTING": ↻ cyan
                    - Unknown: ? white
        """
        status_str = str(status).replace("WorkerStatus.", "")

        status_config = {
            "RUNNING": ("✓", "bold green"),
            "STALLED": ("⚠", "bold yellow"),
            "STOPPED": ("✗", "bold red"),
            "REBOOTING": ("↻", "bold cyan"),
        }

        icon, color = status_config.get(status_str.upper(), ("?", "white"))
        return Text(f"{icon} {status_str}", style=color)

    def _format_uptime_or_downtime(self, physical_worker: PhysicalWorkerEntity) -> str:
        """
        Format the uptime for running workers or downtime for stopped workers.

        For running workers, this method calculates the elapsed time since the
        latest start and returns a human-readable string. For stopped workers,
        it calculates the time since the stop event and prefixes it with "down".
        If no start or stop times are available, returns a placeholder string.

        Args:
            physical_worker (db_scripts.entities.physical_worker_entity.PhysicalWorkerEntity):
                The physical worker entity to calculate uptime/downtime for.

        Returns:
            Human-readable uptime or downtime string.
        """
        status = str(physical_worker.get_status()).replace("WorkerStatus.", "")

        if status == "RUNNING":
            start_time = physical_worker.get_latest_start_time()
            if start_time:
                uptime = datetime.now() - start_time
                return self._format_time_duration(uptime)
        else:
            stop_time = getattr(physical_worker, "get_stop_time", lambda: None)()
            if stop_time:
                downtime = datetime.now() - stop_time
                return f"down {self._format_time_duration(downtime)}"
            return "stopped"

        return "-"

    def _format_time_duration(self, duration: timedelta) -> str:
        """
        Convert a timedelta into a compact, human-readable string.

        Formats the duration using days, hours, minutes, and seconds in a
        concise format suitable for table display.

        Args:
            duration (timedelta): Time duration to format.

        Returns:
            Formatted duration string.
        """
        if duration.days > 0:
            return f"{duration.days}d {duration.seconds // 3600}h"
        if duration.seconds >= 3600:
            return f"{duration.seconds // 3600}h {(duration.seconds % 3600) // 60}m"
        if duration.seconds >= 60:
            return f"{duration.seconds // 60}m"
        return f"{duration.seconds}s"

    def _format_last_heartbeat(self, heartbeat_timestamp: datetime) -> Text:
        """
        Format the last heartbeat timestamp with color coding based on recency.

        Displays a human-readable time difference between the current time
        and the last heartbeat. Uses color to indicate how recent the heartbeat was.

        Args:
            heartbeat_timestamp (datetime): Timestamp of the last heartbeat.

        Returns:
            Rich Text object containing the formatted heartbeat.
        """
        if not heartbeat_timestamp:
            return Text("-", style="dim")

        time_diff = datetime.now() - heartbeat_timestamp

        if time_diff < timedelta(minutes=1):  # Less than 1 minute
            return Text("Just now", style="green")

        if time_diff.total_seconds() < 3600:  # Less than 1 hour
            minutes = int(time_diff.total_seconds() // 60)
            if minutes < 5:
                return Text(f"{minutes}m ago", style="yellow")
            return Text(f"{minutes}m ago", style="orange3")

        # More than 1 hour
        hours = int(time_diff.total_seconds() // 3600)
        return Text(f"{hours}h ago", style="red")

    def _build_summary_panels(self, stats: Dict[str, int], filters: Dict) -> List[Panel]:
        """
        Build Rich summary panels displaying worker statistics and applied filters.

        Creates panels for:\n
        - Applied filters (queues, workers) if any.
        - Logical worker counts, with and without instances.
        - Physical worker counts, categorized by running, stopped, stalled, and rebooting.

        Args:
            stats (Dict[str, int]): Dictionary of worker statistics, as returned by `get_worker_statistics`.
            filters (Dict): Dictionary of applied filters, e.g., queues or worker names.

        Returns:
            List of Rich Panel objects ready for display in the console.
        """
        panels = []

        # Filter information
        if filters:
            filter_parts = []
            if "queues" in filters:
                queues_str = self._get_queues_str(filters["queues"])
                filter_parts.append(f"Queues: {queues_str}")
            if "name" in filters:
                filter_parts.append(f"Workers: {', '.join(filters['name'])}")

            filter_text = "\n".join(filter_parts)
            panels.append(Panel(filter_text, title="[bold blue]Applied Filters[/bold blue]", border_style="blue"))

        # Logical worker summary
        logical_summary = (
            f"Total: [bold white]{stats['total_logical']}[/bold white]\n"
            f"With Instances: [bold green]{stats['logical_with_instances']}[/bold green]\n"
            f"Without Instances: [bold dim]{stats['logical_without_instances']}[/bold dim]"
        )

        panels.append(Panel(logical_summary, title="[bold cyan]Logical Workers[/bold cyan]", border_style="cyan"))

        # Physical worker summary
        if stats["total_physical"] > 0:
            physical_summary = (
                f"Total: [bold white]{stats['total_physical']}[/bold white]\n"
                f"Running: [bold green]{stats['physical_running']}[/bold green]\n"
                f"Stopped: [bold red]{stats['physical_stopped']}[/bold red]"
            )

            if stats["physical_stalled"] > 0:
                physical_summary += f"\nStalled: [bold yellow]{stats['physical_stalled']}[/bold yellow]"
            if stats["physical_rebooting"] > 0:
                physical_summary += f"\nRebooting: [bold cyan]{stats['physical_rebooting']}[/bold cyan]"

            panels.append(
                Panel(physical_summary, title="[bold magenta]Physical Instances[/bold magenta]", border_style="magenta")
            )

        return panels

    def _build_compact_view(self, logical_workers: List[LogicalWorkerEntity], merlin_db: MerlinDatabase) -> str:
        """
        Build a compact text-based view of workers for narrow terminals.

        Displays each logical worker and its physical instances in a concise format.
        Logical workers without instances are marked explicitly as "NO INSTANCES".
        Each physical worker shows status, host, and PID with basic coloring for status.

        Args:
            logical_workers (List[db_scripts.entities.logical_worker_entity.LogicalWorkerEntity]):
                List of logical worker entities.
            merlin_db (db_scripts.merlin_db.MerlinDatabase): Database interface to fetch physical
                worker details.

        Returns:
            Multi-line string representing the compact worker view.
        """
        output_lines = []

        for logical_worker in logical_workers:
            worker_name = logical_worker.get_name()
            physical_worker_ids = logical_worker.get_physical_workers()

            if not physical_worker_ids:
                output_lines.append(f"[bold white]{worker_name}[/bold white]: [bold red]NO INSTANCES[/bold red]")
            else:
                physical_workers = [merlin_db.get("physical_worker", pid) for pid in physical_worker_ids]

                for physical_worker in physical_workers:
                    status = str(physical_worker.get_status()).replace("WorkerStatus.", "")
                    host = physical_worker.get_host() or "?"
                    pid = physical_worker.get_pid() or "-"

                    status_icon = "✓" if status == "RUNNING" else "✗"
                    color = "green" if status == "RUNNING" else "red"

                    output_lines.append(
                        f"[bold white]{worker_name}[/bold white]@[blue]{host}[/blue] "
                        f"[{color}]{status_icon} {status}[/{color}] (PID: {pid})"
                    )

        return "\n".join(output_lines)
