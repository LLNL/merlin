##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""

"""

from datetime import datetime, timedelta
from typing import List, Dict

from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich.panel import Panel
from rich.columns import Columns

from merlin.workers.formatters.worker_formatter import WorkerFormatter


class RichWorkerFormatter(WorkerFormatter):
    """Rich-based formatter with responsive tables and panels."""
    
    def format_and_display(self, logical_workers: List, filters: Dict, merlin_db, console: Console = None):
        """Format and display worker information using Rich components."""
        if console is None:
            console = Console()
            
        # Calculate statistics
        stats = self._get_worker_statistics(logical_workers, merlin_db)
        console_width = console.size.width

        console.print()  # Empty line
        
        # For very narrow terminals (< 60 chars), use compact view
        if console_width < 60:
            console.print("[bold cyan]Worker Status[/bold cyan]")
            if filters:
                filter_text = " | ".join([
                    f"{k}: {','.join(v)}" for k, v in filters.items()
                ])
                console.print(f"[dim]Filters: {filter_text}[/dim]")
            
            console.print(f"[bold]Summary:[/bold] {stats['physical_running']}/{stats['total_physical']} running, {stats['logical_without_instances']} logical workers without instances\n")
            
            compact_view = self._build_compact_view(logical_workers, merlin_db)
            console.print(compact_view)
            return
        
        # For wider terminals, use panel + table layout
        summary_panels = self._build_summary_panels(stats, filters)
        if console_width >= 100 and len(summary_panels) <= 3:
            console.print(Columns(summary_panels, equal=True))
        else:
            # Stack panels vertically for narrow terminals
            for panel in summary_panels:
                console.print(panel)
        
        console.print()  # Empty line
        
        # Show physical workers table if any exist
        if stats['total_physical'] > 0:
            physical_table = self._build_physical_workers_table(logical_workers, console_width, merlin_db)
            console.print(physical_table)
            console.print()  # Empty line
        
        # Show logical workers without instances if any exist
        if stats['logical_without_instances'] > 0:
            no_instances_table = self._build_logical_workers_without_instances_table(logical_workers, console_width)
            console.print(no_instances_table)

    def _format_status(self, status) -> Text:
        """Format worker status with icons and colors."""
        status_str = str(status).replace("WorkerStatus.", "")
        
        status_config = {
            "RUNNING": ("✓", "bold green"),
            "STALLED": ("⚠", "bold yellow"), 
            "STOPPED": ("✗", "bold red"),
            "REBOOTING": ("↻", "bold cyan"),
        }
        
        icon, color = status_config.get(status_str.upper(), ("?", "white"))
        return Text(f"{icon} {status_str}", style=color)

    def _format_uptime_or_downtime(self, physical_worker) -> str:
        """Format uptime for running workers, downtime for stopped workers."""
        status = str(physical_worker.get_status()).replace("WorkerStatus.", "")
        
        if status == "RUNNING":
            start_time = physical_worker.get_latest_start_time()
            if start_time:
                uptime = datetime.now() - start_time
                return self._format_time_duration(uptime)
        else:
            # For stopped workers, show downtime if stop_time is available
            stop_time = getattr(physical_worker, 'get_stop_time', lambda: None)()
            if stop_time:
                downtime = datetime.now() - stop_time
                return f"down {self._format_time_duration(downtime)}"
            else:
                return "stopped"
        
        return "-"

    def _format_time_duration(self, duration: timedelta) -> str:
        """Format time duration in human-readable format."""
        if duration.days > 0:
            return f"{duration.days}d {duration.seconds // 3600}h"
        elif duration.seconds >= 3600:
            return f"{duration.seconds // 3600}h {(duration.seconds % 3600) // 60}m"
        elif duration.seconds >= 60:
            return f"{duration.seconds // 60}m"
        else:
            return f"{duration.seconds}s"

    def _format_last_heartbeat(self, heartbeat_timestamp: datetime) -> Text:
        """Format last heartbeat with color coding based on recency."""
        if not heartbeat_timestamp:
            return Text("-", style="dim")
        
        time_diff = datetime.now() - heartbeat_timestamp
        
        if time_diff < timedelta(minutes=1):
            return Text("Just now", style="green")
        elif time_diff < timedelta(minutes=5):
            return Text(f"{int(time_diff.total_seconds() // 60)}m ago", style="yellow")
        elif time_diff < timedelta(hours=1):
            return Text(f"{int(time_diff.total_seconds() // 60)}m ago", style="orange3")
        else:
            return Text(f"{int(time_diff.total_seconds() // 3600)}h ago", style="red")

    def _get_worker_statistics(self, logical_workers, merlin_db) -> Dict:
        """Calculate comprehensive worker statistics."""        
        stats = {
            'total_logical': len(logical_workers),
            'logical_with_instances': 0,
            'logical_without_instances': 0,
            'total_physical': 0,
            'physical_running': 0,
            'physical_stopped': 0,
            'physical_stalled': 0,
            'physical_rebooting': 0
        }
        
        for logical_worker in logical_workers:
            physical_worker_ids = logical_worker.get_physical_workers()
            
            if physical_worker_ids:
                stats['logical_with_instances'] += 1
                physical_workers = [
                    merlin_db.get("physical_worker", pid) for pid in physical_worker_ids
                ]
                
                for physical_worker in physical_workers:
                    stats['total_physical'] += 1
                    status = str(physical_worker.get_status()).replace("WorkerStatus.", "")
                    
                    if status == "RUNNING":
                        stats['physical_running'] += 1
                    elif status == "STOPPED":
                        stats['physical_stopped'] += 1
                    elif status == "STALLED":
                        stats['physical_stalled'] += 1
                    elif status == "REBOOTING":
                        stats['physical_rebooting'] += 1
            else:
                stats['logical_without_instances'] += 1
        
        return stats

    def _build_summary_panels(self, stats: Dict, filters: Dict) -> List[Panel]:
        """Build summary panels showing different aspects of worker status."""
        panels = []
        
        # Filter information
        if filters:
            filter_parts = []
            if "queues" in filters:
                filter_parts.append(f"Queues: {', '.join(filters['queues'])}")
            if "workers" in filters:
                filter_parts.append(f"Workers: {', '.join(filters['workers'])}")
            
            filter_text = "\n".join(filter_parts)
            panels.append(Panel(filter_text, title="[bold blue]Applied Filters[/bold blue]", border_style="blue"))
        
        # Logical worker summary
        logical_summary = f"Total: [bold white]{stats['total_logical']}[/bold white]\n" \
            f"With Instances: [bold green]{stats['logical_with_instances']}[/bold green]\n" \
            f"Without Instances: [bold dim]{stats['logical_without_instances']}[/bold dim]"
        
        panels.append(Panel(logical_summary, title="[bold cyan]Logical Workers[/bold cyan]", border_style="cyan"))
        
        # Physical worker summary
        if stats['total_physical'] > 0:
            physical_summary = f"Total: [bold white]{stats['total_physical']}[/bold white]\n" \
                f"Running: [bold green]{stats['physical_running']}[/bold green]\n" \
                f"Stopped: [bold red]{stats['physical_stopped']}[/bold red]"
            
            if stats['physical_stalled'] > 0:
                physical_summary += f"\nStalled: [bold yellow]{stats['physical_stalled']}[/bold yellow]"
            if stats['physical_rebooting'] > 0:
                physical_summary += f"\nRebooting: [bold cyan]{stats['physical_rebooting']}[/bold cyan]"
            
            panels.append(Panel(physical_summary, title="[bold magenta]Physical Instances[/bold magenta]", border_style="magenta"))
        
        return panels

    def _build_physical_workers_table(self, logical_workers, console_width: int, merlin_db) -> Table:
        """Build table showing only physical worker instances, responsive to terminal width."""
        table = Table(
            show_header=True, 
            header_style="bold white",
            title="[bold magenta]Physical Worker Instances[/bold magenta]",
        )
        
        # Responsive column configuration based on terminal width
        if console_width < 80:
            # Narrow terminal - show only essential columns
            table.add_column("Worker", style="bold cyan", max_width=12)
            table.add_column("Host", style="blue", max_width=10)
            table.add_column("PID", justify="right", style="yellow", width=8)
            table.add_column("Status", style="bold", width=10)
        elif console_width < 120:
            # Medium terminal - show core columns
            table.add_column("Worker", style="bold cyan", max_width=15)
            table.add_column("Instance", style="bold magenta", max_width=15, no_wrap=True)
            table.add_column("Host", style="blue", max_width=12)
            table.add_column("PID", justify="right", style="yellow", width=8)
            table.add_column("Status", style="bold", width=10)
            table.add_column("Runtime", style="cyan", width=8)
        else:
            # Wide terminal - show all columns
            table.add_column("Logical Worker", style="bold cyan", max_width=15)
            table.add_column("Queues", style="green", max_width=20, no_wrap=True)
            table.add_column("Instance Name", style="bold magenta", max_width=20, no_wrap=True)
            table.add_column("Host", style="blue", max_width=12)
            table.add_column("PID", justify="right", style="yellow", width=8)
            table.add_column("Status", style="bold", width=10)
            table.add_column("Runtime", style="cyan", width=8)
            table.add_column("Heartbeat", style="bright_blue", width=10)
            table.add_column("Restarts", justify="right", style="red", width=8)
        
        # Collect all physical workers
        physical_worker_rows = []
        
        for logical_worker in logical_workers:
            worker_name = logical_worker.get_name()
            queues_str = ", ".join(
                q[len("[merlin]_"):] if q.startswith("[merlin]_") else q
                for q in sorted(logical_worker.get_queues())
            )

            physical_worker_ids = logical_worker.get_physical_workers()
            physical_workers = [
                merlin_db.get("physical_worker", pid) for pid in physical_worker_ids
            ]

            for physical_worker in physical_workers:
                status = str(physical_worker.get_status()).replace("WorkerStatus.", "")
                physical_worker_rows.append({
                    'logical_name': worker_name,
                    'queues': queues_str,
                    'physical_worker': physical_worker,
                    'status': status
                })
        
        # Sort: running first, then by logical worker name, then by instance name
        physical_worker_rows.sort(key=lambda row: (
            0 if row['status'] == "RUNNING" else 1,
            row['logical_name'],
            row['physical_worker'].get_name() or ""
        ))
        
        for row in physical_worker_rows:
            physical_worker = row['physical_worker']
            status = physical_worker.get_status()
            status_str = str(status).replace("WorkerStatus.", "")
            
            # Only show heartbeat for running workers
            heartbeat_text = "-"
            if status_str == "RUNNING":
                heartbeat_text = str(self._format_last_heartbeat(physical_worker.get_heartbeat_timestamp()))
            
            # Responsive row data based on terminal width
            if console_width < 80:
                # Narrow: Worker, Host, PID, Status
                table.add_row(
                    row['logical_name'][:12],  # Truncate long names
                    (physical_worker.get_host() or "-")[:10],
                    str(physical_worker.get_pid()) if physical_worker.get_pid() else "-",
                    self._format_status(status)
                )
            elif console_width < 120:
                # Medium: Worker, Instance, Host, PID, Status, Runtime  
                instance_name = physical_worker.get_name() or "-"
                if len(instance_name) > 15:
                    instance_name = instance_name[:12] + "..."
                
                table.add_row(
                    row['logical_name'],
                    instance_name,
                    physical_worker.get_host() or "-",
                    str(physical_worker.get_pid()) if physical_worker.get_pid() else "-",
                    self._format_status(status),
                    self._format_uptime_or_downtime(physical_worker)
                )
            else:
                # Wide: All columns
                table.add_row(
                    row['logical_name'],
                    row['queues'],
                    physical_worker.get_name() or "-",
                    physical_worker.get_host() or "-",
                    str(physical_worker.get_pid()) if physical_worker.get_pid() else "-",
                    self._format_status(status),
                    self._format_uptime_or_downtime(physical_worker),
                    heartbeat_text,
                    str(physical_worker.get_restart_count())
                )
        
        return table

    def _build_compact_view(self, logical_workers, merlin_db) -> str:
        """Build a compact text view for very narrow terminals."""
        output_lines = []
        
        for logical_worker in logical_workers:
            worker_name = logical_worker.get_name()
            physical_worker_ids = logical_worker.get_physical_workers()
            
            if not physical_worker_ids:
                output_lines.append(f"[bold white]{worker_name}[/bold white]: [bold red]NO INSTANCES[/bold red]")
            else:
                physical_workers = [
                    merlin_db.get("physical_worker", pid) for pid in physical_worker_ids
                ]
                
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
    
    def _build_logical_workers_without_instances_table(self, logical_workers, console_width) -> Table:
        """Build table showing logical workers without physical instances, responsive to width."""
        table = Table(
            show_header=True,
            header_style="bold white",
            title="[bold yellow]Logical Workers Without Instances[/bold yellow]"
        )
        
        # Responsive columns
        if console_width < 80:
            table.add_column("Worker", style="bold white", max_width=20)
            table.add_column("Status", style="bold red", width=12)
        else:
            table.add_column("Worker Name", style="bold white", max_width=25)
            table.add_column("Queues", style="green", max_width=30, no_wrap=True)
            table.add_column("Status", style="bold red", width=12)
        
        workers_without_instances = []
        
        for logical_worker in logical_workers:
            physical_worker_ids = logical_worker.get_physical_workers()
            if not physical_worker_ids:
                workers_without_instances.append(logical_worker)
        
        # Sort by name
        workers_without_instances.sort(key=lambda w: w.get_name())
        
        for logical_worker in workers_without_instances:
            queues_str = ", ".join(
                q[len("[merlin]_"):] if q.startswith("[merlin]_") else q
                for q in sorted(logical_worker.get_queues())
            )
            
            if console_width < 80:
                # Narrow: Just worker name and status
                table.add_row(
                    logical_worker.get_name(),
                    Text("NO INSTANCES", style="bold red")
                )
            else:
                # Wide: Include queues
                table.add_row(
                    logical_worker.get_name(),
                    queues_str,
                    Text("NO INSTANCES", style="bold red")
                )
        
        return table
