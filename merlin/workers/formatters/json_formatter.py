##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
JSON-based worker information formatter for Merlin.

This module provides a JSON formatter for displaying worker information
in a structured, machine-readable format. It is primarily intended for
programmatic consumption by downstream tools, scripts, or external systems
that need to parse and analyze worker data rather than display it in a
human-friendly format.

The formatter includes:\n
    - Detailed records of logical workers and their associated queues
    - Physical worker details such as ID, host, PID, status, restart counts,
      and timestamps
    - Relationships between logical and physical workers
    - Applied filters and generation timestamp metadata
    - Summary statistics for logical and physical workers
"""

import json
from datetime import datetime
from typing import Dict, List

from merlin.db_scripts.entities.logical_worker_entity import LogicalWorkerEntity
from merlin.db_scripts.merlin_db import MerlinDatabase
from merlin.workers.formatters.worker_formatter import WorkerFormatter


class JSONWorkerFormatter(WorkerFormatter):
    """
    JSON formatter for programmatic worker information consumption.

    This formatter generates structured JSON output representing logical
    and physical worker entities. The output includes worker details,
    relationships between logical and physical workers, and comprehensive
    statistics. Designed for use cases where downstream tools or scripts
    need to parse worker information in a machine-readable format.

    Attributes:
        console (rich.console.Console): A Rich Console object used for displaying
            output to the terminal.

    Methods:
        format_and_display: Format and print worker information as structured JSON,
            including details for logical and physical workers, filters, timestamp,
            and summary statistics.
        get_worker_statistics: Compute worker statistics, including counts of logical
            and physical workers by status, for inclusion in JSON output.
    """

    def format_and_display(
        self,
        logical_workers: List[LogicalWorkerEntity],
        filters: Dict,
        merlin_db: MerlinDatabase,
    ):
        """
        Format and display worker information as JSON.

        This method produces JSON output containing:\n
        - A record of applied filters
        - A timestamp of when the report was generated
        - Detailed logical worker entries (name, queues, associated physical workers)
        - Detailed physical worker entries (ID, host, PID, status, restart count, timestamps)
        - A summary of worker statistics

        Args:
            logical_workers (List[db_scripts.entities.logical_worker_entity.LogicalWorkerEntity]):
                A list of logical worker entities to format.
            filters (Dict): A dictionary of filters applied to the query.
            merlin_db (db_scripts.merlin_db.MerlinDatabase): Database interface for retrieving
                physical worker details.
        """
        data = {
            "filters": filters,
            "timestamp": datetime.now().isoformat(),
            "logical_workers": [],
            "summary": self.get_worker_statistics(logical_workers, merlin_db),
        }

        for logical_worker in logical_workers:
            logical_data = {
                "name": logical_worker.get_name(),
                "queues": sorted([
                    q[len("[merlin]_") :] if q.startswith("[merlin]_") else q for q in logical_worker.get_queues()
                ]),
                "physical_workers": [],
            }

            physical_worker_ids = logical_worker.get_physical_workers()
            physical_workers = [merlin_db.get("physical_worker", pid) for pid in physical_worker_ids]

            for physical_worker in physical_workers:
                physical_data = {
                    "id": physical_worker.get_id() if hasattr(physical_worker, "get_id") else None,
                    "name": physical_worker.get_name(),
                    "host": physical_worker.get_host(),
                    "pid": physical_worker.get_pid(),
                    "status": str(physical_worker.get_status()).replace("WorkerStatus.", ""),
                    "restart_count": physical_worker.get_restart_count(),
                    "latest_start_time": (
                        physical_worker.get_latest_start_time().isoformat()
                        if physical_worker.get_latest_start_time()
                        else None
                    ),
                    "heartbeat_timestamp": (
                        physical_worker.get_heartbeat_timestamp().isoformat()
                        if physical_worker.get_heartbeat_timestamp()
                        else None
                    ),
                }
                logical_data["physical_workers"].append(physical_data)

            data["logical_workers"].append(logical_data)

        print(f"data: {data}")
        self.console.print(json.dumps(data, indent=2))
