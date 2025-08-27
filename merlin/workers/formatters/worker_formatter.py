##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Worker formatter base module for displaying worker query results.

This module defines the abstract base class `WorkerFormatter`, which provides a 
standard interface for formatting and displaying information about Merlin workers. 
Worker formatters are responsible for presenting logical and physical worker 
information in a structured, user-friendly manner (e.g., through text, tables, 
or rich console output).

Intended Usage:\n
    Subclasses of `WorkerFormatter` (e.g., those using Rich for terminal 
    visualization) should implement `format_and_display` to render 
    worker information, while reusing `get_worker_statistics` for 
    consistent metrics across implementations.
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Optional

from rich.console import Console

from merlin.db_scripts.entities.logical_worker_entity import LogicalWorkerEntity
from merlin.db_scripts.merlin_db import MerlinDatabase


class WorkerFormatter(ABC):
    """
    Abstract base class for formatting and displaying worker query results.

    Provides a consistent interface for formatting logical and physical worker
    information, including a utility method to calculate worker statistics.

    Methods:
        format_and_display: Abstract method that formats and outputs worker
            information. Must be implemented by subclasses.
        get_worker_statistics: Compute counts and statuses of logical and
            physical workers, including totals and breakdown by status.
    """
    
    @abstractmethod
    def format_and_display(self, logical_workers: List, filters: Dict, merlin_db: MerlinDatabase, console: Optional[Console] = None):
        """
        Format and display information about logical and physical workers.

        This method must be implemented by subclasses to define the output
        format (e.g., JSON, Rich tables, text). Implementations should make
        use of `get_worker_statistics` if worker summary metrics are required.

        Args:
            logical_workers (List[LogicalWorkerEntity]): List of logical worker
                entities to be displayed.
            filters (Dict): Optional filters applied to the worker query.
            merlin_db (MerlinDatabase): Database interface for retrieving
                physical worker details.
            console (Optional[Console]): Rich console object for printing
                output. If None, a default console may be used.
        """
        pass

    def get_worker_statistics(self, logical_workers: List[LogicalWorkerEntity], merlin_db: MerlinDatabase) -> Dict[str, int]:
        """
        Calculate comprehensive statistics for logical and physical workers.

        Iterates through all logical workers and their associated physical 
        instances to compute counts of running, stopped, stalled, and rebooting 
        workers, as well as counts of logical workers with or without instances.

        Args:
            logical_workers (List[db_scripts.entities.logical_worker_entity.LogicalWorkerEntity]):
                List of logical worker entities.
            merlin_db (db_scripts.merlin_db.MerlinDatabase): Database interface to fetch physical
                worker details.

        Returns:
            Dictionary containing worker statistics:\n
                - total_logical: Total number of logical workers.
                - logical_with_instances: Number of logical workers with physical instances.
                - logical_without_instances: Number of logical workers without physical instances.
                - total_physical: Total number of physical workers.
                - physical_running: Count of running physical workers.
                - physical_stopped: Count of stopped physical workers.
                - physical_stalled: Count of stalled physical workers.
                - physical_rebooting: Count of rebooting physical workers.
        """        
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
