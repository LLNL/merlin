##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""

"""

from typing import List, Dict

from rich.console import Console

from merlin.workers.formatters.worker_formatter import WorkerFormatter


class CompactWorkerFormatter(WorkerFormatter):
    """Simple text formatter for CI/scripting environments."""
    
    def format_and_display(self, logical_workers: List, filters: Dict, merlin_db, console: Console = None):
        """Format and display worker information as simple text."""
        if console is None:
            console = Console()
            
        stats = self._get_worker_statistics(logical_workers, merlin_db)
        
        # Simple text summary
        console.print(f"Workers: {stats['physical_running']}/{stats['total_physical']} running")
        
        if filters:
            filter_text = ", ".join([f"{k}={','.join(v)}" for k, v in filters.items()])
            console.print(f"Filters: {filter_text}")
        
        console.print()
        
        # Simple list format
        for logical_worker in logical_workers:
            worker_name = logical_worker.get_name()
            physical_worker_ids = logical_worker.get_physical_workers()
            
            if not physical_worker_ids:
                console.print(f"{worker_name}: NO INSTANCES")
            else:
                physical_workers = [
                    merlin_db.get("physical_worker", pid) for pid in physical_worker_ids
                ]
                
                for physical_worker in physical_workers:
                    status = str(physical_worker.get_status()).replace("WorkerStatus.", "")
                    host = physical_worker.get_host() or "unknown"
                    pid = physical_worker.get_pid() or "N/A"
                    console.print(f"{worker_name}@{host}: {status} (PID: {pid})")
    
    def _get_worker_statistics(self, logical_workers, merlin_db) -> Dict:
        """Calculate basic worker statistics."""
        stats = {
            'total_physical': 0,
            'physical_running': 0,
        }
        
        for logical_worker in logical_workers:
            physical_worker_ids = logical_worker.get_physical_workers()
            physical_workers = [
                merlin_db.get("physical_worker", pid) for pid in physical_worker_ids
            ]
            
            for physical_worker in physical_workers:
                stats['total_physical'] += 1
                status = str(physical_worker.get_status()).replace("WorkerStatus.", "")
                if status == "RUNNING":
                    stats['physical_running'] += 1
        
        return stats
