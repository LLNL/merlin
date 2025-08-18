##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""

"""

import json
from datetime import datetime
from typing import List, Dict

from rich.console import Console

from merlin.workers.formatters.worker_formatter import WorkerFormatter


class JSONWorkerFormatter(WorkerFormatter):
    """JSON formatter for programmatic consumption."""
    
    def format_and_display(self, logical_workers: List, filters: Dict, merlin_db, console: Console = None):
        """Format and display worker information as JSON."""
        if console is None:
            console = Console()
            
        data = {
            "filters": filters,
            "timestamp": datetime.now().isoformat(),
            "logical_workers": [],
            "summary": self._get_worker_statistics(logical_workers, merlin_db)
        }
        
        for logical_worker in logical_workers:
            logical_data = {
                "name": logical_worker.get_name(),
                "queues": [q[len("[merlin]_"):] if q.startswith("[merlin]_") else q
                    for q in sorted(logical_worker.get_queues())],
                "physical_workers": []
            }
            
            physical_worker_ids = logical_worker.get_physical_workers()
            physical_workers = [
                merlin_db.get("physical_worker", pid) for pid in physical_worker_ids
            ]
            
            for physical_worker in physical_workers:
                physical_data = {
                    "id": physical_worker.get_id() if hasattr(physical_worker, 'get_id') else None,
                    "name": physical_worker.get_name(),
                    "host": physical_worker.get_host(),
                    "pid": physical_worker.get_pid(),
                    "status": str(physical_worker.get_status()).replace("WorkerStatus.", ""),
                    "restart_count": physical_worker.get_restart_count(),
                    "latest_start_time": physical_worker.get_latest_start_time().isoformat() if physical_worker.get_latest_start_time() else None,
                    "heartbeat_timestamp": physical_worker.get_heartbeat_timestamp().isoformat() if physical_worker.get_heartbeat_timestamp() else None
                }
                logical_data["physical_workers"].append(physical_data)
            
            data["logical_workers"].append(logical_data)
        
        console.print(json.dumps(data, indent=2))
    
    def _get_worker_statistics(self, logical_workers, merlin_db) -> Dict:
        """Calculate comprehensive worker statistics for JSON output."""
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
