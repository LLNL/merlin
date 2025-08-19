"""
Bridge between worker execution and task flow coordination.
"""

import asyncio
import logging
import json
import time
from typing import Dict, Any, Optional, Callable
from pathlib import Path

from merlin.coordination.task_flow_coordinator import TaskFlowCoordinator, TaskState, CoordinationState

LOG = logging.getLogger(__name__)

class WorkerTaskBridge:
    """Bridge worker execution with task flow coordination."""
    
    def __init__(self, 
                 coordinator: TaskFlowCoordinator,
                 shared_storage_path: str = "/shared/storage"):
        self.coordinator = coordinator
        self.shared_storage_path = Path(shared_storage_path)
        self.results_dir = self.shared_storage_path / "results"
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
        # Result handlers
        self.result_handlers: Dict[str, Callable] = {}
        
        # Monitoring
        self.monitoring_active = False
    
    def register_result_handler(self, task_type: str, handler: Callable):
        """Register a handler for specific task type results."""
        self.result_handlers[task_type] = handler
    
    async def start_monitoring(self):
        """Start monitoring for task results."""
        
        if self.monitoring_active:
            LOG.warning("Task bridge monitoring already active")
            return
        
        self.monitoring_active = True
        LOG.info("Starting worker task bridge monitoring")
        
        try:
            while self.monitoring_active:
                await asyncio.sleep(2.0)  # Check every 2 seconds
                
                # Check for new result files
                await self._process_result_files()
                
                # Check for worker heartbeats
                await self._process_worker_heartbeats()
                
        except Exception as e:
            LOG.error(f"Error in worker task bridge monitoring: {e}", exc_info=True)
        finally:
            self.monitoring_active = False
            LOG.info("Worker task bridge monitoring stopped")
    
    async def _process_result_files(self):
        """Process result files from workers."""
        
        try:
            # Scan for result files
            result_pattern = self.results_dir.glob("task_result_*.json")
            
            for result_file in result_pattern:
                try:
                    # Read result data
                    with open(result_file, 'r') as f:
                        result_data = json.load(f)
                    
                    # Process result
                    await self._handle_task_result(result_data)
                    
                    # Archive processed file
                    archived_file = result_file.with_suffix('.processed')
                    result_file.rename(archived_file)
                    
                except Exception as e:
                    LOG.error(f"Error processing result file {result_file}: {e}")
                    
                    # Move to error directory
                    error_file = result_file.with_suffix('.error')
                    result_file.rename(error_file)
        
        except Exception as e:
            LOG.error(f"Error scanning result files: {e}")
    
    async def _handle_task_result(self, result_data: Dict[str, Any]):
        """Handle individual task result."""
        
        task_id = result_data.get('task_id')
        if not task_id:
            LOG.warning("Result data missing task_id")
            return
        
        LOG.info(f"Processing result for task {task_id}")
        
        # Determine task state from result
        exit_code = result_data.get('exit_code', 1)
        task_state = TaskState.COMPLETED if exit_code == 0 else TaskState.FAILED
        
        # Extract worker information
        worker_id = result_data.get('worker_name') or result_data.get('worker_id')
        error_message = result_data.get('error') or result_data.get('stderr')
        
        # Update coordinator
        self.coordinator._update_task_status(
            task_id=task_id,
            task_state=task_state,
            coordination_state=CoordinationState.COORDINATION_COMPLETE,
            worker_id=worker_id,
            error_message=error_message,
            result_data=result_data
        )
        
        # Call registered handler if available
        task_status = self.coordinator.get_task_status(task_id)
        if task_status:
            task_def = self.coordinator.task_registry.get(task_id)
            if task_def:
                task_type = task_def.task_type.value
                handler = self.result_handlers.get(task_type)
                if handler:
                    try:
                        await handler(task_id, result_data, task_status)
                    except Exception as e:
                        LOG.error(f"Error in result handler for {task_type}: {e}")
    
    async def _process_worker_heartbeats(self):
        """Process worker heartbeat information."""
        
        try:
            heartbeat_pattern = self.shared_storage_path.glob("heartbeat_*.json")
            
            for heartbeat_file in heartbeat_pattern:
                try:
                    # Read heartbeat data
                    with open(heartbeat_file, 'r') as f:
                        heartbeat_data = json.load(f)
                    
                    # Update task states based on heartbeat
                    await self._handle_worker_heartbeat(heartbeat_data)
                    
                except Exception as e:
                    LOG.error(f"Error processing heartbeat file {heartbeat_file}: {e}")
        
        except Exception as e:
            LOG.error(f"Error scanning heartbeat files: {e}")
    
    async def _handle_worker_heartbeat(self, heartbeat_data: Dict[str, Any]):
        """Handle worker heartbeat information."""
        
        worker_id = heartbeat_data.get('worker_id')
        active_tasks = heartbeat_data.get('active_tasks', [])
        
        for task_id in active_tasks:
            # Update task state to running if not already
            current_status = self.coordinator.get_task_status(task_id)
            if current_status and current_status.task_state == TaskState.QUEUED:
                self.coordinator._update_task_status(
                    task_id=task_id,
                    task_state=TaskState.RUNNING,
                    coordination_state=CoordinationState.EXECUTING,
                    worker_id=worker_id
                )
    
    def stop_monitoring(self):
        """Stop bridge monitoring."""
        self.monitoring_active = False
    
    async def publish_task_assignment(self, 
                                    task_id: str, 
                                    worker_id: str,
                                    assignment_data: Dict[str, Any]):
        """Publish task assignment to worker."""
        
        assignment_file = self.shared_storage_path / f"assignment_{task_id}.json"
        
        assignment_info = {
            'task_id': task_id,
            'worker_id': worker_id,
            'assignment_time': time.time(),
            'assignment_data': assignment_data
        }
        
        try:
            with open(assignment_file, 'w') as f:
                json.dump(assignment_info, f, indent=2)
            
            LOG.info(f"Published task assignment for {task_id} to worker {worker_id}")
            
        except Exception as e:
            LOG.error(f"Failed to publish task assignment: {e}")
    
    async def get_worker_metrics(self) -> Dict[str, Any]:
        """Get worker performance metrics."""
        
        metrics = {
            'active_workers': set(),
            'task_throughput': {},
            'worker_utilization': {},
            'error_rates': {}
        }
        
        # Analyze recent results
        try:
            processed_pattern = self.results_dir.glob("task_result_*.processed")
            
            for result_file in processed_pattern:
                try:
                    with open(result_file, 'r') as f:
                        result_data = json.load(f)
                    
                    worker_id = result_data.get('worker_name') or result_data.get('worker_id')
                    if worker_id:
                        metrics['active_workers'].add(worker_id)
                        
                        # Count throughput
                        if worker_id not in metrics['task_throughput']:
                            metrics['task_throughput'][worker_id] = 0
                        metrics['task_throughput'][worker_id] += 1
                        
                        # Track error rates
                        exit_code = result_data.get('exit_code', 1)
                        if worker_id not in metrics['error_rates']:
                            metrics['error_rates'][worker_id] = {'total': 0, 'errors': 0}
                        
                        metrics['error_rates'][worker_id]['total'] += 1
                        if exit_code != 0:
                            metrics['error_rates'][worker_id]['errors'] += 1
                
                except Exception as e:
                    LOG.error(f"Error analyzing result file {result_file}: {e}")
        
        except Exception as e:
            LOG.error(f"Error gathering worker metrics: {e}")
        
        # Convert sets to lists for JSON serialization
        metrics['active_workers'] = list(metrics['active_workers'])
        
        return metrics