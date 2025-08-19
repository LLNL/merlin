"""
Coordinate complete task flow across distributed backend systems.
"""

import asyncio
import logging
import time
import json
from enum import Enum
from typing import Dict, Any, List, Optional, Set
from dataclasses import dataclass, field
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from merlin.factories.task_definition import UniversalTaskDefinition, CoordinationPattern
from merlin.adapters.signature_adapters import SignatureAdapter

LOG = logging.getLogger(__name__)

class TaskState(Enum):
    """Task execution states."""
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    RETRYING = "retrying"
    CANCELLED = "cancelled"

class CoordinationState(Enum):
    """Coordination pattern states."""
    WAITING_FOR_DEPENDENCIES = "waiting_for_dependencies"
    READY_FOR_EXECUTION = "ready_for_execution"
    EXECUTING = "executing"
    WAITING_FOR_GROUP = "waiting_for_group"
    GROUP_COMPLETED = "group_completed"
    CALLBACK_TRIGGERED = "callback_triggered"
    COORDINATION_COMPLETE = "coordination_complete"

@dataclass
class TaskStatus:
    """Complete task status information."""
    task_id: str
    task_state: TaskState
    coordination_state: CoordinationState
    worker_id: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    retry_count: int = 0
    error_message: Optional[str] = None
    result_data: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def duration(self) -> Optional[float]:
        """Calculate task duration in seconds."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'task_id': self.task_id,
            'task_state': self.task_state.value,
            'coordination_state': self.coordination_state.value,
            'worker_id': self.worker_id,
            'start_time': self.start_time,
            'end_time': self.end_time,
            'retry_count': self.retry_count,
            'error_message': self.error_message,
            'result_data': self.result_data,
            'duration': self.duration,
            'metadata': self.metadata
        }

class TaskFlowCoordinator:
    """Coordinate task flow across distributed backends."""
    
    def __init__(self, 
                 signature_adapter: SignatureAdapter,
                 state_storage_path: str = "/shared/storage/state",
                 max_workers: int = 10):
        self.signature_adapter = signature_adapter
        self.state_storage_path = Path(state_storage_path)
        self.state_storage_path.mkdir(parents=True, exist_ok=True)
        
        # Task tracking
        self.task_registry: Dict[str, UniversalTaskDefinition] = {}
        self.task_status_map: Dict[str, TaskStatus] = {}
        self.group_registry: Dict[str, Set[str]] = {}
        self.dependency_graph: Dict[str, Set[str]] = {}
        
        # Execution management
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.running = False
        self.monitoring_interval = 5.0  # seconds
        
        # State persistence
        self.state_file = self.state_storage_path / "coordinator_state.json"
        self._load_state()
    
    async def submit_task_flow(self, 
                              tasks: List[UniversalTaskDefinition]) -> Dict[str, str]:
        """Submit a complete task flow for execution."""
        
        LOG.info(f"Submitting task flow with {len(tasks)} tasks")
        
        submission_results = {}
        
        # Register all tasks
        for task in tasks:
            self._register_task(task)
        
        # Build dependency graph
        self._build_dependency_graph(tasks)
        
        # Submit tasks based on coordination patterns
        for task in tasks:
            try:
                if task.coordination_pattern == CoordinationPattern.SIMPLE:
                    result_id = await self._submit_simple_task(task)
                elif task.coordination_pattern == CoordinationPattern.GROUP:
                    result_id = await self._submit_group_task(task)
                elif task.coordination_pattern == CoordinationPattern.CHAIN:
                    result_id = await self._submit_chain_task(task)
                elif task.coordination_pattern == CoordinationPattern.CHORD:
                    result_id = await self._submit_chord_task(task)
                else:
                    raise ValueError(f"Unsupported coordination pattern: {task.coordination_pattern}")
                
                submission_results[task.task_id] = result_id
                
                # Update task status
                self._update_task_status(
                    task.task_id, 
                    TaskState.QUEUED,
                    CoordinationState.READY_FOR_EXECUTION
                )
                
            except Exception as e:
                LOG.error(f"Failed to submit task {task.task_id}: {e}")
                submission_results[task.task_id] = f"ERROR: {str(e)}"
                self._update_task_status(
                    task.task_id,
                    TaskState.FAILED,
                    CoordinationState.COORDINATION_COMPLETE,
                    error_message=str(e)
                )
        
        # Start monitoring if not already running
        if not self.running:
            asyncio.create_task(self._monitor_task_flow())
        
        # Persist state
        self._save_state()
        
        return submission_results
    
    async def _submit_simple_task(self, task: UniversalTaskDefinition) -> str:
        """Submit a simple task."""
        signature = self.signature_adapter.create_signature(task)
        return self.signature_adapter.submit_task(signature)
    
    async def _submit_group_task(self, task: UniversalTaskDefinition) -> str:
        """Submit a task as part of a group."""
        
        if not task.group_id:
            raise ValueError("Group task must have group_id")
        
        # Register task in group
        if task.group_id not in self.group_registry:
            self.group_registry[task.group_id] = set()
        self.group_registry[task.group_id].add(task.task_id)
        
        # Submit individual task
        signature = self.signature_adapter.create_signature(task)
        return self.signature_adapter.submit_task(signature)
    
    async def _submit_chain_task(self, task: UniversalTaskDefinition) -> str:
        """Submit a task as part of a chain."""
        
        # Check dependencies before submission
        if task.dependencies:
            for dep in task.dependencies:
                dep_status = self.task_status_map.get(dep.task_id)
                if not dep_status or dep_status.task_state != TaskState.COMPLETED:
                    # Task not ready - set to waiting
                    self._update_task_status(
                        task.task_id,
                        TaskState.PENDING,
                        CoordinationState.WAITING_FOR_DEPENDENCIES
                    )
                    return "WAITING_FOR_DEPENDENCIES"
        
        # Dependencies satisfied - submit task
        signature = self.signature_adapter.create_signature(task)
        return self.signature_adapter.submit_task(signature)
    
    async def _submit_chord_task(self, task: UniversalTaskDefinition) -> str:
        """Submit a chord callback task."""
        
        # Chord tasks wait for all dependencies (parallel tasks) to complete
        if task.dependencies:
            completed_deps = 0
            for dep in task.dependencies:
                dep_status = self.task_status_map.get(dep.task_id)
                if dep_status and dep_status.task_state == TaskState.COMPLETED:
                    completed_deps += 1
            
            if completed_deps < len(task.dependencies):
                # Not all dependencies complete - wait
                self._update_task_status(
                    task.task_id,
                    TaskState.PENDING,
                    CoordinationState.WAITING_FOR_GROUP
                )
                return "WAITING_FOR_GROUP"
        
        # All dependencies complete - submit callback
        signature = self.signature_adapter.create_signature(task)
        result = self.signature_adapter.submit_task(signature)
        
        self._update_task_status(
            task.task_id,
            TaskState.QUEUED,
            CoordinationState.CALLBACK_TRIGGERED
        )
        
        return result
    
    def _register_task(self, task: UniversalTaskDefinition):
        """Register task in coordinator."""
        self.task_registry[task.task_id] = task
        
        # Initialize status
        initial_status = TaskStatus(
            task_id=task.task_id,
            task_state=TaskState.PENDING,
            coordination_state=CoordinationState.WAITING_FOR_DEPENDENCIES if task.dependencies else CoordinationState.READY_FOR_EXECUTION,
            metadata={
                'task_type': task.task_type.value,
                'coordination_pattern': task.coordination_pattern.value,
                'group_id': task.group_id,
                'queue_name': task.queue_name,
                'priority': task.priority
            }
        )
        
        self.task_status_map[task.task_id] = initial_status
    
    def _build_dependency_graph(self, tasks: List[UniversalTaskDefinition]):
        """Build dependency graph for task coordination."""
        
        # Initialize dependency tracking
        for task in tasks:
            self.dependency_graph[task.task_id] = set()
            
            for dep in task.dependencies:
                self.dependency_graph[task.task_id].add(dep.task_id)
    
    async def _monitor_task_flow(self):
        """Monitor and coordinate task flow execution."""
        
        self.running = True
        LOG.info("Starting task flow monitoring")
        
        try:
            while self.running:
                await asyncio.sleep(self.monitoring_interval)
                
                # Check for ready tasks
                await self._check_ready_tasks()
                
                # Check for completed groups
                await self._check_completed_groups()
                
                # Update task states
                await self._update_task_states()
                
                # Clean up completed flows
                await self._cleanup_completed_flows()
                
                # Persist state
                self._save_state()
                
        except Exception as e:
            LOG.error(f"Error in task flow monitoring: {e}", exc_info=True)
        finally:
            self.running = False
            LOG.info("Task flow monitoring stopped")
    
    async def _check_ready_tasks(self):
        """Check for tasks ready to execute based on dependencies."""
        
        for task_id, status in self.task_status_map.items():
            if status.coordination_state == CoordinationState.WAITING_FOR_DEPENDENCIES:
                task = self.task_registry[task_id]
                
                # Check if all dependencies are satisfied
                dependencies_satisfied = True
                for dep in task.dependencies:
                    dep_status = self.task_status_map.get(dep.task_id)
                    if not dep_status or dep_status.task_state != TaskState.COMPLETED:
                        dependencies_satisfied = False
                        break
                
                if dependencies_satisfied:
                    LOG.info(f"Task {task_id} dependencies satisfied, submitting for execution")
                    
                    try:
                        # Submit task
                        if task.coordination_pattern == CoordinationPattern.CHAIN:
                            await self._submit_chain_task(task)
                        elif task.coordination_pattern == CoordinationPattern.CHORD:
                            await self._submit_chord_task(task)
                        else:
                            signature = self.signature_adapter.create_signature(task)
                            self.signature_adapter.submit_task(signature)
                        
                        # Update status
                        self._update_task_status(
                            task_id,
                            TaskState.QUEUED,
                            CoordinationState.READY_FOR_EXECUTION
                        )
                        
                    except Exception as e:
                        LOG.error(f"Failed to submit ready task {task_id}: {e}")
                        self._update_task_status(
                            task_id,
                            TaskState.FAILED,
                            CoordinationState.COORDINATION_COMPLETE,
                            error_message=str(e)
                        )
    
    async def _check_completed_groups(self):
        """Check for completed groups and trigger callbacks."""
        
        for group_id, task_ids in self.group_registry.items():
            # Check if all tasks in group are completed
            completed_tasks = 0
            failed_tasks = 0
            
            for task_id in task_ids:
                status = self.task_status_map.get(task_id)
                if status:
                    if status.task_state == TaskState.COMPLETED:
                        completed_tasks += 1
                    elif status.task_state == TaskState.FAILED:
                        failed_tasks += 1
            
            total_tasks = len(task_ids)
            
            if completed_tasks + failed_tasks == total_tasks:
                LOG.info(f"Group {group_id} completed: {completed_tasks} successful, {failed_tasks} failed")
                
                # Update group status
                for task_id in task_ids:
                    status = self.task_status_map.get(task_id)
                    if status and status.coordination_state != CoordinationState.COORDINATION_COMPLETE:
                        self._update_task_status(
                            task_id,
                            status.task_state,
                            CoordinationState.GROUP_COMPLETED
                        )
                
                # Trigger any waiting chord callbacks
                await self._trigger_chord_callbacks(group_id)
    
    async def _trigger_chord_callbacks(self, group_id: str):
        """Trigger chord callback tasks for completed group."""
        
        for task_id, status in self.task_status_map.items():
            if (status.coordination_state == CoordinationState.WAITING_FOR_GROUP and
                self.task_registry[task_id].group_id == group_id):
                
                task = self.task_registry[task_id]
                
                try:
                    # Submit chord callback
                    signature = self.signature_adapter.create_signature(task)
                    self.signature_adapter.submit_task(signature)
                    
                    self._update_task_status(
                        task_id,
                        TaskState.QUEUED,
                        CoordinationState.CALLBACK_TRIGGERED
                    )
                    
                    LOG.info(f"Triggered chord callback task {task_id} for group {group_id}")
                    
                except Exception as e:
                    LOG.error(f"Failed to trigger chord callback {task_id}: {e}")
                    self._update_task_status(
                        task_id,
                        TaskState.FAILED,
                        CoordinationState.COORDINATION_COMPLETE,
                        error_message=str(e)
                    )
    
    async def _update_task_states(self):
        """Update task states based on backend status."""
        # This would query backend systems for task status updates
        # For now, this is a placeholder for backend integration
        pass
    
    async def _cleanup_completed_flows(self):
        """Clean up completed task flows."""
        # Remove completed tasks from active monitoring
        # This helps with memory management for long-running coordinators
        completed_flows = []
        
        for task_id, status in self.task_status_map.items():
            if status.coordination_state == CoordinationState.COORDINATION_COMPLETE:
                completed_flows.append(task_id)
        
        # Archive completed flows (simplified for now)
        if completed_flows:
            LOG.debug(f"Found {len(completed_flows)} completed flows for potential cleanup")
    
    def _update_task_status(self, 
                           task_id: str,
                           task_state: TaskState,
                           coordination_state: CoordinationState,
                           worker_id: Optional[str] = None,
                           error_message: Optional[str] = None,
                           result_data: Optional[Dict[str, Any]] = None):
        """Update task status."""
        
        if task_id not in self.task_status_map:
            LOG.warning(f"Updating status for unknown task: {task_id}")
            return
        
        status = self.task_status_map[task_id]
        
        # Update state
        old_state = status.task_state
        status.task_state = task_state
        status.coordination_state = coordination_state
        
        # Update timing
        current_time = time.time()
        if task_state == TaskState.RUNNING and not status.start_time:
            status.start_time = current_time
        elif task_state in [TaskState.COMPLETED, TaskState.FAILED, TaskState.TIMEOUT]:
            if not status.end_time:
                status.end_time = current_time
        
        # Update other fields
        if worker_id:
            status.worker_id = worker_id
        if error_message:
            status.error_message = error_message
        if result_data:
            status.result_data = result_data
        
        if old_state != task_state:
            LOG.info(f"Task {task_id} state changed: {old_state.value} -> {task_state.value}")
    
    def _save_state(self):
        """Persist coordinator state to disk."""
        try:
            state_data = {
                'task_status_map': {
                    task_id: status.to_dict() 
                    for task_id, status in self.task_status_map.items()
                },
                'group_registry': {
                    group_id: list(task_ids) 
                    for group_id, task_ids in self.group_registry.items()
                },
                'dependency_graph': {
                    task_id: list(deps) 
                    for task_id, deps in self.dependency_graph.items()
                }
            }
            
            with open(self.state_file, 'w') as f:
                json.dump(state_data, f, indent=2)
                
        except Exception as e:
            LOG.error(f"Failed to save coordinator state: {e}")
    
    def _load_state(self):
        """Load coordinator state from disk."""
        try:
            if self.state_file.exists():
                with open(self.state_file, 'r') as f:
                    state_data = json.load(f)
                
                # Restore task status map
                for task_id, status_dict in state_data.get('task_status_map', {}).items():
                    status = TaskStatus(
                        task_id=status_dict['task_id'],
                        task_state=TaskState(status_dict['task_state']),
                        coordination_state=CoordinationState(status_dict['coordination_state']),
                        worker_id=status_dict.get('worker_id'),
                        start_time=status_dict.get('start_time'),
                        end_time=status_dict.get('end_time'),
                        retry_count=status_dict.get('retry_count', 0),
                        error_message=status_dict.get('error_message'),
                        result_data=status_dict.get('result_data'),
                        metadata=status_dict.get('metadata', {})
                    )
                    self.task_status_map[task_id] = status
                
                # Restore group registry
                for group_id, task_ids in state_data.get('group_registry', {}).items():
                    self.group_registry[group_id] = set(task_ids)
                
                # Restore dependency graph
                for task_id, deps in state_data.get('dependency_graph', {}).items():
                    self.dependency_graph[task_id] = set(deps)
                
                LOG.info(f"Loaded coordinator state with {len(self.task_status_map)} tasks")
                
        except Exception as e:
            LOG.error(f"Failed to load coordinator state: {e}")
    
    def get_task_status(self, task_id: str) -> Optional[TaskStatus]:
        """Get current status for a task."""
        return self.task_status_map.get(task_id)
    
    def get_flow_summary(self) -> Dict[str, Any]:
        """Get summary of current task flow."""
        
        state_counts = {}
        for state in TaskState:
            state_counts[state.value] = 0
        
        for status in self.task_status_map.values():
            state_counts[status.task_state.value] += 1
        
        return {
            'total_tasks': len(self.task_status_map),
            'task_states': state_counts,
            'active_groups': len(self.group_registry),
            'monitoring_running': self.running
        }
    
    def stop_monitoring(self):
        """Stop task flow monitoring."""
        self.running = False
        LOG.info("Task flow monitoring stop requested")