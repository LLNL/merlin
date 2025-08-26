##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Universal task definition format for backend independence.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional, Union
from enum import Enum
import json
import time


def _get_current_timestamp() -> float:
    """Get current timestamp - separated for easier mocking in tests."""
    return time.time()


class TaskType(Enum):
    """Standard task types supported across backends."""
    MERLIN_STEP = "merlin_step"
    EXPAND_SAMPLES = "expand_samples"
    CHORD_FINISHER = "chord_finisher"
    GROUP_COORDINATOR = "group_coordinator"
    CHAIN_EXECUTOR = "chain_executor"
    SHUTDOWN_WORKER = "shutdown_worker"


class CoordinationPattern(Enum):
    """Task coordination patterns."""
    SIMPLE = "simple"           # Single task execution
    GROUP = "group"             # Parallel execution, wait for all
    CHAIN = "chain"             # Sequential execution
    CHORD = "chord"             # Parallel execution + callback
    MAP_REDUCE = "map_reduce"   # Map phase + reduce phase


@dataclass
class TaskDependency:
    """Represents a task dependency."""
    task_id: str
    dependency_type: str = "completion"  # completion, success, data
    timeout_seconds: Optional[int] = None


@dataclass
class UniversalTaskDefinition:
    """Backend-agnostic task definition."""
    
    # Core identification
    task_id: str
    task_type: TaskType
    
    # Execution parameters
    script_reference: Optional[str] = None
    config_reference: Optional[str] = None
    workspace_reference: Optional[str] = None
    
    # Data references (for large data objects)
    input_data_references: List[str] = field(default_factory=list)
    output_data_references: List[str] = field(default_factory=list)
    
    # Coordination
    coordination_pattern: CoordinationPattern = CoordinationPattern.SIMPLE
    dependencies: List[TaskDependency] = field(default_factory=list)
    group_id: Optional[str] = None
    callback_task: Optional[str] = None
    
    # Execution context
    queue_name: str = "default"
    priority: int = 0
    retry_limit: int = 3
    timeout_seconds: int = 3600
    
    # Metadata
    created_timestamp: float = field(default_factory=_get_current_timestamp)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'task_id': self.task_id,
            'task_type': self.task_type.value,
            'script_reference': self.script_reference,
            'config_reference': self.config_reference,
            'workspace_reference': self.workspace_reference,
            'input_data_references': self.input_data_references,
            'output_data_references': self.output_data_references,
            'coordination_pattern': self.coordination_pattern.value,
            'dependencies': [
                {'task_id': dep.task_id, 'type': dep.dependency_type, 'timeout': dep.timeout_seconds}
                for dep in self.dependencies
            ],
            'group_id': self.group_id,
            'callback_task': self.callback_task,
            'queue_name': self.queue_name,
            'priority': self.priority,
            'retry_limit': self.retry_limit,
            'timeout_seconds': self.timeout_seconds,
            'created_timestamp': self.created_timestamp,
            'metadata': self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'UniversalTaskDefinition':
        """Create from dictionary."""
        # Convert enums
        task_type = TaskType(data['task_type'])
        coordination_pattern = CoordinationPattern(data['coordination_pattern'])
        
        # Convert dependencies
        dependencies = [
            TaskDependency(dep['task_id'], dep['type'], dep.get('timeout'))
            for dep in data.get('dependencies', [])
        ]
        
        return cls(
            task_id=data['task_id'],
            task_type=task_type,
            script_reference=data.get('script_reference'),
            config_reference=data.get('config_reference'),
            workspace_reference=data.get('workspace_reference'),
            input_data_references=data.get('input_data_references', []),
            output_data_references=data.get('output_data_references', []),
            coordination_pattern=coordination_pattern,
            dependencies=dependencies,
            group_id=data.get('group_id'),
            callback_task=data.get('callback_task'),
            queue_name=data.get('queue_name', 'default'),
            priority=data.get('priority', 0),
            retry_limit=data.get('retry_limit', 3),
            timeout_seconds=data.get('timeout_seconds', 3600),
            created_timestamp=data.get('created_timestamp', time.time()),
            metadata=data.get('metadata', {})
        )
    
    def get_size_bytes(self) -> int:
        """Calculate definition size in bytes."""
        return len(json.dumps(self.to_dict()).encode('utf-8'))