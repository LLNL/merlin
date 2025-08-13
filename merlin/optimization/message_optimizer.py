##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Optimize message sizes for Kafka backend through reference-based data passing.

This module provides message size optimization to achieve the <1KB target
for Kafka message transport while maintaining all necessary task execution
information through external storage references.
"""

import json
import gzip
import base64
import time
import uuid
from typing import Dict, Any, Optional, Union
from pathlib import Path
from dataclasses import dataclass, asdict


@dataclass
class OptimizedTaskMessage:
    """Optimized task message for Kafka backend."""
    task_id: str
    task_type: str
    script_reference: str
    config_reference: str
    workspace_reference: str
    sample_range: Optional[tuple] = None
    priority: int = 0
    retry_count: int = 0
    created_timestamp: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return asdict(self)
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'OptimizedTaskMessage':
        """Create from dictionary."""
        return cls(**data)
    
    def get_size_bytes(self) -> int:
        """Calculate message size in bytes."""
        return len(self.to_json().encode('utf-8'))


class MessageOptimizer:
    """Optimize message sizes for Kafka transport."""
    
    def __init__(self, shared_storage_path: str = "/shared/storage"):
        self.shared_storage_path = Path(shared_storage_path)
        self.target_message_size = 1024  # 1KB target (as per architecture docs)
        self.max_message_size = 4096     # 4KB absolute maximum
    
    def optimize_celery_task_message(self, 
                                   task_type: str,
                                   task_args: tuple,
                                   task_kwargs: Dict[str, Any],
                                   task_id: Optional[str] = None) -> OptimizedTaskMessage:
        """Convert Celery task to optimized Kafka message."""
        
        # Generate task ID if not provided
        if not task_id:
            task_id = str(uuid.uuid4())
        
        # Extract step configuration from args/kwargs
        step_config = self._extract_step_config(task_args, task_kwargs)
        
        # Generate script and config references
        from merlin.execution.script_generator import TaskScriptGenerator, ScriptConfig
        
        generator = TaskScriptGenerator(str(self.shared_storage_path))
        
        script_config = ScriptConfig(
            task_id=task_id,
            task_type=task_type,
            workspace_path=str(self.shared_storage_path / "workspace" / task_id),
            step_config=step_config,
            environment_vars=self._extract_environment_vars(task_kwargs)
        )
        
        # Generate script files
        script_info = generator.generate_merlin_step_script(script_config)
        
        # Create optimized message
        optimized_msg = OptimizedTaskMessage(
            task_id=task_id,
            task_type=task_type,
            script_reference=script_info['script_filename'],
            config_reference=script_info['config_filename'],
            workspace_reference=f"workspace/{task_id}",
            sample_range=self._extract_sample_range(task_kwargs),
            priority=task_kwargs.get('priority', 0),
            created_timestamp=time.time()
        )
        
        # Verify message size
        message_size = optimized_msg.get_size_bytes()
        if message_size > self.max_message_size:
            raise ValueError(f"Optimized message size {message_size} exceeds maximum {self.max_message_size}")
        
        return optimized_msg
    
    def _extract_step_config(self, task_args: tuple, task_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Extract step configuration from task arguments."""
        
        # Handle different task argument patterns
        if task_args and hasattr(task_args[0], '__dict__'):
            # First argument is a step object
            step = task_args[0]
            return {
                'name': getattr(step, 'name', 'unknown'),
                'run': getattr(step, 'run', {}),
                'study': getattr(step, 'study', None),
                'step_type': type(step).__name__
            }
        elif 'step' in task_kwargs:
            # Step provided in kwargs
            step = task_kwargs['step']
            return {
                'name': getattr(step, 'name', 'unknown'),
                'run': getattr(step, 'run', {}),
                'study': getattr(step, 'study', None),
                'step_type': type(step).__name__
            }
        else:
            # Fallback to minimal config
            return {
                'name': task_kwargs.get('step_name', 'unknown'),
                'run': task_kwargs.get('run_config', {}),
                'study': task_kwargs.get('study_id', None)
            }
    
    def _extract_environment_vars(self, task_kwargs: Dict[str, Any]) -> Dict[str, str]:
        """Extract environment variables from task kwargs."""
        env_vars = {}
        
        # Extract adapter config environment
        adapter_config = task_kwargs.get('adapter_config', {})
        if isinstance(adapter_config, dict):
            env_vars.update(adapter_config.get('env_vars', {}))
        
        # Extract direct environment variables
        env_vars.update(task_kwargs.get('env_vars', {}))
        
        return env_vars
    
    def _extract_sample_range(self, task_kwargs: Dict[str, Any]) -> Optional[tuple]:
        """Extract sample range from task kwargs."""
        if 'sample_range' in task_kwargs:
            return tuple(task_kwargs['sample_range'])
        elif 'samples' in task_kwargs:
            samples = task_kwargs['samples']
            if isinstance(samples, (list, tuple)) and len(samples) == 2:
                return tuple(samples)
        return None
    
    def compress_large_data(self, data: Any) -> str:
        """Compress large data objects for reference storage."""
        json_data = json.dumps(data)
        compressed = gzip.compress(json_data.encode('utf-8'))
        return base64.b64encode(compressed).decode('ascii')
    
    def decompress_data(self, compressed_data: str) -> Any:
        """Decompress reference data."""
        compressed_bytes = base64.b64decode(compressed_data.encode('ascii'))
        json_data = gzip.decompress(compressed_bytes).decode('utf-8')
        return json.loads(json_data)
    
    def calculate_optimization_ratio(self, original_size: int, optimized_size: int) -> float:
        """Calculate optimization ratio as percentage reduction."""
        return ((original_size - optimized_size) / original_size) * 100.0