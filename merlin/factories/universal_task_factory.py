##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Universal task factory for creating backend-agnostic tasks.
"""

import uuid
import time
import json
from typing import Dict, Any, List, Optional, Union
from pathlib import Path

from merlin.factories.task_definition import (
    UniversalTaskDefinition, TaskType, CoordinationPattern, TaskDependency
)
from merlin.execution.script_generator import TaskScriptGenerator, ScriptConfig


class UniversalTaskFactory:
    """Create tasks that work across all backends."""
    
    def __init__(self, shared_storage_path: str = "/shared/storage"):
        self.shared_storage_path = Path(shared_storage_path)
        self.script_generator = TaskScriptGenerator(str(shared_storage_path))
        
    def create_merlin_step_task(self,
                               step_config: Dict[str, Any],
                               adapter_config: Optional[Dict[str, Any]] = None,
                               queue_name: str = "default",
                               priority: int = 0,
                               task_id: Optional[str] = None) -> UniversalTaskDefinition:
        """Create a merlin_step task."""
        
        if not task_id:
            step_name = step_config.get('name', 'step')
            task_id = f"{step_name}_{str(uuid.uuid4())}"
        
        # Generate execution script
        script_config = ScriptConfig(
            task_id=task_id,
            task_type=TaskType.MERLIN_STEP.value,
            workspace_path=str(self.shared_storage_path / "workspace" / task_id),
            step_config=step_config,
            environment_vars=adapter_config.get('env_vars', {}) if adapter_config else {}
        )
        
        script_info = self.script_generator.generate_merlin_step_script(script_config)
        
        # Create task definition
        task_def = UniversalTaskDefinition(
            task_id=task_id,
            task_type=TaskType.MERLIN_STEP,
            script_reference=script_info['script_filename'],
            config_reference=script_info['config_filename'],
            workspace_reference=f"workspace/{task_id}",
            queue_name=queue_name,
            priority=priority,
            metadata={
                'step_name': step_config.get('name', 'unknown'),
                'adapter_config': adapter_config or {}
            }
        )
        
        return task_def
    
    def create_sample_expansion_task(self,
                                   study_id: str,
                                   step_name: str,
                                   sample_range: tuple,
                                   samples_reference: str,
                                   queue_name: str = "default",
                                   task_id: Optional[str] = None) -> UniversalTaskDefinition:
        """Create a sample expansion task."""
        
        if not task_id:
            task_id = f"{study_id}_{step_name}_{sample_range[0]}_{sample_range[1]}"
        
        # Generate expansion script
        script_config = ScriptConfig(
            task_id=task_id,
            task_type=TaskType.EXPAND_SAMPLES.value,
            workspace_path=str(self.shared_storage_path / "workspace" / task_id),
            step_config={
                'study_id': study_id,
                'step_name': step_name,
                'sample_range': sample_range,
                'samples_reference': samples_reference
            },
            environment_vars={}
        )
        
        script_info = self.script_generator.generate_sample_expansion_script(script_config, sample_range)
        
        # Create task definition
        task_def = UniversalTaskDefinition(
            task_id=task_id,
            task_type=TaskType.EXPAND_SAMPLES,
            script_reference=script_info['script_filename'],
            config_reference=script_info['config_filename'],
            workspace_reference=f"workspace/{task_id}",
            input_data_references=[samples_reference],
            queue_name=queue_name,
            metadata={
                'study_id': study_id,
                'step_name': step_name,
                'sample_range': sample_range
            }
        )
        
        return task_def
    
    def create_group_tasks(self,
                          task_definitions: List[UniversalTaskDefinition],
                          callback_task: Optional[UniversalTaskDefinition] = None,
                          group_id: Optional[str] = None) -> List[UniversalTaskDefinition]:
        """Create a group of parallel tasks with optional callback."""
        
        if not group_id:
            group_id = str(uuid.uuid4())
        
        # Update all tasks with group information
        group_tasks = []
        for task_def in task_definitions:
            task_def.coordination_pattern = CoordinationPattern.GROUP
            task_def.group_id = group_id
            if callback_task:
                task_def.callback_task = callback_task.task_id
            group_tasks.append(task_def)
        
        # Add callback task if provided
        if callback_task:
            callback_task.coordination_pattern = CoordinationPattern.CHORD
            callback_task.group_id = group_id
            callback_task.dependencies = [
                TaskDependency(task.task_id, "completion") for task in task_definitions
            ]
            group_tasks.append(callback_task)
        
        return group_tasks
    
    def create_chain_tasks(self,
                          task_definitions: List[UniversalTaskDefinition]) -> List[UniversalTaskDefinition]:
        """Create a chain of sequential tasks."""
        
        chain_id = str(uuid.uuid4())
        
        # Set up dependencies for sequential execution
        for i, task_def in enumerate(task_definitions):
            task_def.coordination_pattern = CoordinationPattern.CHAIN
            task_def.group_id = chain_id
            
            if i > 0:
                # Each task depends on the previous one
                prev_task = task_definitions[i-1]
                task_def.dependencies = [TaskDependency(prev_task.task_id, "success")]
        
        return task_definitions
    
    def create_chord_tasks(self,
                          parallel_tasks: List[UniversalTaskDefinition],
                          callback_task: UniversalTaskDefinition) -> List[UniversalTaskDefinition]:
        """Create chord pattern: parallel tasks + callback."""
        
        return self.create_group_tasks(parallel_tasks, callback_task)