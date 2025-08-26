##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Backend-agnostic task registry for Merlin.

This module provides a registry system for backend-agnostic task functions
that can be executed by any backend without Celery dependencies.
"""

import logging
from typing import Any, Callable, Dict

from merlin.common.enums import ReturnCode
# Import specific modules that tests expect to patch at module level
from merlin.common.sample_index import SampleIndex  
from merlin.study.step import Step
from merlin.execution.step_executor import GenericStepExecutor

LOG = logging.getLogger(__name__)


class TaskRegistry:
    """Registry for backend-agnostic task implementations."""
    
    def __init__(self):
        self._tasks: Dict[str, Callable] = {}
        self._registered = False
    
    def register(self, name: str, func: Callable):
        """Register a task function."""
        if name in self._tasks:
            LOG.warning(f"Task {name} already registered, overwriting")
        self._tasks[name] = func
        LOG.debug(f"Registered task: {name}")
    
    def get(self, name: str) -> Callable:
        """Get registered task function, registering tasks if needed."""
        if not self._registered:
            self._register_tasks_on_demand()
        return self._tasks.get(name)
    
    def list_tasks(self) -> list:
        """Get list of registered task names."""
        if not self._registered:
            self._register_tasks_on_demand()
        return list(self._tasks.keys())
    
    def task(self, name: str):
        """Decorator for registering tasks."""
        def decorator(func):
            self.register(name, func)
            return func
        return decorator
    
    def unregister(self, name: str):
        """Unregister a task function."""
        if name in self._tasks:
            del self._tasks[name]
            LOG.debug(f"Unregistered task: {name}")

    def _register_tasks_on_demand(self):
        """Register all generic task implementations when needed."""
        if self._registered:
            return  # Already registered
        
        # Register tasks that might have import dependencies
        try:
            # Try to import optional dependencies that might not be available
            import importlib
            importlib.import_module('merlin.execution.step_executor')
            
            @self.task("merlin_step")
            def generic_merlin_step_implementation(step: Step, adapter_config: Dict[str, str], 
                                                 retry_count: int = 0, max_retries: int = None,
                                                 next_in_chain: Step = None, **kwargs) -> Any:
                """Generic merlin step implementation using extracted logic."""
                from merlin.execution.step_executor import GenericStepExecutor  # pylint: disable=C0415
                executor = GenericStepExecutor()
                result = executor.execute_step(step, adapter_config, retry_count, max_retries, next_in_chain)
                
                # For compatibility, return the return_code like the original function
                return result.return_code

            LOG.debug(f"Registered {len(self._tasks)} generic tasks")
            
        except ImportError as e:
            LOG.warning(f"Could not register some tasks due to missing dependencies: {e}")

        # Always register simple tasks that don't have dependencies
        @self.task("chordfinisher")
        def generic_chordfinisher_implementation(*args, **kwargs) -> str:
            """Generic chord synchronization."""
            return "SYNC"
        
        self._registered = True


# Global task registry for generic tasks
task_registry = TaskRegistry()