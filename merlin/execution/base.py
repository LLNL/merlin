##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

""" """

from abc import ABC, abstractmethod
from typing import Dict, List

from merlin.dag.models import ExecutionPlan, TaskChain
from merlin.execution.models import ExecutionContext, TaskResult


class TaskExecutor(ABC):
    """Abstract base class for different execution strategies."""

    @abstractmethod
    def execute_plan(self, plan: ExecutionPlan, context: ExecutionContext, wait: bool = False, timeout: int = 7200) -> Dict:
        """
        Execute the entire plan and return results.

        Args:
            plan: Execution plan to execute
            context: Execution context
            wait: If True, block until execution completes. Default: False
            timeout: Timeout in seconds when using wait=True. Default: 7200

        Returns:
            Dictionary containing results and execution information
        """
        pass

    @abstractmethod
    def execute_chain(self, chain: TaskChain, context: ExecutionContext) -> List[TaskResult]:
        """Execute a single chain of tasks."""
        pass

    @abstractmethod
    def execute_task(self, task_name: str, context: ExecutionContext) -> TaskResult:
        """Execute a single task."""
        pass
