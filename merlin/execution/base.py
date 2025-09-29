
"""

"""

from abc import ABC, abstractmethod
from typing import Dict, List

from merlin.dag.models import ExecutionPlan, TaskChain
from merlin.execution.models import ExecutionContext, TaskResult


class TaskExecutor(ABC):
    """Abstract base class for different execution strategies."""
    
    @abstractmethod
    def execute_plan(self, plan: ExecutionPlan, context: ExecutionContext) -> Dict[str, TaskResult]:
        """Execute the entire plan and return results."""
        pass
    
    @abstractmethod
    def execute_chain(self, chain: TaskChain, context: ExecutionContext) -> List[TaskResult]:
        """Execute a single chain of tasks."""
        pass
    
    @abstractmethod
    def execute_task(self, task_name: str, context: ExecutionContext) -> TaskResult:
        """Execute a single task."""
        pass