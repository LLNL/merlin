

"""

"""

import time
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Dict, List

from merlin.dag.models import ExecutionPlan, TaskChain
from merlin.execution.base import TaskExecutor
from merlin.execution.models import ExecutionContext, TaskResult, TaskStatus


class LocalExecutor(TaskExecutor):
    """Local executor that runs tasks in the current process."""
    
    def __init__(self, task_runner: Callable = None):
        self.task_runner = task_runner or self._default_task_runner
    
    def execute_plan(self, plan: ExecutionPlan, context: ExecutionContext) -> Dict[str, TaskResult]:
        """Execute plan locally."""
        all_results = {}
        
        for level in plan.levels:
            print(f"Executing depth {level.depth} locally...")
            
            # Execute chains in parallel using ThreadPoolExecutor
            with ThreadPoolExecutor(max_workers=len(level.parallel_chains)) as executor:
                futures = []
                for chain in level.parallel_chains:
                    future = executor.submit(self.execute_chain, chain, context)
                    futures.append(future)
                
                # Collect results
                for future in futures:
                    chain_results = future.result()
                    for result in chain_results:
                        all_results[result.task_name] = result
        
        return all_results
    
    def execute_chain(self, chain: TaskChain, context: ExecutionContext) -> List[TaskResult]:
        """Execute chain locally."""
        results = []
        for task_name in chain.tasks:
            result = self.execute_task(task_name, context)
            results.append(result)
        return results
    
    def execute_task(self, task_name: str, context: ExecutionContext) -> TaskResult:
        """Execute task locally."""
        try:
            start_time = time.time()
            result = self.task_runner(task_name, context)
            end_time = time.time()
            
            return TaskResult(
                task_name=task_name,
                status=TaskStatus.COMPLETED,
                start_time=start_time,
                end_time=end_time,
                result=result
            )
        except Exception as e:
            return TaskResult(
                task_name=task_name,
                status=TaskStatus.FAILED,
                error=str(e)
            )
    
    def _default_task_runner(self, task_name: str, context: ExecutionContext):
        """Default task runner - just simulates work."""
        print(f"  Running {task_name} locally...")
        step_to_execute = context.study.dag.step(task_name)
        # TODO when we create Batch class, use that instead of adapter_config
        adapter_config = context.study.get_adapter_config(override_type="local")
        result = step_to_execute.execute(adapter_config)
        return result
