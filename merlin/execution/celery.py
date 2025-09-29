

"""

"""

import time
import uuid
from typing import Dict, List

from merlin.dag.models import ExecutionLevel, ExecutionPlan, TaskChain
from merlin.execution.base import TaskExecutor
from merlin.execution.models import ExecutionContext, TaskResult, TaskStatus


class CeleryExecutor(TaskExecutor):
    """Celery-based task executor."""
    
    def __init__(self, default_queue: str = "default"):
        from merlin.celery import app
        self.celery_app = app
        self.default_queue = default_queue
        self.active_tasks = {}  # Track running tasks
    
    def execute_plan(self, plan: ExecutionPlan, context: ExecutionContext) -> Dict[str, TaskResult]:
        """
        Execute the plan level by level, respecting dependencies.
        """
        all_results = {}
        
        for level in plan.levels:
            print(f"Executing depth {level.depth} with {len(level.parallel_chains)} parallel chains...")
            
            # Execute all chains in this level in parallel
            level_results = self._execute_level_parallel(level, context)
            all_results.update(level_results)
            
            # Check if any tasks failed - decide whether to continue
            failed_tasks = [name for name, result in level_results.items() 
                           if result.status == TaskStatus.FAILED]
            
            if failed_tasks:
                print(f"Tasks failed at depth {level.depth}: {failed_tasks}")
                # Could implement different failure strategies here
                # For now, let's continue but mark dependent tasks as skipped
                self._mark_dependent_tasks_skipped(plan, level.depth, all_results)
                break
        
        return all_results
    
    def _execute_level_parallel(self, level: ExecutionLevel, context: ExecutionContext) -> Dict[str, TaskResult]:
        """Execute all chains in a level in parallel."""
        level_results = {}
        
        # Submit all chains to Celery
        chain_futures = []
        for chain in level.parallel_chains:
            future = self._submit_chain_to_celery(chain, context)
            chain_futures.append((chain, future))
        
        # Wait for all chains to complete
        for chain, future in chain_futures:
            try:
                chain_results = future.get(timeout=3600)  # 1 hour timeout
                for result in chain_results:
                    level_results[result.task_name] = result
            except Exception as e:
                # Mark all tasks in chain as failed
                for task in chain.tasks:
                    level_results[task] = TaskResult(
                        task_name=task,
                        status=TaskStatus.FAILED,
                        error=str(e)
                    )
        
        return level_results
    
    def _submit_chain_to_celery(self, chain: TaskChain, context: ExecutionContext):
        """Submit a chain to Celery as a chain of tasks."""
        # This would use Celery's chain primitive
        # Simplified example:
        celery_chain = self._build_celery_chain(chain, context)
        return celery_chain.apply_async(queue=self.default_queue)
    
    def _build_celery_chain(self, chain: TaskChain, context: ExecutionContext):
        """Build a Celery chain from a TaskChain."""
        # This is where you'd integrate with your actual Celery tasks
        # Simplified mock implementation
        from celery import chain as celery_chain
        
        # Assuming you have a generic Celery task that can execute any step
        celery_tasks = []
        for task_name in chain.tasks:
            celery_tasks.append(
                self.celery_app.signature(
                    'merlin:execute_step',
                    args=[task_name, context.study_name, context.parameter_info],
                    queue=self.default_queue
                )
            )
        
        return celery_chain(*celery_tasks)
    
    def _mark_dependent_tasks_skipped(self, plan: ExecutionPlan, failed_depth: int, results: Dict[str, TaskResult]):
        """Mark tasks that depend on failed tasks as skipped."""
        for level in plan.levels:
            if level.depth > failed_depth:
                for task in level.get_all_tasks():
                    results[task] = TaskResult(
                        task_name=task,
                        status=TaskStatus.SKIPPED,
                        error="Dependency failed"
                    )
    
    # TODO not sure if these will even be needed for Celery
    # def execute_chain(self, chain: TaskChain, context: ExecutionContext) -> List[TaskResult]:
    #     """Execute a single chain."""
    #     results = []
    #     for task_name in chain.tasks:
    #         result = self.execute_task(task_name, context)
    #         results.append(result)
            
    #         # Stop chain if task failed
    #         if result.status == TaskStatus.FAILED:
    #             break
                
    #     return results
    
    # def execute_task(self, task_name: str, context: ExecutionContext) -> TaskResult:
    #     """Execute a single task via Celery."""
    #     celery_id = str(uuid.uuid4())
        
    #     try:
    #         # Submit to Celery
    #         async_result = self.celery_app.send_task(
    #             'merlin.execute_step',
    #             args=[task_name, context.study_name, context.parameter_info],
    #             task_id=celery_id,
    #             queue=self.default_queue
    #         )
            
    #         # Wait for result
    #         start_time = time.time()
    #         result = async_result.get(timeout=1800)  # 30 min timeout
    #         end_time = time.time()
            
    #         return TaskResult(
    #             task_name=task_name,
    #             status=TaskStatus.COMPLETED,
    #             start_time=start_time,
    #             end_time=end_time,
    #             result=result,
    #             celery_id=celery_id
    #         )
            
    #     except Exception as e:
    #         return TaskResult(
    #             task_name=task_name,
    #             status=TaskStatus.FAILED,
    #             error=str(e),
    #             celery_id=celery_id
    #         )