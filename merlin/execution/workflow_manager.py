

"""

"""

import time
import uuid
from typing import Dict

from merlin.execution.base import TaskExecutor
from merlin.execution.models import ExecutionContext, TaskResult, TaskStatus
from merlin.study.study import MerlinStudy


class WorkflowManager:
    """High-level workflow manager that ties everything together."""
    
    def __init__(self, study: MerlinStudy, executor: TaskExecutor):
        self.study = study
        self.dag = self.study.dag
        self.executor = executor
    
    def run_workflow(self, source_node: str = "_source") -> Dict[str, TaskResult]:
        """Run the complete workflow."""
        print("=== WORKFLOW EXECUTION ===")
        
        # 1. Generate execution plan
        print("Generating execution plan...")
        execution_plan = self.dag.group_tasks(source_node)
        
        print(f"Plan generated: {len(execution_plan.levels)} levels")
        for level in execution_plan.levels:
            print(f"  Depth {level.depth}: {len(level.parallel_chains)} chains")
            for chain in level.parallel_chains:
                print(f"    {chain}")
        
        # 2. Create execution context
        context = ExecutionContext(
            study=self.study,
            parameter_info=self.dag.parameter_info,
            execution_id=str(uuid.uuid4()),
            metadata={"started_at": time.time()}  # TODO not sure what to do with metadata yet
        )
        
        # 3. Execute the plan
        print("\nExecuting plan...")
        results = self.executor.execute_plan(execution_plan, context)
        
        # 4. Report results
        print("\n=== EXECUTION RESULTS ===")
        completed = sum(1 for r in results.values() if r.status == TaskStatus.COMPLETED)
        failed = sum(1 for r in results.values() if r.status == TaskStatus.FAILED)
        
        print(f"Completed: {completed}, Failed: {failed}, Total: {len(results)}")
        
        return results