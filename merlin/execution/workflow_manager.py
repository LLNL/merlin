##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

""" """

import time
import uuid
from typing import Dict

from merlin.dag.dag import DAG as ExecutionDAG
from merlin.execution.base import TaskExecutor
from merlin.execution.models import ExecutionContext, TaskStatus
from merlin.study.study import MerlinStudy


class WorkflowManager:
    """High-level workflow manager that ties everything together."""

    def __init__(self, study: MerlinStudy, executor: TaskExecutor):
        self.study = study
        # Create a new DAG using the execution framework's DAG class
        # This allows the old study.dag to remain unchanged for backwards compatibility
        old_dag = study.dag
        self.dag = ExecutionDAG(
            old_dag.maestro_adjacency_table,
            old_dag.maestro_values,
            old_dag.column_labels,
            old_dag.study_name,
            old_dag.parameter_info,
        )
        self.executor = executor

    def run_workflow(self, source_node: str = "_source", wait: bool = False, timeout: int = 7200) -> Dict:
        """
        Run the complete workflow.

        Args:
            source_node: Starting node for workflow execution. Default: "_source"
            wait: If True, block until workflow completes. Default: False (non-blocking)
            timeout: Timeout in seconds when using wait=True. Default: 7200 (2 hours)

        Returns:
            Dictionary containing execution results and workflow information.
            For CeleryExecutor, includes: 'results', 'async_result', 'workflow_id'
        """
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
            metadata={"started_at": time.time()},  # TODO not sure what to do with metadata yet
        )

        # 3. Execute the plan
        print("\nExecuting plan...")
        result = self.executor.execute_plan(execution_plan, context, wait=wait, timeout=timeout)

        # 4. Report results (handle both old and new return formats)
        if isinstance(result, dict) and "results" in result:
            # New format from CeleryExecutor
            results = result["results"]
        else:
            # Old format (just results dict)
            results = result

        print("\n=== EXECUTION RESULTS ===")
        completed = sum(1 for r in results.values() if r.status == TaskStatus.COMPLETED)
        failed = sum(1 for r in results.values() if r.status == TaskStatus.FAILED)

        print(f"Completed: {completed}, Failed: {failed}, Total: {len(results)}")

        return result if isinstance(result, dict) and "results" in result else {"results": result}
