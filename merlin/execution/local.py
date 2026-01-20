##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
LocalExecutor with sample expansion support.
"""

import json
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List

from merlin.common.enums import ReturnCode
from merlin.dag.models import ExecutionPlan, TaskChain
from merlin.execution.base import TaskExecutor
from merlin.execution.models import ExecutionContext, TaskResult, TaskStatus
from merlin.execution.sample_expander import SampleExpander


# Success return codes that indicate task completed successfully
# SOFT_FAIL is included because it allows dependent tasks to continue
SUCCESS_CODES = {ReturnCode.OK, ReturnCode.DRY_OK, ReturnCode.SOFT_FAIL}


def write_status(status_file: str, status: str, return_code=None, elapsed_time=None):
    """Write status information to a JSON file."""
    status_data = {"status": status, "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")}
    if return_code is not None:
        status_data["return_code"] = return_code
    if elapsed_time is not None:
        status_data["elapsed_time"] = elapsed_time

    with open(status_file, "w") as f:
        json.dump(status_data, f, indent=2)


class LocalExecutor(TaskExecutor):
    """Local process pool executor with sample expansion support."""

    def __init__(self, max_workers: int = 4):
        """
        Initialize LocalExecutor.

        Args:
            max_workers: Maximum number of parallel worker processes (default: 4)
        """
        self.sample_expander = SampleExpander()
        self.max_workers = max_workers

    def execute_plan(
        self, plan: ExecutionPlan, context: ExecutionContext, wait: bool = True, timeout: int = 7200
    ) -> Dict[str, TaskResult]:
        """
        Execute plan level-by-level using local process pool.

        Args:
            plan: Execution plan to execute
            context: Execution context
            wait: Ignored for LocalExecutor (always blocks). Included for API compatibility.
            timeout: Ignored for LocalExecutor. Included for API compatibility.

        Returns:
            Dictionary mapping task names to TaskResults
        """
        all_results = {}

        # Create process pool
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            for level in plan.levels:
                print(f"Executing depth {level.depth} with {len(level.parallel_chains)} parallel chains...")

                # Execute level and wait for completion
                level_results = self._execute_level_parallel(level, context, executor)
                all_results.update(level_results)

                # Check for failures
                failed = [k for k, v in level_results.items() if v.status == TaskStatus.FAILED]
                if failed:
                    print(f"Level {level.depth} had failures: {failed}")
                    print("Stopping execution due to failures")
                    break

        return all_results

    def _execute_level_parallel(
        self, level, context: ExecutionContext, executor: ProcessPoolExecutor
    ) -> Dict[str, TaskResult]:
        """
        Execute all chains in a level using process pool.

        Args:
            level: ExecutionLevel to execute
            context: Execution context
            executor: ProcessPoolExecutor to use

        Returns:
            Dictionary mapping task names to TaskResults
        """
        level_results = {}

        for chain in level.parallel_chains:
            # Skip virtual nodes
            has_real_tasks = self._has_real_tasks(chain, context)
            if not has_real_tasks:
                for task in chain.tasks:
                    level_results[task] = TaskResult(task_name=task, status=TaskStatus.SKIPPED, error="Virtual node")
                continue

            # Expand chain with samples
            expanded_positions = self.sample_expander.expand_chain(chain, context)

            # Log expansion details
            total_expanded = sum(len(pos) for pos in expanded_positions)
            chain_name = chain.tasks[0] if chain.tasks else "unknown"
            print(
                f"  Chain '{chain_name}' expanded to {total_expanded} tasks "
                f"across {len(expanded_positions)} positions"
            )

            # Execute chain with dependencies (sequential positions, parallel samples)
            position_results = self._execute_chain_with_dependencies(expanded_positions, context, executor)
            level_results.update(position_results)

        return level_results

    def _execute_chain_with_dependencies(
        self, expanded_positions: List[List[Dict]], context: ExecutionContext, executor: ProcessPoolExecutor
    ) -> Dict[str, TaskResult]:
        """
        Execute a chain with multiple positions sequentially.

        Each position must complete before the next position starts.
        Within each position, tasks execute in parallel.

        Args:
            expanded_positions: 2D structure [[pos0_tasks], [pos1_tasks], ...]
            context: Execution context
            executor: ProcessPoolExecutor to use

        Returns:
            Dictionary mapping task names to TaskResults
        """
        all_results = {}
        adapter_config = context.study.get_adapter_config(override_type="local")
        # Add task_server field so Step._update_status_file knows we're running locally
        adapter_config["task_server"] = "local"

        # Execute each position sequentially
        for position_idx, position_tasks in enumerate(expanded_positions):
            print(f"    Executing position {position_idx} with {len(position_tasks)} tasks...")

            # Submit all tasks at this position (parallel)
            position_futures = {}
            for task_info in position_tasks:
                future = executor.submit(self._execute_step_wrapper, task_info["step"], adapter_config)
                position_futures[future] = task_info

            # Wait for this position to complete before moving to next
            for future in as_completed(position_futures):
                task_info = position_futures[future]
                task_name = task_info["step"].name()

                try:
                    return_code = future.result(timeout=3600)  # 1 hour timeout per task

                    # Check for success codes (OK=0, DRY_OK=103, etc.)
                    if return_code in SUCCESS_CODES or return_code in {rc.value for rc in SUCCESS_CODES}:
                        all_results[task_name] = TaskResult(
                            task_name=task_name, status=TaskStatus.COMPLETED, result=return_code
                        )
                    else:
                        all_results[task_name] = TaskResult(
                            task_name=task_name,
                            status=TaskStatus.FAILED,
                            error=f"Task returned non-zero exit code: {return_code}",
                        )
                except Exception as e:
                    all_results[task_name] = TaskResult(task_name=task_name, status=TaskStatus.FAILED, error=str(e))

        return all_results

    @staticmethod
    def _execute_step_wrapper(step, adapter_config):
        """
        Wrapper for executing a step in a subprocess.

        This is a static method because it needs to be pickleable
        for process pool execution.

        Args:
            step: Step object to execute
            adapter_config: Adapter configuration

        Returns:
            Return code (0 for success, non-zero for failure)
        """
        import os
        import traceback

        from merlin.common.enums import ReturnCode

        try:
            # Get workspace
            workspace = step.get_workspace()
            step_name = step.name()

            # Check if already completed
            finished_file = f"{workspace}/MERLIN_FINISHED"
            if os.path.exists(finished_file):
                import logging

                LOG = logging.getLogger(__name__)
                LOG.info(f"Skipping step '{step_name}' in '{workspace}' (already finished).")
                return ReturnCode.OK

            # Get max_retries from step (default to 10 if not specified)
            try:
                max_retries = step.max_retries
            except (AttributeError, KeyError):
                max_retries = 10

            # Execute step with retry logic for RESTART
            retry_count = 0
            return_code = None
            while retry_count <= max_retries:
                # Execute step (handles script generation and execution internally)
                return_code = step.execute(adapter_config)

                # Check if we need to restart
                if return_code == ReturnCode.RESTART or return_code == ReturnCode.RESTART.value:
                    retry_count += 1
                    import logging

                    LOG = logging.getLogger(__name__)
                    LOG.info(f"Step '{step_name}' requested restart (attempt {retry_count}/{max_retries})")

                    # Check if max retries exceeded
                    if retry_count > max_retries:
                        LOG.info(f"Step '{step_name}' exceeded max retries ({max_retries}), returning SOFT_FAIL")
                        return_code = ReturnCode.SOFT_FAIL
                        break

                    # Mark step for restart and continue loop
                    step.restart = True
                    continue

                # Not a restart - break out of loop
                break

            # Touch MERLIN_FINISHED if successful (OK, DRY_OK, or SOFT_FAIL)
            if return_code in (
                ReturnCode.OK,
                ReturnCode.OK.value,
                ReturnCode.DRY_OK,
                ReturnCode.DRY_OK.value,
                ReturnCode.SOFT_FAIL,
                ReturnCode.SOFT_FAIL.value,
            ):
                open(finished_file, "w").close()

            return return_code

        except Exception as e:
            # Log exception
            import logging

            LOG = logging.getLogger(__name__)
            LOG.error(f"Error executing step {step.name()}: {e}")
            LOG.debug(traceback.format_exc())

            # Re-raise to let the caller handle it
            raise

    def _has_real_tasks(self, chain: TaskChain, context: ExecutionContext) -> bool:
        """
        Check if a chain has real tasks (not just virtual nodes).

        Args:
            chain: TaskChain to check
            context: Execution context

        Returns:
            True if chain has real tasks, False if only virtual nodes
        """
        for task in chain.tasks:
            try:
                step = context.study.dag.step(task)
                if step is not None:
                    return True
            except (AttributeError, KeyError, TypeError):
                pass
        return False

    def execute_chain(self, chain: TaskChain, context: ExecutionContext) -> List[TaskResult]:
        """Execute chain locally (legacy method for compatibility)."""
        results = []
        for task_name in chain.tasks:
            result = self.execute_task(task_name, context)
            results.append(result)
        return results

    def execute_task(self, task_name: str, context: ExecutionContext) -> TaskResult:
        """Execute task locally (legacy method for compatibility)."""
        try:
            start_time = time.time()
            step_to_execute = context.study.dag.step(task_name)
            adapter_config = context.study.get_adapter_config(override_type="local")
            result = step_to_execute.execute(adapter_config)
            end_time = time.time()

            return TaskResult(
                task_name=task_name, status=TaskStatus.COMPLETED, start_time=start_time, end_time=end_time, result=result
            )
        except Exception as e:
            return TaskResult(task_name=task_name, status=TaskStatus.FAILED, error=str(e))
