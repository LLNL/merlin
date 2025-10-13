

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
        from merlin.execution.sample_expander import SampleExpander

        self.celery_app = app
        self.default_queue = default_queue
        self.active_tasks = {}  # Track running tasks
        self.sample_expander = SampleExpander()
    
    def execute_plan(self, plan: ExecutionPlan, context: ExecutionContext) -> Dict[str, TaskResult]:
        """
        Execute the plan level by level, respecting dependencies.

        Each level is queued to Celery and we wait for completion before
        proceeding to the next level to enforce dependencies.
        Workers must be running separately to execute the tasks.
        """
        all_results = {}

        for level in plan.levels:
            print(f"Executing depth {level.depth} with {len(level.parallel_chains)} parallel chains...")

            # Queue all chains in this level to Celery and WAIT for completion
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
        """Execute all chains in a level in parallel, with sample expansion and dependencies."""
        from celery import group

        level_results = {}
        all_chain_sigs = []  # Collect signatures for all chains at this level

        # Expand and build each chain
        for chain in level.parallel_chains:
            # Check if chain has real tasks (not virtual nodes)
            has_real_tasks = False
            for task in chain.tasks:
                try:
                    step = context.study.dag.step(task)
                    if step is not None:
                        has_real_tasks = True
                        break
                except (AttributeError, KeyError, TypeError):
                    pass

            if not has_real_tasks:
                # This is a virtual chain (e.g., _source only), mark as skipped
                for task in chain.tasks:
                    level_results[task] = TaskResult(
                        task_name=task,
                        status=TaskStatus.SKIPPED,
                        error="Virtual node, no execution needed"
                    )
                continue

            # Expand chain into 2D structure: [[pos0_tasks], [pos1_tasks], ...]
            expanded_positions = self.sample_expander.expand_chain(chain, context)

            # Log expansion details
            total_expanded = sum(len(pos) for pos in expanded_positions)
            print(f"  Chain '{chain.tasks[0] if chain.tasks else 'unknown'}' expanded to {total_expanded} tasks across {len(expanded_positions)} positions")

            # Build chain with dependencies
            chain_sigs = self._build_chain_with_dependencies(expanded_positions, context)
            all_chain_sigs.extend(chain_sigs)
            print(f"  Created {len(chain_sigs)} Celery signatures for this chain")

            # Mark tasks as queued
            for position_tasks in expanded_positions:
                for task_info in position_tasks:
                    task_name = task_info['step'].name()
                    level_results[task_name] = TaskResult(
                        task_name=task_name,
                        status=TaskStatus.COMPLETED,  # Indicates successfully queued
                        result=None
                    )

        # Count tasks for reporting
        total_tasks = len(level_results)
        print(f"Expanded {len(level.parallel_chains)} chains into {total_tasks} tasks with dependencies")

        # Execute all chains as group (parallel chains, but each chain maintains internal dependencies)
        if all_chain_sigs:
            print(f"Submitting {len(all_chain_sigs)} chain signatures to Celery...")
            task_group = group(all_chain_sigs)
            async_result = task_group.apply_async()

            # CRITICAL FIX: Wait for this level to complete before proceeding to next level
            # This ensures dependencies between levels are properly enforced
            print(f"Waiting for level {level.depth} to complete...")
            try:
                # Wait for all tasks in this level to complete
                # Timeout set to 1 hour per level (can be adjusted)
                results = async_result.get(timeout=3600)
                print(f"Level {level.depth} completed successfully")
            except Exception as e:
                print(f"Level {level.depth} failed with error: {e}")
                # Mark tasks as failed
                for task_name in level_results.keys():
                    level_results[task_name] = TaskResult(
                        task_name=task_name,
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
        from celery import chain as celery_chain
        from merlin.common.tasks import merlin_step

        # Get adapter config for tasks
        adapter_config = context.study.get_adapter_config(override_type="celery")

        # Build signatures for each task in the chain (skip virtual nodes)
        celery_tasks = []
        for task_name in chain.tasks:
            try:
                # Get Step object from DAG
                step = context.study.dag.step(task_name)

                # Skip virtual nodes (like _source)
                if step is None:
                    continue

                # Create signature for merlin_step task
                sig = merlin_step.s(
                    step,
                    adapter_config=adapter_config
                )
                sig.set(queue=step.get_task_queue())
                celery_tasks.append(sig)
            except (AttributeError, KeyError, TypeError):
                # This is a virtual node, skip it
                continue

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

    def _create_task_signature(self, task_info: Dict, adapter_config: Dict):
        """
        Create a Celery signature for a task.

        Args:
            task_info: Dictionary containing step and metadata
            adapter_config: Adapter configuration

        Returns:
            Celery signature
        """
        from merlin.common.tasks import merlin_step

        step = task_info['step']
        sig = merlin_step.s(step, adapter_config=adapter_config)
        sig.set(queue=step.get_task_queue())
        return sig

    def _link_chain_positions(self, all_chains: List[List]) -> List:
        """
        Link tasks at different chain positions with dependencies using Celery chains.

        Args:
            all_chains: 2D list [[pos0_tasks], [pos1_tasks], ...]

        Returns:
            List of Celery chain() primitives, one per parallel sample
        """
        from celery import chain

        if len(all_chains) == 0:
            return []

        if len(all_chains) == 1:
            # Single position - no linking needed
            return all_chains[0]

        # Multi-position chain: use Celery's chain() primitive
        # Build one chain per parallel sample/task
        chains = []
        num_parallel = len(all_chains[0])  # Number of parallel tasks

        for i in range(num_parallel):
            # Collect tasks at position i across all chain positions
            task_sequence = [all_chains[j][i] for j in range(len(all_chains))]
            # Create a Celery chain
            chains.append(chain(*task_sequence))

        return chains

    def _build_chain_with_dependencies(self, expanded_positions: List[List[Dict]], context: ExecutionContext) -> List:
        """
        Build a chain with dependencies from expanded positions.

        Args:
            expanded_positions: 2D structure from SampleExpander
            context: Execution context

        Returns:
            List of signatures with dependencies properly linked
        """
        from celery import group

        adapter_config = context.study.get_adapter_config(override_type="celery")

        # Convert each position's tasks to signatures
        all_sig_chains = []
        for position_tasks in expanded_positions:
            position_sigs = [
                self._create_task_signature(task_info, adapter_config)
                for task_info in position_tasks
            ]
            all_sig_chains.append(position_sigs)

        # Link positions with dependencies
        if len(all_sig_chains) > 1:
            # Multi-position chain: use linking logic
            linked_sigs = self._link_chain_positions(all_sig_chains)
        else:
            # Single position: just return the signatures
            linked_sigs = all_sig_chains[0] if all_sig_chains else []

        return linked_sigs

    def execute_chain(self, chain: TaskChain, context: ExecutionContext) -> List[TaskResult]:
        """Execute a single chain via Celery chain primitive."""
        from celery import chain as celery_chain
        from merlin.common.tasks import merlin_step

        results = []
        adapter_config = context.study.get_adapter_config(override_type="celery")

        # Build Celery chain (skip virtual nodes)
        sigs = []
        real_task_names = []
        for task_name in chain.tasks:
            try:
                step = context.study.dag.step(task_name)

                # Skip virtual nodes
                if step is None:
                    results.append(TaskResult(
                        task_name=task_name,
                        status=TaskStatus.SKIPPED,
                        error="Virtual node, no execution needed"
                    ))
                    continue

                sig = merlin_step.s(step, adapter_config=adapter_config)
                sig.set(queue=step.get_task_queue())
                sigs.append(sig)
                real_task_names.append(task_name)
            except (AttributeError, KeyError, TypeError):
                # This is a virtual node, skip it
                results.append(TaskResult(
                    task_name=task_name,
                    status=TaskStatus.SKIPPED,
                    error="Virtual node, no execution needed"
                ))

        # Execute chain (only if there are real tasks)
        if sigs:
            try:
                start_time = time.time()
                async_result = celery_chain(*sigs).apply_async()
                result = async_result.get(timeout=3600)  # 1 hour timeout
                end_time = time.time()

                # Create success results for all real tasks
                for task_name in real_task_names:
                    results.append(TaskResult(
                        task_name=task_name,
                        status=TaskStatus.COMPLETED,
                        start_time=start_time,
                        end_time=end_time
                    ))
            except Exception as e:
                # Mark all real tasks in chain as failed
                for task_name in real_task_names:
                    results.append(TaskResult(
                        task_name=task_name,
                        status=TaskStatus.FAILED,
                        error=str(e)
                    ))

        return results

    def execute_task(self, task_name: str, context: ExecutionContext) -> TaskResult:
        """Execute a single task via Celery."""
        from merlin.common.tasks import merlin_step

        try:
            # Get Step object from DAG
            step = context.study.dag.step(task_name)

            # Skip virtual nodes
            if step is None:
                return TaskResult(
                    task_name=task_name,
                    status=TaskStatus.SKIPPED,
                    error="Virtual node, no execution needed"
                )

            celery_id = str(uuid.uuid4())

            # Get adapter config
            adapter_config = context.study.get_adapter_config(override_type="celery")

            # Create Celery signature
            sig = merlin_step.s(step, adapter_config=adapter_config)
            sig.set(queue=step.get_task_queue())

            # Submit to Celery
            start_time = time.time()
            async_result = sig.apply_async(task_id=celery_id)

            # Wait for result
            result = async_result.get(timeout=1800)  # 30 min timeout
            end_time = time.time()

            return TaskResult(
                task_name=task_name,
                status=TaskStatus.COMPLETED,
                start_time=start_time,
                end_time=end_time,
                result=result,
                celery_id=celery_id
            )
        except (AttributeError, KeyError, TypeError):
            # This is a virtual node
            return TaskResult(
                task_name=task_name,
                status=TaskStatus.SKIPPED,
                error="Virtual node, no execution needed"
            )
        except Exception as e:
            return TaskResult(
                task_name=task_name,
                status=TaskStatus.FAILED,
                error=str(e),
                celery_id=celery_id if 'celery_id' in locals() else None
            )