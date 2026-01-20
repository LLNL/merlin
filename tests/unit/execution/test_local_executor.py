##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the LocalExecutor class.
"""

from concurrent.futures import Future
from unittest.mock import MagicMock, Mock, patch

import pytest

from merlin.dag.models import ExecutionLevel, ExecutionPlan, TaskChain
from merlin.execution.local import LocalExecutor
from merlin.execution.models import TaskResult, TaskStatus


class TestLocalExecutorInit:
    """Tests for LocalExecutor.__init__()"""

    def test_init_default_max_workers(self):
        """Test that default max_workers is 4"""
        executor = LocalExecutor()
        assert executor.max_workers == 4
        assert executor.sample_expander is not None

    def test_init_custom_max_workers(self):
        """Test that custom max_workers is set correctly"""
        executor = LocalExecutor(max_workers=8)
        assert executor.max_workers == 8
        assert executor.sample_expander is not None

    def test_init_creates_sample_expander(self):
        """Test that __init__ creates a SampleExpander instance"""
        executor = LocalExecutor()
        from merlin.execution.sample_expander import SampleExpander

        assert isinstance(executor.sample_expander, SampleExpander)


class TestHasRealTasks:
    """Tests for LocalExecutor._has_real_tasks()"""

    def test_has_real_tasks_returns_true_for_real_tasks(self):
        """Test _has_real_tasks returns True for chains with real tasks"""
        executor = LocalExecutor()

        # Mock step (real task)
        mock_step = Mock()
        mock_step.name.return_value = "real_task"

        # Mock context
        context = Mock()
        context.study.dag.step.return_value = mock_step

        chain = TaskChain(tasks=["real_task"], depth=1)

        result = executor._has_real_tasks(chain, context)
        assert result is True

    def test_has_real_tasks_returns_false_for_virtual_nodes(self):
        """Test _has_real_tasks returns False for virtual nodes"""
        executor = LocalExecutor()

        # Mock context with None step (virtual node)
        context = Mock()
        context.study.dag.step.return_value = None

        chain = TaskChain(tasks=["_source"], depth=0)

        result = executor._has_real_tasks(chain, context)
        assert result is False

    def test_has_real_tasks_handles_exceptions(self):
        """Test _has_real_tasks handles exceptions gracefully"""
        executor = LocalExecutor()

        # Mock context that raises exception
        context = Mock()
        context.study.dag.step.side_effect = AttributeError("Step not found")

        chain = TaskChain(tasks=["invalid_task"], depth=1)

        result = executor._has_real_tasks(chain, context)
        assert result is False

    def test_has_real_tasks_with_mixed_tasks(self):
        """Test _has_real_tasks returns True if any task is real"""
        executor = LocalExecutor()

        # Mock context that returns None for first task, real step for second
        mock_step = Mock()
        context = Mock()
        context.study.dag.step.side_effect = [None, mock_step]

        chain = TaskChain(tasks=["virtual_task", "real_task"], depth=1)

        result = executor._has_real_tasks(chain, context)
        assert result is True


class TestExecuteStepWrapper:
    """Tests for LocalExecutor._execute_step_wrapper()"""

    def test_execute_step_wrapper_is_static_method(self):
        """Test that _execute_step_wrapper is a static method (pickleable)"""
        # Static methods can be called without instantiation
        import inspect

        # Check that it's decorated as @staticmethod
        assert isinstance(inspect.getattr_static(LocalExecutor, "_execute_step_wrapper"), staticmethod)

    @patch("os.path.exists")
    def test_execute_step_wrapper_skips_completed_tasks(self, mock_exists):
        """Test _execute_step_wrapper skips tasks with MERLIN_FINISHED"""
        # Mock that MERLIN_FINISHED exists
        mock_exists.return_value = True

        mock_step = Mock()
        mock_step.get_workspace.return_value = "/workspace/task1"
        mock_step.name.return_value = "task1"

        adapter_config = {"type": "local"}

        result = LocalExecutor._execute_step_wrapper(mock_step, adapter_config)

        # Should return 0 without calling execute
        assert result == 0
        mock_step.execute.assert_not_called()

    @patch("os.path.exists")
    @patch("builtins.open", new_callable=MagicMock)
    def test_execute_step_wrapper_executes_and_creates_finished_file(self, mock_open, mock_exists):
        """Test _execute_step_wrapper executes step and creates MERLIN_FINISHED"""
        # Mock that MERLIN_FINISHED doesn't exist
        mock_exists.return_value = False

        mock_step = Mock()
        mock_step.get_workspace.return_value = "/workspace/task1"
        mock_step.name.return_value = "task1"
        mock_step.execute.return_value = 0  # Success
        mock_step.max_retries = 10  # Required for retry logic

        adapter_config = {"type": "local"}

        result = LocalExecutor._execute_step_wrapper(mock_step, adapter_config)

        # Should execute and return 0
        assert result == 0
        mock_step.execute.assert_called_once_with(adapter_config)

        # Should create MERLIN_FINISHED
        mock_open.assert_called_once_with("/workspace/task1/MERLIN_FINISHED", "w")

    @patch("os.path.exists")
    def test_execute_step_wrapper_returns_non_zero_on_failure(self, mock_exists):
        """Test _execute_step_wrapper returns non-zero on task failure"""
        mock_exists.return_value = False

        mock_step = Mock()
        mock_step.get_workspace.return_value = "/workspace/task1"
        mock_step.name.return_value = "task1"
        mock_step.execute.return_value = 1  # Failure
        mock_step.max_retries = 10  # Required for retry logic

        adapter_config = {"type": "local"}

        result = LocalExecutor._execute_step_wrapper(mock_step, adapter_config)

        assert result == 1

    @patch("os.path.exists")
    def test_execute_step_wrapper_raises_exception_on_error(self, mock_exists):
        """Test _execute_step_wrapper re-raises exceptions"""
        mock_exists.return_value = False

        mock_step = Mock()
        mock_step.get_workspace.return_value = "/workspace/task1"
        mock_step.name.return_value = "task1"
        mock_step.execute.side_effect = RuntimeError("Execution failed")
        mock_step.max_retries = 10  # Required for retry logic

        adapter_config = {"type": "local"}

        with pytest.raises(RuntimeError, match="Execution failed"):
            LocalExecutor._execute_step_wrapper(mock_step, adapter_config)


class TestExecuteChainWithDependencies:
    """Tests for LocalExecutor._execute_chain_with_dependencies()"""

    @patch("merlin.execution.local.as_completed")
    def test_execute_chain_with_dependencies_sequential_positions(self, mock_as_completed):
        """Test _execute_chain_with_dependencies executes positions sequentially"""
        executor = LocalExecutor()

        # Mock two positions with one task each
        mock_step1 = Mock()
        mock_step1.name.return_value = "step1"

        mock_step2 = Mock()
        mock_step2.name.return_value = "step2"

        expanded_positions = [
            [{"step": mock_step1, "sample_id": None}],
            [{"step": mock_step2, "sample_id": None}],
        ]

        # Mock context
        context = Mock()
        context.study.get_adapter_config.return_value = {"type": "local"}

        # Mock executor
        mock_executor = Mock()

        # Mock futures
        future1 = Mock(spec=Future)
        future1.result.return_value = 0

        future2 = Mock(spec=Future)
        future2.result.return_value = 0

        mock_executor.submit.side_effect = [future1, future2]

        # Mock as_completed to return futures in order
        mock_as_completed.side_effect = [[future1], [future2]]

        result = executor._execute_chain_with_dependencies(expanded_positions, context, mock_executor)

        # Should have 2 results
        assert len(result) == 2
        assert "step1" in result
        assert "step2" in result
        assert result["step1"].status == TaskStatus.COMPLETED
        assert result["step2"].status == TaskStatus.COMPLETED

        # Submit should be called twice (once per position)
        assert mock_executor.submit.call_count == 2

    @patch("merlin.execution.local.as_completed")
    def test_execute_chain_with_dependencies_parallel_samples(self, mock_as_completed):
        """Test _execute_chain_with_dependencies executes samples in parallel"""
        executor = LocalExecutor()

        # Mock one position with 3 samples
        mock_steps = [Mock() for _ in range(3)]
        for i, step in enumerate(mock_steps):
            step.name.return_value = f"step_sample{i}"

        expanded_positions = [[{"step": step, "sample_id": i} for i, step in enumerate(mock_steps)]]

        context = Mock()
        context.study.get_adapter_config.return_value = {"type": "local"}

        mock_executor = Mock()

        # Mock futures for all samples
        futures = [Mock(spec=Future) for _ in range(3)]
        for future in futures:
            future.result.return_value = 0

        mock_executor.submit.side_effect = futures
        mock_as_completed.return_value = futures

        result = executor._execute_chain_with_dependencies(expanded_positions, context, mock_executor)

        # Should have 3 results (all samples executed in parallel)
        assert len(result) == 3

        # Submit should be called 3 times (once per sample)
        assert mock_executor.submit.call_count == 3

    @patch("merlin.execution.local.as_completed")
    def test_execute_chain_with_dependencies_handles_failures(self, mock_as_completed):
        """Test _execute_chain_with_dependencies handles task failures"""
        executor = LocalExecutor()

        mock_step = Mock()
        mock_step.name.return_value = "failing_step"

        expanded_positions = [[{"step": mock_step, "sample_id": None}]]

        context = Mock()
        context.study.get_adapter_config.return_value = {"type": "local"}

        mock_executor = Mock()

        future = Mock(spec=Future)
        future.result.return_value = 1  # Non-zero exit code

        mock_executor.submit.return_value = future
        mock_as_completed.return_value = [future]

        result = executor._execute_chain_with_dependencies(expanded_positions, context, mock_executor)

        assert len(result) == 1
        assert result["failing_step"].status == TaskStatus.FAILED
        assert "non-zero exit code" in result["failing_step"].error

    @patch("merlin.execution.local.as_completed")
    def test_execute_chain_with_dependencies_handles_exceptions(self, mock_as_completed):
        """Test _execute_chain_with_dependencies handles exceptions"""
        executor = LocalExecutor()

        mock_step = Mock()
        mock_step.name.return_value = "error_step"

        expanded_positions = [[{"step": mock_step, "sample_id": None}]]

        context = Mock()
        context.study.get_adapter_config.return_value = {"type": "local"}

        mock_executor = Mock()

        future = Mock(spec=Future)
        future.result.side_effect = RuntimeError("Execution error")

        mock_executor.submit.return_value = future
        mock_as_completed.return_value = [future]

        result = executor._execute_chain_with_dependencies(expanded_positions, context, mock_executor)

        assert len(result) == 1
        assert result["error_step"].status == TaskStatus.FAILED
        assert "Execution error" in result["error_step"].error


class TestExecuteLevelParallel:
    """Tests for LocalExecutor._execute_level_parallel()"""

    @patch.object(LocalExecutor, "_has_real_tasks")
    def test_execute_level_parallel_skips_virtual_nodes(self, mock_has_real_tasks):
        """Test _execute_level_parallel skips virtual nodes"""
        executor = LocalExecutor()

        mock_has_real_tasks.return_value = False  # All virtual

        context = Mock()
        mock_executor = Mock()

        level = ExecutionLevel(depth=0, parallel_chains=[TaskChain(tasks=["_source"], depth=0)])

        result = executor._execute_level_parallel(level, context, mock_executor)

        # Should have result for _source marked as SKIPPED
        assert len(result) == 1
        assert "_source" in result
        assert result["_source"].status == TaskStatus.SKIPPED
        assert result["_source"].error == "Virtual node"

    @patch.object(LocalExecutor, "_has_real_tasks")
    @patch.object(LocalExecutor, "_execute_chain_with_dependencies")
    def test_execute_level_parallel_executes_real_chains(self, mock_execute_chain, mock_has_real_tasks):
        """Test _execute_level_parallel executes real task chains"""
        executor = LocalExecutor()

        mock_has_real_tasks.return_value = True  # Real tasks

        # Mock sample expander
        executor.sample_expander = Mock()
        executor.sample_expander.expand_chain.return_value = [[{"step": Mock(), "sample_id": None}]]

        # Mock chain execution
        mock_step = Mock()
        mock_step.name.return_value = "task1"
        mock_execute_chain.return_value = {"task1": TaskResult(task_name="task1", status=TaskStatus.COMPLETED)}

        context = Mock()
        mock_executor = Mock()

        level = ExecutionLevel(depth=1, parallel_chains=[TaskChain(tasks=["task1"], depth=1)])

        result = executor._execute_level_parallel(level, context, mock_executor)

        # Should have result for task1
        assert len(result) == 1
        assert "task1" in result
        assert result["task1"].status == TaskStatus.COMPLETED

        # Should call expand_chain and execute_chain_with_dependencies
        executor.sample_expander.expand_chain.assert_called_once()
        mock_execute_chain.assert_called_once()


class TestExecutePlan:
    """Tests for LocalExecutor.execute_plan()"""

    @patch("merlin.execution.local.ProcessPoolExecutor")
    @patch.object(LocalExecutor, "_execute_level_parallel")
    def test_execute_plan_with_no_samples(self, mock_execute_level, mock_pool_executor):
        """Test execute_plan with workflow without samples"""
        executor = LocalExecutor(max_workers=4)

        # Mock execution results
        mock_execute_level.side_effect = [
            {"_source": TaskResult(task_name="_source", status=TaskStatus.SKIPPED)},
            {
                "step1": TaskResult(task_name="step1", status=TaskStatus.COMPLETED),
                "step2": TaskResult(task_name="step2", status=TaskStatus.COMPLETED),
            },
            {"step3": TaskResult(task_name="step3", status=TaskStatus.COMPLETED)},
        ]

        # Create simple plan
        plan = ExecutionPlan(
            [
                ExecutionLevel(depth=0, parallel_chains=[TaskChain(tasks=["_source"], depth=0)]),
                ExecutionLevel(
                    depth=1,
                    parallel_chains=[
                        TaskChain(tasks=["step1"], depth=1),
                        TaskChain(tasks=["step2"], depth=1),
                    ],
                ),
                ExecutionLevel(depth=2, parallel_chains=[TaskChain(tasks=["step3"], depth=2)]),
            ]
        )

        context = Mock()

        # Mock ProcessPoolExecutor context manager
        mock_pool = Mock()
        mock_pool_executor.return_value.__enter__.return_value = mock_pool

        result = executor.execute_plan(plan, context)

        # Should have 4 results (all tasks)
        assert len(result) == 4
        assert "_source" in result
        assert "step1" in result
        assert "step2" in result
        assert "step3" in result

        # Should execute all 3 levels
        assert mock_execute_level.call_count == 3

    @patch("merlin.execution.local.ProcessPoolExecutor")
    @patch.object(LocalExecutor, "_execute_level_parallel")
    def test_execute_plan_stops_on_failure(self, mock_execute_level, mock_pool_executor):
        """Test execute_plan stops execution when a level fails"""
        executor = LocalExecutor()

        # Mock execution with failure in second level
        mock_execute_level.side_effect = [
            {"step1": TaskResult(task_name="step1", status=TaskStatus.COMPLETED)},
            {"step2": TaskResult(task_name="step2", status=TaskStatus.FAILED, error="Task failed")},
            {"step3": TaskResult(task_name="step3", status=TaskStatus.COMPLETED)},
        ]

        plan = ExecutionPlan(
            [
                ExecutionLevel(depth=0, parallel_chains=[TaskChain(tasks=["step1"], depth=0)]),
                ExecutionLevel(depth=1, parallel_chains=[TaskChain(tasks=["step2"], depth=1)]),
                ExecutionLevel(depth=2, parallel_chains=[TaskChain(tasks=["step3"], depth=2)]),
            ]
        )

        context = Mock()

        mock_pool = Mock()
        mock_pool_executor.return_value.__enter__.return_value = mock_pool

        result = executor.execute_plan(plan, context)

        # Should only have results from first 2 levels
        assert len(result) == 2
        assert "step1" in result
        assert "step2" in result
        assert "step3" not in result  # Third level should not execute

        # Should only execute 2 levels (stops after failure)
        assert mock_execute_level.call_count == 2

    @patch("merlin.execution.local.ProcessPoolExecutor")
    @patch.object(LocalExecutor, "_execute_level_parallel")
    def test_execute_plan_respects_max_workers(self, mock_execute_level, mock_pool_executor):
        """Test execute_plan uses correct max_workers"""
        executor = LocalExecutor(max_workers=8)

        mock_execute_level.return_value = {"task1": TaskResult(task_name="task1", status=TaskStatus.COMPLETED)}

        plan = ExecutionPlan([ExecutionLevel(depth=0, parallel_chains=[TaskChain(tasks=["task1"], depth=0)])])

        context = Mock()

        mock_pool = Mock()
        mock_pool_executor.return_value.__enter__.return_value = mock_pool

        executor.execute_plan(plan, context)

        # Should create ProcessPoolExecutor with max_workers=8
        mock_pool_executor.assert_called_once_with(max_workers=8)


class TestLegacyMethods:
    """Tests for legacy execute_chain and execute_task methods"""

    @patch.object(LocalExecutor, "execute_task")
    def test_execute_chain_calls_execute_task_for_each_task(self, mock_execute_task):
        """Test execute_chain calls execute_task for each task in chain"""
        executor = LocalExecutor()

        mock_execute_task.side_effect = [
            TaskResult(task_name="task1", status=TaskStatus.COMPLETED),
            TaskResult(task_name="task2", status=TaskStatus.COMPLETED),
        ]

        context = Mock()
        chain = TaskChain(tasks=["task1", "task2"], depth=1)

        results = executor.execute_chain(chain, context)

        assert len(results) == 2
        assert mock_execute_task.call_count == 2

    def test_execute_task_executes_step(self):
        """Test execute_task executes a single step"""
        executor = LocalExecutor()

        mock_step = Mock()
        mock_step.execute.return_value = 0

        context = Mock()
        context.study.dag.step.return_value = mock_step
        context.study.get_adapter_config.return_value = {"type": "local"}

        result = executor.execute_task("task1", context)

        assert result.task_name == "task1"
        assert result.status == TaskStatus.COMPLETED
        mock_step.execute.assert_called_once()

    def test_execute_task_handles_exceptions(self):
        """Test execute_task handles exceptions gracefully"""
        executor = LocalExecutor()

        mock_step = Mock()
        mock_step.execute.side_effect = RuntimeError("Task error")

        context = Mock()
        context.study.dag.step.return_value = mock_step
        context.study.get_adapter_config.return_value = {"type": "local"}

        result = executor.execute_task("task1", context)

        assert result.task_name == "task1"
        assert result.status == TaskStatus.FAILED
        assert "Task error" in result.error
