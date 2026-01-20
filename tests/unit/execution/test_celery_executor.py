##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the CeleryExecutor class.
"""

import json
import os
import sys
import tempfile
from unittest.mock import Mock, patch

import pytest

from merlin.dag.models import ExecutionLevel, ExecutionPlan, TaskChain
from merlin.execution.models import TaskStatus


# Create mock modules for Celery dependencies
@pytest.fixture(autouse=True)
def mock_celery_imports():
    """Mock the Celery-related imports that happen inside CeleryExecutor methods."""
    # Create mock app
    mock_app = Mock()

    # Create mock celery module
    mock_celery_module = Mock()
    mock_celery_module.app = mock_app

    # Create mock merlin.celery module
    mock_merlin_celery = Mock()
    mock_merlin_celery.app = mock_app

    # Create mock celery primitives module
    mock_celery = Mock()
    mock_celery.chain = Mock()
    mock_celery.group = Mock()

    # Patch sys.modules for imports that happen inside methods
    with patch.dict(
        sys.modules,
        {
            "merlin.celery": mock_merlin_celery,
            "celery": mock_celery,
        },
    ):
        yield {
            "app": mock_app,
            "chain": mock_celery.chain,
            "group": mock_celery.group,
        }


class TestCeleryExecutorInit:
    """Tests for CeleryExecutor.__init__()"""

    def test_init_default_queue(self, mock_celery_imports):
        """Test that default queue is 'default'"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()
        assert executor.default_queue == "default"
        assert executor.sample_expander is not None
        assert executor.active_tasks == {}

    def test_init_custom_queue(self, mock_celery_imports):
        """Test that custom queue is set correctly"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor(default_queue="my_queue")
        assert executor.default_queue == "my_queue"

    def test_init_creates_sample_expander(self, mock_celery_imports):
        """Test that __init__ creates a SampleExpander instance"""
        from merlin.execution.celery import CeleryExecutor
        from merlin.execution.sample_expander import SampleExpander

        executor = CeleryExecutor()
        assert isinstance(executor.sample_expander, SampleExpander)


class TestCreateBatches:
    """Tests for CeleryExecutor._create_batches()"""

    def test_create_batches_empty_list(self, mock_celery_imports):
        """Test _create_batches with empty list returns empty list"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()
        result = executor._create_batches([], batch_size=100)
        assert result == []

    def test_create_batches_50_tasks_one_batch(self, mock_celery_imports):
        """Test _create_batches with 50 tasks creates 1 batch"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()
        task_infos = [{"task": f"task_{i}"} for i in range(50)]
        result = executor._create_batches(task_infos, batch_size=100)
        assert len(result) == 1
        assert len(result[0]) == 50

    def test_create_batches_100_tasks_one_batch(self, mock_celery_imports):
        """Test _create_batches with exactly 100 tasks creates 1 batch"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()
        task_infos = [{"task": f"task_{i}"} for i in range(100)]
        result = executor._create_batches(task_infos, batch_size=100)
        assert len(result) == 1
        assert len(result[0]) == 100

    def test_create_batches_250_tasks_three_batches(self, mock_celery_imports):
        """Test _create_batches with 250 tasks creates 3 batches"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()
        task_infos = [{"task": f"task_{i}"} for i in range(250)]
        result = executor._create_batches(task_infos, batch_size=100)
        assert len(result) == 3
        assert len(result[0]) == 100
        assert len(result[1]) == 100
        assert len(result[2]) == 50

    def test_create_batches_custom_batch_size(self, mock_celery_imports):
        """Test _create_batches with custom batch_size"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()
        task_infos = [{"task": f"task_{i}"} for i in range(25)]
        result = executor._create_batches(task_infos, batch_size=10)
        assert len(result) == 3
        assert len(result[0]) == 10
        assert len(result[1]) == 10
        assert len(result[2]) == 5


class TestCreateTaskSignature:
    """Tests for CeleryExecutor._create_task_signature()"""

    def test_create_task_signature_basic(self, mock_celery_imports):
        """Test _create_task_signature creates a signature"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()

        mock_step = Mock()
        mock_step.get_task_queue.return_value = "test_queue"

        mock_sig = Mock()

        task_info = {"step": mock_step}
        adapter_config = {"type": "celery"}

        # Patch merlin_step where it's imported
        with patch("merlin.common.tasks.merlin_step") as mock_merlin_step:
            mock_merlin_step.s.return_value = mock_sig
            result = executor._create_task_signature(task_info, adapter_config)

            # Should create signature with step and adapter_config
            mock_merlin_step.s.assert_called_once_with(mock_step, adapter_config=adapter_config)
            # Should set queue
            mock_sig.set.assert_called_once_with(queue="test_queue")
            assert result == mock_sig


class TestLinkChainPositions:
    """Tests for CeleryExecutor._link_chain_positions()"""

    def test_link_chain_positions_empty_list(self, mock_celery_imports):
        """Test _link_chain_positions with empty list"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()
        result = executor._link_chain_positions([])
        assert result == []

    def test_link_chain_positions_single_position(self, mock_celery_imports):
        """Test _link_chain_positions with single position returns tasks unchanged"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()

        mock_sigs = [Mock(), Mock(), Mock()]  # 3 parallel tasks
        all_chains = [mock_sigs]

        result = executor._link_chain_positions(all_chains)

        # Single position - no chaining needed, return as-is
        assert result == mock_sigs

    def test_link_chain_positions_multi_position(self, mock_celery_imports):
        """Test _link_chain_positions with multiple positions creates chains"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()

        # 2 positions with 2 parallel tasks each
        pos0_sigs = [Mock(), Mock()]
        pos1_sigs = [Mock(), Mock()]
        all_chains = [pos0_sigs, pos1_sigs]

        mock_chain_result = Mock()
        mock_celery_imports["chain"].return_value = mock_chain_result

        result = executor._link_chain_positions(all_chains)

        # Should create 2 chains (one per parallel sample)
        assert len(result) == 2
        # chain() should be called twice
        assert mock_celery_imports["chain"].call_count == 2

    def test_link_chain_positions_three_positions(self, mock_celery_imports):
        """Test _link_chain_positions with 3 positions creates proper chains"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()

        # 3 positions with 1 parallel task each
        pos0_sigs = [Mock()]
        pos1_sigs = [Mock()]
        pos2_sigs = [Mock()]
        all_chains = [pos0_sigs, pos1_sigs, pos2_sigs]

        mock_chain_result = Mock()
        mock_celery_imports["chain"].return_value = mock_chain_result

        result = executor._link_chain_positions(all_chains)

        # Should create 1 chain
        assert len(result) == 1
        # chain() should be called once with all 3 positions
        assert mock_celery_imports["chain"].call_count == 1


class TestBuildChainWithDependencies:
    """Tests for CeleryExecutor._build_chain_with_dependencies()"""

    def test_build_chain_with_dependencies_single_position(self, mock_celery_imports):
        """Test _build_chain_with_dependencies with single position"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()

        mock_step = Mock()
        mock_step.get_task_queue.return_value = "test_queue"
        mock_sig = Mock()

        expanded_positions = [[{"step": mock_step, "sample_id": 0}]]

        context = Mock()
        context.study.get_adapter_config.return_value = {"type": "celery"}

        # Mock _create_task_signature to avoid internal import issues
        with patch.object(executor, "_create_task_signature") as mock_create_sig:
            mock_create_sig.return_value = mock_sig
            result = executor._build_chain_with_dependencies(expanded_positions, context)

            # Should create signature
            mock_create_sig.assert_called_once()
            # Should return list of signatures (no linking for single position)
            assert result == [mock_sig]

    def test_build_chain_with_dependencies_multi_position(self, mock_celery_imports):
        """Test _build_chain_with_dependencies with multiple positions"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()

        mock_step1 = Mock()
        mock_step1.get_task_queue.return_value = "test_queue"
        mock_step2 = Mock()
        mock_step2.get_task_queue.return_value = "test_queue"
        mock_sig1 = Mock()
        mock_sig2 = Mock()

        mock_chain = Mock()
        mock_celery_imports["chain"].return_value = mock_chain

        expanded_positions = [
            [{"step": mock_step1, "sample_id": 0}],
            [{"step": mock_step2, "sample_id": 0}],
        ]

        context = Mock()
        context.study.get_adapter_config.return_value = {"type": "celery"}

        # Mock _create_task_signature to avoid internal import issues
        with patch.object(executor, "_create_task_signature") as mock_create_sig:
            mock_create_sig.side_effect = [mock_sig1, mock_sig2]
            result = executor._build_chain_with_dependencies(expanded_positions, context)

            # Should create 2 signatures
            assert mock_create_sig.call_count == 2
            # Result should be from link_chain_positions (which calls chain)
            assert result == [mock_chain]


class TestExecutePlanVirtualNodes:
    """Tests for virtual node handling in execute_plan()"""

    def test_execute_plan_skips_virtual_nodes(self, mock_celery_imports):
        """Test execute_plan skips virtual nodes like _source"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()

        # Mock context with _source returning None (virtual node)
        context = Mock()
        context.study.dag.step.return_value = None
        context.study.workspace = "/workspace"

        plan = ExecutionPlan([ExecutionLevel(depth=0, parallel_chains=[TaskChain(tasks=["_source"], depth=0)])])

        result = executor.execute_plan(plan, context, wait=False)

        # _source should be marked as SKIPPED
        assert "_source" in result["results"]
        assert result["results"]["_source"].status == TaskStatus.SKIPPED
        assert "Virtual node" in result["results"]["_source"].error

    def test_execute_plan_mixed_virtual_and_real(self, mock_celery_imports):
        """Test execute_plan handles mix of virtual and real tasks"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()

        mock_step = Mock()
        mock_step.get_task_queue.return_value = "test_queue"
        mock_step.name.return_value = "real_task"

        # _source returns None, real_task returns mock_step
        def step_side_effect(task_name):
            if task_name == "_source":
                return None
            return mock_step

        context = Mock()
        context.study.dag.step.side_effect = step_side_effect
        context.study.workspace = "/workspace"
        context.study.sample_labels = []
        context.study.samples = []

        # Mock sample_expander
        executor.sample_expander = Mock()
        executor.sample_expander.expand_chain.return_value = [[{"step": mock_step, "sample_id": None}]]

        # Mock async result
        mock_async = Mock()
        mock_async.id = "test-workflow-id"
        mock_chain_workflow = Mock()
        mock_chain_workflow.apply_async.return_value = mock_async
        mock_celery_imports["chain"].return_value = mock_chain_workflow

        plan = ExecutionPlan(
            [
                ExecutionLevel(depth=0, parallel_chains=[TaskChain(tasks=["_source"], depth=0)]),
                ExecutionLevel(depth=1, parallel_chains=[TaskChain(tasks=["real_task"], depth=1)]),
            ]
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            context.study.workspace = tmpdir
            context.study.expanded_spec.name = "test_study"
            context.study.get_adapter_config.return_value = {"type": "celery"}

            with patch("merlin.common.tasks.merlin_step") as mock_merlin_step:
                mock_sig = Mock()
                mock_merlin_step.s.return_value = mock_sig
                result = executor.execute_plan(plan, context, wait=False)

        # _source should be SKIPPED
        assert result["results"]["_source"].status == TaskStatus.SKIPPED
        # real_task should be tracked
        assert "real_task" in result["results"]


class TestExecutePlanWorkflow:
    """Tests for execute_plan workflow building"""

    def test_execute_plan_creates_workflow_info(self, mock_celery_imports):
        """Test execute_plan creates WORKFLOW_INFO.json"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()

        mock_step = Mock()
        mock_step.get_task_queue.return_value = "test_queue"
        mock_step.name.return_value = "task1"

        # Mock sample_expander
        executor.sample_expander = Mock()
        executor.sample_expander.expand_chain.return_value = [[{"step": mock_step, "sample_id": None}]]

        # Mock async result
        mock_async = Mock()
        mock_async.id = "workflow-123"
        mock_chain_workflow = Mock()
        mock_chain_workflow.apply_async.return_value = mock_async
        mock_celery_imports["chain"].return_value = mock_chain_workflow

        context = Mock()
        context.study.dag.step.return_value = mock_step
        context.study.get_adapter_config.return_value = {"type": "celery"}
        context.study.sample_labels = []
        context.study.samples = []

        plan = ExecutionPlan([ExecutionLevel(depth=0, parallel_chains=[TaskChain(tasks=["task1"], depth=0)])])

        with tempfile.TemporaryDirectory() as tmpdir:
            context.study.workspace = tmpdir
            context.study.expanded_spec.name = "test_study"

            with patch("merlin.common.tasks.merlin_step") as mock_merlin_step:
                mock_sig = Mock()
                mock_merlin_step.s.return_value = mock_sig
                executor.execute_plan(plan, context, wait=False)

                # Check WORKFLOW_INFO.json was created
                workflow_info_path = os.path.join(tmpdir, "WORKFLOW_INFO.json")
                assert os.path.exists(workflow_info_path)

                with open(workflow_info_path) as f:
                    info = json.load(f)

                assert info["workflow_id"] == "workflow-123"
                assert info["study_name"] == "test_study"
                assert info["status"] == "SUBMITTED"

    def test_execute_plan_returns_workflow_id(self, mock_celery_imports):
        """Test execute_plan returns workflow_id and async_result"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()

        mock_step = Mock()
        mock_step.get_task_queue.return_value = "test_queue"
        mock_step.name.return_value = "task1"

        executor.sample_expander = Mock()
        executor.sample_expander.expand_chain.return_value = [[{"step": mock_step, "sample_id": None}]]

        mock_async = Mock()
        mock_async.id = "workflow-456"
        mock_chain_workflow = Mock()
        mock_chain_workflow.apply_async.return_value = mock_async
        mock_celery_imports["chain"].return_value = mock_chain_workflow

        context = Mock()
        context.study.dag.step.return_value = mock_step
        context.study.get_adapter_config.return_value = {"type": "celery"}
        context.study.sample_labels = []
        context.study.samples = []

        plan = ExecutionPlan([ExecutionLevel(depth=0, parallel_chains=[TaskChain(tasks=["task1"], depth=0)])])

        with tempfile.TemporaryDirectory() as tmpdir:
            context.study.workspace = tmpdir
            context.study.expanded_spec.name = "test_study"

            with patch("merlin.common.tasks.merlin_step") as mock_merlin_step:
                mock_sig = Mock()
                mock_merlin_step.s.return_value = mock_sig
                result = executor.execute_plan(plan, context, wait=False)

        assert result["workflow_id"] == "workflow-456"
        assert result["async_result"] == mock_async

    def test_execute_plan_no_tasks_returns_none(self, mock_celery_imports):
        """Test execute_plan with no real tasks returns None workflow_id"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()

        context = Mock()
        context.study.dag.step.return_value = None  # All virtual nodes
        context.study.workspace = "/workspace"

        plan = ExecutionPlan([ExecutionLevel(depth=0, parallel_chains=[TaskChain(tasks=["_source"], depth=0)])])

        result = executor.execute_plan(plan, context, wait=False)

        assert result["workflow_id"] is None
        assert result["async_result"] is None


class TestExecutePlanWaitBehavior:
    """Tests for execute_plan wait parameter behavior"""

    def test_execute_plan_wait_false_returns_immediately(self, mock_celery_imports):
        """Test execute_plan with wait=False returns immediately"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()

        mock_step = Mock()
        mock_step.get_task_queue.return_value = "test_queue"
        mock_step.name.return_value = "task1"

        executor.sample_expander = Mock()
        executor.sample_expander.expand_chain.return_value = [[{"step": mock_step, "sample_id": None}]]

        mock_async = Mock()
        mock_async.id = "workflow-789"
        mock_chain_workflow = Mock()
        mock_chain_workflow.apply_async.return_value = mock_async
        mock_celery_imports["chain"].return_value = mock_chain_workflow

        context = Mock()
        context.study.dag.step.return_value = mock_step
        context.study.get_adapter_config.return_value = {"type": "celery"}
        context.study.sample_labels = []
        context.study.samples = []

        plan = ExecutionPlan([ExecutionLevel(depth=0, parallel_chains=[TaskChain(tasks=["task1"], depth=0)])])

        with tempfile.TemporaryDirectory() as tmpdir:
            context.study.workspace = tmpdir
            context.study.expanded_spec.name = "test_study"

            with patch("merlin.common.tasks.merlin_step") as mock_merlin_step:
                mock_sig = Mock()
                mock_merlin_step.s.return_value = mock_sig
                executor.execute_plan(plan, context, wait=False)

        # async_result.get() should NOT be called when wait=False
        mock_async.get.assert_not_called()

    def test_execute_plan_wait_true_blocks(self, mock_celery_imports):
        """Test execute_plan with wait=True blocks until completion"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()

        mock_step = Mock()
        mock_step.get_task_queue.return_value = "test_queue"
        mock_step.name.return_value = "task1"

        executor.sample_expander = Mock()
        executor.sample_expander.expand_chain.return_value = [[{"step": mock_step, "sample_id": None}]]

        mock_async = Mock()
        mock_async.id = "workflow-wait"
        mock_async.get.return_value = None  # Simulate completion
        mock_chain_workflow = Mock()
        mock_chain_workflow.apply_async.return_value = mock_async
        mock_celery_imports["chain"].return_value = mock_chain_workflow

        context = Mock()
        context.study.dag.step.return_value = mock_step
        context.study.get_adapter_config.return_value = {"type": "celery"}
        context.study.sample_labels = []
        context.study.samples = []

        plan = ExecutionPlan([ExecutionLevel(depth=0, parallel_chains=[TaskChain(tasks=["task1"], depth=0)])])

        with tempfile.TemporaryDirectory() as tmpdir:
            context.study.workspace = tmpdir
            context.study.expanded_spec.name = "test_study"

            with patch("merlin.common.tasks.merlin_step") as mock_merlin_step:
                mock_sig = Mock()
                mock_merlin_step.s.return_value = mock_sig
                executor.execute_plan(plan, context, wait=True, timeout=60)

        # async_result.get() should be called with timeout
        mock_async.get.assert_called_once_with(timeout=60)

    def test_execute_plan_wait_true_updates_workflow_info_on_completion(self, mock_celery_imports):
        """Test execute_plan with wait=True updates WORKFLOW_INFO.json on completion"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()

        mock_step = Mock()
        mock_step.get_task_queue.return_value = "test_queue"
        mock_step.name.return_value = "task1"

        executor.sample_expander = Mock()
        executor.sample_expander.expand_chain.return_value = [[{"step": mock_step, "sample_id": None}]]

        mock_async = Mock()
        mock_async.id = "workflow-complete"
        mock_async.get.return_value = None
        mock_chain_workflow = Mock()
        mock_chain_workflow.apply_async.return_value = mock_async
        mock_celery_imports["chain"].return_value = mock_chain_workflow

        context = Mock()
        context.study.dag.step.return_value = mock_step
        context.study.get_adapter_config.return_value = {"type": "celery"}
        context.study.sample_labels = []
        context.study.samples = []

        plan = ExecutionPlan([ExecutionLevel(depth=0, parallel_chains=[TaskChain(tasks=["task1"], depth=0)])])

        with tempfile.TemporaryDirectory() as tmpdir:
            context.study.workspace = tmpdir
            context.study.expanded_spec.name = "test_study"

            with patch("merlin.common.tasks.merlin_step") as mock_merlin_step:
                mock_sig = Mock()
                mock_merlin_step.s.return_value = mock_sig
                executor.execute_plan(plan, context, wait=True)

                # Check WORKFLOW_INFO.json was updated to COMPLETED
                workflow_info_path = os.path.join(tmpdir, "WORKFLOW_INFO.json")
                with open(workflow_info_path) as f:
                    info = json.load(f)

                assert info["status"] == "COMPLETED"
                assert "completed_at" in info

    def test_execute_plan_wait_true_handles_failure(self, mock_celery_imports):
        """Test execute_plan with wait=True handles workflow failure"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()

        mock_step = Mock()
        mock_step.get_task_queue.return_value = "test_queue"
        mock_step.name.return_value = "task1"

        executor.sample_expander = Mock()
        executor.sample_expander.expand_chain.return_value = [[{"step": mock_step, "sample_id": None}]]

        mock_async = Mock()
        mock_async.id = "workflow-fail"
        mock_async.get.side_effect = Exception("Task execution failed")
        mock_chain_workflow = Mock()
        mock_chain_workflow.apply_async.return_value = mock_async
        mock_celery_imports["chain"].return_value = mock_chain_workflow

        context = Mock()
        context.study.dag.step.return_value = mock_step
        context.study.get_adapter_config.return_value = {"type": "celery"}
        context.study.sample_labels = []
        context.study.samples = []

        plan = ExecutionPlan([ExecutionLevel(depth=0, parallel_chains=[TaskChain(tasks=["task1"], depth=0)])])

        with tempfile.TemporaryDirectory() as tmpdir:
            context.study.workspace = tmpdir
            context.study.expanded_spec.name = "test_study"

            with patch("merlin.common.tasks.merlin_step") as mock_merlin_step:
                mock_sig = Mock()
                mock_merlin_step.s.return_value = mock_sig
                result = executor.execute_plan(plan, context, wait=True)

                # Check tasks marked as failed
                assert result["results"]["task1"].status == TaskStatus.FAILED
                assert "Task execution failed" in result["results"]["task1"].error

                # Check WORKFLOW_INFO.json was updated to FAILED
                workflow_info_path = os.path.join(tmpdir, "WORKFLOW_INFO.json")
                with open(workflow_info_path) as f:
                    info = json.load(f)

                assert info["status"] == "FAILED"
                assert "error" in info


class TestExecutePlanBatching:
    """Tests for execute_plan batching and chain/group pattern"""

    def test_execute_plan_uses_chain_of_groups(self, mock_celery_imports):
        """Test execute_plan builds chain(group(...), group(...)) pattern"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()

        mock_step = Mock()
        mock_step.get_task_queue.return_value = "test_queue"
        mock_step.name.return_value = "task1"

        # Mock sample_expander to return 3 samples
        mock_steps = [Mock() for _ in range(3)]
        for i, s in enumerate(mock_steps):
            s.get_task_queue.return_value = "test_queue"
            s.name.return_value = f"task_sample{i}"

        executor.sample_expander = Mock()
        executor.sample_expander.expand_chain.return_value = [[{"step": s, "sample_id": i} for i, s in enumerate(mock_steps)]]

        mock_group_result = Mock()
        mock_celery_imports["group"].return_value = mock_group_result

        mock_chain_result = Mock()
        mock_chain_result.apply_async.return_value = Mock(id="workflow-chain-group")
        mock_celery_imports["chain"].return_value = mock_chain_result

        context = Mock()
        context.study.dag.step.return_value = mock_step
        context.study.get_adapter_config.return_value = {"type": "celery"}
        context.study.sample_labels = []
        context.study.samples = []

        plan = ExecutionPlan([ExecutionLevel(depth=0, parallel_chains=[TaskChain(tasks=["task1"], depth=0)])])

        with tempfile.TemporaryDirectory() as tmpdir:
            context.study.workspace = tmpdir
            context.study.expanded_spec.name = "test_study"

            with patch("merlin.common.tasks.merlin_step") as mock_merlin_step:
                mock_sig = Mock()
                mock_merlin_step.s.return_value = mock_sig
                executor.execute_plan(plan, context, wait=False)

        # group() should be called to create batch groups
        assert mock_celery_imports["group"].called
        # chain() should be called to chain the groups
        assert mock_celery_imports["chain"].called


class TestExecuteChainMethod:
    """Tests for CeleryExecutor.execute_chain()"""

    def test_execute_chain_skips_virtual_nodes(self, mock_celery_imports):
        """Test execute_chain skips virtual nodes"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()

        context = Mock()
        context.study.dag.step.return_value = None  # Virtual node
        context.study.get_adapter_config.return_value = {"type": "celery"}

        chain_obj = TaskChain(tasks=["_source"], depth=0)

        results = executor.execute_chain(chain_obj, context)

        assert len(results) == 1
        assert results[0].status == TaskStatus.SKIPPED
        assert "Virtual node" in results[0].error

    def test_execute_chain_executes_real_tasks(self, mock_celery_imports):
        """Test execute_chain executes real tasks"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()

        mock_step = Mock()
        mock_step.get_task_queue.return_value = "test_queue"

        mock_async = Mock()
        mock_async.get.return_value = 0
        mock_chain_result = Mock()
        mock_chain_result.apply_async.return_value = mock_async
        mock_celery_imports["chain"].return_value = mock_chain_result

        context = Mock()
        context.study.dag.step.return_value = mock_step
        context.study.get_adapter_config.return_value = {"type": "celery"}

        chain_obj = TaskChain(tasks=["task1"], depth=1)

        with patch("merlin.common.tasks.merlin_step") as mock_merlin_step:
            mock_sig = Mock()
            mock_merlin_step.s.return_value = mock_sig
            results = executor.execute_chain(chain_obj, context)

        assert len(results) == 1
        assert results[0].status == TaskStatus.COMPLETED


class TestExecuteTaskMethod:
    """Tests for CeleryExecutor.execute_task()"""

    def test_execute_task_skips_virtual_node(self, mock_celery_imports):
        """Test execute_task skips virtual nodes"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()

        context = Mock()
        context.study.dag.step.return_value = None

        result = executor.execute_task("_source", context)

        assert result.status == TaskStatus.SKIPPED
        assert "Virtual node" in result.error

    def test_execute_task_executes_real_task(self, mock_celery_imports):
        """Test execute_task executes real task"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()

        mock_step = Mock()
        mock_step.get_task_queue.return_value = "test_queue"

        mock_sig = Mock()
        mock_async = Mock()
        mock_async.get.return_value = 0
        mock_sig.apply_async.return_value = mock_async

        context = Mock()
        context.study.dag.step.return_value = mock_step
        context.study.get_adapter_config.return_value = {"type": "celery"}

        with patch("merlin.common.tasks.merlin_step") as mock_merlin_step:
            mock_merlin_step.s.return_value = mock_sig
            result = executor.execute_task("task1", context)

        assert result.status == TaskStatus.COMPLETED
        assert result.celery_id is not None

    def test_execute_task_handles_exception(self, mock_celery_imports):
        """Test execute_task handles exceptions"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()

        mock_step = Mock()
        mock_step.get_task_queue.return_value = "test_queue"

        context = Mock()
        context.study.dag.step.return_value = mock_step
        context.study.get_adapter_config.return_value = {"type": "celery"}

        # Create mock merlin_step module and patch it in sys.modules
        mock_merlin_step = Mock()
        mock_sig = Mock()
        mock_sig.apply_async.side_effect = Exception("Connection failed")
        mock_merlin_step.s.return_value = mock_sig

        mock_tasks_module = Mock()
        mock_tasks_module.merlin_step = mock_merlin_step

        with patch.dict(sys.modules, {"merlin.common.tasks": mock_tasks_module}):
            result = executor.execute_task("task1", context)

        assert result.status == TaskStatus.FAILED
        assert "Connection failed" in result.error


class TestMarkDependentTasksSkipped:
    """Tests for CeleryExecutor._mark_dependent_tasks_skipped()"""

    def test_mark_dependent_tasks_skipped(self, mock_celery_imports):
        """Test _mark_dependent_tasks_skipped marks downstream tasks"""
        from merlin.execution.celery import CeleryExecutor

        executor = CeleryExecutor()

        # Create mock level that returns tasks
        mock_level2 = Mock()
        mock_level2.depth = 2
        mock_level2.get_all_tasks.return_value = ["task2a", "task2b"]

        mock_level3 = Mock()
        mock_level3.depth = 3
        mock_level3.get_all_tasks.return_value = ["task3"]

        plan = Mock()
        plan.levels = [
            Mock(depth=0),
            Mock(depth=1),
            mock_level2,
            mock_level3,
        ]

        results = {}

        # Mark tasks after depth 1 as skipped
        executor._mark_dependent_tasks_skipped(plan, failed_depth=1, results=results)

        # Tasks at depth 2 and 3 should be skipped
        assert "task2a" in results
        assert "task2b" in results
        assert "task3" in results
        assert results["task2a"].status == TaskStatus.SKIPPED
        assert results["task2b"].status == TaskStatus.SKIPPED
        assert results["task3"].status == TaskStatus.SKIPPED
        assert "Dependency failed" in results["task2a"].error
