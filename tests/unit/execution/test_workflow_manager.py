##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the WorkflowManager class.
"""

from collections import OrderedDict
from unittest.mock import Mock

from merlin.execution.models import TaskResult, TaskStatus
from merlin.execution.workflow_manager import WorkflowManager


def create_mock_study_with_dag():
    """
    Create a properly configured mock study with dag attributes.

    WorkflowManager creates its own ExecutionDAG from study.dag's attributes,
    so we need to provide proper values for those attributes.
    """
    mock_study = Mock()
    mock_dag = Mock()

    # Provide proper values for DAG attributes needed by ExecutionDAG constructor
    mock_dag.maestro_adjacency_table = OrderedDict()
    mock_dag.maestro_values = OrderedDict()
    mock_dag.column_labels = []
    mock_dag.study_name = "test_study"
    mock_dag.parameter_info = {}

    mock_study.dag = mock_dag
    return mock_study, mock_dag


class TestWorkflowManagerInit:
    """Tests for WorkflowManager.__init__()"""

    def test_init_sets_study_and_executor(self):
        """Test that __init__ sets study and executor, and creates its own DAG"""
        mock_study, mock_dag = create_mock_study_with_dag()
        mock_executor = Mock()

        manager = WorkflowManager(study=mock_study, executor=mock_executor)

        assert manager.study == mock_study
        # DAG is created from study.dag's attributes, not the same object
        assert manager.dag is not mock_dag
        assert manager.dag.study_name == "test_study"
        assert manager.executor == mock_executor


class TestRunWorkflowBasic:
    """Tests for basic run_workflow functionality"""

    def test_run_workflow_generates_execution_plan(self):
        """Test that run_workflow generates execution plan from DAG"""
        mock_study, _ = create_mock_study_with_dag()

        mock_plan = Mock()
        mock_plan.levels = []

        mock_executor = Mock()
        mock_executor.execute_plan.return_value = {"results": {}}

        manager = WorkflowManager(study=mock_study, executor=mock_executor)
        # Mock the manager's dag.group_tasks after creation
        manager.dag.group_tasks = Mock(return_value=mock_plan)
        manager.run_workflow()

        # Should call group_tasks with default source_node
        manager.dag.group_tasks.assert_called_once_with("_source")

    def test_run_workflow_custom_source_node(self):
        """Test that run_workflow uses custom source_node"""
        mock_study, _ = create_mock_study_with_dag()

        mock_plan = Mock()
        mock_plan.levels = []

        mock_executor = Mock()
        mock_executor.execute_plan.return_value = {"results": {}}

        manager = WorkflowManager(study=mock_study, executor=mock_executor)
        manager.dag.group_tasks = Mock(return_value=mock_plan)
        manager.run_workflow(source_node="custom_start")

        manager.dag.group_tasks.assert_called_once_with("custom_start")

    def test_run_workflow_creates_execution_context(self):
        """Test that run_workflow creates ExecutionContext with correct fields"""
        mock_study, _ = create_mock_study_with_dag()

        mock_plan = Mock()
        mock_plan.levels = []

        mock_executor = Mock()
        mock_executor.execute_plan.return_value = {"results": {}}

        manager = WorkflowManager(study=mock_study, executor=mock_executor)
        manager.dag.group_tasks = Mock(return_value=mock_plan)
        manager.dag.parameter_info = {"param1": "value1"}
        manager.run_workflow()

        # Check that execute_plan was called with an ExecutionContext
        call_args = mock_executor.execute_plan.call_args
        context = call_args[0][1]  # Second positional argument

        assert context.study == mock_study
        assert context.parameter_info == {"param1": "value1"}
        assert context.execution_id is not None
        assert "started_at" in context.metadata


class TestRunWorkflowWaitBehavior:
    """Tests for run_workflow wait parameter behavior"""

    def test_run_workflow_wait_false_passes_to_executor(self):
        """Test that run_workflow(wait=False) passes wait=False to executor"""
        mock_study, _ = create_mock_study_with_dag()

        mock_plan = Mock()
        mock_plan.levels = []

        mock_executor = Mock()
        mock_executor.execute_plan.return_value = {"results": {}}

        manager = WorkflowManager(study=mock_study, executor=mock_executor)
        manager.dag.group_tasks = Mock(return_value=mock_plan)
        manager.run_workflow(wait=False)

        # Check wait=False was passed
        call_kwargs = mock_executor.execute_plan.call_args[1]
        assert call_kwargs["wait"] is False

    def test_run_workflow_wait_true_passes_to_executor(self):
        """Test that run_workflow(wait=True) passes wait=True to executor"""
        mock_study, _ = create_mock_study_with_dag()

        mock_plan = Mock()
        mock_plan.levels = []

        mock_executor = Mock()
        mock_executor.execute_plan.return_value = {"results": {}}

        manager = WorkflowManager(study=mock_study, executor=mock_executor)
        manager.dag.group_tasks = Mock(return_value=mock_plan)
        manager.run_workflow(wait=True)

        # Check wait=True was passed
        call_kwargs = mock_executor.execute_plan.call_args[1]
        assert call_kwargs["wait"] is True

    def test_run_workflow_default_wait_is_false(self):
        """Test that run_workflow defaults to wait=False (non-blocking)"""
        mock_study, _ = create_mock_study_with_dag()

        mock_plan = Mock()
        mock_plan.levels = []

        mock_executor = Mock()
        mock_executor.execute_plan.return_value = {"results": {}}

        manager = WorkflowManager(study=mock_study, executor=mock_executor)
        manager.dag.group_tasks = Mock(return_value=mock_plan)
        manager.run_workflow()  # No wait argument

        # Check wait=False (default)
        call_kwargs = mock_executor.execute_plan.call_args[1]
        assert call_kwargs["wait"] is False

    def test_run_workflow_timeout_passes_to_executor(self):
        """Test that run_workflow passes timeout to executor"""
        mock_study, _ = create_mock_study_with_dag()

        mock_plan = Mock()
        mock_plan.levels = []

        mock_executor = Mock()
        mock_executor.execute_plan.return_value = {"results": {}}

        manager = WorkflowManager(study=mock_study, executor=mock_executor)
        manager.dag.group_tasks = Mock(return_value=mock_plan)
        manager.run_workflow(wait=True, timeout=60)

        # Check timeout was passed
        call_kwargs = mock_executor.execute_plan.call_args[1]
        assert call_kwargs["timeout"] == 60

    def test_run_workflow_default_timeout_is_7200(self):
        """Test that run_workflow defaults to timeout=7200 (2 hours)"""
        mock_study, _ = create_mock_study_with_dag()

        mock_plan = Mock()
        mock_plan.levels = []

        mock_executor = Mock()
        mock_executor.execute_plan.return_value = {"results": {}}

        manager = WorkflowManager(study=mock_study, executor=mock_executor)
        manager.dag.group_tasks = Mock(return_value=mock_plan)
        manager.run_workflow()

        # Check timeout=7200 (default)
        call_kwargs = mock_executor.execute_plan.call_args[1]
        assert call_kwargs["timeout"] == 7200


class TestRunWorkflowReturnFormats:
    """Tests for run_workflow return format handling"""

    def test_run_workflow_handles_new_format_with_results_key(self):
        """Test run_workflow handles new format {'results': {...}, 'workflow_id': ...}"""
        mock_study, _ = create_mock_study_with_dag()

        mock_plan = Mock()
        mock_plan.levels = []

        mock_executor = Mock()
        # New format from CeleryExecutor
        mock_executor.execute_plan.return_value = {
            "results": {"task1": TaskResult(task_name="task1", status=TaskStatus.COMPLETED)},
            "workflow_id": "workflow-123",
            "async_result": Mock(),
        }

        manager = WorkflowManager(study=mock_study, executor=mock_executor)
        manager.dag.group_tasks = Mock(return_value=mock_plan)
        result = manager.run_workflow()

        # Should return the dict as-is
        assert "results" in result
        assert "workflow_id" in result
        assert result["workflow_id"] == "workflow-123"

    def test_run_workflow_handles_old_format_dict_only(self):
        """Test run_workflow handles old format (just results dict)"""
        mock_study, _ = create_mock_study_with_dag()

        mock_plan = Mock()
        mock_plan.levels = []

        mock_executor = Mock()
        # Old format from LocalExecutor (just results dict)
        mock_executor.execute_plan.return_value = {"task1": TaskResult(task_name="task1", status=TaskStatus.COMPLETED)}

        manager = WorkflowManager(study=mock_study, executor=mock_executor)
        manager.dag.group_tasks = Mock(return_value=mock_plan)
        result = manager.run_workflow()

        # Should wrap in {"results": ...}
        assert "results" in result
        assert "task1" in result["results"]


class TestRunWorkflowResultCounting:
    """Tests for run_workflow result counting"""

    def test_run_workflow_counts_completed_tasks(self):
        """Test that run_workflow correctly counts completed tasks"""
        mock_study, _ = create_mock_study_with_dag()

        mock_plan = Mock()
        mock_plan.levels = []

        mock_executor = Mock()
        mock_executor.execute_plan.return_value = {
            "results": {
                "task1": TaskResult(task_name="task1", status=TaskStatus.COMPLETED),
                "task2": TaskResult(task_name="task2", status=TaskStatus.COMPLETED),
                "task3": TaskResult(task_name="task3", status=TaskStatus.FAILED),
            }
        }

        manager = WorkflowManager(study=mock_study, executor=mock_executor)
        manager.dag.group_tasks = Mock(return_value=mock_plan)
        result = manager.run_workflow()

        # Should have 3 results
        assert len(result["results"]) == 3

    def test_run_workflow_counts_failed_tasks(self):
        """Test that run_workflow correctly counts failed tasks"""
        mock_study, _ = create_mock_study_with_dag()

        mock_plan = Mock()
        mock_plan.levels = []

        mock_executor = Mock()
        mock_executor.execute_plan.return_value = {
            "results": {
                "task1": TaskResult(task_name="task1", status=TaskStatus.COMPLETED),
                "task2": TaskResult(task_name="task2", status=TaskStatus.FAILED, error="Error"),
                "task3": TaskResult(task_name="task3", status=TaskStatus.FAILED, error="Error"),
            }
        }

        manager = WorkflowManager(study=mock_study, executor=mock_executor)
        manager.dag.group_tasks = Mock(return_value=mock_plan)
        result = manager.run_workflow()

        # Count failed tasks
        failed_count = sum(1 for r in result["results"].values() if r.status == TaskStatus.FAILED)
        assert failed_count == 2


class TestRunWorkflowWithLevels:
    """Tests for run_workflow with execution levels"""

    def test_run_workflow_handles_multi_level_plan(self):
        """Test run_workflow handles execution plan with multiple levels"""
        mock_study, _ = create_mock_study_with_dag()

        # Create mock plan with multiple levels
        mock_level0 = Mock()
        mock_level0.depth = 0
        mock_level0.parallel_chains = [Mock()]

        mock_level1 = Mock()
        mock_level1.depth = 1
        mock_level1.parallel_chains = [Mock(), Mock()]

        mock_level2 = Mock()
        mock_level2.depth = 2
        mock_level2.parallel_chains = [Mock()]

        mock_plan = Mock()
        mock_plan.levels = [mock_level0, mock_level1, mock_level2]

        mock_executor = Mock()
        mock_executor.execute_plan.return_value = {"results": {}}

        manager = WorkflowManager(study=mock_study, executor=mock_executor)
        manager.dag.group_tasks = Mock(return_value=mock_plan)
        manager.run_workflow()

        # Should have called execute_plan with the plan
        call_args = mock_executor.execute_plan.call_args[0]
        assert call_args[0] == mock_plan

    def test_run_workflow_handles_empty_plan(self):
        """Test run_workflow handles empty execution plan"""
        mock_study, _ = create_mock_study_with_dag()

        mock_plan = Mock()
        mock_plan.levels = []

        mock_executor = Mock()
        mock_executor.execute_plan.return_value = {"results": {}}

        manager = WorkflowManager(study=mock_study, executor=mock_executor)
        manager.dag.group_tasks = Mock(return_value=mock_plan)
        result = manager.run_workflow()

        assert "results" in result
        assert len(result["results"]) == 0


class TestRunWorkflowExecutionId:
    """Tests for execution ID generation"""

    def test_run_workflow_generates_unique_execution_id(self):
        """Test that run_workflow generates unique execution IDs"""
        mock_study, _ = create_mock_study_with_dag()

        mock_plan = Mock()
        mock_plan.levels = []

        mock_executor = Mock()
        mock_executor.execute_plan.return_value = {"results": {}}

        manager = WorkflowManager(study=mock_study, executor=mock_executor)
        manager.dag.group_tasks = Mock(return_value=mock_plan)

        # Run workflow twice and collect execution IDs
        manager.run_workflow()
        context1 = mock_executor.execute_plan.call_args_list[0][0][1]

        manager.run_workflow()
        context2 = mock_executor.execute_plan.call_args_list[1][0][1]

        # Execution IDs should be different
        assert context1.execution_id != context2.execution_id

    def test_run_workflow_execution_id_is_uuid_format(self):
        """Test that execution_id is in UUID format"""
        import uuid

        mock_study, _ = create_mock_study_with_dag()

        mock_plan = Mock()
        mock_plan.levels = []

        mock_executor = Mock()
        mock_executor.execute_plan.return_value = {"results": {}}

        manager = WorkflowManager(study=mock_study, executor=mock_executor)
        manager.dag.group_tasks = Mock(return_value=mock_plan)
        manager.run_workflow()

        context = mock_executor.execute_plan.call_args[0][1]

        # Should be valid UUID
        try:
            uuid.UUID(context.execution_id)
            is_valid_uuid = True
        except ValueError:
            is_valid_uuid = False

        assert is_valid_uuid


class TestRunWorkflowMetadata:
    """Tests for workflow metadata"""

    def test_run_workflow_includes_started_at_in_metadata(self):
        """Test that run_workflow includes started_at timestamp in metadata"""
        mock_study, _ = create_mock_study_with_dag()

        mock_plan = Mock()
        mock_plan.levels = []

        mock_executor = Mock()
        mock_executor.execute_plan.return_value = {"results": {}}

        manager = WorkflowManager(study=mock_study, executor=mock_executor)
        manager.dag.group_tasks = Mock(return_value=mock_plan)
        manager.run_workflow()

        context = mock_executor.execute_plan.call_args[0][1]

        assert "started_at" in context.metadata
        assert isinstance(context.metadata["started_at"], float)
