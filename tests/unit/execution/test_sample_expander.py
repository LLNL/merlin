##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the SampleExpander class.
"""

from unittest.mock import Mock, patch

import numpy as np

from merlin.dag.models import TaskChain
from merlin.execution.sample_expander import SampleExpander


class TestSampleExpanderInit:
    """Tests for SampleExpander.__init__()"""

    def test_init_default_level_max_dirs(self):
        """Test that default level_max_dirs is 25"""
        expander = SampleExpander()
        assert expander.level_max_dirs == 25

    def test_init_custom_level_max_dirs(self):
        """Test that custom level_max_dirs is set correctly"""
        expander = SampleExpander(level_max_dirs=50)
        assert expander.level_max_dirs == 50


class TestNeedsExpansion:
    """Tests for SampleExpander.needs_expansion()"""

    def test_needs_expansion_no_labels(self):
        """Test needs_expansion returns False when no labels"""
        expander = SampleExpander()

        # Mock context with no labels
        context = Mock()
        context.study.sample_labels = []
        context.study.dag = Mock()

        chain = TaskChain(tasks=["step1"], depth=1)

        result = expander.needs_expansion(chain, context)
        assert result is False

    def test_needs_expansion_none_labels(self):
        """Test needs_expansion returns False when labels is None"""
        expander = SampleExpander()

        context = Mock()
        context.study.sample_labels = None
        context.study.dag = Mock()

        chain = TaskChain(tasks=["step1"], depth=1)

        result = expander.needs_expansion(chain, context)
        assert result is False

    def test_needs_expansion_with_expandable_step(self):
        """Test needs_expansion returns True when step needs expansion"""
        expander = SampleExpander()

        # Mock step that needs expansion
        mock_step = Mock()
        mock_step.check_if_expansion_needed.return_value = True

        # Mock context
        context = Mock()
        context.study.sample_labels = ["X0", "X1"]
        context.study.dag.step.return_value = mock_step

        chain = TaskChain(tasks=["step1"], depth=1)

        result = expander.needs_expansion(chain, context)
        assert result is True
        mock_step.check_if_expansion_needed.assert_called_once_with(["X0", "X1"])

    def test_needs_expansion_no_expandable_steps(self):
        """Test needs_expansion returns False when no steps need expansion"""
        expander = SampleExpander()

        # Mock step that doesn't need expansion
        mock_step = Mock()
        mock_step.check_if_expansion_needed.return_value = False

        context = Mock()
        context.study.sample_labels = ["X0", "X1"]
        context.study.dag.step.return_value = mock_step

        chain = TaskChain(tasks=["step1", "step2"], depth=1)

        result = expander.needs_expansion(chain, context)
        assert result is False

    def test_needs_expansion_virtual_node(self):
        """Test needs_expansion handles virtual nodes (None steps)"""
        expander = SampleExpander()

        context = Mock()
        context.study.sample_labels = ["X0", "X1"]
        context.study.dag.step.return_value = None  # Virtual node

        chain = TaskChain(tasks=["_source"], depth=0)

        result = expander.needs_expansion(chain, context)
        assert result is False


class TestExpandChainNoSamples:
    """Tests for expand_chain() with no samples"""

    def test_expand_chain_no_samples(self):
        """Test expand_chain returns single task when no samples"""
        expander = SampleExpander()

        # Mock step
        mock_step = Mock()
        mock_step.get_workspace.return_value = "/workspace/step1"
        mock_step.clone_changing_workspace_and_cmd.return_value = mock_step
        mock_step.check_if_expansion_needed.return_value = False

        # Mock context with no samples
        context = Mock()
        context.study.samples = None
        context.study.sample_labels = []
        context.study.dag.step.return_value = mock_step

        chain = TaskChain(tasks=["step1"], depth=1)

        result = expander.expand_chain(chain, context)

        # Should return 2D structure with single position, single task
        assert len(result) == 1  # One position
        assert len(result[0]) == 1  # One task in position
        assert result[0][0]["step"] == mock_step
        assert result[0][0]["sample_id"] is None
        assert result[0][0]["sample_values"] is None
        assert result[0][0]["workspace"] == "/workspace/step1"
        assert result[0][0]["chain_position"] == 0
        assert result[0][0]["original_chain"] == chain

    def test_expand_chain_empty_samples(self):
        """Test expand_chain with empty samples array"""
        expander = SampleExpander()

        mock_step = Mock()
        mock_step.get_workspace.return_value = "/workspace/step1"
        mock_step.clone_changing_workspace_and_cmd.return_value = mock_step
        mock_step.check_if_expansion_needed.return_value = False

        context = Mock()
        context.study.samples = np.array([])  # Empty array
        context.study.sample_labels = []
        context.study.dag.step.return_value = mock_step

        chain = TaskChain(tasks=["step1"], depth=1)

        result = expander.expand_chain(chain, context)

        assert len(result) == 1
        assert len(result[0]) == 1
        assert result[0][0]["sample_id"] is None


class TestExpandChainWithSamples:
    """Tests for expand_chain() with samples"""

    @patch("merlin.execution.sample_expander.uniform_directories")
    @patch("merlin.execution.sample_expander.create_hierarchy")
    @patch("merlin.execution.sample_expander.parameter_substitutions_for_cmd")
    @patch("merlin.execution.sample_expander.parameter_substitutions_for_sample")
    def test_expand_chain_single_step_multiple_samples(
        self, mock_param_sub_sample, mock_param_sub_cmd, mock_create_hierarchy, mock_uniform_dirs
    ):
        """Test expand_chain creates one task per sample"""
        expander = SampleExpander()

        # Mock samples (3 samples)
        samples = np.array([[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]])
        labels = ["X0", "X1"]

        # Mock directory structure
        mock_uniform_dirs.return_value = [1]  # Single directory level

        # Mock sample index
        mock_sample_index = Mock()
        mock_sample_index.make_directory_string.return_value = "00:01:02"
        mock_sample_index.get_path_to_sample.side_effect = lambda i: f"{i:02d}"
        mock_create_hierarchy.return_value = mock_sample_index

        # Mock parameter substitutions
        mock_param_sub_cmd.return_value = [("$(MERLIN_GLOB_PATH)", "*/")]
        mock_param_sub_sample.side_effect = lambda s, labels, i, p: [("$(X0)", str(s[0])), ("$(X1)", str(s[1]))]

        # Mock step
        mock_step = Mock()
        mock_step.get_workspace.return_value = "/workspace/hello"
        mock_step.check_if_expansion_needed.return_value = True

        # Create cloned steps for each stage
        mock_step_with_glob = Mock()
        mock_step_with_glob.get_workspace.return_value = "/workspace/hello"
        mock_step_with_glob.check_if_expansion_needed.return_value = True

        mock_expanded_steps = []
        for i in range(3):
            expanded = Mock()
            expanded.get_workspace.return_value = f"/workspace/hello/{i:02d}"
            mock_expanded_steps.append(expanded)

        # Setup clone behavior
        mock_step.clone_changing_workspace_and_cmd.return_value = mock_step_with_glob
        mock_step_with_glob.clone_changing_workspace_and_cmd.side_effect = mock_expanded_steps

        # Mock context
        context = Mock()
        context.study.samples = samples
        context.study.sample_labels = labels
        context.study.dag.step.return_value = mock_step

        chain = TaskChain(tasks=["hello"], depth=1)

        result = expander.expand_chain(chain, context)

        # Should return 2D structure: [[sample0, sample1, sample2]]
        assert len(result) == 1  # One position (single step in chain)
        assert len(result[0]) == 3  # Three samples

        # Check each sample
        for i in range(3):
            task_info = result[0][i]
            assert task_info["sample_id"] == i
            assert task_info["sample_values"] == {"X0": samples[i][0], "X1": samples[i][1]}
            assert task_info["workspace"] == f"/workspace/hello/{i:02d}"
            assert task_info["chain_position"] == 0
            assert task_info["original_chain"] == chain

    @patch("merlin.execution.sample_expander.uniform_directories")
    @patch("merlin.execution.sample_expander.create_hierarchy")
    @patch("merlin.execution.sample_expander.parameter_substitutions_for_cmd")
    @patch("merlin.execution.sample_expander.parameter_substitutions_for_sample")
    def test_expand_chain_multi_position_chain(
        self, mock_param_sub_sample, mock_param_sub_cmd, mock_create_hierarchy, mock_uniform_dirs
    ):
        """Test expand_chain with multi-step chain (collect -> translate)"""
        expander = SampleExpander()

        # Mock samples (2 samples)
        samples = np.array([[1.0, 2.0], [3.0, 4.0]])
        labels = ["X0", "X1"]

        # Mock directory structure
        mock_uniform_dirs.return_value = [1]

        mock_sample_index = Mock()
        mock_sample_index.make_directory_string.return_value = "00:01"
        mock_sample_index.get_path_to_sample.side_effect = lambda i: f"{i:02d}"
        mock_create_hierarchy.return_value = mock_sample_index

        mock_param_sub_cmd.return_value = [("$(MERLIN_GLOB_PATH)", "*/")]
        mock_param_sub_sample.side_effect = lambda s, labels, i, p: [("$(X0)", str(s[0])), ("$(X1)", str(s[1]))]

        # Mock two different steps
        mock_collect = Mock()
        mock_collect.get_workspace.return_value = "/workspace/collect"
        mock_collect.check_if_expansion_needed.return_value = False

        mock_translate = Mock()
        mock_translate.get_workspace.return_value = "/workspace/translate"
        mock_translate.check_if_expansion_needed.return_value = False

        # Setup cloned steps
        mock_collect_glob = Mock()
        mock_collect_glob.get_workspace.return_value = "/workspace/collect"
        mock_collect_glob.check_if_expansion_needed.return_value = False

        mock_translate_glob = Mock()
        mock_translate_glob.get_workspace.return_value = "/workspace/translate"
        mock_translate_glob.check_if_expansion_needed.return_value = False

        mock_collect.clone_changing_workspace_and_cmd.return_value = mock_collect_glob
        mock_translate.clone_changing_workspace_and_cmd.return_value = mock_translate_glob

        # Mock context with two steps
        context = Mock()
        context.study.samples = samples
        context.study.sample_labels = labels

        def get_step(task_name):
            if task_name == "collect":
                return mock_collect
            elif task_name == "translate":
                return mock_translate
            return None

        context.study.dag.step.side_effect = get_step

        chain = TaskChain(tasks=["collect", "translate"], depth=2)

        result = expander.expand_chain(chain, context)

        # Should return 2D structure: [[collect], [translate]]
        assert len(result) == 2  # Two positions
        assert len(result[0]) == 1  # One task at position 0 (no sample expansion)
        assert len(result[1]) == 1  # One task at position 1 (no sample expansion)

        # Check positions
        assert result[0][0]["chain_position"] == 0
        assert result[1][0]["chain_position"] == 1


class TestGlobPathCalculation:
    """Tests for MERLIN_GLOB_PATH calculation"""

    @patch("merlin.execution.sample_expander.uniform_directories")
    @patch("merlin.execution.sample_expander.create_hierarchy")
    @patch("merlin.execution.sample_expander.parameter_substitutions_for_cmd")
    def test_glob_path_calculation_10_samples(self, mock_param_sub_cmd, mock_create_hierarchy, mock_uniform_dirs):
        """Test MERLIN_GLOB_PATH is calculated correctly for 10 samples"""
        expander = SampleExpander()

        # Mock 10 samples
        samples = np.array([[i, i + 1] for i in range(10)])
        labels = ["X0", "X1"]

        # With 10 samples and level_max_dirs=25, should create [1] directory level
        # glob_path should be "*/*" (1 level + 1 for execution dir)
        mock_uniform_dirs.return_value = [1]

        mock_sample_index = Mock()
        mock_sample_index.make_directory_string.return_value = ":".join([f"{i:02d}" for i in range(10)])
        mock_sample_index.get_path_to_sample.side_effect = lambda i: f"{i:02d}"
        mock_create_hierarchy.return_value = mock_sample_index

        # Capture the glob_path passed to parameter_substitutions_for_cmd
        captured_glob_path = None

        def capture_glob_path(glob_path, sample_paths):
            nonlocal captured_glob_path
            captured_glob_path = glob_path
            return [("$(MERLIN_GLOB_PATH)", glob_path)]

        mock_param_sub_cmd.side_effect = capture_glob_path

        # Mock step
        mock_step = Mock()
        mock_step.get_workspace.return_value = "/workspace/hello"
        mock_step.check_if_expansion_needed.return_value = False
        mock_step.clone_changing_workspace_and_cmd.return_value = mock_step

        context = Mock()
        context.study.samples = samples
        context.study.sample_labels = labels
        context.study.dag.step.return_value = mock_step

        chain = TaskChain(tasks=["hello"], depth=1)

        expander.expand_chain(chain, context)

        # glob_path should be "*/*" (1 directory level + 1 execution dir level)
        assert captured_glob_path == "*/*"

    @patch("merlin.execution.sample_expander.uniform_directories")
    @patch("merlin.execution.sample_expander.create_hierarchy")
    @patch("merlin.execution.sample_expander.parameter_substitutions_for_cmd")
    def test_glob_path_calculation_100_samples(self, mock_param_sub_cmd, mock_create_hierarchy, mock_uniform_dirs):
        """Test MERLIN_GLOB_PATH is calculated correctly for 100 samples"""
        expander = SampleExpander()

        # Mock 100 samples
        samples = np.array([[i, i + 1] for i in range(100)])
        labels = ["X0", "X1"]

        # With 100 samples and level_max_dirs=25, should create [4, 25] directory levels
        # glob_path should be "*/*/*" (2 levels + 1 for execution dir)
        mock_uniform_dirs.return_value = [4, 25]

        mock_sample_index = Mock()
        mock_sample_index.make_directory_string.return_value = ":".join(
            [f"{i:02d}/{j:02d}" for i in range(4) for j in range(25)]
        )
        mock_sample_index.get_path_to_sample.side_effect = lambda i: f"{i // 25:02d}/{i % 25:02d}"
        mock_create_hierarchy.return_value = mock_sample_index

        captured_glob_path = None

        def capture_glob_path(glob_path, sample_paths):
            nonlocal captured_glob_path
            captured_glob_path = glob_path
            return [("$(MERLIN_GLOB_PATH)", glob_path)]

        mock_param_sub_cmd.side_effect = capture_glob_path

        mock_step = Mock()
        mock_step.get_workspace.return_value = "/workspace/hello"
        mock_step.check_if_expansion_needed.return_value = False
        mock_step.clone_changing_workspace_and_cmd.return_value = mock_step

        context = Mock()
        context.study.samples = samples
        context.study.sample_labels = labels
        context.study.dag.step.return_value = mock_step

        chain = TaskChain(tasks=["hello"], depth=1)

        expander.expand_chain(chain, context)

        # glob_path should be "*/*/*" (2 directory levels + 1 execution dir level)
        assert captured_glob_path == "*/*/*"


class TestWorkspaceIsolation:
    """Tests for sample workspace isolation"""

    @patch("merlin.execution.sample_expander.uniform_directories")
    @patch("merlin.execution.sample_expander.create_hierarchy")
    @patch("merlin.execution.sample_expander.parameter_substitutions_for_cmd")
    @patch("merlin.execution.sample_expander.parameter_substitutions_for_sample")
    def test_workspace_isolation_unique_paths(
        self, mock_param_sub_sample, mock_param_sub_cmd, mock_create_hierarchy, mock_uniform_dirs
    ):
        """Test each sample gets a unique workspace path"""
        expander = SampleExpander()

        # Mock 5 samples
        samples = np.array([[i, i + 1] for i in range(5)])
        labels = ["X0", "X1"]

        mock_uniform_dirs.return_value = [1]

        mock_sample_index = Mock()
        mock_sample_index.make_directory_string.return_value = "00:01:02:03:04"
        mock_sample_index.get_path_to_sample.side_effect = lambda i: f"{i:02d}"
        mock_create_hierarchy.return_value = mock_sample_index

        mock_param_sub_cmd.return_value = [("$(MERLIN_GLOB_PATH)", "*/")]
        mock_param_sub_sample.side_effect = lambda s, labels, i, p: []

        # Mock step
        base_workspace = "/workspace/hello"
        mock_step = Mock()
        mock_step.get_workspace.return_value = base_workspace
        mock_step.check_if_expansion_needed.return_value = True

        mock_step_with_glob = Mock()
        mock_step_with_glob.get_workspace.return_value = base_workspace
        mock_step_with_glob.check_if_expansion_needed.return_value = True

        # Create expanded steps with unique workspaces
        mock_expanded_steps = []
        for i in range(5):
            expanded = Mock()
            expanded.get_workspace.return_value = f"{base_workspace}/{i:02d}"
            mock_expanded_steps.append(expanded)

        mock_step.clone_changing_workspace_and_cmd.return_value = mock_step_with_glob
        mock_step_with_glob.clone_changing_workspace_and_cmd.side_effect = mock_expanded_steps

        context = Mock()
        context.study.samples = samples
        context.study.sample_labels = labels
        context.study.dag.step.return_value = mock_step

        chain = TaskChain(tasks=["hello"], depth=1)

        result = expander.expand_chain(chain, context)

        # Extract all workspaces
        workspaces = [task_info["workspace"] for task_info in result[0]]

        # Check all workspaces are unique
        assert len(workspaces) == len(set(workspaces))

        # Check each workspace follows expected pattern
        expected_workspaces = [f"{base_workspace}/{i:02d}" for i in range(5)]
        assert workspaces == expected_workspaces


class TestParameterSubstitutions:
    """Tests for parameter substitutions"""

    @patch("merlin.execution.sample_expander.uniform_directories")
    @patch("merlin.execution.sample_expander.create_hierarchy")
    @patch("merlin.execution.sample_expander.parameter_substitutions_for_cmd")
    @patch("merlin.execution.sample_expander.parameter_substitutions_for_sample")
    def test_parameter_substitutions_applied(
        self, mock_param_sub_sample, mock_param_sub_cmd, mock_create_hierarchy, mock_uniform_dirs
    ):
        """Test parameter substitutions are applied correctly"""
        expander = SampleExpander()

        samples = np.array([[1.5, 2.5], [3.5, 4.5]])
        labels = ["X0", "X1"]

        mock_uniform_dirs.return_value = [1]

        mock_sample_index = Mock()
        mock_sample_index.make_directory_string.return_value = "00:01"
        mock_sample_index.get_path_to_sample.side_effect = lambda i: f"{i:02d}"
        mock_create_hierarchy.return_value = mock_sample_index

        mock_param_sub_cmd.return_value = [("$(MERLIN_GLOB_PATH)", "*/")]

        # Track calls to parameter_substitutions_for_sample
        sample_sub_calls = []

        def track_sample_subs(sample, labels, sample_id, relative_path):
            sample_sub_calls.append(
                {"sample": sample.tolist(), "labels": labels, "sample_id": sample_id, "relative_path": relative_path}
            )
            return [("$(X0)", str(sample[0])), ("$(X1)", str(sample[1]))]

        mock_param_sub_sample.side_effect = track_sample_subs

        mock_step = Mock()
        mock_step.get_workspace.return_value = "/workspace/hello"
        mock_step.check_if_expansion_needed.return_value = True

        mock_step_with_glob = Mock()
        mock_step_with_glob.get_workspace.return_value = "/workspace/hello"
        mock_step_with_glob.check_if_expansion_needed.return_value = True

        mock_expanded_steps = [Mock(), Mock()]
        for i, m in enumerate(mock_expanded_steps):
            m.get_workspace.return_value = f"/workspace/hello/{i:02d}"

        mock_step.clone_changing_workspace_and_cmd.return_value = mock_step_with_glob
        mock_step_with_glob.clone_changing_workspace_and_cmd.side_effect = mock_expanded_steps

        context = Mock()
        context.study.samples = samples
        context.study.sample_labels = labels
        context.study.dag.step.return_value = mock_step

        chain = TaskChain(tasks=["hello"], depth=1)

        result = expander.expand_chain(chain, context)

        # Check parameter_substitutions_for_sample was called for each sample
        assert len(sample_sub_calls) == 2

        # Check first sample
        assert sample_sub_calls[0]["sample"] == [1.5, 2.5]
        assert sample_sub_calls[0]["labels"] == labels
        assert sample_sub_calls[0]["sample_id"] == 0
        assert sample_sub_calls[0]["relative_path"] == "00"

        # Check second sample
        assert sample_sub_calls[1]["sample"] == [3.5, 4.5]
        assert sample_sub_calls[1]["labels"] == labels
        assert sample_sub_calls[1]["sample_id"] == 1
        assert sample_sub_calls[1]["relative_path"] == "01"

        # Check sample_values in result
        assert result[0][0]["sample_values"] == {"X0": 1.5, "X1": 2.5}
        assert result[0][1]["sample_values"] == {"X0": 3.5, "X1": 4.5}
