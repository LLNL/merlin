from typing import Dict, List
from merlin.common.sample_index import uniform_directories
from merlin.common.sample_index_factory import create_hierarchy
from merlin.dag.models import TaskChain
from merlin.execution.models import ExecutionContext
from merlin.spec.expansion import parameter_substitutions_for_sample, parameter_substitutions_for_cmd
import logging

LOG = logging.getLogger(__name__)


class SampleExpander:
    """
    Handles expansion of task chains based on samples.

    This class takes a TaskChain and expands it into multiple chains,
    one for each sample, with parameter substitutions applied.
    """

    def __init__(self, level_max_dirs: int = 25):
        self.level_max_dirs = level_max_dirs

    def needs_expansion(
        self,
        chain: TaskChain,
        context: ExecutionContext
    ) -> bool:
        """
        Check if a chain needs sample expansion.

        Args:
            chain: The TaskChain to check
            context: Execution context with study info

        Returns:
            True if expansion needed, False otherwise
        """
        # Check if any step in chain needs expansion
        dag = context.study.dag
        labels = context.study.sample_labels

        if not labels or len(labels) == 0:
            return False

        for task_name in chain.tasks:
            step = dag.step(task_name)
            if step is not None and step.check_if_expansion_needed(labels):
                return True

        return False

    def expand_chain(
        self,
        chain: TaskChain,
        context: ExecutionContext
    ) -> List[List[Dict]]:
        """
        Expand a chain into sample-specific chains, preserving chain structure.

        Args:
            chain: The TaskChain to expand
            context: Execution context

        Returns:
            2D list structure: [[step0_samples...], [step1_samples...], ...]
            Each inner list contains dictionaries with:
                - 'step': Step object
                - 'sample_id': int (or None if no expansion)
                - 'sample_values': Dict (or None if no expansion)
                - 'workspace': str
                - 'chain_position': int (position in original chain)
                - 'original_chain': TaskChain (the original chain)
        """
        samples = context.study.samples
        labels = context.study.sample_labels
        dag = context.study.dag

        # STEP 1: Calculate glob_path and sample_paths for ALL steps (even those not expanded)
        # This is needed for steps like 'collect' that reference $(MERLIN_GLOB_PATH)
        glob_path = ""
        sample_paths = ""

        if samples is not None and len(samples) > 0:
            # Create sample hierarchy to get glob_path and sample_paths
            directory_sizes = uniform_directories(
                len(samples),
                bundle_size=1,
                level_max_dirs=self.level_max_dirs
            )

            # Build glob_path (e.g., "*/*/*/*/*")
            # CRITICAL FIX: Add one extra level for Maestro execution directories (samples0-1.ext, etc.)
            # Files end up in: workspace/sample_dir/execution_dir/file.json
            # So we need: */* to match sample_dir/execution_dir
            # Note: No trailing slash since the command adds one
            glob_path = "/".join(["*"] * (len(directory_sizes) + 1))
            LOG.info(f"Calculated glob_path: '{glob_path}' from directory_sizes={directory_sizes} (+1 for execution dir) for {len(samples)} samples")

            # Create sample index to get all sample paths
            sample_index = create_hierarchy(
                len(samples),
                bundle_size=1,
                directory_sizes=directory_sizes,
                root="",
                n_digits=len(str(self.level_max_dirs))
            )

            # Build sample_paths string (e.g., "00/00/00:00/00/01:...")
            sample_paths = sample_index.make_directory_string()

        # STEP 2: Apply MERLIN_GLOB_PATH and MERLIN_PATHS_ALL to ALL steps in chain
        # This must happen BEFORE checking if expansion is needed
        steps_with_glob = []
        for task_name in chain.tasks:
            step = dag.step(task_name)
            if step is not None:
                # Clone with glob substitutions
                step_with_glob = step.clone_changing_workspace_and_cmd(
                    cmd_replacement_pairs=parameter_substitutions_for_cmd(glob_path, sample_paths)
                )
                steps_with_glob.append((task_name, step_with_glob))

        # STEP 3: Check if expansion is needed (after glob substitution)
        needs_expansion = False
        if labels and len(labels) > 0:
            for task_name, step in steps_with_glob:
                if step.check_if_expansion_needed(labels):
                    needs_expansion = True
                    break

        LOG.info(f"Sample expansion check: needs_expansion={needs_expansion}, num_samples={len(samples) if samples is not None else 0}, labels={labels}")

        if not needs_expansion:
            # No expansion needed - return steps with glob substitutions applied
            # Group by chain position (2D structure)
            result = []
            for position, (task_name, step) in enumerate(steps_with_glob):
                result.append([{
                    'step': step,
                    'sample_id': None,
                    'sample_values': None,
                    'workspace': step.get_workspace(),
                    'chain_position': position,
                    'original_chain': chain
                }])
            return result

        # STEP 4: Expand for each sample
        # Group by chain position (2D structure: position -> samples)

        # Recreate sample_index for iteration
        directory_sizes = uniform_directories(
            len(samples),
            bundle_size=1,
            level_max_dirs=self.level_max_dirs
        )

        sample_index = create_hierarchy(
            len(samples),
            bundle_size=1,
            directory_sizes=directory_sizes,
            root="",
            n_digits=len(str(self.level_max_dirs))
        )

        # Build 2D structure: iterate over chain positions first, then samples
        result = []
        for position, (task_name, step_with_glob) in enumerate(steps_with_glob):
            position_tasks = []  # All samples for this position

            for sample_id, sample in enumerate(samples):
                # Get relative path for this sample
                relative_path = sample_index.get_path_to_sample(sample_id)

                # Get parameter substitutions for this sample
                substitutions = parameter_substitutions_for_sample(
                    sample,
                    labels,
                    sample_id,
                    relative_path
                )

                # CRITICAL FIX: Append sample path to workspace for sample-specific directories
                # Each sample should run in its own subdirectory
                sample_workspace = f"{step_with_glob.get_workspace()}/{relative_path}".rstrip("/")
                LOG.info(f"Expanded sample {sample_id} workspace: {sample_workspace}")

                # Clone step (already has glob substitutions) with sample substitutions and workspace
                expanded_step = step_with_glob.clone_changing_workspace_and_cmd(
                    cmd_replacement_pairs=substitutions,
                    new_workspace=sample_workspace
                )

                position_tasks.append({
                    'step': expanded_step,
                    'sample_id': sample_id,
                    'sample_values': dict(zip(labels, sample)),
                    'workspace': expanded_step.get_workspace(),
                    'chain_position': position,
                    'original_chain': chain
                })

            result.append(position_tasks)

        # Log expansion results
        total_tasks = sum(len(pos_tasks) for pos_tasks in result)
        LOG.info(f"Sample expansion complete: created {total_tasks} tasks across {len(result)} positions for chain with {len(chain.tasks)} original tasks")

        return result
