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
    ) -> List[Dict]:
        """
        Expand a chain into sample-specific chains.

        Args:
            chain: The TaskChain to expand
            context: Execution context

        Returns:
            List of dictionaries, each containing:
                - 'chain': TaskChain object
                - 'step': Step object
                - 'sample_id': int
                - 'sample_values': Dict
                - 'workspace': str
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

            # Build glob_path (e.g., "*/*/*/*/")
            glob_path = "*/" * len(directory_sizes)

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

        if not needs_expansion:
            # No expansion needed - return steps with glob substitutions applied
            result = []
            for task_name, step in steps_with_glob:
                result.append({
                    'chain': TaskChain([task_name], chain.depth),
                    'step': step,
                    'sample_id': None,
                    'sample_values': None,
                    'workspace': step.get_workspace()
                })
            return result

        # STEP 4: Expand for each sample
        expanded_tasks = []

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

        for sample_id, sample in enumerate(samples):
            # Get relative path for this sample
            relative_path = sample_index.get_path_to_sample(sample_id)

            for task_name, step_with_glob in steps_with_glob:
                # Get parameter substitutions for this sample
                substitutions = parameter_substitutions_for_sample(
                    sample,
                    labels,
                    sample_id,
                    relative_path
                )

                # Clone step (already has glob substitutions) with sample substitutions
                expanded_step = step_with_glob.clone_changing_workspace_and_cmd(
                    cmd_replacement_pairs=substitutions
                )

                expanded_tasks.append({
                    'chain': TaskChain([task_name], chain.depth),
                    'step': expanded_step,
                    'sample_id': sample_id,
                    'sample_values': dict(zip(labels, sample)),
                    'workspace': expanded_step.get_workspace()
                })

        return expanded_tasks
