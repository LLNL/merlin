from typing import Dict, List
from merlin.common.sample_index import uniform_directories
from merlin.common.sample_index_factory import create_hierarchy
from merlin.dag.models import TaskChain
from merlin.execution.models import ExecutionContext
from merlin.spec.expansion import parameter_substitutions_for_sample
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
        if not self.needs_expansion(chain, context):
            # No expansion needed - return original chain info
            dag = context.study.dag
            result = []
            for task_name in chain.tasks:
                step = dag.step(task_name)
                if step is not None:
                    result.append({
                        'chain': TaskChain([task_name], chain.depth),
                        'step': step,
                        'sample_id': None,
                        'sample_values': None,
                        'workspace': step.get_workspace()
                    })
            return result

        samples = context.study.samples
        labels = context.study.sample_labels
        dag = context.study.dag

        # Create sample hierarchy
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

        # Expand for each sample
        expanded_tasks = []

        for sample_id, sample in enumerate(samples):
            # Get relative path for this sample
            relative_path = sample_index.get_path_to_sample(sample_id)

            for task_name in chain.tasks:
                step = dag.step(task_name)

                # Skip virtual nodes
                if step is None:
                    continue

                # Get parameter substitutions for this sample
                substitutions = parameter_substitutions_for_sample(
                    sample,
                    labels,
                    sample_id,
                    relative_path
                )

                # Clone step with substitutions
                expanded_step = step.clone_changing_workspace_and_cmd(
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
