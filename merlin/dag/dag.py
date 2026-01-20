##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Holds the Merlin Directed Acyclic Graph (DAG) class.
"""
from collections import OrderedDict
from typing import Dict, List

from merlin.dag.models import ExecutionLevel, ExecutionPlan, TaskChain
from merlin.study.step import Step


# TODO make this an interface, separate from Maestro.
class DAG:
    """
    Refactored DAG class with cleaner data structures.

    This class provides methods on a task graph that Merlin needs for staging
    tasks in Celery. It is initialized from a Maestro `ExecutionGraph`, and the
    major entry point is the group_tasks method, which provides an ExecutionPlan
    with clear structure instead of nested lists.

    Attributes:
        backwards_adjacency (Dict): A dictionary mapping each task to its parent tasks.
        column_labels (List[str]): A list of column labels provided in the spec file.
        maestro_adjacency_table (OrderedDict): An ordered dict showing adjacency of nodes.
        maestro_values (OrderedDict): An ordered dict of the values at each node.
        parameter_info (Dict): A dict containing information about parameters in the study.
        study_name (str): The name of the study.
    """

    def __init__(
        self,
        maestro_adjacency_table: OrderedDict,
        maestro_values: OrderedDict,
        column_labels: List[str],
        study_name: str,
        parameter_info: Dict,
    ):
        self.maestro_adjacency_table: OrderedDict = maestro_adjacency_table
        self.maestro_values: OrderedDict = maestro_values
        self.column_labels: List[str] = column_labels
        self.study_name: str = study_name
        self.parameter_info: Dict = parameter_info
        self.backwards_adjacency: Dict = {}
        self.calc_backwards_adjacency()

    def step(self, task_name: str):
        """Return a Step object for the given task name."""
        return Step(self.maestro_values[task_name], self.study_name, self.parameter_info)

    def calc_depth(self, node: str, depths: Dict, current_depth: int = 0):
        """Calculate the depth of the given node and its children."""
        if node not in depths:
            depths[node] = current_depth
        else:
            depths[node] = max(depths[node], current_depth)

        for child in self.children(node):
            self.calc_depth(child, depths, current_depth=depths[node] + 1)

    def group_by_depth(self, depths: Dict) -> List[ExecutionLevel]:
        """
        Group DAG tasks by depth, returning ExecutionLevels instead of nested lists.

        Each task starts as its own single-task chain. The find_independent_chains
        method will later coalesce compatible tasks into longer chains.
        """
        # Group tasks by depth
        depth_groups = {}
        for node, depth in depths.items():
            if depth not in depth_groups:
                depth_groups[depth] = []
            depth_groups[depth].append(node)

        # Create ExecutionLevels with each task as its own chain initially
        levels = []
        for depth in sorted(depth_groups.keys()):
            chains = [TaskChain([task], depth) for task in depth_groups[depth]]
            level = ExecutionLevel(depth, chains)
            levels.append(level)

        return levels

    def children(self, task_name: str) -> List:
        """Return the children of the task."""
        return self.maestro_adjacency_table[task_name]

    def num_children(self, task_name: str) -> int:
        """Find the number of children for the given task."""
        return len(self.children(task_name))

    def parents(self, task_name: str) -> List:
        """Return the parents of the task."""
        return self.backwards_adjacency.get(task_name, [])

    def num_parents(self, task_name: str) -> int:
        """Find the number of parents for the given task."""
        return len(self.parents(task_name))

    def find_chain_containing_task(self, task_name: str, levels: List[ExecutionLevel]) -> TaskChain:
        """Find the chain containing the given task."""
        for level in levels:
            chain = level.find_chain_containing_task(task_name)
            if chain:
                return chain
        return None

    def calc_backwards_adjacency(self):
        """Initialize the backwards adjacency table."""
        self.backwards_adjacency = {}
        for parent in self.maestro_adjacency_table:
            for task_name in self.maestro_adjacency_table[parent]:
                if task_name in self.backwards_adjacency:
                    self.backwards_adjacency[task_name].append(parent)
                else:
                    self.backwards_adjacency[task_name] = [parent]

    def compatible_merlin_expansion(self, task1: str, task2: str) -> bool:
        """Check if two tasks are compatible for Merlin expansion."""
        step1 = self.step(task1)
        step2 = self.step(task2)
        return step1.check_if_expansion_needed(self.column_labels) == step2.check_if_expansion_needed(self.column_labels)

    def find_independent_chains(self, levels: List[ExecutionLevel]) -> List[ExecutionLevel]:
        """
        Finds independent chains and coalesces them to maximize parallelism.

        This is much cleaner than the original nested list manipulation!
        """
        for level in levels:
            for chain in level.parallel_chains:
                # Process each task in the chain (need to iterate safely since we're modifying)
                tasks_to_process = chain.tasks.copy()
                for task_name in tasks_to_process:
                    # Skip if task is not in chain anymore (may have been moved)
                    if task_name not in chain.tasks:
                        continue

                    # Check if this task can be coalesced with its child
                    if self.num_children(task_name) == 1 and task_name != "_source":
                        child = self.children(task_name)[0]

                        if self.num_parents(child) == 1 and self.compatible_merlin_expansion(child, task_name):

                            # Find the child's chain and remove it from there
                            child_chain = self.find_chain_containing_task(child, levels)
                            if child_chain and child_chain != chain:
                                child_chain.remove_task(child)
                                # Add child to current chain
                                chain.add_task(child)

        # Clean up empty chains
        for level in levels:
            level.remove_empty_chains()

        # Remove empty levels
        non_empty_levels = [level for level in levels if level.parallel_chains]

        return non_empty_levels

    def group_tasks(self, source_node: str) -> ExecutionPlan:
        """
        Group independent tasks in a DAG, returning a clean ExecutionPlan.

        This is much more intuitive than the original nested list approach!
        """
        # Calculate depths
        depths = {}
        self.calc_depth(source_node, depths)

        # Group by depth into ExecutionLevels
        levels = self.group_by_depth(depths)

        # Find and coalesce independent chains
        optimized_levels = self.find_independent_chains(levels)

        # Return a clean ExecutionPlan
        return ExecutionPlan(optimized_levels)
