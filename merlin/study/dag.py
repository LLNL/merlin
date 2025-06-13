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

from merlin.study.step import Step


# TODO make this an interface, separate from Maestro.
class DAG:
    """
    This class provides methods on a task graph that Merlin needs for staging
    tasks in Celery. It is initialized from a Maestro `ExecutionGraph`, and the
    major entry point is the group_tasks method, which provides groups of
    independent chains of tasks.

    Attributes:
        backwards_adjacency (Dict): A dictionary mapping each task to its parent tasks for reverse
            traversal.
        column_labels (List[str]): A list of column labels provided in the spec file.
        maestro_adjacency_table (OrderedDict): An ordered dict showing adjacency of nodes. Comes from
            a maestrowf `ExecutionGraph`.
        maestro_values (OrderedDict): An ordered dict of the values at each node. Comes from a maestrowf
            `ExecutionGraph`.
        parameter_info (Dict): A dict containing information about parameters in the study.
        study_name (str): The name of the study.

    Methods:
        calc_backwards_adjacency: Initializes the backwards adjacency table.
        calc_depth: Calculate the depth of the given node and its children.
        children: Return the children of the task.
        compatible_merlin_expansion: Check if two tasks are compatible for Merlin expansion.
        find_chain: Find the chain containing the task.
        find_independent_chains: Finds independent chains and adjusts with the groups of chains
            to maximize parallelism.
        group_by_depth: Group Directed Acyclic Graph (DAG) tasks by depth.
        group_tasks: Group independent tasks in a DAG.
        num_children: Find the number of children for the given task in the DAG.
        num_parents: Find the number of parents for the given task in the DAG.
        parents: Return the parents of the task.
        step: Return a [`Step`][study.step.Step] object for the given task name.
    """

    def __init__(
        self,
        maestro_adjacency_table: OrderedDict,
        maestro_values: OrderedDict,
        column_labels: List[str],
        study_name: str,
        parameter_info: Dict,
    ):  # pylint: disable=R0913
        """
        Initializes a Directed Acyclic Graph (DAG) object, which represents a task graph used by Merlin
        for staging tasks in Celery. The DAG is initialized from a Maestro `ExecutionGraph` by unpacking
        its adjacency table and node values.

        Args:
            maestro_adjacency_table: An ordered dictionary representing the adjacency
                relationships between tasks in the graph. This comes from a Maestro `ExecutionGraph`.
            maestro_values: An ordered dictionary containing the values or metadata
                associated with each task in the graph. This also comes from a Maestro `ExecutionGraph`.
            column_labels: A list of column labels provided in the specification file,
                typically used to identify parameters or task attributes.
            study_name: The name of the study to which this DAG belongs.
            parameter_info: A dictionary containing information about the parameters in the study,
                such as their names and values.
        """
        # We used to store the entire maestro ExecutionGraph here but now it's
        # unpacked so we're only storing the 2 attributes from it that we use:
        # the adjacency table and the values. This had to happen to get pickle
        # to work for Celery.
        self.maestro_adjacency_table: OrderedDict = maestro_adjacency_table
        self.maestro_values: OrderedDict = maestro_values
        self.column_labels: List[str] = column_labels
        self.study_name: str = study_name
        self.parameter_info: Dict = parameter_info
        self.backwards_adjacency: Dict = {}
        self.calc_backwards_adjacency()

    def step(self, task_name: str) -> Step:
        """
        Return a Step object for the given task name.

        Args:
            task_name: The task name.

        Returns:
            A Merlin [`Step`][study.step.Step] object representing the
                task's configuration and parameters.
        """
        return Step(self.maestro_values[task_name], self.study_name, self.parameter_info)

    def calc_depth(self, node: str, depths: Dict, current_depth: int = 0):
        """
        Calculate the depth of the given node and its children. This recursive
        method will update `depths` in place.

        Args:
            node: The node to start at.
            depths: The dictionary of depths to update.
            current_depth: The current depth in the graph traversal.
        """
        if node not in depths:
            depths[node] = current_depth
        else:
            depths[node] = max(depths[node], current_depth)

        for child in self.children(node):
            self.calc_depth(child, depths, current_depth=depths[node] + 1)

    @staticmethod
    def group_by_depth(depths: Dict) -> List[List[List]]:
        """
        Group Directed Acyclic Graph (DAG) tasks by depth.

        This method only groups by depth, and has one task in every chain.
        [`find_independent_chains`][study.dag.DAG.find_independent_chains] is used
        to figure out how to coalesce chains across depths.

        Args:
            depths: The dictionary of depths to group by.

        Returns:
            A list of lists of lists ordered by depth.

        Example:
            This method will return a list that could look something like this:

            ```python
            [[["tasks"], ["with"], ["Depth 0"]], [["tasks"], ["with"], ["Depth 1"]]]
            ```

            Here, the outer index of this list is the depth, the middle index is
            which chain of tasks in that depth, and the inner index is the task
            id in that chain.
        """
        groups = {}
        for node in depths:
            depth = depths[node]

            if depth not in groups:
                groups[depth] = [node]
            else:
                groups[depth].append(node)

        # return an dict ordered by depth
        ordered_groups = OrderedDict(sorted(groups.items(), key=lambda t: t[0]))

        list_of_groups_of_chains = [[[g] for g in x] for x in ordered_groups.values()]

        return list_of_groups_of_chains

    def children(self, task_name: str) -> List:
        """
        Return the children of the task.

        Args:
            task_name: The name of the task to get the children of.

        Returns:
            List of children of this task.
        """
        return self.maestro_adjacency_table[task_name]

    def num_children(self, task_name: str) -> int:
        """
        Find the number of children for the given task in the Directed Acyclic Graph (DAG).

        Args:
            task_name: The name of the task to count the children of.

        Returns:
            Number of children this task has.
        """
        return len(self.children(task_name))

    def parents(self, task_name: str) -> List:
        """
        Return the parents of the task.

        Args:
            task_name: The name of the task to get the parents of.

        Returns:
            List of parents of this task.
        """
        return self.backwards_adjacency[task_name]

    def num_parents(self, task_name: str) -> int:
        """
        Find the number of parents for the given task in the Directed Acyclic Graph (DAG).

        Args:
            task_name: The name of the task to count the parents of.

        Returns:
            Number of parents this task has.
        """
        return len(self.parents(task_name))

    @staticmethod
    def find_chain(task_name: str, list_of_groups_of_chains: List[List[List]]) -> List:
        """
        Find the chain containing the task.

        Args:
            task_name: The task to search for.
            list_of_groups_of_chains: List of groups of chains to search for the task.

        Returns:
            The list representing the chain containing task_name, or None if not found.
        """
        for group in list_of_groups_of_chains:
            for chain in group:
                if task_name in chain:
                    return chain
        return None

    def calc_backwards_adjacency(self):
        """
        Initializes the backwards adjacency table.

        This method constructs a mapping of each task to its parent tasks in the Directed
        Acyclic Graph (DAG). The backwards adjacency table allows for reverse traversal
        of the graph, enabling the identification of dependencies for each task.

        The method iterates through each parent task in the `maestro_adjacency_table`
        and updates the `backwards_adjacency` dictionary. For each task that is a child
        of a parent, it adds the parent to the list of that task's parents in the
        `backwards_adjacency` table.

        This is essential for operations that require knowledge of a task's dependencies,
        such as determining the order of execution or identifying independent tasks.

        Example:
            If the `maestro_adjacency_table` is structured as follows:

            ```python
            {
                'A': ['B', 'C'],
                'B': ['D'],
                'C': ['D']
            }
            ```

            After calling this method, the `backwards_adjacency` will be:

            ```python
            {
                'B': ['A'],
                'C': ['A'],
                'D': ['B', 'C']
            }
            ```
        """
        for parent in self.maestro_adjacency_table:
            for task_name in self.maestro_adjacency_table[parent]:
                if task_name in self.backwards_adjacency:
                    self.backwards_adjacency[task_name].append(parent)
                else:
                    self.backwards_adjacency[task_name] = [parent]

    def compatible_merlin_expansion(self, task1: str, task2: str) -> bool:
        """
        Check if two tasks are compatible for Merlin expansion.

        This method compares the expansion needs of two tasks to determine
        if they can be expanded together.

        Args:
            task1: The first task.
            task2: The second task.

        Returns:
            True if compatible, False otherwise.
        """
        step1 = self.step(task1)
        step2 = self.step(task2)
        return step1.check_if_expansion_needed(self.column_labels) == step2.check_if_expansion_needed(self.column_labels)

    def find_independent_chains(self, list_of_groups_of_chains: List[List[List]]) -> List[List[List]]:
        """
        Finds independent chains and adjusts with the groups of chains to maximize parallelism.

        This method looks for opportunities to move tasks in deeper groups of chains
        into chains in shallower groups, thus increasing available parallelism in execution.

        Args:
            list_of_groups_of_chains: List of list of lists, as returned by
                [`group_by_depth`][study.dag.DAG.group_by_depth].

        Returns:
            Adjusted list of groups of chains to maximize parallelism.

        Example:
            Given input chains, the method may return a modified structure that allows
            for more tasks to be executed in parallel. For example, we might start with
            this:

            ```python
            [[["task1"], ["with"], ["Depth 0"]], [["task2"], ["has"], ["Depth 1"]]]
            ```

            and finish with this:

            ```python
            [[["task1", "has"], ["with", "task2"], ["Depth 0"]], ["Depth 1"]]]
            ```
        """
        for group in list_of_groups_of_chains:
            for chain in group:
                for task_name in chain:
                    if self.num_children(task_name) == 1 and task_name != "_source":
                        child = self.children(task_name)[0]

                        if self.num_parents(child) == 1 and self.compatible_merlin_expansion(child, task_name):
                            self.find_chain(child, list_of_groups_of_chains).remove(child)
                            chain.append(child)

        new_list = [[chain for chain in group if len(chain) > 0] for group in list_of_groups_of_chains]
        new_list_2 = [group for group in new_list if len(group) > 0]

        return new_list_2

    def group_tasks(self, source_node: str) -> List[List[List]]:
        """
        Group independent tasks in a Directed Acyclic Graph (DAG).

        Starts from a source node and works down, grouping tasks by depth,
        then identifies independent parallel chains in those groups.

        Args:
            source_node: The source node from which to start grouping tasks.

        Returns:
            A list of independent chains of tasks.
        """
        depths = {}
        self.calc_depth(source_node, depths)
        groups_of_chains = self.group_by_depth(depths)

        return self.find_independent_chains(groups_of_chains)
