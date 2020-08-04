###############################################################################
# Copyright (c) 2019, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.7.2.
#
# For details, see https://github.com/LLNL/merlin.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################

"""
Holds DAG class. TODO make this an interface, separate from Maestro.
"""
from collections import OrderedDict

from merlin.study.step import Step


class DAG:
    """
    This class provides methods on a task graph that Merlin needs for staging
    tasks in celery. It is initialized from am maestro ExecutionGraph, and the
    major entry point is the group_tasks method, which provides groups of
    independent chains of tasks.
    """

    def __init__(self, maestro_dag, labels):
        """
        :param `maestro_dag`: A maestrowf ExecutionGraph.
        """
        self.dag = maestro_dag
        self.backwards_adjacency = {}
        self.calc_backwards_adjacency()
        self.labels = labels

    def step(self, task_name):
        """Return a Step object for the given task name

        :param `task_name`: The task name.
        :return: A Merlin Step object.
        """
        return Step(self.dag.values[task_name])

    def calc_depth(self, node, depths, current_depth=0):
        """Calculate the depth of the given node and its children.

        :param `node`: The node (str) to start at.
        :param `depths`: the dictionary of depths to update.
        :param `current_depth`: the current depth in the graph traversal.
        """
        if node not in depths:
            depths[node] = current_depth
        else:
            depths[node] = max(depths[node], current_depth)

        for child in self.children(node):
            self.calc_depth(child, depths, current_depth=depths[node] + 1)

    @staticmethod
    def group_by_depth(depths):
        """Group DAG tasks by depth.

        :param `depths`: the dictionary of depths to group by

        :return: a list of lists of lists ordered by depth

        ([[["tasks"],["with"],["Depth 0"]],[["tasks"],["with"],["Depth 1"]]])

        The outer index of this list is the depth, the middle index is which
        chain of tasks in that depth, and the inner index is the task id in
        that chain.

        This method only groups by depth, and has one task in every chain.
        find_independent_chains is used to figure out how to coalesce chains
        across depths.
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

    def children(self, task_name):
        """ Return the children of the task.
        :param `task_name`: The name of the task to get the children of.

        :return: list of children of this task.
        """
        return self.dag.adjacency_table[task_name]

    def num_children(self, task_name):
        """ Find the number of children for the given task in the dag.
        :param `task_name`: The name of the task to count the children of.

        :return : number of children this task has
        """
        return len(self.children(task_name))

    def parents(self, task_name):
        """ Return the parents of the task.
        :param `task_name` : The name of the task to get the parents of.

        :return : list of parents of this task"""
        return self.backwards_adjacency[task_name]

    def num_parents(self, task_name):
        """find the number of parents for the given task in the dag
        :param `task_name` : The name of the task to count the parents of

        :return : number of parents this task has"""
        return len(self.parents(task_name))

    @staticmethod
    def find_chain(task_name, list_of_groups_of_chains):
        """find the chain containing the task
        :param `task_name` : The task to search for.
        :param `list_of_groups_of_chains` : list of groups of chains to search
            for the task

        :return : the list representing the chain containing task_name"""
        for group in list_of_groups_of_chains:
            for chain in group:
                if task_name in chain:
                    return chain
        return None

    def calc_backwards_adjacency(self):
        """ initializes our backwards adjacency table """
        for parent in self.dag.adjacency_table:
            for task_name in self.dag.adjacency_table[parent]:
                if task_name in self.backwards_adjacency:
                    self.backwards_adjacency[task_name].append(parent)
                else:
                    self.backwards_adjacency[task_name] = [parent]

    def compatible_merlin_expansion(self, task1, task2):
        """
        TODO
        """
        step1 = self.step(task1)
        step2 = self.step(task2)
        return step1.needs_merlin_expansion(
            self.labels
        ) == step2.needs_merlin_expansion(self.labels)

    def find_independent_chains(self, list_of_groups_of_chains):
        """
        Finds independent chains and adjusts with the groups of chains to
        maximalize parallelism

        :param list_of_groups_of_chains: List of list of lists, as returned by
            self.group_by_depth

        e.g.,

        ([[["task1"],["with"],["Depth 0"]],[["task2"],["has"],["Depth 1"]]])

        :return : This takes the groups of chains and looks for opportunities
            to move tasks in deeper groups of chains into chains in shallower
            groups, thus increasing available parallelism in the execution.

            Depending on the precise parental relationships between the tasks
            in the graph the output may be something like:

            ([[["task1", "has"],["with","task2"],["Depth 0"]],["Depth 1"]]])
        """
        for group in list_of_groups_of_chains:
            for chain in group:
                for task_name in chain:

                    if self.num_children(task_name) == 1 and task_name != "_source":

                        child = self.children(task_name)[0]

                        if self.num_parents(child) == 1:

                            if self.compatible_merlin_expansion(child, task_name):

                                self.find_chain(child, list_of_groups_of_chains).remove(
                                    child
                                )

                                chain.append(child)

        new_list = [
            [chain for chain in group if len(chain) > 0]
            for group in list_of_groups_of_chains
        ]
        new_list_2 = [group for group in new_list if len(group) > 0]

        return new_list_2

    def group_tasks(self, source_node):
        """Group independent tasks in a directed acyclic graph (DAG).

        Starts from a source node and works down, grouping tasks by
        depth, then identify independent parallel chains in those groups.

        :param dag : The DAG
        :param source_node: The source node.
        """
        depths = {}
        self.calc_depth(source_node, depths)
        groups_of_chains = self.group_by_depth(depths)

        return self.find_independent_chains(groups_of_chains)
