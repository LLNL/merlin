

"""

"""

from dataclasses import dataclass
from typing import List


@dataclass
class TaskChain:
    """
    Represents a sequence of tasks that must execute sequentially within a workflow.
    
    A `TaskChain` defines a linear dependency chain where each task must complete
    before the next task can begin. All tasks within a chain execute on the same
    execution level (depth) but maintain strict ordering amongst themselves.
    
    This is a lightweight data structure that stores task names as strings rather
    than full Step objects for efficient serialization in distributed environments
    like Celery.

    Note:
        Tasks within a chain have sequential dependencies, but different chains
        at the same depth can execute in parallel. Use [`ExecutionLevel`][dag.models.ExecutionLevel]
        to group parallel chains.
    
    Attributes:
        tasks (List[str]): Ordered list of task names that comprise this chain.
            Tasks execute in the order they appear in this list.
        depth (int): The execution depth/level of this chain within the overall
            workflow. All tasks in this chain execute at the same depth.
    
    Example:
        ```python
        # Create a chain of preprocessing tasks
        preprocess_chain = TaskChain(
            tasks=["validate_input", "clean_data", "normalize"],
            depth=1
        )
        
        print(preprocess_chain)  # "validate_input → clean_data → normalize"
        print(len(preprocess_chain))  # 3
        
        # Check if a specific task is in this chain
        if preprocess_chain.contains_task("clean_data"):
            print("Data cleaning is part of preprocessing")
        ```
    """
    tasks: List[str]
    depth: int
    
    def __str__(self) -> str:
        """
        Return a human-readable representation of the task chain.
        
        Returns:
            Tasks joined by arrows (→) to show execution flow.
        """
        return " → ".join(self.tasks)
    
    def __len__(self) -> int:
        """
        Return the number of tasks in this chain.
        
        Returns:
            Count of tasks in the chain.
        """
        return len(self.tasks)
    
    def add_task(self, task: str):
        """
        Add a task to the end of this chain.
        
        The new task will execute after all existing tasks in the chain
        have completed successfully.
        
        Args:
            task (str): Name of the task to append to the chain.
        
        Example:
            ```python
            chain = TaskChain(tasks=["setup"], depth=0)
            chain.add_task("configure")
            print(chain)  # "setup → configure"
            ```
        """
        self.tasks.append(task)
    
    def remove_task(self, task: str):
        """
        Remove the first occurrence of a task from this chain.
        
        If the task appears multiple times in the chain, only the first
        occurrence is removed. If the task is not found, no action is taken.

        Warning:
            Removing tasks from the middle of a chain may break dependencies.
            Ensure that removing a task doesn't create gaps in the workflow logic.
        
        Args:
            task (str): Name of the task to remove from the chain.
        
        Example:
            ```python
            chain = TaskChain(tasks=["a", "b", "c"], depth=1)
            chain.remove_task("b")
            print(chain)  # "a → c"
            ```
        """
        if task in self.tasks:
            self.tasks.remove(task)
    
    def contains_task(self, task: str) -> bool:
        """
        Check if this chain contains the specified task.
        
        Args:
            task (str): Name of the task to search for.
        
        Returns:
            bool: True if the task is found in this chain, False otherwise.
        
        Example:
            ```python
            chain = TaskChain(tasks=["init", "process", "cleanup"], depth=2)
            assert chain.contains_task("process") == True
            assert chain.contains_task("missing") == False
            ```
        """
        return task in self.tasks


@dataclass
class ExecutionLevel:
    """
    Represents all task chains that can execute in parallel at a given workflow depth.
    
    An `ExecutionLevel` groups [`TaskChains`][dag.models.TaskChain] that have no
    dependencies between them, allowing them to execute concurrently. All chains within
    a level must complete before the workflow can proceed to the next depth level.
    
    This structure enables efficient parallel execution while maintaining proper
    dependency ordering across the overall workflow.

    Note:
        While chains within a level execute in parallel, tasks within each
        individual chain still execute sequentially according to their `TaskChain` ordering.
    
    Attributes:
        depth (int): The execution depth of this level within the workflow.
            Lower depths execute before higher depths.
        parallel_chains (List[TaskChain]): List of task chains that can execute
            concurrently at this depth level.
    
    Example:
        ```python
        # Create parallel analysis chains at depth 2
        analysis_level = ExecutionLevel(
            depth=2,
            parallel_chains=[
                TaskChain(tasks=["analyze_cpu"], depth=2),
                TaskChain(tasks=["analyze_memory"], depth=2),
                TaskChain(tasks=["analyze_disk"], depth=2)
            ]
        )
        
        print(analysis_level)
        # Level 2: ['analyze_cpu', 'analyze_memory', 'analyze_disk']
        
        # All three analysis tasks can run simultaneously
        all_tasks = analysis_level.get_all_tasks()
        print(f"Parallel tasks: {all_tasks}")
        ```
    """
    depth: int
    parallel_chains: List[TaskChain]
    
    def __str__(self) -> str:
        """
        Return a human-readable representation of this execution level.
        
        Returns:
            Formatted string showing depth and all parallel chains.
        """
        chain_strs = [str(chain) for chain in self.parallel_chains]
        return f"Level {self.depth}: {chain_strs}"
    
    def add_chain(self, chain: TaskChain):
        """
        Add a parallel task chain to this execution level.
        
        The added chain will execute in parallel with all other chains
        at this level, but its internal tasks will still execute sequentially.

        Warning:
            The chain's depth should match this level's depth for consistency,
            though this is not enforced by this method.
        
        Args:
            chain (TaskChain): The task chain to add to this level's parallel execution.
        
        Example:
            ```python
            level = ExecutionLevel(depth=1, parallel_chains=[])
            
            # Add independent processing chains
            level.add_chain(TaskChain(tasks=["process_images"], depth=1))
            level.add_chain(TaskChain(tasks=["process_text"], depth=1))
            
            # Both chains will now execute in parallel
            ```
        """
        self.parallel_chains.append(chain)
    
    def get_all_tasks(self) -> List[str]:
        """
        Retrieve all task names from all chains in this execution level.
        
        This flattens the parallel chain structure to return a single list
        containing every task that will execute at this level.

        Note:
            The order of tasks in the returned list reflects the order of chains
            and tasks within chains, but does not imply execution order since
            chains execute in parallel.
        
        Returns:
            List[str]: Flat list of all task names across all parallel chains.
        
        Example:
            ```python
            level = ExecutionLevel(
                depth=1,
                parallel_chains=[
                    TaskChain(tasks=["a", "b"], depth=1),
                    TaskChain(tasks=["c"], depth=1)
                ]
            )
            
            all_tasks = level.get_all_tasks()
            print(all_tasks)  # ["a", "b", "c"]
            ```
        """
        return [task for chain in self.parallel_chains for task in chain.tasks]
    
    def find_chain_containing_task(self, task: str) -> TaskChain:
        """
        Locate the task chain containing a specific task.
        
        Searches through all parallel chains in this level to find which
        chain contains the specified task.
        
        Args:
            task (str): Name of the task to locate.
        
        Returns:
            The [`TaskChain`][dag.models.TaskChain] containing the task, or
                None if the task is not found in any chain at this level.
        
        Example:
            ```python
            level = ExecutionLevel(
                depth=2,
                parallel_chains=[
                    TaskChain(tasks=["prep", "analyze"], depth=2),
                    TaskChain(tasks=["validate", "report"], depth=2)
                ]
            )
            
            chain = level.find_chain_containing_task("analyze")
            print(chain)  # TaskChain with ["prep", "analyze"]
            
            missing = level.find_chain_containing_task("missing")
            print(missing)  # None
            ```
        """
        for chain in self.parallel_chains:
            if chain.contains_task(task):
                return chain
        return None
    
    def remove_empty_chains(self):
        """
        Remove any task chains that contain no tasks.
        
        This cleanup method filters out chains that have become empty,
        which can occur after task removal operations or during workflow
        construction.
        
        Example:
            ```python
            level = ExecutionLevel(
                depth=1,
                parallel_chains=[
                    TaskChain(tasks=["active_task"], depth=1),
                    TaskChain(tasks=[], depth=1),  # Empty chain
                    TaskChain(tasks=["another_task"], depth=1)
                ]
            )
            
            level.remove_empty_chains()
            print(len(level.parallel_chains))  # 2 (empty chain removed)
            ```
        
        Note:
            This operation modifies the parallel_chains list in-place.
        """
        self.parallel_chains = [chain for chain in self.parallel_chains if len(chain.tasks) > 0]


class ExecutionPlan:
    """
    Container for a complete workflow execution plan with multiple depth levels.
    
    An `ExecutionPlan` represents the full structure of a workflow, organizing
    tasks into levels that execute sequentially, with parallel chains within
    each level. This provides a clear, queryable representation of complex
    workflow dependencies and execution ordering.
    
    The plan enforces that:

    - All tasks at depth N complete before any tasks at depth N+1 begin
    - Tasks within the same [`ExecutionLevel`][dag.models.ExecutionLevel] can
      execute in parallel
    - Tasks within the same [`TaskChain`][dag.models.TaskChain] execute sequentially

    Note:
        `ExecutionPlan` is designed for planning and analysis, not execution.
        Use appropriate [`TaskExecutor`][execution.base.TaskExecutor] implementations
        to actually run the tasks.
    
    Attributes:
        levels (List[ExecutionLevel]): Ordered list of execution levels,
            typically sorted by depth for sequential execution.
    
    Example:
        ```python
        # Create a multi-level execution plan
        plan = ExecutionPlan([
            ExecutionLevel(depth=0, parallel_chains=[
                TaskChain(tasks=["initialize"], depth=0)
            ]),
            ExecutionLevel(depth=1, parallel_chains=[
                TaskChain(tasks=["process_a", "analyze_a"], depth=1),
                TaskChain(tasks=["process_b"], depth=1)
            ]),
            ExecutionLevel(depth=2, parallel_chains=[
                TaskChain(tasks=["finalize"], depth=2)
            ])
        ])
        
        print(f"Plan has {len(plan.levels)} levels")
        print(f"Total tasks: {len(plan.get_all_tasks())}")
        print(f"Max execution depth: {plan.get_max_depth()}")
        
        # Find where a specific task will execute
        location = plan.find_task_location("process_a")
        print(f"Task 'process_a' at depth {location[0]}, chain {location[1]}")
        ```
    """
    
    def __init__(self, levels: List[ExecutionLevel] = None):
        """
        Constructor for `ExecutionPlan`.
        
        Args:
            levels (Optional[List[ExecutionLevel]]): An optional list of execution levels
                to add to the plan.
        """
        self.levels = levels or []
    
    def __str__(self) -> str:
        """
        Return a human-readable representation of the entire execution plan.
        
        Returns:
            Multi-line string showing all levels and their chains.
                Each level is displayed on a separate line.
        """
        return "\n".join(str(level) for level in self.levels)
    
    def get_level(self, depth: int) -> ExecutionLevel:
        """
        Retrieve the execution level at a specific depth.
        
        Args:
            depth (int): The depth level to retrieve.
        
        Returns:
            The [`ExecutionLevel`][dag.models.ExecutionLevel] at the
                specified depth, or None if no level exists at that depth.
        
        Example:
            ```python
            plan = ExecutionPlan([
                ExecutionLevel(depth=0, parallel_chains=[...]),
                ExecutionLevel(depth=2, parallel_chains=[...])  # Note: no depth 1
            ])
            
            level_0 = plan.get_level(0)  # Returns ExecutionLevel
            level_1 = plan.get_level(1)  # Returns None
            level_2 = plan.get_level(2)  # Returns ExecutionLevel
            ```
        """
        for level in self.levels:
            if level.depth == depth:
                return level
        return None
    
    def get_max_depth(self) -> int:
        """
        Determine the maximum depth level in this execution plan.

        Note:
            This represents the total number of sequential execution phases
            in the workflow minus one (since depths are 0-indexed).
        
        Returns:
            The highest depth value among all levels, or 0 if no levels exist.
        
        Example:
            ```python
            plan = ExecutionPlan([
                ExecutionLevel(depth=0, parallel_chains=[...]),
                ExecutionLevel(depth=3, parallel_chains=[...]),
                ExecutionLevel(depth=1, parallel_chains=[...])
            ])
            
            max_depth = plan.get_max_depth()  # Returns 3
            ```
        """
        return max(level.depth for level in self.levels) if self.levels else 0
    
    def get_all_tasks(self) -> List[str]:
        """
        Retrieve all task names from the entire execution plan.
        
        This flattens the complete plan structure (levels → chains → tasks)
        into a single list containing every task in the workflow.
        
        Note:
            The order reflects the structure (level order, then chain order within
            levels, then task order within chains) but does not imply execution
            order due to parallelism.

        Returns:
            Complete list of all task names across all levels and chains.
        
        Example:
            ```python
            plan = ExecutionPlan([
                ExecutionLevel(depth=0, parallel_chains=[
                    TaskChain(tasks=["init"], depth=0)
                ]),
                ExecutionLevel(depth=1, parallel_chains=[
                    TaskChain(tasks=["a", "b"], depth=1),
                    TaskChain(tasks=["c"], depth=1)
                ])
            ])
            
            all_tasks = plan.get_all_tasks()
            print(all_tasks)  # ["init", "a", "b", "c"]
            ```
        """
        return [task for level in self.levels for task in level.get_all_tasks()]
    
    def find_task_location(self, task: str) -> tuple:
        """
        Locate a specific task within the execution plan structure.
        
        Searches through all levels and chains to find the exact location
        of a task within the plan hierarchy.
        
        Args:
            task (str): Name of the task to locate.
        
        Returns:
            Optional[Tuple[int, int]]: Tuple of (depth, chain_index) if found,
                where:
                - depth: The execution level depth containing the task
                - chain_index: Index of the chain within that level's parallel_chains
                Returns None if the task is not found.
        
        Example:
            ```python
            plan = ExecutionPlan([
                ExecutionLevel(depth=0, parallel_chains=[
                    TaskChain(tasks=["init"], depth=0)  # chain_index=0
                ]),
                ExecutionLevel(depth=1, parallel_chains=[
                    TaskChain(tasks=["a"], depth=1),    # chain_index=0
                    TaskChain(tasks=["b"], depth=1)     # chain_index=1
                ])
            ])
            
            location = plan.find_task_location("b")
            print(location)  # (1, 1) - depth 1, chain index 1
            
            missing = plan.find_task_location("missing")
            print(missing)  # None
            ```
        """
        for level in self.levels:
            for chain_idx, chain in enumerate(level.parallel_chains):
                if chain.contains_task(task):
                    return (level.depth, chain_idx)
        return None
