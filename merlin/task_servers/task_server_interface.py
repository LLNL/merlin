##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Task server interface definition for Merlin.

This module defines the abstract interface that all task server implementations
must follow, enabling pluggable task distribution systems.
"""

import logging
from abc import ABC, abstractmethod
from typing import List, Optional, Dict, Any

from merlin.db_scripts.merlin_db import MerlinDatabase
from merlin.spec.specification import MerlinSpec


LOG = logging.getLogger(__name__)


class TaskDependency:
    """Represents task dependencies for workflow coordination"""
    def __init__(self, 
                 task_pattern: str,
                 dependency_type: str = "all_success"):
        self.task_pattern = task_pattern  # e.g., "generate_data_*"
        self.dependency_type = dependency_type


class TaskServerInterface(ABC):
    """
    Abstract interface for task server implementations.
    
    This interface defines the contract that all task server implementations
    must follow. It provides a database-first design where tasks are created
    and stored in the database before submission, and uses MerlinSpec objects
    for configuration.
    
    Key Design Principles:
    - Database-first: Tasks stored in database before submission
    - MerlinSpec integration: Worker configuration through existing spec system
    - Display-oriented: Information methods output to console for user feedback
    - Flexible submission: Support for various task server capabilities
    """

    def __init__(self):
        """
        Initialize the task server interface.
        
        All task server implementations inherit access to the MerlinDatabase
        for retrieving task information and updating status.
        """
        self.merlin_db = MerlinDatabase()

    @property
    @abstractmethod
    def server_type(self) -> str:
        """
        Return the type/name of this task server (e.g., 'celery', 'kafka').
        
        This property allows the system to dynamically determine the task server
        type without hardcoding it, enabling proper support for multiple task
        server implementations.
        
        Returns:
            The task server type string.
        """
        raise NotImplementedError()

    @abstractmethod
    def submit_task(self, task_id: str):
        """
        Submit a single task for execution.
        
        The task must already exist in the database as a TaskInstanceModel.
        The implementation should retrieve the task details from the database
        and submit it to the appropriate task distribution system.

        Args:
            task_id: The ID of the task to submit 
            
        Returns:
            The task server's internal task ID (may differ from input task_id).
        """
        raise NotImplementedError()

    @abstractmethod
    def submit_tasks(self, task_ids: List[str], **kwargs):
        """
        Submit multiple tasks for execution.
        
        This method provides flexible task submission with optional parameters
        that different task servers can use according to their capabilities.

        Args:
            task_ids: A list of task IDs to submit 
            **kwargs: Optional parameters that may include:
                - dependencies: List[str] - Task dependencies
                - priority: int - Task priority level
                - retry_policy: Dict - Retry configuration
                - batch_size: int - Preferred batching size
                - queue_name: str - Target queue override
                
        Returns:
            List of task server internal task IDs.
        """
        raise NotImplementedError()

    @abstractmethod
    def submit_task_group(self, 
                         group_id: str,
                         task_ids: List[str], 
                         callback_task_id: Optional[str] = None,
                         **kwargs) -> str:
        """
        Submit a group of tasks with optional callback task.
        
        This method enables coordination of related tasks, particularly useful
        for implementing workflow dependencies and Celery chord functionality.
        
        Args:
            group_id: Unique identifier for the task group
            task_ids: List of task IDs to execute in parallel (must exist in database)
            callback_task_id: Optional task to execute after group completion
            **kwargs: Additional group-specific parameters
            
        Returns:
            Group submission ID from the task server
        """
        raise NotImplementedError()

    @abstractmethod  
    def submit_coordinated_tasks(self,
                                coordination_id: str,
                                header_task_ids: List[str],
                                body_task_id: str,
                                **kwargs) -> str:
        """
        Submit coordinated tasks (group of tasks with callback).
        
        This method handles workflow patterns where multiple tasks must complete 
        before a dependent task can execute. Essential for supporting Merlin's 
        depends=[step_*] syntax. Different backends implement coordination using 
        their native mechanisms:
        - Celery: Uses chords (group + callback)
        - Kafka: Uses topic-based coordination with completion messages
        - Redis: Uses atomic counters with trigger logic
        
        Args:
            coordination_id: Unique identifier for the task coordination
            header_task_ids: List of tasks to execute in parallel (must exist in database)
            body_task_id: Task to execute after all header tasks complete
            **kwargs: Additional coordination-specific parameters
            
        Returns:
            Coordination submission ID from the task server
        """
        raise NotImplementedError()

    @abstractmethod
    def submit_dependent_tasks(self,
                              task_ids: List[str],
                              dependencies: Optional[List[TaskDependency]] = None,
                              **kwargs) -> List[str]:
        """
        Submit tasks with explicit dependency relationships.
        
        This method analyzes dependencies and submits tasks using appropriate
        coordination mechanisms (groups, chords, etc.) to ensure proper execution order.
        
        Args:
            task_ids: List of tasks to submit (must exist in database)
            dependencies: List of dependency specifications
            **kwargs: Additional parameters
            
        Returns:
            List of task/group submission IDs from the task server
        """
        raise NotImplementedError()

    @abstractmethod
    def get_group_status(self, group_id: str) -> Dict[str, Any]:
        """
        Get status of a task group or chord.
        
        Args:
            group_id: ID of the group/chord to check
            
        Returns:
            Dictionary containing group status and member task statuses
        """
        raise NotImplementedError()

    @abstractmethod
    def cancel_task(self, task_id: str):
        """
        Cancel a currently running task.
        
        If the task is not running or doesn't exist, implementations should
        log a warning and continue gracefully.

        Args:
            task_id: The ID of the task to cancel.
            
        Returns:
            True if cancellation was successful, False otherwise.
        """
        raise NotImplementedError()

    @abstractmethod
    def cancel_tasks(self, task_ids: List[str]):
        """
        Cancel multiple running tasks.

        Args:
            task_ids: A list of task IDs to cancel.
            
        Returns:
            Dictionary mapping task_id to cancellation success (bool).
        """
        raise NotImplementedError()

    @abstractmethod
    def start_workers(self, spec: MerlinSpec):
        """
        Start workers using configuration from MerlinSpec.
        
        This method leverages the existing Merlin configuration system
        to start workers with appropriate settings for queues, concurrency,
        and other worker parameters.

        Args:
            spec: MerlinSpec object containing worker configuration.
        """
        raise NotImplementedError()

    @abstractmethod
    def stop_workers(self, names: Optional[List[str]] = None):
        """
        Stop currently running workers.

        If no worker names are provided, this will stop all currently running 
        workers. Otherwise, only the specified workers will be stopped.

        Args:
            names: Optional list of specific worker names to shut down.
        """
        raise NotImplementedError()

    @abstractmethod
    def display_queue_info(self, queues: Optional[List[str]] = None):
        """
        Display information about queues to the console.

        Shows queue statistics such as pending tasks, active tasks, and
        consumer information. If no queues are specified, displays info
        for all available queues.

        Args:
            queues: Optional list of specific queue names to display.
        """
        raise NotImplementedError()

    @abstractmethod
    def display_connected_workers(self):
        """
        Display information about connected workers to the console.
        
        Shows worker status, assigned queues, current tasks, and other
        relevant worker information.
        """
        raise NotImplementedError()

    @abstractmethod
    def display_running_tasks(self):
        """
        Display the IDs of currently running tasks to the console.
        
        Provides a snapshot of active task execution for monitoring
        and debugging purposes.
        """
        raise NotImplementedError()

    @abstractmethod
    def purge_tasks(self, queues: List[str], force: bool = False) -> int:
        """
        Remove all pending tasks from specified queues.
        
        Args:
            queues: List of queue names to purge.
            force: If True, purge without confirmation.
            
        Returns:
            Number of tasks purged.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_workers(self) -> List[str]:
        """
        Get a list of all currently connected workers.
        
        Returns:
            List of worker names/identifiers.
        """
        raise NotImplementedError()

    @abstractmethod
    def get_active_queues(self) -> Dict[str, List[str]]:
        """
        Get a mapping of active queues to their connected workers.
        
        Returns:
            Dictionary mapping queue names to lists of worker names.
        """
        raise NotImplementedError()

    @abstractmethod
    def check_workers_processing(self, queues: List[str]) -> bool:
        """
        Check if any workers are currently processing tasks from specified queues.
        
        Args:
            queues: List of queue names to check.
            
        Returns:
            True if any workers are processing tasks, False otherwise.
        """
        raise NotImplementedError()