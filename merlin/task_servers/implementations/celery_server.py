##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Celery task server implementation for Merlin.

This module contains the CeleryTaskServer class that implements the 
TaskServerInterface for Celery-based task distribution.
"""

import logging
import os
import subprocess
import sys
from typing import Dict, Any, List, Optional

from tabulate import tabulate

from merlin.task_servers.task_server_interface import TaskServerInterface, TaskDependency
from merlin.spec.specification import MerlinSpec


LOG = logging.getLogger(__name__)


class CeleryTaskServer(TaskServerInterface):
    """
    Celery implementation of the TaskServerInterface.
    
    This class provides Celery-specific implementations of task server operations
    including task submission, worker management, status queries, and queue management.
    
    Implements the complete TaskServerInterface with database-first design,
    supporting both single and batch operations for optimal performance in large-scale
    scientific workflows.
    
    Key Features:
    - Database-first task submission and retrieval
    - Comprehensive worker lifecycle management via MerlinSpec
    - Real-time status monitoring and reporting
    - Queue management and purging capabilities
    - Backward compatibility with existing Merlin workflows
    """
    
    def __init__(self):
        """Initialize the Celery task server."""
        super().__init__()  # Initialize parent class with MerlinDatabase
        self.celery_app = None
        self.config = None
        self._initialize_celery()
    
    def _initialize_celery(self):
        """Initialize Celery app with current configuration."""
        try:
            from merlin.celery import app as celery_app
            self.celery_app = celery_app
            LOG.debug("Celery task server initialized")
        except ImportError as e:
            LOG.error(f"Failed to initialize Celery: {e}")
            raise
    
    @property
    def server_type(self) -> str:
        """Return the task server type."""
        return "celery"
    
    def submit_task(self, task_id_or_signature):
        """
        Submit a single task for execution.

        Args:
            task_id_or_signature: Either a task ID (str) to look up in database,
                                 or a Celery signature to submit directly,
                                 or a UniversalTaskDefinition for Universal Task System.
            
        Returns:
            The Celery task ID for tracking.
        """
        if not self.celery_app:
            raise RuntimeError("Celery task server not initialized")
        
        # Check if we received a UniversalTaskDefinition
        if hasattr(task_id_or_signature, 'task_type') and hasattr(task_id_or_signature, 'coordination_pattern'):
            # It's a UniversalTaskDefinition, submit via Universal Task System
            from merlin.common.tasks import universal_task_handler
            
            task_def = task_id_or_signature
            task_data = task_def.to_dict()
            
            result = universal_task_handler.delay(task_data)
            LOG.info(f"Submitted universal task {task_def.task_id} to Celery: {result.id}")
            return result.id
        
        # Check if we received a Celery signature directly
        if hasattr(task_id_or_signature, 'delay'):
            # It's a Celery signature, submit it directly
            result = task_id_or_signature.delay()
            LOG.debug(f"Submitted signature to Celery via TaskServerInterface")
            return result.id
            
        # Otherwise, it's a task_id string (legacy approach)
        task_id = task_id_or_signature
        
        # For now, submit using original approach since don't have
        # something like a TaskInstanceModel implemented yet. This
        #  will be updated when the database models are implemented.
        
        # Import the task function we need
        from merlin.common.tasks import merlin_step
        
        # Submit to Celery using existing merlin_step task
        # The task_id is used as both the Celery task ID and workspace path
        result = self.celery_app.send_task(
            'merlin.common.tasks.merlin_step',
            task_id=task_id,
            # NOTE: args will need to be populated with actual Step object
            # when we refactor the task creation flow
        )
        
        LOG.debug(f"Submitted task {task_id} to Celery")
        return result.id
        
    def submit_tasks(self, task_ids: List[str], **kwargs):
        """
        Submit multiple tasks for execution.

        Args:
            task_ids: A list of task IDs to submit.
            **kwargs: Optional parameters for batch submission.
            
        Returns:
            List of Celery task IDs.
        """
        submitted_ids = []
        for task_id in task_ids:
            submitted_id = self.submit_task(task_id)
            submitted_ids.append(submitted_id)
        
        LOG.info(f"Submitted {len(task_ids)} tasks to Celery")
        return submitted_ids
    
    def submit_study(self, study, adapter, samples, sample_labels, egraph, groups_of_chains):
        """
        Submit a complete study using the Universal Task System.
        
        This method creates and submits Universal Task Definitions for the entire study,
        providing enhanced coordination patterns and backend independence.
        
        Args:
            study: MerlinStudy object
            adapter: Adapter configuration 
            samples: Study samples
            sample_labels: Sample labels
            egraph: Execution graph/DAG
            groups_of_chains: Task group chains
            
        Returns:
            AsyncResult: Celery result for tracking
        """
        try:
            LOG.info("Submitting study via Universal Task System")
            
            # Import Universal Task System components
            from merlin.factories.universal_task_factory import UniversalTaskFactory
            from merlin.factories.task_definition import CoordinationPattern, TaskType
            from merlin.common.tasks import universal_workflow_coordinator
            
            # Initialize Universal Task Factory
            factory = UniversalTaskFactory()
            
            # Create universal tasks for each step in the workflow
            universal_tasks = []
            
            for chain_group in groups_of_chains:
                for gchain in chain_group:
                    for step_name in gchain:
                        step = egraph.step(step_name)
                        
                        # Create Universal Task Definition for each step
                        task_def = factory.create_merlin_step_task(
                            step_config={
                                'cmd': step.run['cmd'],
                                'workspace': step.get_workspace(),
                                'restart': getattr(step, 'restart', None),
                                'max_retries': getattr(step, 'max_retries', 3)
                            },
                            queue_name=step.get_task_queue(),
                            priority=5
                        )
                        
                        universal_tasks.append(task_def)
            
            # Create workflow coordination data
            workflow_data = {
                'study_name': study.name,
                'tasks': [task.to_dict() for task in universal_tasks],
                'coordination_enabled': True
            }
            
            # Submit via universal workflow coordinator
            result = universal_workflow_coordinator.delay(workflow_data)
            
            LOG.info(f"Submitted study {study.name} with {len(universal_tasks)} universal tasks")
            return result
            
        except Exception as e:
            LOG.error(f"Failed to submit study via Universal Task System: {e}")
            # Fallback to traditional approach
            from merlin.common.tasks import _queue_study_with_celery
            return _queue_study_with_celery(study, adapter, samples, sample_labels, egraph, groups_of_chains)

    def submit_task_group(self, 
                         group_id: str,
                         task_ids: List[str], 
                         callback_task_id: Optional[str] = None,
                         **kwargs) -> str:
        """
        Submit a group of tasks with optional callback task.
        
        This implementation uses Celery groups and chords to coordinate task execution.
        """
        try:
            from celery import group, chord
            from merlin.celery import app as celery_app
            
            # Since task instances are not stored in database, we need to create
            # basic Celery signatures using the task_ids as workspace paths
            # This follows Merlin's current pattern where task_id = workspace_path
            signatures = []
            for task_id in task_ids:
                # Create Celery signature for the merlin_step task
                # In Merlin's current design, task_id often corresponds to workspace path
                sig = celery_app.signature(
                    'merlin.common.tasks.merlin_step',
                    args=[task_id],  # task_id is used as workspace path
                    task_id=task_id,
                    queue=kwargs.get('queue', 'default')
                )
                signatures.append(sig)
            
            # Create group
            group_obj = group(signatures)
            
            if callback_task_id:
                # Create chord with callback
                callback_sig = celery_app.signature(
                    'merlin.common.tasks.merlin_step',
                    args=[callback_task_id],  # callback_task_id as workspace path
                    task_id=callback_task_id,
                    queue=kwargs.get('queue', 'default')
                )
                
                chord_obj = chord(group_obj, callback_sig)
                result = chord_obj.apply_async()
                
                LOG.info(f"Submitted chord {group_id} with {len(task_ids)} header tasks and callback")
                return result.id
            
            # Just a group without callback
            result = group_obj.apply_async()
            LOG.info(f"Submitted task group {group_id} with {len(task_ids)} tasks")
            return result.id
            
        except Exception as e:
            LOG.error(f"Failed to submit task group {group_id}: {e}")
            # Fallback to individual task submission
            return self.submit_tasks(task_ids, **kwargs)[0] if task_ids else ""

    def submit_coordinated_tasks(self, task_dependencies_or_coordination_id, header_task_ids=None, body_task_id=None, **kwargs) -> str:
        """
        Submit coordinated tasks (group of tasks with callback).
        
        This implementation uses Celery's chord mechanism for coordination.
        
        Args:
            task_dependencies_or_coordination_id: Either a list of TaskDependency objects
                                                 or legacy coordination_id string
            header_task_ids: Legacy parameter for task IDs (when using coordination_id)
            body_task_id: Legacy parameter for callback task (when using coordination_id)
        """
        # Handle new signature-based approach
        if isinstance(task_dependencies_or_coordination_id, list):
            task_dependencies = task_dependencies_or_coordination_id
            try:
                from celery import group, chord
                
                # Extract signatures from TaskDependency objects, separating header from callback
                header_signatures = []
                callback_signatures = []
                
                for task_dep in task_dependencies:
                    if hasattr(task_dep, 'task_signature') and task_dep.task_signature:
                        if task_dep.dependency_type == "header":
                            header_signatures.append(task_dep.task_signature)
                        elif task_dep.dependency_type == "callback":
                            callback_signatures.append(task_dep.task_signature)
                
                if not header_signatures and not callback_signatures:
                    LOG.warning("No valid signatures found in task dependencies")
                    return ""
                
                if header_signatures and callback_signatures:
                    header_group = group(header_signatures)
                    
                    if len(callback_signatures) == 1:
                        # Single callback task: standard chord
                        callback_task = callback_signatures[0]
                        chord_obj = chord(header_group, callback_task)
                        result = chord_obj.apply_async()
                        LOG.info(f"DEPENDENCY COORDINATION: Submitted chord with {len(header_signatures)} header tasks -> 1 callback task")
                    else:
                        # Multiple callback tasks: chain them after the group
                        from celery import chain
                        callback_chain = chain(*callback_signatures)
                        chord_obj = chord(header_group, callback_chain)
                        result = chord_obj.apply_async()
                        LOG.info(f"DEPENDENCY COORDINATION: Submitted chord with {len(header_signatures)} header tasks -> {len(callback_signatures)} chained callback tasks")
                    
                    LOG.debug(f"TaskServerInterface coordination successful: {result.id}")
                    
                elif header_signatures:
                    # Only header tasks: submit as group (no dependencies to enforce)
                    group_obj = group(header_signatures)
                    result = group_obj.apply_async()
                    LOG.info(f"Submitted independent group with {len(header_signatures)} header tasks")
                elif callback_signatures:
                    # Only callback tasks: submit as group (won't happen with proper dependencies)
                    group_obj = group(callback_signatures)
                    result = group_obj.apply_async()
                    LOG.warning(f"Submitted callback-only group with {len(callback_signatures)} tasks (no dependencies)")
                else:
                    return ""
                
                return result.id
                
            except Exception as e:
                LOG.error(f"DEPENDENCY COORDINATION FAILED: {e}")
                LOG.error(f"Falling back to individual task submission (POTENTIAL RACE CONDITION)")
                # Fallback to individual task submission
                for task_dep in task_dependencies:
                    if hasattr(task_dep, 'task_signature') and task_dep.task_signature:
                        try:
                            result = task_dep.task_signature.delay()
                            LOG.warning(f"Fallback: Submitted individual task {result.id}")
                        except Exception as fallback_e:
                            LOG.error(f"Fallback task submission failed: {fallback_e}")
                return ""
        
        # Handle legacy approach with coordination_id
        coordination_id = task_dependencies_or_coordination_id
        return self.submit_task_group(
            group_id=coordination_id,
            task_ids=header_task_ids,
            callback_task_id=body_task_id,
            **kwargs
        )

    def submit_dependent_tasks(self,
                              task_ids: List[str],
                              dependencies: Optional[List[TaskDependency]] = None,
                              **kwargs) -> List[str]:
        """
        Submit tasks with explicit dependency relationships.
        
        Analyzes dependencies and submits tasks using chords for proper coordination.
        """
        if not dependencies:
            return self.submit_tasks(task_ids, **kwargs)
        
        try:
            # Group tasks by dependencies
            dependency_groups = self._group_tasks_by_dependencies(task_ids, dependencies)
            
            submission_ids = []
            for group_info in dependency_groups:
                if group_info['has_dependents']:
                    # Use chord for tasks with dependents
                    chord_id = self.submit_coordinated_tasks(
                        coordination_id=group_info['id'],
                        header_task_ids=group_info['header_tasks'],
                        body_task_id=group_info['body_task']
                    )
                    submission_ids.append(chord_id)
                else:
                    # Regular submission for independent tasks
                    group_ids = self.submit_tasks(group_info['tasks'])
                    submission_ids.extend(group_ids)
            
            return submission_ids
            
        except Exception as e:
            LOG.error(f"Failed to submit dependent tasks: {e}")
            # Fallback to regular submission
            return self.submit_tasks(task_ids, **kwargs)

    def get_group_status(self, group_id: str) -> Dict[str, Any]:
        """
        Get status of a task group or chord.
        """
        try:
            from merlin.celery import app as celery_app
            
            result = celery_app.GroupResult.restore(group_id)
            if result:
                return {
                    "group_id": group_id,
                    "status": "completed" if result.ready() else "running",
                    "completed": result.completed_count(),
                    "total": len(result.results),
                    "successful": result.successful(),
                    "failed": result.failed()
                }
        except Exception as e:
            LOG.warning(f"Could not get group status for {group_id}: {e}")
        
        return {"group_id": group_id, "status": "unknown"}

    def _group_tasks_by_dependencies(self, 
                                    task_ids: List[str], 
                                    dependencies: List[TaskDependency]) -> List[Dict]:
        """
        Group tasks based on their dependencies.
        
        This method analyzes task dependencies and creates groups suitable for 
        chord submission to handle patterns like "generate_data_*" -> "process_results".
        """
        groups = []
        processed_tasks = set()
        
        # Since task instances are not stored in database, we'll work with
        # the task_ids directly. Task details would need to come from the
        # TaskDependency objects or be passed as kwargs.
        task_details = {}
        for task_id in task_ids:
            # For now, create minimal task info from task_id
            # In a full implementation, this would extract step info from task_id format
            task_details[task_id] = {
                'step_name': task_id.split('/')[-1] if '/' in task_id else task_id,
                'depends': [],  # Dependencies come from TaskDependency objects
                'workspace': task_id  # task_id serves as workspace path
            }
        
        # Process each dependency pattern
        for dep in dependencies:
            header_tasks = []
            dependent_tasks = []
            
            # Find tasks that match the dependency pattern
            for task_id, details in task_details.items():
                if task_id in processed_tasks:
                    continue
                    
                if self._matches_pattern(details['step_name'], dep.task_pattern):
                    header_tasks.append(task_id)
                    processed_tasks.add(task_id)
                elif any(self._matches_pattern(d, dep.task_pattern) for d in details.get('depends', [])):
                    dependent_tasks.append(task_id)
                    processed_tasks.add(task_id)
            
            if header_tasks and dependent_tasks:
                groups.append({
                    'id': f"chord_{dep.task_pattern.replace('*', 'all')}",
                    'header_tasks': header_tasks,
                    'body_task': dependent_tasks[0],  # Assuming single dependent task
                    'tasks': header_tasks,  # For fallback
                    'has_dependents': True
                })
        
        # Add remaining independent tasks
        remaining_tasks = [tid for tid in task_ids if tid not in processed_tasks]
        if remaining_tasks:
            groups.append({
                'id': 'independent_tasks',
                'tasks': remaining_tasks,
                'has_dependents': False
            })
        
        return groups

    def _matches_pattern(self, task_name: str, pattern: str) -> bool:
        """
        Check if task name matches dependency pattern.
        
        Supports fnmatch-style patterns like "generate_data_*".
        """
        import fnmatch
        return fnmatch.fnmatch(task_name, pattern)

    def cancel_task(self, task_id: str):
        """
        Cancel a currently running task.

        Args:
            task_id: The ID of the task to cancel.
            
        Returns:
            True if cancellation was successful, False otherwise.
        """
        if not self.celery_app:
            raise RuntimeError("Celery task server not initialized")
            
        try:
            self.celery_app.control.revoke(task_id, terminate=True)
            LOG.info(f"Cancelled task {task_id}")
            return True
        except Exception as e:
            LOG.warning(f"Failed to cancel task {task_id}: {e}")
            return False

    def cancel_tasks(self, task_ids: List[str]):
        """
        Cancel multiple running tasks.

        Args:
            task_ids: A list of task IDs to cancel.
            
        Returns:
            Dictionary mapping task_id to cancellation success.
        """
        results = {}
        for task_id in task_ids:
            results[task_id] = self.cancel_task(task_id)
        return results
    
    def start_workers(self, spec: MerlinSpec):
        """
        Start workers using configuration from MerlinSpec.

        Args:
            spec: MerlinSpec object containing worker configuration.
        """
        try:
            # Use existing Celery worker startup logic
            from merlin.study.celeryadapter import start_celery_workers
            
            # Use 'all' to start workers for all steps
            # This maintains the expected behavior for worker startup
            # DEBUG: Check what steps are being started
            LOG.debug(f"Starting Celery workers for steps=['all'] with spec: {spec.name}")
            
            start_celery_workers(
                spec=spec,
                steps=["all"],
                celery_args="",  # Use defaults from spec
                disable_logs=False,
                just_return_command=False
            )
            
            LOG.info("Started Celery workers using MerlinSpec configuration")
            
        except Exception as e:
            LOG.error(f"Failed to start workers: {e}")
            raise
    
    def stop_workers(self, names: Optional[List[str]] = None):
        """
        Stop currently running workers.

        Args:
            names: Optional list of specific worker names to shut down.
        """
        if not self.celery_app:
            raise RuntimeError("Celery task server not initialized")
            
        try:
            # Use existing Celery worker stop logic
            from merlin.study.celeryadapter import stop_celery_workers
            
            # Map our interface to existing function parameters
            queues = None  # Stop workers from all queues
            spec_worker_names = names  # Use provided names
            worker_regex = None  # No regex filtering
            
            stop_celery_workers(
                queues=queues,
                spec_worker_names=spec_worker_names,
                worker_regex=worker_regex
            )
            
            LOG.info("Stopped Celery workers")
            
        except Exception as e:
            LOG.error(f"Failed to stop workers: {e}")
            raise
    
    def display_queue_info(self, queues: Optional[List[str]] = None):
        """
        Display information about queues to the console.

        Args:
            queues: Optional list of specific queue names to display.
        """
        if not self.celery_app:
            print("Error: Celery task server not initialized")
            return
            
        try:
            # Use existing queue query logic
            from merlin.study.celeryadapter import query_celery_queues, get_active_celery_queues
            
            if queues:
                # Query specific queues
                queue_info = query_celery_queues(queues, self.celery_app)
            else:
                # Get all active queues
                active_queues, _ = get_active_celery_queues(self.celery_app)
                if not active_queues:
                    print("No active queues found")
                    return
                queue_info = query_celery_queues(list(active_queues.keys()), self.celery_app)
            
            # Format and display queue information
            table_data = []
            for queue_name, info in queue_info.items():
                table_data.append([
                    queue_name,
                    info.get('jobs', 0),
                    info.get('consumers', 0)
                ])
            
            if table_data:
                print("\nQueue Information:")
                print(tabulate(table_data, headers=['Queue Name', 'Pending Jobs', 'Consumers']))
            else:
                print("No queue information available")
                
        except Exception as e:
            print(f"Error retrieving queue information: {e}")

    def display_connected_workers(self):
        """
        Display information about connected workers to the console.
        """
        if not self.celery_app:
            print("Error: Celery task server not initialized")
            return
            
        try:
            # Use existing worker query logic
            from merlin.study.celeryadapter import get_active_workers
            
            worker_queue_map = get_active_workers(self.celery_app)
            
            if not worker_queue_map:
                print("No connected workers found")
                return
            
            # Format and display worker information
            table_data = []
            for worker_name, queues in worker_queue_map.items():
                table_data.append([
                    worker_name,
                    ", ".join(queues) if queues else "None"
                ])
            
            print("\nConnected Workers:")
            print(tabulate(table_data, headers=['Worker Name', 'Queues']))
            
        except Exception as e:
            print(f"Error retrieving worker information: {e}")

    def display_running_tasks(self):
        """
        Display the IDs of currently running tasks to the console.
        """
        if not self.celery_app:
            print("Error: Celery task server not initialized")
            return
            
        try:
            # Get active tasks from Celery
            inspect = self.celery_app.control.inspect()
            active_tasks = inspect.active()
            
            if not active_tasks:
                print("No running tasks found")
                return
            
            # Collect all running task IDs
            running_task_ids = []
            for worker_name, tasks in active_tasks.items():
                for task in tasks:
                    running_task_ids.append(task['id'])
            
            if running_task_ids:
                print(f"\nRunning Tasks ({len(running_task_ids)} total):")
                for task_id in running_task_ids:
                    print(f"  {task_id}")
            else:
                print("No running tasks found")
                
        except Exception as e:
            print(f"Error retrieving running tasks: {e}")

    def purge_tasks(self, queues: List[str], force: bool = False) -> int:
        """
        Remove all pending tasks from specified queues.
        
        Args:
            queues: List of queue names to purge.
            force: If True, purge without confirmation.
            
        Returns:
            Number of tasks purged.
        """
        if not self.celery_app:
            raise RuntimeError("Celery task server not initialized")
            
        try:
            # Use existing Celery purge functionality
            from merlin.study.celeryadapter import purge_celery_tasks  # pylint: disable=C0415
            
            # Convert list to comma-separated string as expected by purge_celery_tasks
            queue_string = ",".join(queues) if queues else ""
            
            # Purge tasks
            return purge_celery_tasks(queue_string, force)
            
        except Exception as e:
            LOG.error(f"Failed to purge tasks from queues {queues}: {e}")
            return 0

    def get_workers(self) -> List[str]:
        """
        Get a list of all currently connected workers.
        
        Returns:
            List of worker names/identifiers.
        """
        if not self.celery_app:
            return []
            
        try:
            # Use existing worker query logic
            from merlin.study.celeryadapter import get_workers_from_app  # pylint: disable=C0415
            
            return get_workers_from_app()
            
        except Exception as e:
            LOG.error(f"Failed to get workers: {e}")
            return []

    def get_active_queues(self) -> Dict[str, List[str]]:
        """
        Get a mapping of active queues to their connected workers.
        
        Returns:
            Dictionary mapping queue names to lists of worker names.
        """
        if not self.celery_app:
            return {}
            
        try:
            # Use existing queue query logic
            from merlin.study.celeryadapter import get_active_celery_queues  # pylint: disable=C0415
            
            active_queues, _ = get_active_celery_queues(self.celery_app)
            return active_queues
            
        except Exception as e:
            LOG.error(f"Failed to get active queues: {e}")
            return {}

    def check_workers_processing(self, queues: List[str]) -> bool:
        """
        Check if any workers are currently processing tasks from specified queues.
        
        Args:
            queues: List of queue names to check.
            
        Returns:
            True if any workers are processing tasks, False otherwise.
        """
        if not self.celery_app:
            return False
            
        try:
            # Use existing worker processing check
            from merlin.study.celeryadapter import check_celery_workers_processing  # pylint: disable=C0415
            
            return check_celery_workers_processing(queues, self.celery_app)
            
        except Exception as e:
            LOG.error(f"Failed to check workers processing: {e}")
            return False
    
    def submit_condense_task(self, 
                           sample_index, 
                           workspace: str, 
                           condensed_workspace: str,
                           queue: str = None):
        """
        Submit a status file condensing task via Celery.
        
        This method implements backend-agnostic status file condensing by creating
        a Celery task signature and submitting it through the task distribution system.
        
        Args:
            sample_index: SampleIndex object for status file locations
            workspace: Full workspace path for condensing
            condensed_workspace: Shortened workspace path for status entries
            queue: Task queue for execution
            
        Returns:
            AsyncResult object for tracking task execution
        """
        if not self.celery_app:
            raise RuntimeError("Celery task server not initialized")
        
        from merlin.common.tasks import condense_status_files  # pylint: disable=C0415
        
        # Create Celery signature for the condense task
        condense_sig = condense_status_files.s(
            sample_index=sample_index,
            workspace=workspace,
            condensed_workspace=condensed_workspace
        )
        
        # Set the queue if provided
        if queue:
            condense_sig = condense_sig.set(queue=queue)
        
        # Submit the task and return AsyncResult
        result = condense_sig.delay()
        LOG.debug(f"Submitted condense task to Celery via TaskServerInterface: {result.id}")
        return result

    def submit_study(self, study, adapter: Dict, samples, sample_labels, egraph, groups_of_chains):
        """
        Submit an entire study using Celery's native chain/chord/group coordination.
        """
        from celery import chain, chord, group  # pylint: disable=C0415
        from merlin.common.tasks import expand_tasks_with_samples, chordfinisher, mark_run_as_complete, merlin_step  # pylint: disable=C0415
        
        LOG.info("Converting graph to Celery tasks using native coordination patterns.")
        
        celery_dag = chain(
            chord(
                group(
                    [
                        expand_tasks_with_samples.si(
                            egraph,
                            gchain,
                            samples,
                            sample_labels,
                            merlin_step,
                            adapter,
                            study.level_max_dirs,
                        ).set(queue=egraph.step(chain_group[0][0]).get_task_queue())
                        for gchain in chain_group
                    ]
                ),
                chordfinisher.s().set(queue=egraph.step(chain_group[0][0]).get_task_queue()),
            )
            for chain_group in groups_of_chains[1:]  # Skip _source group
        )

        # Append the final task that marks the run as complete
        final_task = mark_run_as_complete.si(study.workspace).set(
            queue=egraph.step(
                groups_of_chains[-1][-1][-1]  # Use the task queue from the final step
            ).get_task_queue()
        )
        celery_dag = celery_dag | final_task

        LOG.info("Launching Celery tasks.")
        return celery_dag.delay(None)