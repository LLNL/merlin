##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Provides a concrete implementation of the
[`MerlinWorkerHandler`][workers.handlers.worker_handler.MerlinWorkerHandler] for Celery.

This module defines the `CeleryWorkerHandler` class, which is responsible for launching,
stopping, and querying Celery-based worker processes. It supports additional options
such as echoing launch commands, overriding default worker arguments, and disabling logs.
"""

import logging
from typing import List, Dict, Any

from merlin.spec.specification import MerlinSpec
from merlin.workers import CeleryWorker
from merlin.workers.handlers.worker_handler import MerlinWorkerHandler
from merlin.workers.worker import MerlinWorker


LOG = logging.getLogger("merlin")


class CeleryWorkerHandler(MerlinWorkerHandler):
    """
    Worker handler for launching and managing Celery-based Merlin workers.

    This class implements the abstract methods defined in
    [`MerlinWorkerHandler`][workers.handlers.worker_handler.MerlinWorkerHandler] to provide
    Celery-specific behavior, including launching workers with optional command-line overrides,
    stopping workers, and querying their status.

    Methods:
        launch_workers: Launch or echo Celery workers with optional arguments.
        stop_workers: Attempt to stop active Celery workers.
        query_workers: Return a basic summary of Celery worker status.
    """

    def launch_workers(
        self,
        workers: List[MerlinWorker] = None,
        spec: MerlinSpec = None,
        steps: List[str] = None,
        worker_args: str = "",
        disable_logs: bool = False,
        just_return_command: bool = False,
        **kwargs
    ) -> str:
        """
        Launch a list of Celery worker instances.

        This method can work with either pre-created worker instances or create
        workers from a MerlinSpec specification.

        Args:
            workers: Pre-created list of CeleryWorker instances to launch.
            spec: MerlinSpec to create workers from (if workers not provided).
            steps: Specific steps to create workers for (when using spec).
            worker_args: Additional arguments for worker processes.
            disable_logs: If True, suppress worker logging.
            just_return_command: If True, return commands without executing.
            **kwargs: Additional keyword arguments for backward compatibility.

        Returns:
            A string describing the launched workers or commands.

        Raises:
            ValueError: If neither workers nor spec is provided.
        """
        if workers is None and spec is None:
            raise ValueError("Must provide either workers list or spec")

        # Handle backward compatibility with old interface
        echo_only = kwargs.get("echo_only", just_return_command)
        override_args = kwargs.get("override_args", worker_args)

        # Create workers from spec if not provided
        if workers is None:
            workers = self.create_workers_from_spec(spec, steps or ["all"])

        launched_commands = []
        launched_count = 0

        for worker in workers:
            if not isinstance(worker, CeleryWorker):
                LOG.warning(f"Skipping non-Celery worker: {worker.name}")
                continue

            try:
                if echo_only:
                    # Return command without executing
                    command = worker.get_launch_command(override_args=override_args, disable_logs=disable_logs)
                    launched_commands.append(command)
                    print(f"Celery worker command: {command}")
                else:
                    # Launch the worker
                    worker.launch_worker(override_args=override_args, disable_logs=disable_logs)
                    launched_count += 1
                    LOG.info(f"Successfully launched Celery worker: {worker.name}")

            except Exception as e:
                LOG.error(f"Failed to launch Celery worker {worker.name}: {e}")
                continue

        if echo_only:
            return "\n".join(launched_commands)
        else:
            return f"Launched {launched_count} Celery workers"

    def stop_workers(self, worker_names: List[str] = None, **kwargs):
        """
        Attempt to stop Celery workers.

        Args:
            worker_names: Specific worker names to stop (if None, stops all).
            **kwargs: Additional keyword arguments.
        """
        # TODO: Implement proper Celery worker stopping
        # This would typically use Celery's control interface
        LOG.warning("Celery worker stopping not yet implemented in new handler")

    def query_workers(self, **kwargs) -> Dict[str, Any]:
        """
        Query the status of Celery workers.

        Args:
            **kwargs: Additional keyword arguments.

        Returns:
            A dictionary containing information about Celery worker status.
        """
        # TODO: Implement proper Celery worker status querying
        # This would typically use Celery's inspect interface
        return {
            'handler_type': 'celery',
            'status': 'query not yet implemented in new handler'
        }

    def create_workers_from_spec(self, spec: MerlinSpec, steps: List[str]) -> List[CeleryWorker]:
        """
        Create CeleryWorker instances from a MerlinSpec.

        This method leverages existing Celery worker creation logic but adapts
        it to work with the new worker handler architecture.

        Args:
            spec: The MerlinSpec containing worker definitions.
            steps: List of steps to create workers for.

        Returns:
            List of created CeleryWorker instances.
        """
        workers = []
        
        # Get worker configuration from spec
        workers_config = spec.merlin.get("resources", {}).get("workers", {})
        if not workers_config:
            LOG.warning("No workers defined in spec")
            return workers

        # Get overlap setting
        overlap = spec.merlin.get("resources", {}).get("overlap", False)

        # Filter steps if specific steps requested
        steps_provided = "all" not in steps
        
        for worker_name, worker_config in workers_config.items():
            # Check if this worker should handle the requested steps
            worker_steps = worker_config.get("steps", steps)
            if steps_provided:
                # Only include workers that handle at least one of the requested steps
                if not any(step in worker_steps for step in steps):
                    continue

            # Get queues for this worker's steps
            try:
                worker_queues = set()
                for step in worker_steps:
                    if steps_provided and step not in steps:
                        continue
                    # Get queue for this step
                    queue_list = spec.get_queue_list([step])
                    if queue_list:
                        worker_queues.update(queue_list)
                
                if not worker_queues:
                    LOG.warning(f"No queues found for worker {worker_name}")
                    continue

            except Exception as e:
                LOG.error(f"Failed to determine queues for worker {worker_name}: {e}")
                continue

            # Create worker configuration
            config = {
                'args': worker_config.get('args', ''),
                'queues': worker_queues,
                'batch': worker_config.get('batch', {}),
                'machines': worker_config.get('machines', []),
                'nodes': worker_config.get('nodes', 1),
                'steps': worker_steps,
                'original_config': worker_config
            }

            # Create environment from spec
            env = {}
            if hasattr(spec, 'environment') and spec.environment:
                env_vars = spec.environment.get('variables', {})
                for var_name, var_val in env_vars.items():
                    env[str(var_name)] = str(var_val)

            # Create the Celery worker
            try:
                celery_worker = CeleryWorker(worker_name, config, env, overlap=overlap)
                workers.append(celery_worker)
                LOG.debug(f"Created Celery worker: {worker_name}")
                
            except Exception as e:
                LOG.error(f"Failed to create Celery worker {worker_name}: {e}")
                continue

        LOG.info(f"Created {len(workers)} Celery workers from spec")
        return workers
