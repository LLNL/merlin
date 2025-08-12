##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Implements a Kafka-based worker handler for the Merlin framework.

This module defines the `KafkaWorkerHandler` class, which manages the lifecycle
of Kafka-based workers. It provides functionality to launch, stop, and query
Kafka workers that consume tasks from Apache Kafka topics.
"""

import json
import logging
from typing import List, Dict, Any

from merlin.spec.specification import MerlinSpec
from merlin.workers.handlers.worker_handler import MerlinWorkerHandler
from merlin.workers.kafka_worker import KafkaWorker
from merlin.workers.worker import MerlinWorker


LOG = logging.getLogger(__name__)


class KafkaWorkerHandler(MerlinWorkerHandler):
    """
    Worker handler for managing Kafka-based Merlin workers.

    This class provides functionality to launch, stop, and query Kafka workers
    that process tasks from Kafka topics. It implements the `MerlinWorkerHandler`
    interface to provide consistent worker management across different task servers.

    Attributes:
        launched_workers (List[str]): Track workers that have been launched.

    Methods:
        launch_workers: Launch a list of Kafka worker instances.
        stop_workers: Stop running Kafka workers.
        query_workers: Query the status of running Kafka workers.
        create_workers_from_spec: Create KafkaWorker instances from a MerlinSpec.
    """

    def __init__(self):
        """Initialize the Kafka worker handler."""
        super().__init__()
        self.launched_workers = []

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
        Launch a list of Kafka worker instances.

        This method can work with either pre-created worker instances or create
        workers from a MerlinSpec specification.

        Args:
            workers: Pre-created list of KafkaWorker instances to launch.
            spec: MerlinSpec to create workers from (if workers not provided).
            steps: Specific steps to create workers for (when using spec).
            worker_args: Additional arguments for worker processes.
            disable_logs: If True, suppress worker logging.
            just_return_command: If True, return commands without executing.
            **kwargs: Additional keyword arguments.

        Returns:
            A string describing the launched workers or commands.

        Raises:
            ValueError: If neither workers nor spec is provided.
        """
        if workers is None and spec is None:
            raise ValueError("Must provide either workers list or spec")

        # Create workers from spec if not provided
        if workers is None:
            workers = self.create_workers_from_spec(spec, steps or ["all"])

        launched_commands = []
        launched_count = 0

        for worker in workers:
            if not isinstance(worker, KafkaWorker):
                LOG.warning(f"Skipping non-Kafka worker: {worker.name}")
                continue

            try:
                if just_return_command:
                    # Return command without executing
                    command = worker.get_launch_command()
                    launched_commands.append(command)
                    print(f"Kafka worker command: {command}")
                else:
                    # Launch the worker
                    worker.launch_worker()
                    self.launched_workers.append(worker.name)
                    launched_count += 1
                    LOG.info(f"Successfully launched Kafka worker: {worker.name}")

            except Exception as e:
                LOG.error(f"Failed to launch Kafka worker {worker.name}: {e}")
                continue

        if just_return_command:
            return "\n".join(launched_commands)
        else:
            return f"Launched {launched_count} Kafka workers"

    def stop_workers(self, worker_names: List[str] = None, **kwargs):
        """
        Stop Kafka workers by sending control messages.

        This method sends stop commands to Kafka workers via the control topic.
        In a full implementation, this would track worker PIDs or use other
        mechanisms for more reliable worker shutdown.

        Args:
            worker_names: Specific worker names to stop (if None, stops all).
            **kwargs: Additional keyword arguments.
        """
        try:
            from kafka import KafkaProducer  # pylint: disable=import-outside-toplevel
        except ImportError:
            LOG.error("kafka-python package required to stop Kafka workers")
            LOG.error("Please install: pip install kafka-python")
            return

        # Default Kafka configuration
        producer_config = {
            'bootstrap_servers': ['localhost:9092'],
            'value_serializer': lambda x: json.dumps(x).encode()
        }

        try:
            producer = KafkaProducer(**producer_config)

            # Send stop command to control topic
            stop_message = {
                'action': 'stop_workers',
                'timestamp': time.time(),
                'worker_names': worker_names or self.launched_workers
            }

            producer.send('merlin_control', value=stop_message)
            producer.flush()
            producer.close()

            LOG.info(f"Sent stop command to Kafka workers: {worker_names or 'all'}")

            # Clear launched workers list
            if worker_names is None:
                self.launched_workers.clear()
            else:
                self.launched_workers = [w for w in self.launched_workers if w not in worker_names]

        except Exception as e:
            LOG.error(f"Failed to stop Kafka workers: {e}")

    def query_workers(self, **kwargs) -> Dict[str, Any]:
        """
        Query the status of Kafka workers.

        This is a simplified implementation that returns information about
        launched workers. In a full implementation, this would query Kafka
        consumer groups and topic assignments to get real-time status.

        Args:
            **kwargs: Additional keyword arguments.

        Returns:
            A dictionary containing information about Kafka worker status.
        """
        try:
            from kafka import KafkaAdminClient  # pylint: disable=import-outside-toplevel
            from kafka.admin.config_resource import ConfigResource, ConfigResourceType  # pylint: disable=import-outside-toplevel
        except ImportError:
            LOG.warning("kafka-python package required for detailed worker status")
            return {
                'launched_workers': self.launched_workers,
                'worker_count': len(self.launched_workers),
                'status': 'limited (kafka-python not available)'
            }

        # Basic implementation - return launched workers
        status_info = {
            'launched_workers': self.launched_workers,
            'worker_count': len(self.launched_workers),
            'handler_type': 'kafka',
            'status': 'active' if self.launched_workers else 'inactive'
        }

        # Try to get additional Kafka cluster info if possible
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=['localhost:9092'],
                client_id='merlin_admin'
            )
            
            # Get basic cluster metadata
            metadata = admin_client.describe_cluster()
            status_info['cluster_info'] = {
                'cluster_id': str(metadata.cluster_id) if metadata.cluster_id else 'unknown',
                'controller': metadata.controller.id if metadata.controller else 'unknown'
            }
            
            admin_client.close()

        except Exception as e:
            LOG.debug(f"Could not retrieve Kafka cluster info: {e}")
            status_info['cluster_info'] = 'unavailable'

        return status_info

    def create_workers_from_spec(self, spec: MerlinSpec, steps: List[str]) -> List[KafkaWorker]:
        """
        Create KafkaWorker instances from a MerlinSpec.

        Args:
            spec: The MerlinSpec containing worker definitions.
            steps: List of steps to create workers for.

        Returns:
            List of created KafkaWorker instances.
        """
        workers = []
        
        # Get worker configuration from spec
        workers_config = spec.merlin.get("resources", {}).get("workers", {})
        if not workers_config:
            LOG.warning("No workers defined in spec")
            return workers

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
                    # Map step to queue - simplified approach
                    queue_name = spec.get_queue_list([step])
                    if queue_name:
                        worker_queues.update(queue_name)
                
                if not worker_queues:
                    LOG.warning(f"No queues found for worker {worker_name}")
                    continue

            except Exception as e:
                LOG.error(f"Failed to determine queues for worker {worker_name}: {e}")
                continue

            # Create Kafka-specific configuration
            kafka_config = {
                'consumer': {
                    'bootstrap_servers': ['localhost:9092'],
                    'group_id': f'merlin_workers_{worker_name}',
                    'auto_offset_reset': 'earliest',
                    'enable_auto_commit': True
                }
            }

            # Override with any Kafka-specific config from spec
            if 'kafka' in worker_config:
                kafka_config.update(worker_config['kafka'])

            # Create worker configuration
            config = {
                'queues': worker_queues,
                'kafka_config': kafka_config,
                'consumer_group': f'merlin_workers_{worker_name}',
                'steps': worker_steps,
                'original_config': worker_config
            }

            # Create environment from spec
            env = {}
            if hasattr(spec, 'environment') and spec.environment:
                env_vars = spec.environment.get('variables', {})
                for var_name, var_val in env_vars.items():
                    env[str(var_name)] = str(var_val)

            # Create the Kafka worker
            try:
                kafka_worker = KafkaWorker(worker_name, config, env)
                workers.append(kafka_worker)
                LOG.debug(f"Created Kafka worker: {worker_name}")
                
            except Exception as e:
                LOG.error(f"Failed to create Kafka worker {worker_name}: {e}")
                continue

        LOG.info(f"Created {len(workers)} Kafka workers from spec")
        return workers


# Import time here to avoid circular imports
import time  # pylint: disable=wrong-import-position