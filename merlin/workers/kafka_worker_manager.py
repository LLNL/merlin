##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Implements a Kafka-based MerlinWorker.

This module defines the `KafkaWorker` class, which extends the abstract
`MerlinWorker` base class to implement worker launching and management using
Apache Kafka. Kafka workers are responsible for processing tasks from specified
topics and provide an alternative to Celery-based task distribution.
"""

import json
import logging
import os
import signal
import subprocess
import time
from pathlib import Path
from typing import Dict, Any, List

from merlin.db_scripts.merlin_db import MerlinDatabase
from merlin.exceptions import MerlinWorkerLaunchError
from merlin.workers.worker import MerlinWorker
from merlin.optimization.message_optimizer import OptimizedTaskMessage


LOG = logging.getLogger("merlin")


class KafkaWorkerManager(MerlinWorker):
    """
    Manager for Kafka-based Merlin worker lifecycle and configuration.

    This class provides logic for validating configuration, constructing launch
    commands, and managing Kafka workers that process jobs from specific topics.

    Attributes:
        name (str): The name of the worker.
        config (dict): Configuration settings for the worker.
        env (dict): Environment variables used by the worker process.
        kafka_config (dict): Kafka-specific configuration settings.
        queues (set): Topics the worker listens to (mapped from queues).
        consumer_group (str): Kafka consumer group for this worker.

    Methods:
        get_launch_command: Construct the Kafka worker launch command.
        launch_worker: Launch the worker using subprocess.
        get_metadata: Return identifying metadata about the worker.
    """

    def __init__(
        self,
        name: str,
        config: Dict,
        env: Dict[str, str] = None,
    ):
        """
        Constructor for Kafka workers.

        Sets up attributes used throughout this worker object and saves this worker to the database.

        Args:
            name: The name of the worker.
            config: A dictionary containing configuration settings for this worker including:
                - `kafka_config`: Kafka-specific settings (bootstrap_servers, etc.)
                - `queues`: A set of task topics for this worker to watch
                - `consumer_group`: Kafka consumer group (defaults to 'merlin_workers')
            env: A dictionary of environment variables set by the user.
        """
        super().__init__(name, config, env)
        self.kafka_config = self.config.get("kafka_config", {})
        self.queues = self.config.get("queues", {"default"})
        self.consumer_group = self.config.get("consumer_group", "merlin_workers")
        
        # Set default Kafka configuration
        if not self.kafka_config:
            self.kafka_config = {
                'consumer': {
                    'bootstrap_servers': ['localhost:9092'],
                    'group_id': self.consumer_group,
                    'auto_offset_reset': 'earliest',
                    'enable_auto_commit': True
                }
            }

        # Add this worker to the database
        merlin_db = MerlinDatabase()
        merlin_db.create("logical_worker", self.name, self.queues)

    def get_launch_command(self, override_args: str = "") -> str:
        """
        Build the command to launch this Kafka worker.

        Args:
            override_args: Additional arguments (currently unused for Kafka workers).

        Returns:
            A shell command string suitable for subprocess execution.
        """
        # Create configuration for the worker
        worker_config = {
            'kafka': self.kafka_config,
            'queues': list(self.queues),
            'worker_name': self.name
        }
        
        # Use the kafka task consumer script from our implementations
        kafka_consumer_path = os.path.join(
            os.path.dirname(__file__),
            "..", "task_servers", "implementations", "kafka_task_consumer.py"
        )
        
        # Construct command to run the Kafka worker
        config_json = json.dumps(worker_config).replace('"', '\\"')
        launch_cmd = f'python {kafka_consumer_path} --config "{config_json}"'
        
        return os.path.expandvars(launch_cmd)

    def launch_worker(self, override_args: str = ""):
        """
        Launch the worker as a subprocess using the constructed launch command.

        Args:
            override_args: Optional CLI arguments (currently unused for Kafka workers).

        Raises:
            MerlinWorkerLaunchError: If the worker fails to launch.
        """
        launch_cmd = self.get_launch_command(override_args=override_args)
        try:
            LOG.info(f"Launching Kafka worker '{self.name}'")
            LOG.debug(f"Launch command: {launch_cmd}")
            
            # Launch worker as subprocess
            subprocess.Popen(
                launch_cmd,
                env=self.env,
                shell=True,
                universal_newlines=True
            )
            
            LOG.debug(f"Launched Kafka worker '{self.name}' successfully")
            
        except Exception as e:
            LOG.error(f"Cannot start Kafka worker '{self.name}': {e}")
            raise MerlinWorkerLaunchError from e

    def get_metadata(self) -> Dict:
        """
        Return metadata about this worker instance.

        Returns:
            A dictionary containing key details about this worker.
        """
        return {
            "name": self.name,
            "queues": list(self.queues),
            "consumer_group": self.consumer_group,
            "kafka_config": self.kafka_config,
            "worker_type": "kafka"
        }


def _check_kafka_dependencies():
    """Check if required Kafka dependencies are available."""
    try:
        import kafka  # pylint: disable=import-outside-toplevel,unused-import
    except ImportError:
        LOG.error("kafka-python package required for Kafka workers")
        LOG.error("Please install: pip install kafka-python")
        raise ImportError("Missing kafka-python dependency")


class KafkaTaskWorkerRuntime:
    """
    Direct Kafka worker implementation for task processing.
    
    This class handles the actual Kafka message consumption and task execution,
    providing the runtime component that processes messages from Kafka topics.
    Note: This is embedded in the manager file for backwards compatibility.
    The main implementation is now in kafka_task_consumer.py
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Kafka task worker.
        
        Args:
            config: Configuration containing kafka settings and queues
        """
        _check_kafka_dependencies()
        
        self.config = config
        self.running = False
        self.consumer = None
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        LOG.info(f"Received signal {signum}, shutting down worker...")
        self.stop()
        
    def start(self):
        """Start consuming tasks from Kafka topics."""
        try:
            from kafka import KafkaConsumer  # pylint: disable=C0415
        except ImportError:
            LOG.error("kafka-python package required for Kafka worker")
            LOG.error("Please install: pip install kafka-python")
            raise
        
        # Setup consumer configuration
        consumer_config = self.config.get('kafka', {}).get('consumer', {})
        consumer_config.setdefault('bootstrap_servers', ['localhost:9092'])
        consumer_config.setdefault('value_deserializer', lambda x: json.loads(x.decode()))
        consumer_config.setdefault('auto_offset_reset', 'earliest')
        consumer_config.setdefault('enable_auto_commit', True)
        consumer_config.setdefault('group_id', 'merlin_workers')
        
        # Subscribe to task topics based on configured queues
        topics = [f"merlin_tasks_{queue}" for queue in self.config.get('queues', ['default'])]
        topics.append('merlin_control')  # Always listen for control messages
        
        self.consumer = KafkaConsumer(*topics, **consumer_config)
        
        LOG.info(f"Kafka worker started, consuming from topics: {topics}")
        
        self.running = True
        try:
            for message in self.consumer:
                if not self.running:
                    break
                
                try:
                    # Handle different message types
                    if message.topic == 'merlin_control':
                        self._handle_control_message(message.value)
                    else:
                        self._handle_task_message(message.value)
                        
                except Exception as e:
                    LOG.error(f"Failed to process message from {message.topic}: {e}")
                    # Continue processing other messages
                    
        except KeyboardInterrupt:
            LOG.info("Worker interrupted by user")
        finally:
            self.stop()
    
    def _handle_control_message(self, message: Dict[str, Any]):
        """Handle control messages (stop, cancel, etc.)."""
        action = message.get('action')
        
        if action == 'stop_workers':
            LOG.info("Received stop_workers command")
            self.stop()
        elif action == 'cancel':
            task_id = message.get('task_id')
            LOG.info(f"Received cancel command for task {task_id}")
            # In a full implementation, would track and cancel running tasks
        else:
            LOG.warning(f"Unknown control action: {action}")
    
    def _handle_task_message(self, task_data: Dict[str, Any]):
        """
        Handle task execution messages using script-based execution.
        
        This method replaces direct Celery function calls with script execution.
        """
        try:
            # Parse optimized task message
            task_msg = OptimizedTaskMessage.from_dict(task_data)
            
            LOG.info(f"Processing task {task_msg.task_id} of type {task_msg.task_type}")
            
            start_time = time.time()
            
            # Execute task via script (replaces direct function calls)
            result = self._execute_task_script(task_msg)
            
            execution_time = time.time() - start_time
            
            if result.get('status') == 'completed':
                LOG.info(f"Task {task_msg.task_id} completed successfully in {execution_time:.2f}s")
            else:
                LOG.error(f"Task {task_msg.task_id} failed: {result.get('error', 'Unknown error')}")
            
            # Store result
            self._store_result(task_msg.task_id, {
                'status': 'SUCCESS' if result.get('status') == 'completed' else 'FAILURE',
                'result': result,
                'execution_time': execution_time,
                'completed_at': time.time() if result.get('status') == 'completed' else None,
                'failed_at': time.time() if result.get('status') != 'completed' else None
            })
            
        except Exception as e:
            LOG.error(f"Error processing task message: {e}", exc_info=True)
            
            # Store error result
            task_id = task_data.get('task_id', 'unknown')
            self._store_result(task_id, {
                'status': 'FAILURE', 
                'error': str(e),
                'failed_at': time.time()
            })
            
    def _execute_task_script(self, task_msg: OptimizedTaskMessage, shared_storage_path: str = "/shared/storage") -> Dict[str, Any]:
        """
        Execute task using generated script instead of direct function calls.
        
        This method replaces direct Celery function calls with script execution,
        eliminating Celery context dependencies and enabling backend independence.
        """
        shared_storage = Path(shared_storage_path)
        scripts_dir = shared_storage / "scripts"
        workspace_dir = shared_storage / "workspace"
        
        # Construct script path
        script_path = scripts_dir / task_msg.script_reference
        
        if not script_path.exists():
            raise FileNotFoundError(f"Script not found: {script_path}")
        
        # Make sure script is executable
        script_path.chmod(0o755)
        
        LOG.info(f"Executing script: {script_path}")
        
        try:
            # Execute script with timeout
            result = subprocess.run(
                [str(script_path)],
                capture_output=True,
                text=True,
                timeout=3600,  # 1 hour timeout
                cwd=str(workspace_dir / task_msg.task_id)
            )
            
            # Parse result
            execution_result = {
                'task_id': task_msg.task_id,
                'exit_code': result.returncode,
                'stdout': result.stdout,
                'stderr': result.stderr,
                'execution_time': time.time(),
                'worker_name': self.name,
                'status': 'completed' if result.returncode == 0 else 'failed'
            }
            
            # Try to load result metadata if available
            result_file = workspace_dir / task_msg.task_id / 'step_result.json'
            if result_file.exists():
                with open(result_file, 'r') as f:
                    step_result = json.load(f)
                    execution_result.update(step_result)
            
            return execution_result
            
        except subprocess.TimeoutExpired:
            LOG.error(f"Task {task_msg.task_id} timed out")
            return {
                'task_id': task_msg.task_id,
                'exit_code': 124,
                'status': 'timeout',
                'error': 'Task execution timed out',
                'worker_name': self.name
            }
        except Exception as e:
            LOG.error(f"Script execution failed: {e}")
            return {
                'task_id': task_msg.task_id,
                'exit_code': 1,
                'status': 'error',
                'error': str(e),
                'worker_name': self.name
            }
    
    def _store_result(self, task_id: str, result_data: Dict[str, Any]):
        """Store task result (simplified implementation)."""
        # In a full implementation, this would use a proper result backend
        # For now, just log the result
        status = result_data.get('status')
        LOG.debug(f"Task {task_id} result: {status}")
        
        # If we have a result store available, use it
        try:
            from merlin.execution.memory_result_store import MemoryResultStore  # pylint: disable=C0415
            # In practice, this would be injected or configured
            store = MemoryResultStore()
            store.store_result(task_id, result_data)
        except Exception:
            # Result storage is not critical for task execution
            pass
    
    def stop(self):
        """Stop the worker gracefully."""
        LOG.info("Stopping Kafka worker...")
        self.running = False
        
        if self.consumer:
            try:
                self.consumer.close()
                LOG.debug("Kafka consumer closed")
            except Exception as e:
                LOG.warning(f"Error closing Kafka consumer: {e}")