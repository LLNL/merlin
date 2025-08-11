##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Kafka worker implementation for consuming and executing Merlin tasks.

This module provides a Kafka consumer that acts as the equivalent of Celery's
built-in workers. It consumes messages from Kafka topics and executes Merlin
steps using the same business logic that Celery workers use.

Note: Celery has built-in workers (no separate file needed), but Kafka requires
this custom worker implementation to bridge Kafka messages to Merlin execution.
"""

import json
import logging
import signal
import time
from typing import Dict, Any, List

LOG = logging.getLogger(__name__)


class KafkaWorker:
    """
    Simple Kafka worker that consumes tasks and executes them using existing Merlin logic.
    
    This worker bridges Kafka task distribution with Merlin's existing task execution,
    providing backend independence without duplicating business logic.
    """
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Kafka worker.
        
        Args:
            config: Configuration containing kafka settings and queues
        """
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
        Handle task execution messages.
        
        This method bridges Kafka messages to existing Merlin task execution logic.
        """
        task_id = task_data.get('task_id', 'unknown')
        task_type = task_data.get('task_type')
        parameters = task_data.get('parameters', {})
        
        LOG.info(f"Processing task {task_id} of type {task_type}")
        
        try:
            start_time = time.time()
            result = self._execute_task(task_type, parameters)
            execution_time = time.time() - start_time
            
            LOG.info(f"Task {task_id} completed successfully in {execution_time:.2f}s")
            
            # Store result if we have a result store available
            self._store_result(task_id, {
                'status': 'SUCCESS',
                'result': result,
                'execution_time': execution_time,
                'completed_at': time.time()
            })
            
        except Exception as e:
            LOG.error(f"Task {task_id} failed: {e}")
            
            # Store error result
            self._store_result(task_id, {
                'status': 'FAILURE', 
                'error': str(e),
                'failed_at': time.time()
            })
            
    def _execute_task(self, task_type: str, parameters: Dict[str, Any]) -> Any:
        """
        Execute a task using existing Merlin business logic.
        
        This method provides the bridge between Kafka task distribution and 
        existing Merlin task implementations.
        """
        # Try task registry first (if available)
        try:
            from merlin.execution.task_registry import task_registry  # pylint: disable=C0415
            
            task_func = task_registry.get(task_type)
            if task_func:
                LOG.debug(f"Executing {task_type} using task registry")
                return task_func(**parameters)
        except ImportError:
            LOG.debug("Task registry not available, using direct task imports")
        
        # Fall back to existing Celery task implementations
        # This provides backward compatibility with existing task logic
        if task_type == 'merlin_step':
            from merlin.common.tasks import merlin_step  # pylint: disable=C0415
            # Extract step and adapter_config from parameters
            step = parameters.get('step')
            adapter_config = parameters.get('adapter_config', {})
            return merlin_step(step, adapter_config=adapter_config)
            
        elif task_type == 'condense_status_files':
            from merlin.common.tasks import condense_status_files  # pylint: disable=C0415
            return condense_status_files(**parameters)
            
        elif task_type == 'expand_tasks_with_samples':
            from merlin.common.tasks import expand_tasks_with_samples  # pylint: disable=C0415
            return expand_tasks_with_samples(**parameters)
            
        elif task_type == 'queue_merlin_study':
            from merlin.common.tasks import queue_merlin_study  # pylint: disable=C0415
            return queue_merlin_study(**parameters)
            
        elif task_type == 'shutdown_workers':
            from merlin.common.tasks import shutdown_workers  # pylint: disable=C0415
            return shutdown_workers(**parameters)
            
        elif task_type == 'mark_run_as_complete':
            from merlin.common.tasks import mark_run_as_complete  # pylint: disable=C0415
            return mark_run_as_complete(**parameters)
            
        elif task_type == 'chordfinisher':
            from merlin.common.tasks import chordfinisher  # pylint: disable=C0415
            return chordfinisher(**parameters)
            
        else:
            raise ValueError(f"Unknown task type: {task_type}")
    
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


def main():
    """Standalone entry point for testing."""
    import argparse
    import sys
    
    parser = argparse.ArgumentParser(description='Start Kafka worker')
    parser.add_argument('--config', help='JSON config string')
    parser.add_argument('--queues', nargs='+', default=['default'],
                       help='Queues to consume from')
    parser.add_argument('--kafka-servers', default='localhost:9092',
                       help='Kafka bootstrap servers')
    
    args = parser.parse_args()
    
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Parse config
    if args.config:
        import json
        config = json.loads(args.config)
    else:
        config = {
            'kafka': {
                'consumer': {
                    'bootstrap_servers': [args.kafka_servers],
                    'group_id': 'merlin_workers'
                }
            },
            'queues': args.queues
        }
    
    # Start worker
    worker = KafkaWorker(config)
    try:
        worker.start()
    except KeyboardInterrupt:
        LOG.info("Worker stopped by user")
    except Exception as e:
        LOG.error(f"Worker failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()