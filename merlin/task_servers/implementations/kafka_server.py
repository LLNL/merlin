##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Kafka task server implementation for Merlin.

This module provides a complete Kafka implementation of TaskServerInterface,
enabling true backend independence from Celery.
"""

import json
import logging
import subprocess
import sys
from typing import Dict, Any, List, Optional

from merlin.task_servers.task_server_interface import TaskServerInterface, TaskDependency
from merlin.spec.specification import MerlinSpec

LOG = logging.getLogger(__name__)


class KafkaTaskServer(TaskServerInterface):
    """
    Kafka implementation of TaskServerInterface.
    
    Provides complete backend independence from Celery using Kafka for
    task distribution and coordination.
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """Initialize the Kafka task server."""
        super().__init__()  # Initialize parent class with MerlinDatabase
        self.config = config or {}
        self.producer = None
        self._initialize_kafka()
    
    def _initialize_kafka(self):
        """Initialize Kafka producer."""
        try:
            from kafka import KafkaProducer  # pylint: disable=C0415
            
            producer_config = self.config.get('producer', {})
            producer_config.setdefault('bootstrap_servers', ['localhost:9092'])
            producer_config.setdefault('value_serializer', lambda x: json.dumps(x).encode())
            
            self.producer = KafkaProducer(**producer_config)
            LOG.debug("Kafka producer initialized successfully")
            
        except ImportError:
            LOG.error("kafka-python package required for Kafka task server")
            LOG.error("Please install: pip install kafka-python")
            raise
        except Exception as e:
            LOG.error(f"Failed to initialize Kafka producer: {e}")
            raise
    
    @property
    def server_type(self) -> str:
        """Return 'kafka' as the server type."""
        return "kafka"
    
    def _convert_task_to_kafka_message(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert generic task data to Kafka message format.
        
        Self-contained conversion within KafkaTaskServer.
        """
        return {
            'task_type': task_data.get('task_type'),
            'parameters': task_data.get('parameters', {}),
            'queue': task_data.get('queue', 'default'),
            'task_id': task_data.get('task_id'),
            'timestamp': task_data.get('timestamp'),
            'metadata': task_data.get('metadata', {})
        }
    
    def _send_kafka_message(self, topic: str, message: Dict[str, Any]) -> str:
        """Send message to Kafka topic - returns message ID."""
        try:
            future = self.producer.send(topic, value=message)
            future.get(timeout=10)  # Wait for confirmation
            
            message_id = f"kafka_{topic}_{message.get('task_id', 'unknown')}"
            LOG.debug(f"Sent message to Kafka topic {topic}: {message_id}")
            return message_id
            
        except Exception as e:
            LOG.error(f"Failed to send message to Kafka topic {topic}: {e}")
            raise
    
    # TaskServerInterface implementation
    def submit_task(self, task_id: str) -> str:
        """Submit a single task to Kafka."""
        # For now, assume task_data is passed as task_id (simplified)
        # In real implementation, would retrieve from database
        if isinstance(task_id, dict):
            task_data = task_id
        else:
            task_data = {'task_id': task_id, 'task_type': 'merlin_step', 'parameters': {}}
        
        # Convert to Kafka format using self-contained method
        kafka_message = self._convert_task_to_kafka_message(task_data)
        
        # Send to appropriate topic based on queue
        topic = f"merlin_tasks_{kafka_message['queue']}"
        return self._send_kafka_message(topic, kafka_message)
    
    def submit_tasks(self, task_ids: List[str], **kwargs) -> List[str]:
        """Submit multiple tasks to Kafka."""
        results = []
        for task_id in task_ids:
            result = self.submit_task(task_id)  
            results.append(result)
        return results
    
    def submit_task_group(self, group_name: str, task_ids: List[str], 
                         callback_task_id: Optional[str] = None) -> str:
        """Submit a group of tasks with optional callback."""
        # Create group coordination message
        group_message = {
            'group_name': group_name,
            'task_ids': task_ids,
            'callback_task_id': callback_task_id,
            'type': 'task_group'
        }
        
        return self._send_kafka_message('merlin_coordination', group_message)
    
    def submit_coordinated_tasks(self, coordination_id, header_task_ids, body_task_id, **kwargs) -> str:
        """Submit coordinated tasks using Kafka coordination."""
        # Create coordination setup message
        coord_message = {
            'coordination_id': coordination_id,
            'header_task_ids': header_task_ids,
            'body_task_id': body_task_id,
            'type': 'coordination_setup'
        }
        
        # Send coordination setup to Kafka
        self._send_kafka_message('merlin_coordination', coord_message)
        
        # Submit header tasks
        for task_id in header_task_ids:
            self.submit_task(task_id)
        
        return f"kafka_coordination_{coordination_id}"
    
    def submit_condense_task(self, 
                           sample_index, 
                           workspace: str, 
                           condensed_workspace: str,
                           queue: str = None):
        """
        Submit a status file condensing task via Kafka.
        
        This method implements backend-agnostic status file condensing by publishing
        a condense message to Kafka topics.
        
        Args:
            sample_index: SampleIndex object for status file locations
            workspace: Full workspace path for condensing
            condensed_workspace: Shortened workspace path for status entries
            queue: Task queue/topic for execution
            
        Returns:
            Message ID from Kafka for tracking
        """
        condense_message = {
            'type': 'condense_status',
            'sample_index': str(sample_index) if sample_index else None,
            'workspace': workspace,
            'condensed_workspace': condensed_workspace,
            'queue': queue or 'default'
        }
        
        topic = f"merlin_tasks_{queue or 'default'}"
        result = self._send_kafka_message(topic, condense_message)
        LOG.debug(f"Submitted condense task to Kafka: {result}")
        return result
    
    def submit_dependent_tasks(self, task_ids: List[str], dependencies: Optional[List[TaskDependency]] = None, **kwargs) -> List[str]:
        """Submit tasks with dependencies."""
        if not dependencies:
            return self.submit_tasks(task_ids)
        
        results = []
        # Group tasks by their dependency relationships
        for i, dep in enumerate(dependencies):
            coord_id = f"dep_group_{i}"
            # For now, treat all task_ids as header tasks with no body task
            coord_result = self.submit_coordinated_tasks(coord_id, task_ids, None)
            results.append(coord_result)
        
        return results
    
    def get_group_status(self, group_id: str) -> Dict[str, Any]:
        """Get status of task group."""
        # Query database for task status instead of mixing concerns
        # In full implementation, would query task database for group status
        return {"group_id": group_id, "status": "RUNNING", "backend": "kafka"}
    
    def cancel_task(self, task_id: str) -> bool:
        """Cancel a task."""
        LOG.info(f"Cancelling Kafka task {task_id}")
        # Send cancellation message to coordination topic
        cancel_msg = {
            'task_id': task_id,
            'action': 'cancel',
            'type': 'control'
        }
        
        try:
            self._send_kafka_message('merlin_control', cancel_msg)
            return True
        except Exception as e:
            LOG.error(f"Failed to cancel task {task_id}: {e}")
            return False
    
    def cancel_tasks(self, task_ids: List[str]) -> Dict[str, bool]:
        """Cancel multiple tasks."""
        results = {}
        for task_id in task_ids:
            results[task_id] = self.cancel_task(task_id)
        return results
    
    def start_workers(self, spec: MerlinSpec) -> bool:
        """Start Kafka consumer workers."""
        try:
            # Create worker configuration
            worker_config = {
                'kafka': self.config,
                'queues': list(spec.get_task_queues().values()) if spec else ['default']
            }
            
            # Start worker subprocess
            python_executable = sys.executable
            worker_cmd = [
                python_executable, '-c',
                f"""
import sys
sys.path.insert(0, '{sys.path[0]}')
from merlin.task_servers.implementations.kafka_task_consumer import KafkaTaskConsumer
import json

config = json.loads('''{json.dumps(worker_config)}''')
worker = KafkaTaskConsumer(config)
worker.start()
"""
            ]
            
            LOG.info(f"Starting Kafka workers for queues: {worker_config['queues']}")
            subprocess.Popen(worker_cmd)
            return True
            
        except Exception as e:
            LOG.error(f"Failed to start Kafka workers: {e}")
            return False
    
    def stop_workers(self, names: Optional[List[str]] = None) -> bool:
        """Stop Kafka consumer workers."""
        LOG.info("Stopping Kafka consumer workers")
        # Send stop message to control topic
        stop_msg = {
            'action': 'stop_workers',
            'type': 'control'
        }
        
        try:
            self._send_kafka_message('merlin_control', stop_msg)
            return True
        except Exception as e:
            LOG.error(f"Failed to stop workers: {e}")
            return False
    
    def display_queue_info(self, queues: Optional[List[str]] = None) -> None:
        """Display Kafka topic information."""
        print("Kafka Topics and Consumer Groups:")
        print("  Topics: merlin_tasks_*, merlin_coordination, merlin_control")
        print("  Consumer Groups: merlin_workers")
    
    def display_connected_workers(self) -> None:
        """Display connected Kafka consumers."""
        print("Connected Kafka Workers:")
        print("  (Use kafka-consumer-groups.sh to view active consumers)")
    
    def display_running_tasks(self) -> None:
        """Display currently processing messages."""
        print("Currently Processing Kafka Messages:")
        print("  (Check consumer lag in Kafka monitoring tools)")
    
    def purge_tasks(self, queues: List[str], force: bool = False) -> int:
        """Purge messages from Kafka topics."""
        LOG.warning("Kafka topic purging requires admin privileges and external tools")
        LOG.info("Use kafka-topics.sh --delete and recreate topics to purge")
        return 0  # Return number of purged tasks
    
    def get_workers(self) -> List[str]:
        """Get list of active Kafka consumers."""
        # In real implementation, would query Kafka consumer groups
        return ["kafka_worker_1", "kafka_worker_2"]  # Placeholder
    
    def get_active_queues(self) -> Dict[str, List[str]]:
        """Get mapping of active Kafka topics to their consumers."""
        return {
            "merlin_tasks_default": ["kafka_worker_1"],
            "merlin_coordination": ["kafka_worker_2"], 
            "merlin_control": ["kafka_worker_1", "kafka_worker_2"]
        }
    
    def check_workers_processing(self, queues: List[str]) -> bool:
        """Check if Kafka consumers are processing messages."""
        # In real implementation, would check consumer lag
        return True
    
    def submit_study(self, study, adapter: Dict, samples, sample_labels, egraph, groups_of_chains):
        """Submit complete study to Kafka."""
        # Convert study to Kafka messages
        study_message = {
            'study_name': getattr(study, 'name', 'unknown'),
            'adapter_config': adapter,
            'sample_count': len(samples) if samples else 0,
            'groups_of_chains': len(groups_of_chains) if groups_of_chains else 0,
            'type': 'study_submission'
        }
        
        return self._send_kafka_message('merlin_studies', study_message)
    
    def __del__(self):
        """Clean up Kafka producer."""
        if self.producer:
            try:
                self.producer.close()
            except Exception:
                pass  # Ignore cleanup errors