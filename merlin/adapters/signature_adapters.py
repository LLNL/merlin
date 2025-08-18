##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Backend-specific adapters for task execution.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from merlin.factories.task_definition import UniversalTaskDefinition


class SignatureAdapter(ABC):
    """Base class for backend-specific signature adapters."""
    
    @abstractmethod
    def create_signature(self, task_def: UniversalTaskDefinition) -> Any:
        """Create backend-specific signature from universal definition."""
        pass
    
    @abstractmethod
    def submit_task(self, signature: Any) -> str:
        """Submit task using backend-specific signature."""
        pass
    
    @abstractmethod
    def submit_group(self, signatures: List[Any]) -> str:
        """Submit group of tasks."""
        pass
    
    @abstractmethod
    def submit_chain(self, signatures: List[Any]) -> str:
        """Submit chain of tasks."""
        pass
    
    @abstractmethod
    def submit_chord(self, parallel_signatures: List[Any], callback_signature: Any) -> str:
        """Submit chord pattern."""
        pass


class CelerySignatureAdapter(SignatureAdapter):
    """Adapter for Celery backend."""
    
    def __init__(self, task_registry: Dict[str, Any]):
        self.task_registry = task_registry
    
    def create_signature(self, task_def: UniversalTaskDefinition) -> Any:
        """Create Celery signature from universal definition."""
        
        # Get Celery task function
        task_func = self._get_task_function(task_def.task_type.value)
        
        # Convert task definition to Celery signature
        if task_def.task_type.value == "merlin_step":
            return task_func.s(
                task_id=task_def.task_id,
                script_reference=task_def.script_reference,
                config_reference=task_def.config_reference,
                workspace_reference=task_def.workspace_reference
            ).set(
                queue=task_def.queue_name,
                priority=task_def.priority,
                retry=task_def.retry_limit,
                time_limit=task_def.timeout_seconds
            )
        else:
            # Handle other task types
            return task_func.s(
                task_definition=task_def.to_dict()
            ).set(
                queue=task_def.queue_name,
                priority=task_def.priority
            )
    
    def submit_task(self, signature: Any) -> str:
        """Submit single Celery task."""
        result = signature.apply_async()
        return result.id
    
    def submit_group(self, signatures: List[Any]) -> str:
        """Submit Celery group."""
        from celery import group
        job = group(signatures)
        result = job.apply_async()
        return result.id
    
    def submit_chain(self, signatures: List[Any]) -> str:
        """Submit Celery chain."""
        from celery import chain
        job = chain(signatures)
        result = job.apply_async()
        return result.id
    
    def submit_chord(self, parallel_signatures: List[Any], callback_signature: Any) -> str:
        """Submit Celery chord."""
        from celery import chord
        job = chord(parallel_signatures)(callback_signature)
        result = job.apply_async()
        return result.id
    
    def _get_task_function(self, task_type: str):
        """Get Celery task function by type."""
        return self.task_registry.get(task_type)


class KafkaSignatureAdapter(SignatureAdapter):
    """Adapter for Kafka backend."""
    
    def __init__(self, kafka_producer, topic_manager):
        self.producer = kafka_producer
        self.topic_manager = topic_manager
    
    def create_signature(self, task_def: UniversalTaskDefinition) -> Dict[str, Any]:
        """Create Kafka message from universal definition."""
        
        # Kafka "signature" is just the optimized message
        return {
            'task_definition': task_def.to_dict(),
            'topic': self.topic_manager.get_topic_for_queue(task_def.queue_name),
            'partition_key': task_def.group_id or task_def.task_id
        }
    
    def submit_task(self, signature: Dict[str, Any]) -> str:
        """Submit task to Kafka topic."""
        
        future = self.producer.send(
            signature['topic'],
            value=signature['task_definition'],
            key=signature['partition_key']
        )
        
        result = future.get(timeout=10)
        return f"{result.topic}:{result.partition}:{result.offset}"
    
    def submit_group(self, signatures: List[Dict[str, Any]]) -> str:
        """Submit group of tasks to Kafka."""
        
        # For groups, we need to coordinate across tasks
        group_id = signatures[0]['task_definition']['group_id']
        
        # Submit all tasks
        task_ids = []
        for sig in signatures:
            result_id = self.submit_task(sig)
            task_ids.append(result_id)
        
        # Create coordination message if there's a callback
        callback_tasks = [s for s in signatures 
                         if s['task_definition']['coordination_pattern'] == 'chord']
        
        if callback_tasks:
            # Submit callback task with dependencies
            for callback_sig in callback_tasks:
                self.submit_task(callback_sig)
        
        return group_id
    
    def submit_chain(self, signatures: List[Dict[str, Any]]) -> str:
        """Submit chain of tasks to Kafka."""
        
        # Submit all tasks - dependencies are embedded in task definitions
        chain_id = signatures[0]['task_definition']['group_id']
        
        for sig in signatures:
            self.submit_task(sig)
        
        return chain_id
    
    def submit_chord(self, parallel_signatures: List[Dict[str, Any]], 
                    callback_signature: Dict[str, Any]) -> str:
        """Submit chord pattern to Kafka."""
        
        # Combine parallel and callback signatures
        all_signatures = parallel_signatures + [callback_signature]
        return self.submit_group(all_signatures)