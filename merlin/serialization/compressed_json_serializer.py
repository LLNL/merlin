##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Compressed JSON serialization for compact message format.
"""

import json
import gzip
import base64
from typing import Dict, Any, Optional
from dataclasses import asdict

class CompressedJsonSerializer:
    """Serialize task definitions using gzip-compressed JSON with field optimization."""
    
    def __init__(self, compression_level: int = 6):
        self.compression_level = compression_level
    
    def serialize_task_definition(self, task_def: 'UniversalTaskDefinition') -> bytes:
        """Serialize task definition to compact binary format."""
        
        # Convert to dictionary
        task_dict = task_def.to_dict()
        
        # Optimize dictionary for compression
        optimized_dict = self._optimize_for_compression(task_dict)
        
        # Serialize to JSON
        json_data = json.dumps(optimized_dict, separators=(',', ':'))
        
        # Compress
        compressed_data = gzip.compress(
            json_data.encode('utf-8'), 
            compresslevel=self.compression_level
        )
        
        return compressed_data
    
    def deserialize_task_definition(self, data: bytes) -> Dict[str, Any]:
        """Deserialize task definition from binary format."""
        
        # Decompress
        json_data = gzip.decompress(data).decode('utf-8')
        
        # Parse JSON
        task_dict = json.loads(json_data)
        
        # Restore optimized fields
        restored_dict = self._restore_from_compression(task_dict)
        
        return restored_dict
    
    def _optimize_for_compression(self, task_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Optimize dictionary structure for better compression."""
        
        # Use shorter field names
        field_mapping = {
            'task_id': 'tid',
            'task_type': 'tt',
            'script_reference': 'sr',
            'config_reference': 'cr',
            'workspace_reference': 'wr',
            'input_data_references': 'idr',
            'output_data_references': 'odr',
            'coordination_pattern': 'cp',
            'dependencies': 'deps',
            'group_id': 'gid',
            'callback_task': 'cb',
            'queue_name': 'qn',
            'priority': 'pr',
            'retry_limit': 'rl',
            'timeout_seconds': 'ts',
            'created_timestamp': 'ct',
            'metadata': 'md'
        }
        
        optimized = {}
        for key, value in task_dict.items():
            new_key = field_mapping.get(key, key)
            optimized[new_key] = value
        
        return optimized
    
    def _restore_from_compression(self, optimized_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Restore original field names from optimized dictionary."""
        
        # Reverse field mapping
        reverse_mapping = {
            'tid': 'task_id',
            'tt': 'task_type',
            'sr': 'script_reference',
            'cr': 'config_reference',
            'wr': 'workspace_reference',
            'idr': 'input_data_references',
            'odr': 'output_data_references',
            'cp': 'coordination_pattern',
            'deps': 'dependencies',
            'gid': 'group_id',
            'cb': 'callback_task',
            'qn': 'queue_name',
            'pr': 'priority',
            'rl': 'retry_limit',
            'ts': 'timeout_seconds',
            'ct': 'created_timestamp',
            'md': 'metadata'
        }
        
        restored = {}
        for key, value in optimized_dict.items():
            original_key = reverse_mapping.get(key, key)
            restored[original_key] = value
        
        return restored
    
    def calculate_compression_ratio(self, original_data: Dict[str, Any]) -> float:
        """Calculate compression ratio for given data."""
        
        # Original size
        original_json = json.dumps(original_data)
        original_size = len(original_json.encode('utf-8'))
        
        # Compressed size (simulate)
        optimized = self._optimize_for_compression(original_data)
        compressed_json = json.dumps(optimized, separators=(',', ':'))
        compressed_bytes = gzip.compress(compressed_json.encode('utf-8'))
        compressed_size = len(compressed_bytes)
        
        # Return compression ratio as percentage
        return ((original_size - compressed_size) / original_size) * 100.0