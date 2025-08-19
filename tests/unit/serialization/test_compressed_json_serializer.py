##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Test compressed JSON serialization functionality.
"""

import pytest
import json
from unittest.mock import Mock, patch

from merlin.serialization.compressed_json_serializer import CompressedJsonSerializer
from merlin.factories.task_definition import UniversalTaskDefinition, TaskType, CoordinationPattern


class TestCompressedJsonSerializer:
    
    @pytest.fixture
    def serializer(self):
        return CompressedJsonSerializer()
    
    @pytest.fixture
    def simple_task_definition(self):
        """Create a simple task definition for testing."""
        return UniversalTaskDefinition(
            task_id='test_task_123',
            task_type=TaskType.MERLIN_STEP,
            queue_name='default',
            priority=1,
            coordination_pattern=CoordinationPattern.SIMPLE
        )
    
    @pytest.fixture
    def complex_task_definition(self):
        """Create a complex task definition with all fields."""
        from merlin.factories.task_definition import TaskDependency
        
        return UniversalTaskDefinition(
            task_id='complex_task_456',
            task_type=TaskType.MERLIN_STEP,
            script_reference='script_ref_789',
            config_reference='config_ref_101',
            queue_name='high_priority',
            priority=9,
            coordination_pattern=CoordinationPattern.CHORD_CALLBACK,
            dependencies=[
                TaskDependency(task_id='dep_1', dependency_type='success'),
                TaskDependency(task_id='dep_2', dependency_type='completion')
            ],
            metadata={'key1': 'value1', 'key2': 'value2'},
            step_name='complex_step',
            study_reference='study_999',
            sample_range={'start': 0, 'end': 1000}
        )
    
    def test_serialization_deserialization(self, serializer, simple_task_definition):
        """Test basic serialization and deserialization."""
        
        # Serialize
        serialized_data = serializer.serialize_task_definition(simple_task_definition)
        
        # Should return bytes
        assert isinstance(serialized_data, bytes)
        assert len(serialized_data) > 0
        
        # Deserialize
        deserialized_dict = serializer.deserialize_task_definition(serialized_data)
        
        # Should return dictionary with key fields
        assert isinstance(deserialized_dict, dict)
        assert deserialized_dict['tid'] == 'test_task_123'  # Shortened field name
        assert deserialized_dict['tt'] == 'MERLIN_STEP'  # Task type preserved
        assert deserialized_dict['qn'] == 'default'
        assert deserialized_dict['pr'] == 1
    
    def test_field_optimization(self, serializer, complex_task_definition):
        """Test that field names are optimized for compression."""
        
        serialized_data = serializer.serialize_task_definition(complex_task_definition)
        deserialized_dict = serializer.deserialize_task_definition(serialized_data)
        
        # Check that field names are shortened
        expected_fields = {
            'tid': 'complex_task_456',  # task_id
            'tt': 'MERLIN_STEP',        # task_type
            'sr': 'script_ref_789',     # script_reference
            'cr': 'config_ref_101',     # config_reference
            'qn': 'high_priority',      # queue_name
            'pr': 9,                    # priority
            'cp': 'CHORD_CALLBACK',     # coordination_pattern
            'sn': 'complex_step',       # step_name
            'str': 'study_999',         # study_reference
        }
        
        for short_key, expected_value in expected_fields.items():
            assert deserialized_dict[short_key] == expected_value
    
    def test_compression_effectiveness(self, serializer, complex_task_definition):
        """Test that compression reduces message size significantly."""
        
        # Get original size (uncompressed JSON)
        original_dict = complex_task_definition.__dict__.copy()
        
        # Convert enums to strings for JSON serialization
        original_dict['task_type'] = original_dict['task_type'].name
        original_dict['coordination_pattern'] = original_dict['coordination_pattern'].name
        
        original_json = json.dumps(original_dict).encode('utf-8')
        original_size = len(original_json)
        
        # Get compressed size
        compressed_data = serializer.serialize_task_definition(complex_task_definition)
        compressed_size = len(compressed_data)
        
        # Calculate compression ratio
        compression_ratio = serializer.calculate_compression_ratio(original_json)
        
        # Should achieve significant compression
        assert compressed_size < original_size
        assert compression_ratio > 0.3  # At least 30% reduction
        print(f"Compression: {original_size}B → {compressed_size}B ({compression_ratio:.1%} reduction)")
    
    def test_large_task_definition_compression(self, serializer):
        """Test compression on large task definitions."""
        
        # Create task with large metadata
        large_metadata = {f'key_{i}': f'very_long_value_that_should_compress_well_{i}' for i in range(100)}
        
        large_task = UniversalTaskDefinition(
            task_id='large_task_with_lots_of_metadata',
            task_type=TaskType.MERLIN_STEP,
            metadata=large_metadata,
            queue_name='large_queue_name_that_is_very_descriptive',
            priority=5
        )
        
        # Serialize
        serialized_data = serializer.serialize_task_definition(large_task)
        
        # Calculate original size estimate
        original_dict = large_task.__dict__.copy()
        original_dict['task_type'] = original_dict['task_type'].name
        original_dict['coordination_pattern'] = original_dict['coordination_pattern'].name
        original_size = len(json.dumps(original_dict).encode('utf-8'))
        
        compressed_size = len(serialized_data)
        compression_ratio = 1 - (compressed_size / original_size)
        
        # Large repetitive data should compress very well
        assert compression_ratio > 0.8  # At least 80% reduction
        print(f"Large task compression: {original_size}B → {compressed_size}B ({compression_ratio:.1%} reduction)")
    
    def test_different_compression_levels(self, serializer):
        """Test different compression levels."""
        
        task = UniversalTaskDefinition(
            task_id='test_compression_levels',
            task_type=TaskType.MERLIN_STEP,
            metadata={'large_data': 'x' * 1000}  # 1KB of repeated data
        )
        
        # Test different compression levels
        sizes = {}
        for level in [1, 6, 9]:  # Fast, default, best compression
            serializer.compression_level = level
            serialized = serializer.serialize_task_definition(task)
            sizes[level] = len(serialized)
        
        # Higher compression levels should produce smaller sizes
        assert sizes[9] <= sizes[6] <= sizes[1]
        print(f"Compression levels: 1={sizes[1]}B, 6={sizes[6]}B, 9={sizes[9]}B")
    
    def test_complex_coordination_patterns(self, serializer):
        """Test serialization of complex coordination patterns."""
        
        from merlin.factories.task_definition import TaskDependency
        
        # Create task with complex dependencies
        complex_task = UniversalTaskDefinition(
            task_id='complex_coordination_task',
            task_type=TaskType.SAMPLE_EXPANSION,
            coordination_pattern=CoordinationPattern.CHORD_CALLBACK,
            dependencies=[
                TaskDependency(task_id=f'dep_task_{i}', dependency_type='success')
                for i in range(10)
            ]
        )
        
        # Serialize and deserialize
        serialized = serializer.serialize_task_definition(complex_task)
        deserialized = serializer.deserialize_task_definition(serialized)
        
        # Validate complex fields were preserved
        assert deserialized['cp'] == 'CHORD_CALLBACK'
        assert len(deserialized['deps']) == 10
        assert all(dep['tid'].startswith('dep_task_') for dep in deserialized['deps'])
    
    @patch('merlin.serialization.compressed_json_serializer.gzip.compress')
    def test_compression_error_handling(self, mock_compress, serializer, simple_task_definition):
        """Test error handling during compression."""
        
        # Mock compression failure
        mock_compress.side_effect = Exception("Compression failed")
        
        # Should handle compression errors gracefully
        with pytest.raises(Exception):
            serializer.serialize_task_definition(simple_task_definition)
    
    def test_round_trip_integrity(self, serializer, complex_task_definition):
        """Test that serialization->deserialization preserves data integrity."""
        
        # Serialize
        serialized = serializer.serialize_task_definition(complex_task_definition)
        
        # Deserialize
        deserialized = serializer.deserialize_task_definition(serialized)
        
        # Reconstruct task definition from deserialized data
        # This would normally be done by the task factory
        reconstructed_id = deserialized['tid']
        reconstructed_type = deserialized['tt']
        
        assert reconstructed_id == complex_task_definition.task_id
        assert reconstructed_type == complex_task_definition.task_type.name