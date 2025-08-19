##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Test sample expansion optimization functionality.
"""

import pytest
import tempfile
import shutil
from unittest.mock import Mock, patch

from merlin.optimization.sample_expansion import SampleExpansionOptimizer, SampleRange


class TestSampleExpansionOptimizer:
    
    @pytest.fixture
    def temp_dir(self):
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def optimizer(self, temp_dir):
        return SampleExpansionOptimizer(base_dir=temp_dir)
    
    def test_sample_range_creation(self, optimizer):
        """Test basic sample range creation."""
        
        ranges = optimizer.create_sample_ranges(total_samples=100, max_batch_size=25)
        
        assert len(ranges) == 4  # 100 samples / 25 per batch
        
        # Validate ranges cover all samples
        total_covered = sum(r.end - r.start for r in ranges)
        assert total_covered == 100
        
        # Validate ranges are contiguous
        for i in range(1, len(ranges)):
            assert ranges[i].start == ranges[i-1].end
    
    def test_sample_range_properties(self, optimizer):
        """Test SampleRange dataclass properties."""
        
        sample_range = SampleRange(start=0, end=50, batch_id='batch_1')
        
        assert sample_range.start == 0
        assert sample_range.end == 50
        assert sample_range.batch_id == 'batch_1'
        assert sample_range.size == 50
    
    def test_optimal_batch_size_calculation(self, optimizer):
        """Test intelligent batch size calculation."""
        
        # Small dataset: should use default batch size
        ranges_small = optimizer.create_sample_ranges(100, max_batch_size=50)
        assert len(ranges_small) == 2
        
        # Medium dataset: should use square root scaling
        ranges_medium = optimizer.create_sample_ranges(10000, max_batch_size=200)
        expected_batches = int((10000 ** 0.5))  # Square root
        assert len(ranges_medium) >= expected_batches * 0.8  # Variance
        assert len(ranges_medium) <= expected_batches * 1.2
        
        # Large dataset: should use efficient batching
        ranges_large = optimizer.create_sample_ranges(50000, max_batch_size=500)
        assert len(ranges_large) >= 100  # Creates reasonable number of batches
        assert all(r.size <= 500 for r in ranges_large)  # MAX batch size
    
    def test_memory_usage_estimation(self, optimizer):
        """Test memory usage estimation for batch sizing."""
        
        # Test memory estimation
        estimated_memory = optimizer.estimate_memory_usage(1000, avg_sample_size_kb=1)
        assert estimated_memory == 1000  # 1000 samples * 1KB each
        
        # Test batch size recommendation based on memory
        recommended_batch = optimizer.recommend_batch_size(
            total_samples=10000, 
            available_memory_mb=100,
            avg_sample_size_kb=1
        )
        
        # Should recommend batch size that fits in available memory
        estimated_batch_memory = recommended_batch * 1 / 1024  # Convert to MB
        assert estimated_batch_memory <= 100
    
    def test_sample_storage_and_retrieval(self, optimizer):
        """Test reference-based sample storage and retrieval."""
        
        # Create test sample data
        sample_data = [
            {'param1': i, 'param2': f'value_{i}', 'param3': i * 2}
            for i in range(100)
        ]
        
        # Store samples and get reference
        reference = optimizer.store_samples_reference('test_study_123', sample_data)
        assert reference.startswith('test_study_123_')
        assert len(reference) > len('test_study_123_')  # Should have timestamp/hash
        
        # Create sample range
        sample_range = SampleRange(start=10, end=20, batch_id='batch_1')
        
        # Retrieve sample range
        retrieved_samples = optimizer.load_sample_range(reference, sample_range)
        
        # Validate retrieved samples
        assert len(retrieved_samples) == 10  # 20 - 10
        assert retrieved_samples[0] == sample_data[10]  # First sample in range
        assert retrieved_samples[-1] == sample_data[19]  # Last sample in range
    
    def test_large_dataset_handling(self, optimizer):
        """Test handling of very large datasets."""
        
        # Test with large dataset (50,000 samples)
        ranges = optimizer.create_sample_ranges(50000, max_batch_size=500)
        
        # Should create reasonable number of batches
        assert len(ranges) >= 100  # At least 100 batches for 50K samples
        assert len(ranges) <= 500  # But not too many batches
        
        # All ranges should respect max batch size
        for r in ranges:
            assert r.size <= 500
        
        # Total coverage should equal dataset size
        total_samples = sum(r.size for r in ranges)
        assert total_samples == 50000
    
    def test_sample_expansion_task_creation(self, optimizer):
        """Test integration with task factory for sample expansion."""
        
        from merlin.factories.universal_task_factory import UniversalTaskFactory
        
        # Create factory
        factory = UniversalTaskFactory(optimizer.base_dir)
        
        # Create sample expansion task
        sample_range = SampleRange(start=0, end=100, batch_id='batch_1')
        task_def = factory.create_sample_expansion_task(
            study_id='test_study',
            step_name='expand_samples',
            sample_range=sample_range
        )
        
        # Validate task definition
        assert task_def.task_type.name == 'SAMPLE_EXPANSION'
        assert task_def.study_reference == 'test_study'
        assert task_def.step_name == 'expand_samples'
        assert task_def.sample_range == sample_range
    
    @patch('merlin.optimization.sample_expansion.time.time')
    def test_reference_generation(self, mock_time, optimizer):
        """Test that sample references are generated correctly."""
        
        # Mock timestamp for consistent testing
        mock_time.return_value = 1234567890
        
        sample_data = [{'test': 'data'}]
        reference = optimizer.store_samples_reference('study_abc', sample_data)
        
        # Study ID and timestamp
        assert 'study_abc' in reference
        assert '1234567890' in reference or reference.endswith(str(hash(str(sample_data)) % 10000).zfill(4))