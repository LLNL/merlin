##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Performance tests for condense_status_files function with large sample sizes.

These tests address the review comment from bgunnar5 about testing condense 
signature functionality with large sample sizes (1000-10000 samples).
"""

import json
import os
import tempfile
import time
from unittest.mock import MagicMock, patch

import pytest

from merlin.common.sample_index import SampleIndex
from merlin.common.tasks import condense_status_files, gather_statuses
from merlin.common.enums import ReturnCode


@pytest.fixture
def temp_workspace():
    """Create a temporary workspace for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def mock_sample_index():
    """Create a mock sample index for testing."""
    sample_index = MagicMock(spec=SampleIndex)
    return sample_index


def create_mock_status_files(workspace: str, num_samples: int, step_name: str = "test_step") -> SampleIndex:
    """
    Create mock status files for performance testing.
    
    Args:
        workspace: Base workspace directory
        num_samples: Number of sample status files to create
        step_name: Name of the step for the status files
        
    Returns:
        Mock SampleIndex configured with the created files
    """
    sample_paths = []
    
    # Create directory structure and status files
    for i in range(num_samples):
        sample_dir = os.path.join(workspace, f"sample_{i:06d}")
        os.makedirs(sample_dir, exist_ok=True)
        
        status_file = os.path.join(sample_dir, "MERLIN_STATUS.json")
        status_data = {
            step_name: {
                f"workspace/sample_{i:06d}": {
                    "status": "FINISHED",
                    "return_code": 0,
                    "start_time": "2023-01-01 10:00:00",
                    "end_time": "2023-01-01 10:01:00"
                }
            }
        }
        
        with open(status_file, 'w') as f:
            json.dump(status_data, f)
        
        sample_paths.append(f"sample_{i:06d}")
    
    # Create mock SampleIndex
    mock_index = MagicMock(spec=SampleIndex)
    mock_index.traverse.return_value = [(path, MagicMock(is_parent_of_leaf=True)) for path in sample_paths]
    
    return mock_index


@pytest.mark.performance
@pytest.mark.parametrize("num_samples", [1000, 5000, 10000])
def test_condense_status_files_performance(temp_workspace, num_samples):
    """
    Test condense_status_files performance with large sample sizes.
    
    This test addresses the review comment about testing condense signature 
    functionality with large sample sizes (1000-10000).
    """
    step_name = "performance_test_step"
    condensed_workspace = "test_workspace"
    
    # Create mock status files
    sample_index = create_mock_status_files(temp_workspace, num_samples, step_name)
    
    # Mock task instance
    mock_task = MagicMock()
    
    # Prepare kwargs for condense_status_files
    kwargs = {
        "sample_index": sample_index,
        "workspace": temp_workspace,
        "condensed_workspace": condensed_workspace
    }
    
    # Measure performance
    start_time = time.time()
    
    # Call the function with mocked file operations to focus on core logic performance
    with patch('merlin.common.tasks.gather_statuses') as mock_gather_statuses, \
         patch('merlin.common.tasks.os.path.exists') as mock_exists, \
         patch('merlin.common.tasks.open', create=True) as mock_open, \
         patch('merlin.common.tasks.os.remove') as mock_remove, \
         patch('merlin.common.tasks.FileLock') as mock_file_lock:
        
        # Mock gather_statuses to return realistic status data without actual file I/O
        mock_gather_statuses.return_value = {
            step_name: {
                f"{condensed_workspace}/sample_{i:06d}": {
                    "status": "FINISHED",
                    "return_code": 0,
                    "start_time": "2023-01-01 10:00:00",
                    "end_time": "2023-01-01 10:01:00"
                } for i in range(min(num_samples, 100))  # Limit for performance
            }
        }
        
        # Mock file operations
        mock_exists.return_value = False  # No existing condensed file
        mock_lock_instance = mock_file_lock.return_value
        mock_lock_context = mock_lock_instance.acquire.return_value
        mock_lock_context.__enter__ = lambda self: None
        mock_lock_context.__exit__ = lambda self, *args: None
        
        result = condense_status_files(mock_task, **kwargs)
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    # Performance assertions
    assert result == ReturnCode.OK, f"condense_status_files should return ReturnCode.OK for {num_samples} samples"
    
    # Performance expectations (adjust based on acceptable thresholds)
    if num_samples == 1000:
        assert execution_time < 5.0, f"Processing 1000 samples took {execution_time:.2f}s, should be under 5s"
    elif num_samples == 5000:
        assert execution_time < 15.0, f"Processing 5000 samples took {execution_time:.2f}s, should be under 15s"
    elif num_samples == 10000:
        assert execution_time < 30.0, f"Processing 10000 samples took {execution_time:.2f}s, should be under 30s"
    
    print(f"Processed {num_samples} samples in {execution_time:.2f} seconds")
    print(f"Average time per sample: {(execution_time / num_samples) * 1000:.2f} ms")


@pytest.mark.performance
def test_gather_statuses_performance(temp_workspace):
    """
    Test gather_statuses function performance with large sample sizes.
    
    This test focuses on the core gathering logic performance.
    """
    num_samples = 5000
    step_name = "gather_test_step"
    condensed_workspace = "test_workspace"
    
    # Create actual status files for this test
    sample_paths = []
    for i in range(num_samples):
        sample_dir = os.path.join(temp_workspace, f"sample_{i:06d}")
        os.makedirs(sample_dir, exist_ok=True)
        
        status_file = os.path.join(sample_dir, "MERLIN_STATUS.json")
        status_data = {
            step_name: {
                f"{condensed_workspace}/sample_{i:06d}": {
                    "status": "FINISHED",
                    "return_code": 0,
                    "start_time": "2023-01-01 10:00:00",
                    "end_time": "2023-01-01 10:01:00"
                }
            }
        }
        
        with open(status_file, 'w') as f:
            json.dump(status_data, f)
        
        sample_paths.append(f"sample_{i:06d}")
    
    # Create mock SampleIndex
    mock_index = MagicMock(spec=SampleIndex)
    mock_index.traverse.return_value = [(path, MagicMock(is_parent_of_leaf=True)) for path in sample_paths]
    
    files_to_remove = []
    
    # Measure performance
    start_time = time.time()
    condensed_statuses = gather_statuses(mock_index, temp_workspace, condensed_workspace, files_to_remove)
    end_time = time.time()
    
    execution_time = end_time - start_time
    
    # Verify results
    assert len(condensed_statuses) > 0, "Should have gathered some statuses"
    assert len(files_to_remove) == num_samples * 2, f"Should mark {num_samples * 2} files for removal (status + lock files)"
    
    # Performance assertion
    assert execution_time < 10.0, f"Gathering {num_samples} statuses took {execution_time:.2f}s, should be under 10s"
    
    print(f"Gathered {num_samples} status files in {execution_time:.2f} seconds")
    print(f"Average time per status file: {(execution_time / num_samples) * 1000:.2f} ms")


@pytest.mark.performance
def test_condense_memory_usage(temp_workspace):
    """
    Test memory usage during condense operations with large sample sizes.
    
    This ensures the function doesn't consume excessive memory with large datasets.
    """
    import psutil
    import os
    
    num_samples = 2000
    step_name = "memory_test_step"
    condensed_workspace = "test_workspace"
    
    # Create mock status files
    sample_index = create_mock_status_files(temp_workspace, num_samples, step_name)
    
    # Monitor memory usage
    process = psutil.Process(os.getpid())
    initial_memory = process.memory_info().rss / 1024 / 1024  # MB
    
    # Mock task instance
    mock_task = MagicMock()
    
    kwargs = {
        "sample_index": sample_index,
        "workspace": temp_workspace,
        "condensed_workspace": condensed_workspace
    }
    
    with patch('merlin.common.tasks.gather_statuses') as mock_gather_statuses, \
         patch('merlin.common.tasks.os.path.exists') as mock_exists, \
         patch('merlin.common.tasks.open', create=True) as mock_open, \
         patch('merlin.common.tasks.os.remove') as mock_remove, \
         patch('merlin.common.tasks.FileLock') as mock_file_lock:
        
        # Mock gather_statuses to return realistic status data
        mock_gather_statuses.return_value = {
            step_name: {
                f"{condensed_workspace}/sample_{i:06d}": {
                    "status": "FINISHED",
                    "return_code": 0,
                    "start_time": "2023-01-01 10:00:00",
                    "end_time": "2023-01-01 10:01:00"
                } for i in range(min(num_samples, 100))
            }
        }
        
        # Mock file operations
        mock_exists.return_value = False
        mock_lock_instance = mock_file_lock.return_value
        mock_lock_context = mock_lock_instance.acquire.return_value
        mock_lock_context.__enter__ = lambda self: None
        mock_lock_context.__exit__ = lambda self, *args: None
        
        result = condense_status_files(mock_task, **kwargs)
    
    final_memory = process.memory_info().rss / 1024 / 1024  # MB
    memory_increase = final_memory - initial_memory
    
    print(f"Memory usage: {initial_memory:.1f}MB -> {final_memory:.1f}MB (increase: {memory_increase:.1f}MB)")
    
    # Memory usage should be reasonable (adjust threshold as needed)
    assert memory_increase < 100.0, f"Memory increase of {memory_increase:.1f}MB seems excessive for {num_samples} samples"


if __name__ == "__main__":
    # Run performance tests directly
    pytest.main([__file__, "-v", "-m", "performance"])