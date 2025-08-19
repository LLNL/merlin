##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Optimized sample expansion using range-based references.
"""

import json
import math
from typing import List, Dict, Any, Tuple, Iterator
from pathlib import Path
from dataclasses import dataclass


@dataclass
class SampleRange:
    """Represents a range of samples for batch processing."""
    start_index: int
    end_index: int
    batch_id: str
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
    
    @property
    def size(self) -> int:
        """Number of samples in this range."""
        return self.end_index - self.start_index
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'start_index': self.start_index,
            'end_index': self.end_index,
            'batch_id': self.batch_id,
            'metadata': self.metadata
        }


class SampleExpansionOptimizer:
    """Optimize sample expansion for large datasets."""
    
    def __init__(self, shared_storage_path: str = "/shared/storage"):
        self.shared_storage_path = Path(shared_storage_path)
        self.samples_dir = self.shared_storage_path / "samples"
        self.samples_dir.mkdir(parents=True, exist_ok=True)
    
    def create_sample_ranges(self,
                           total_samples: int,
                           max_batch_size: int = 100,
                           min_batch_size: int = 10) -> List[SampleRange]:
        """Create optimal sample ranges for batch processing."""
        
        if total_samples <= min_batch_size:
            # Small sample set - single batch
            return [SampleRange(0, total_samples, "batch_0")]
        
        # Calculate optimal batch size
        optimal_batch_size = self._calculate_optimal_batch_size(
            total_samples, max_batch_size, min_batch_size
        )
        
        # Create ranges
        ranges = []
        current_start = 0
        batch_index = 0
        
        while current_start < total_samples:
            current_end = min(current_start + optimal_batch_size, total_samples)
            
            sample_range = SampleRange(
                start_index=current_start,
                end_index=current_end,
                batch_id=f"batch_{batch_index}",
                metadata={
                    'total_samples': total_samples,
                    'batch_size': current_end - current_start,
                    'batch_index': batch_index
                }
            )
            
            ranges.append(sample_range)
            
            current_start = current_end
            batch_index += 1
        
        return ranges
    
    def _calculate_optimal_batch_size(self,
                                    total_samples: int,
                                    max_batch_size: int,
                                    min_batch_size: int) -> int:
        """Calculate optimal batch size based on total samples."""
        
        # Use square root scaling for large datasets
        if total_samples <= 100:
            return min(total_samples, max_batch_size)
        elif total_samples <= 1000:
            return min(50, max_batch_size)
        elif total_samples <= 10000:
            return min(100, max_batch_size)
        else:
            # For very large datasets, use square root scaling
            sqrt_samples = int(math.sqrt(total_samples))
            return min(max(sqrt_samples, min_batch_size), max_batch_size)
    
    def store_samples_reference(self,
                              study_id: str,
                              samples_data: List[Dict[str, Any]]) -> str:
        """Store samples data and return reference path."""
        
        samples_file = self.samples_dir / f"{study_id}_samples.json"
        
        # Store samples data
        with open(samples_file, 'w') as f:
            json.dump(samples_data, f, indent=2)
        
        return str(samples_file.relative_to(self.shared_storage_path))
    
    def create_sample_expansion_tasks(self,
                                    study_id: str,
                                    step_name: str,
                                    samples_data: List[Dict[str, Any]],
                                    max_batch_size: int = 100) -> List[Dict[str, Any]]:
        """Create optimized sample expansion tasks."""
        
        # Store samples data
        samples_reference = self.store_samples_reference(study_id, samples_data)
        
        # Create sample ranges
        sample_ranges = self.create_sample_ranges(
            len(samples_data), max_batch_size
        )
        
        # Create task definitions for each range
        tasks = []
        for sample_range in sample_ranges:
            task_data = {
                'task_id': f"{study_id}_{step_name}_{sample_range.batch_id}",
                'study_id': study_id,
                'step_name': step_name,
                'sample_range': [sample_range.start_index, sample_range.end_index],
                'samples_reference': samples_reference,
                'batch_metadata': sample_range.metadata
            }
            tasks.append(task_data)
        
        return tasks
    
    def estimate_memory_usage(self,
                            samples_data: List[Dict[str, Any]],
                            batch_size: int) -> Dict[str, float]:
        """Estimate memory usage for different batch sizes."""
        
        # Estimate single sample size
        sample_size_bytes = len(json.dumps(samples_data[0]).encode('utf-8')) if samples_data else 0
        
        # Calculate memory estimates
        single_sample_mb = sample_size_bytes / (1024 * 1024)
        batch_memory_mb = single_sample_mb * batch_size
        total_memory_mb = single_sample_mb * len(samples_data)
        
        return {
            'single_sample_mb': single_sample_mb,
            'batch_memory_mb': batch_memory_mb,
            'total_memory_mb': total_memory_mb,
            'recommended_batch_size': min(batch_size, int(100 / single_sample_mb)) if single_sample_mb > 0 else batch_size
        }
    
    def load_sample_range(self,
                         samples_reference: str,
                         sample_range: SampleRange) -> List[Dict[str, Any]]:
        """Load specific range of samples from reference."""
        
        samples_file = self.shared_storage_path / samples_reference
        
        with open(samples_file, 'r') as f:
            all_samples = json.load(f)
        
        return all_samples[sample_range.start_index:sample_range.end_index]