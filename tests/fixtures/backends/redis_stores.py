"""
Fixtures for the `redis_stores.py` module.
"""

import pytest

from redis import Redis

from merlin.db_scripts.data_models import (
    LogicalWorkerModel, 
    PhysicalWorkerModel, 
    RunModel, 
    StudyModel
)


@pytest.fixture
def redis_stores_mock_redis(mocker):
    """Create a mock Redis client."""
    redis_mock = mocker.MagicMock(spec=Redis)
    return redis_mock


@pytest.fixture
def redis_stores_test_models():
    """Create test model instances for tests."""
    
    # Sample study model
    study = StudyModel(
        id="study1",
        name="Test Study",
        runs=["run1"],
    )
    
    # Sample run model
    run = RunModel(
        id="run1",
        study_id="study1",
        workers=["lw1"],
    )
    
    # Sample logical worker model
    logical_worker = LogicalWorkerModel(
        id="lw1",
        name="logical_worker",
        queues=["queue1", "queue2"],
        physical_workers=["pw1"],
        runs=["run1"],
    )
    
    # Sample physical worker model
    physical_worker = PhysicalWorkerModel(
        id="pw1",
        name="Worker 1",
        logical_worker_id="lw1",
    )
    
    return {
        "study": study,
        "run": run,
        "logical_worker": logical_worker,
        "physical_worker": physical_worker
    }

@pytest.fixture(scope="session")
def redis_stores_create_redis_hash_data():
    """
    """
    def _create_redis_hash_data(obj):
        """Create a dict that simulates Redis hash data for a model."""
        # Simple conversion - in real code, this would be more complex
        result = {}
        for key, value in obj.__dict__.items():
            if isinstance(value, (str, int, float, bool, type(None))):
                result[key] = str(value)
            elif isinstance(value, dict):
                result[key] = str(value)  # In real code, this would be JSON
            elif isinstance(value, list):
                result[key] = str(value)  # In real code, this would be JSON
        return result
    return _create_redis_hash_data
