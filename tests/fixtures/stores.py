"""
Fixtures related to database stores.

These can be used by any kind of store (e.g. Redis, SQLite).
"""

import pytest
from pytest_mock import MockerFixture
from redis import Redis
from unittest.mock import MagicMock

from merlin.db_scripts.data_models import LogicalWorkerModel, PhysicalWorkerModel, RunModel, StudyModel
from tests.fixture_types import FixtureTuple


@pytest.fixture
def mock_redis(mocker):
    """Create a mock Redis client."""
    redis_mock = mocker.MagicMock(spec=Redis)
    return redis_mock


@pytest.fixture
def test_models():
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

    return {"study": study, "run": run, "logical_worker": logical_worker, "physical_worker": physical_worker}


@pytest.fixture(scope="session")
def create_redis_hash_data():
    """ """

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


@pytest.fixture
def mock_sqlite_connection(mocker: MockerFixture) -> FixtureTuple[MagicMock]:
    """
    Create a mocked SQLiteConnection context manager.

    Args:
        mocker: PyTest mocker fixture.

    Returns:
        A tuple of (mock_connection, mock_cursor) for easy access in tests.
    """
    # Create mock cursor
    mock_cursor = mocker.MagicMock()
    
    # Create mock connection
    mock_conn = mocker.MagicMock()
    mock_conn.execute.return_value = mock_cursor
    
    # Create mock context manager
    mock_context_manager = mocker.MagicMock()
    mock_context_manager.__enter__.return_value = mock_conn
    mock_context_manager.__exit__.return_value = None
    
    # Mock the SQLiteConnection class
    mock_sqlite_connection = mocker.patch('merlin.backends.sqlite.sqlite_store_base.SQLiteConnection')
    mock_sqlite_connection.return_value = mock_context_manager
    
    return mock_conn, mock_cursor
