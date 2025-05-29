"""
Tests for the `backends/utils.py` module.
"""
import pytest

from merlin.backends.utils import get_not_found_error_class
from merlin.db_scripts.data_models import (
    BaseDataModel, LogicalWorkerModel, PhysicalWorkerModel, RunModel, StudyModel
)
from merlin.exceptions import RunNotFoundError, StudyNotFoundError, WorkerNotFoundError


@pytest.mark.parametrize(
    "db_model, expected_error",
    [
        (StudyModel, StudyNotFoundError),
        (RunModel, RunNotFoundError),
        (LogicalWorkerModel, WorkerNotFoundError),
        (PhysicalWorkerModel, WorkerNotFoundError),
    ],
)
def test_get_not_found_error_class_valid_model(db_model: BaseDataModel, expected_error: Exception):
    """
    Test the `get_not_found_error_class` function returns the correct error class.

    Args:
        db_model: The type of model to get the error for.
        error_type: The error that's supposed to be raised.
    """
    assert get_not_found_error_class(db_model) is expected_error


def test_get_not_found_error_class_unknown_model():
    """
    Test the `get_not_found_error_class` function returns the broad Exception for
    an unknown data model.
    """
    class UnknownModel:
        pass
    assert get_not_found_error_class(UnknownModel) is Exception
