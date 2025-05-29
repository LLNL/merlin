"""
Utility functions for the modules in the `backend/` package.
"""
from typing import Type, TypeVar

from merlin.db_scripts.data_models import LogicalWorkerModel, PhysicalWorkerModel, RunModel, StudyModel
from merlin.exceptions import RunNotFoundError, StudyNotFoundError, WorkerNotFoundError

T = TypeVar("T")


def get_not_found_error_class(model_class: Type[T]) -> Exception:
    """
    Get the appropriate not found error class based on the model type.

    Returns:
        The error class to use.
    """
    error_map = {
        LogicalWorkerModel: WorkerNotFoundError,
        PhysicalWorkerModel: WorkerNotFoundError,
        RunModel: RunNotFoundError,
        StudyModel: StudyNotFoundError,
    }
    return error_map.get(model_class, Exception)