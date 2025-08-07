##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the merlin/backends/filter_support_mixin.py module.
"""

import pytest
from pytest_mock import MockerFixture

from merlin.backends.filter_support_mixin import FilterSupportMixin
from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.data_models import BaseDataModel, LogicalWorkerModel, PhysicalWorkerModel, RunModel, StudyModel


@pytest.fixture()
def filter_support_backend_test_instance(mocker: MockerFixture) -> ResultsBackend:
    """
    Provides a concrete test instance of the `ResultsBackend` class with `FilterSupportMixin`
    for use in tests.

    This fixture dynamically creates a subclass of `ResultsBackend` called `Test` and
    overrides its abstract methods, allowing it to be instantiated. The abstract methods
    are bypassed by setting the `__abstractmethods__` attribute to an empty frozenset.
    The resulting instance is initialized with the provided `results_backend_test_name`.

    Args:
        mocker: PyTest mocker fixture.

    Returns:
        A concrete instance of the `ResultsBackend` class for testing purposes.
    """

    class Test(ResultsBackend, FilterSupportMixin):
        def __init__(self, backend_name: str):
            stores = {
                "study": mocker.MagicMock(),
                "run": mocker.MagicMock(),
                "logical_worker": mocker.MagicMock(),
                "physical_worker": mocker.MagicMock(),
            }
            super().__init__(backend_name, stores)

    Test.__abstractmethods__ = frozenset()
    return Test("test-filter-support-backend")


@pytest.mark.parametrize(
    "db_model, model_type",
    [
        (StudyModel, "study"),
        (RunModel, "run"),
        (LogicalWorkerModel, "logical_worker"),
        (PhysicalWorkerModel, "physical_worker"),
    ],
)
def test_retrieve_all_filtered(
    mocker: MockerFixture,
    filter_support_backend_test_instance: FilterSupportMixin,
    db_model: BaseDataModel,
    model_type: str,
):
    """
    Test retrieving filtered entities from a store that supports filtering.

    Args:
        mocker (MockerFixture): PyTest mocker fixture.
        filter_support_backend_test_instance (FilterSupportMixin): A fixture representing a `ResultsBackend`
            that also mixes in filtering support via `FilterSupportMixin`.
        db_model (BaseDataModel): The database model class representing the entity type being tested.
        model_type (str): A string identifier for the type of entity being tested. This corresponds to
            the key used in the `ResultsBackend.stores` dictionary.
    """
    filters = {"name": "example"}
    mock_model_instance = mocker.MagicMock(spec=db_model)
    filter_support_backend_test_instance.stores[model_type].retrieve_all_filtered.return_value = [mock_model_instance]

    # Call the method under test
    entities = filter_support_backend_test_instance.retrieve_all_filtered(model_type, filters)

    # Verify
    filter_support_backend_test_instance.stores[model_type].retrieve_all_filtered.assert_called_once_with(filters)
    assert len(entities) == 1, "Should retrieve one filtered entity."
    assert isinstance(entities[0], db_model), f"Retrieved entity should be of type {type(db_model)}."
