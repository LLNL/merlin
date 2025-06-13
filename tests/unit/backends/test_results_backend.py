##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `results_backend.py` file.
"""

import uuid

import pytest
from pytest_mock import MockerFixture

from merlin.backends.results_backend import ResultsBackend
from merlin.db_scripts.data_models import BaseDataModel, LogicalWorkerModel, PhysicalWorkerModel, RunModel, StudyModel
from merlin.exceptions import UnsupportedDataModelError
from tests.fixture_types import FixtureStr


@pytest.fixture()
def results_backend_test_name() -> FixtureStr:
    """
    Defines a specific name to use for the results backend tests. This helps ensure
    that even if changes were made to the tests, as long as this fixture is still used
    the tests should expect the same backend name.

    Returns:
        A string representing the name of the test backend.
    """
    return "test-backend"


@pytest.fixture()
def results_backend_test_instance(mocker: MockerFixture, results_backend_test_name: FixtureStr) -> ResultsBackend:
    """
    Provides a concrete test instance of the `ResultsBackend` class for use in tests.

    This fixture dynamically creates a subclass of `ResultsBackend` called `Test` and
    overrides its abstract methods, allowing it to be instantiated. The abstract methods
    are bypassed by setting the `__abstractmethods__` attribute to an empty frozenset.
    The resulting instance is initialized with the provided `results_backend_test_name`.

    Args:
        results_backend_test_name (FixtureStr): A fixture that provides the name of the
            results backend to be used in the test.

    Returns:
        A concrete instance of the `ResultsBackend` class for testing purposes.
    """

    class Test(ResultsBackend):
        def __init__(self, backend_name: str):
            stores = {
                "study": mocker.MagicMock(),
                "run": mocker.MagicMock(),
                "logical_worker": mocker.MagicMock(),
                "physical_worker": mocker.MagicMock(),
            }
            super().__init__(backend_name, stores)

    Test.__abstractmethods__ = frozenset()
    return Test(results_backend_test_name)


class TestResultsBackend:
    """
    Test suite for the `ResultsBackend` class.

    This class contains unit tests to validate the behavior of the `ResultsBackend` class, which serves as an
    abstract base class for implementing backends that manage entity storage and retrieval in the Merlin framework.

    The tests are divided into two categories:
    1. **Concrete Method Tests**:
    - Validates the behavior of implemented methods, such as `get_name`, ensuring they return the expected values.

    2. **Abstract Method Tests**:
    - Ensures that abstract methods (`get_version`, `get_connection_string`, `flush_database`, `save`, `retrieve`,
        `retrieve_all`, and `delete`) raise `NotImplementedError` when invoked without being implemented in a subclass.

    Fixtures are used to provide test instances of the `ResultsBackend` class and mock objects where necessary.
    This ensures the tests focus on the correctness of the abstract base class and its contract for subclasses.

    These tests ensure the integrity of the `ResultsBackend` class as a foundational component for entity storage
    and retrieval in the framework.
    """

    #########################
    # Concrete Method Tests #
    #########################

    def test_get_name(self, results_backend_test_instance: ResultsBackend, results_backend_test_name: FixtureStr):
        """
        Test the `get_name` method to ensure it returns the correct value.

        Args:
            results_backend_test_instance (ResultsBackend): A fixture that provides a test instance
                of the `ResultsBackend` class.
            results_backend_test_name (FixtureStr): A fixture that provides the name of the
                results backend to be used in the test.
        """
        assert (
            results_backend_test_instance.get_name() == results_backend_test_name
        ), f"get_name should return {results_backend_test_name}"

    #########################
    # Abstract Method Tests #
    #########################

    def test_get_version_raises_exception_if_not_implemented(self, results_backend_test_instance: ResultsBackend):
        """
        Test that the `get_version` abstract method raises an exception if it's not implemented.

        Args:
            results_backend_test_instance (ResultsBackend): A fixture that provides a test instance
                of the `ResultsBackend` class.
        """
        with pytest.raises(NotImplementedError):
            results_backend_test_instance.get_version()

    def test_get_connection_string(self, mocker: MockerFixture, results_backend_test_instance: ResultsBackend):
        """
        Test that the `get_connection_string` method makes the appropriate call.

        Args:
            mocker (MockerFixture): A built-in fixture from the pytest-mock library to create a Mock object
            results_backend_test_instance (ResultsBackend): A fixture that provides a test instance
                of the `ResultsBackend` class.
        """
        mock_get_conn_str = mocker.patch("merlin.config.results_backend.get_connection_string")
        results_backend_test_instance.get_connection_string()
        mock_get_conn_str.assert_called_once()

    def test_flush_database_raises_exception_if_not_implemented(self, results_backend_test_instance: ResultsBackend):
        """
        Test that the `flush_database` abstract method raises an exception if it's not implemented.

        Args:
            results_backend_test_instance (ResultsBackend): A fixture that provides a test instance
                of the `ResultsBackend` class.
        """
        with pytest.raises(NotImplementedError):
            results_backend_test_instance.flush_database()

    @pytest.mark.parametrize(
        "db_model, model_type",
        [
            (StudyModel, "study"),
            (RunModel, "run"),
            (LogicalWorkerModel, "logical_worker"),
            (PhysicalWorkerModel, "physical_worker"),
        ],
    )
    def test_save_valid_entity(
        self,
        mocker: MockerFixture,
        results_backend_test_instance: ResultsBackend,
        db_model: BaseDataModel,
        model_type: str,
    ):
        """
        Test saving a valid entity to the Redis store.

        This is a parametrized test that ensures saving to the Redis store works for all valid model types.
        Currently this includes:
        - studies
        - runs
        - logical workers
        - physical workers

        Args:
            mocker (MockerFixture): A built-in fixture from the pytest-mock library to create a Mock object.
            results_backend_test_instance (ResultsBackend): A fixture representing a `ResultsBackend` instance with
                mocked Redis client and stores.
            db_model (BaseDataModel): The database model class representing the entity type being tested.
            model_type (str): A string identifier for the type of entity being tested. This corresponds to
                the key used in the `ResultsBackend.stores` dictionary.
        """
        entity = mocker.MagicMock(spec=db_model)
        results_backend_test_instance.save(entity)
        results_backend_test_instance.stores[model_type].save.assert_called_once_with(entity)

    def test_save_invalid_entity(self, mocker: MockerFixture, results_backend_test_instance: ResultsBackend):
        """
        Test saving an invalid entity raises UnsupportedDataModelError.

        Args:
            mocker (MockerFixture): A built-in fixture from the pytest-mock library to create a Mock object.
            results_backend_test_instance (ResultsBackend): A fixture representing a `ResultsBackend` instance with
                mocked Redis client and stores.
        """
        invalid_entity = mocker.MagicMock(spec=object)  # Not a subclass of BaseDataModel
        with pytest.raises(UnsupportedDataModelError, match="Unsupported data model of type"):
            results_backend_test_instance.save(invalid_entity)

    @pytest.mark.parametrize(
        "db_model, model_type",
        [
            (StudyModel, "study"),
            (RunModel, "run"),
            (LogicalWorkerModel, "logical_worker"),
            (PhysicalWorkerModel, "physical_worker"),
        ],
    )
    def test_retrieve_valid_entity_by_id(
        self,
        mocker: MockerFixture,
        results_backend_test_instance: ResultsBackend,
        db_model: BaseDataModel,
        model_type: str,
    ):
        """
        Test retrieving a valid entity from the Redis store by ID.

        Args:
            mocker (MockerFixture): A built-in fixture from the pytest-mock library to create a Mock object.
            results_backend_test_instance (ResultsBackend): A fixture representing a `ResultsBackend` instance with
                mocked Redis client and stores.
            db_model (BaseDataModel): The database model class representing the entity type being tested.
            model_type (str): A string identifier for the type of entity being tested. This corresponds to
                the key used in the `ResultsBackend.stores` dictionary.
        """
        test_id = str(uuid.uuid4())
        results_backend_test_instance.stores[model_type].retrieve.return_value = mocker.MagicMock(spec=db_model)
        entity = results_backend_test_instance.retrieve(test_id, model_type)
        results_backend_test_instance.stores[model_type].retrieve.assert_called_once_with(test_id)
        assert isinstance(entity, db_model), f"Retrieved entity should be of type {type(db_model)}."

    @pytest.mark.parametrize(
        "db_model, model_type",
        [(StudyModel, "study"), (PhysicalWorkerModel, "physical_worker")],
    )
    def test_retrieve_valid_entity_by_name(
        self,
        mocker: MockerFixture,
        results_backend_test_instance: ResultsBackend,
        db_model: BaseDataModel,
        model_type: str,
    ):
        """
        Test retrieving a valid entity from the Redis store by name. This test only applies to study and
        physical worker entities.

        Args:
            mocker (MockerFixture): A built-in fixture from the pytest-mock library to create a Mock object.
            results_backend_test_instance (ResultsBackend): A fixture representing a `ResultsBackend` instance with
                mocked Redis client and stores.
            db_model (BaseDataModel): The database model class representing the entity type being tested.
            model_type (str): A string identifier for the type of entity being tested. This corresponds to
                the key used in the `ResultsBackend.stores` dictionary.
        """
        test_name = "entity_name"
        results_backend_test_instance.stores[model_type].retrieve.return_value = mocker.MagicMock(spec=db_model)
        entity = results_backend_test_instance.retrieve(test_name, model_type)
        results_backend_test_instance.stores[model_type].retrieve.assert_called_once_with(test_name, by_name=True)
        assert isinstance(entity, db_model), f"Retrieved entity should be of type {type(db_model)}."

    def test_retrieve_invalid_store_type(self, results_backend_test_instance: ResultsBackend):
        """
        Test retrieving from an invalid store type raises ValueError.

        Args:
            results_backend_test_instance (ResultsBackend): A fixture representing a `ResultsBackend` instance with
                mocked Redis client and stores.
        """
        invalid_store_type = "invalid_store"
        with pytest.raises(ValueError, match=f"Invalid store type '{invalid_store_type}'."):
            results_backend_test_instance.retrieve(str(uuid.uuid4()), invalid_store_type)

    @pytest.mark.parametrize(
        "db_model, model_type",
        [
            (StudyModel, "study"),
            (RunModel, "run"),
            (LogicalWorkerModel, "logical_worker"),
            (PhysicalWorkerModel, "physical_worker"),
        ],
    )
    def test_retrieve_all(
        self,
        mocker: MockerFixture,
        results_backend_test_instance: ResultsBackend,
        db_model: BaseDataModel,
        model_type: str,
    ):
        """
        Test retrieving all entities from a valid store.

        Args:
            mocker (MockerFixture): A built-in fixture from the pytest-mock library to create a Mock object.
            results_backend_test_instance (ResultsBackend): A fixture representing a `ResultsBackend` instance with
                mocked Redis client and stores.
            db_model (BaseDataModel): The database model class representing the entity type being tested.
            model_type (str): A string identifier for the type of entity being tested. This corresponds to
                the key used in the `ResultsBackend.stores` dictionary.
        """
        results_backend_test_instance.stores[model_type].retrieve_all.return_value = [mocker.MagicMock(spec=db_model)]
        entities = results_backend_test_instance.retrieve_all(model_type)
        results_backend_test_instance.stores[model_type].retrieve_all.assert_called_once()
        assert len(entities) == 1, "Should retrieve one entity."
        assert isinstance(entities[0], db_model), f"Retrieved entity should be of type {type(db_model)}."

    @pytest.mark.parametrize("model_type", ["study", "run", "logical_worker", "physical_worker"])
    def test_delete_valid_entity_by_id(self, results_backend_test_instance: ResultsBackend, model_type: str):
        """
        Test deleting a valid entity from the Redis store by ID.

        Args:
            results_backend_test_instance (ResultsBackend): A fixture representing a `ResultsBackend` instance with
                mocked Redis client and stores.
            model_type (str): A string identifier for the type of entity being tested. This corresponds to
                the key used in the `ResultsBackend.stores` dictionary.
        """
        test_id = str(uuid.uuid4())
        results_backend_test_instance.delete(test_id, model_type)
        results_backend_test_instance.stores[model_type].delete.assert_called_once_with(test_id)

    @pytest.mark.parametrize("model_type", ["study", "physical_worker"])
    def test_delete_valid_entity_by_name(self, results_backend_test_instance: ResultsBackend, model_type: str):
        """
        Test deleting a valid entity from the Redis store by name.

        Args:
            results_backend_test_instance (ResultsBackend): A fixture representing a `ResultsBackend` instance with
                mocked Redis client and stores.
            model_type (str): A string identifier for the type of entity being tested. This corresponds to
                the key used in the `ResultsBackend.stores` dictionary.
        """
        test_name = "entity_name"
        results_backend_test_instance.delete(test_name, model_type)
        results_backend_test_instance.stores[model_type].delete.assert_called_once_with(test_name, by_name=True)

    def test_delete_invalid_store_type(self, results_backend_test_instance: ResultsBackend):
        """
        Test deleting from an invalid store type raises ValueError.

        Args:
            results_backend_test_instance (ResultsBackend): A fixture representing a `ResultsBackend` instance with
                mocked Redis client and stores.
        """
        invalid_store_type = "invalid_store"
        with pytest.raises(ValueError, match=f"Invalid store type '{invalid_store_type}'."):
            results_backend_test_instance.delete(str(uuid.uuid4()), invalid_store_type)
