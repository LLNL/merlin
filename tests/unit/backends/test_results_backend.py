"""
Tests for the `results_backend.py` file.
"""

import pytest
from pytest_mock import MockerFixture

from merlin.backends.results_backend import ResultsBackend
from tests.fixture_types import FixtureStr


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

    def test_get_connection_string_raises_exception_if_not_implemented(self, results_backend_test_instance: ResultsBackend):
        """
        Test that the `get_connection_string` abstract method raises an exception if it's not implemented.

        Args:
            results_backend_test_instance (ResultsBackend): A fixture that provides a test instance
                of the `ResultsBackend` class.
        """
        with pytest.raises(NotImplementedError):
            results_backend_test_instance.get_connection_string()

    def test_flush_database_raises_exception_if_not_implemented(self, results_backend_test_instance: ResultsBackend):
        """
        Test that the `flush_database` abstract method raises an exception if it's not implemented.

        Args:
            results_backend_test_instance (ResultsBackend): A fixture that provides a test instance
                of the `ResultsBackend` class.
        """
        with pytest.raises(NotImplementedError):
            results_backend_test_instance.flush_database()

    def test_save_raises_exception_if_not_implemented(
        self, mocker: MockerFixture, results_backend_test_instance: ResultsBackend
    ):
        """
        Test that the `save` abstract method raises an exception if it's not implemented.

        Args:
            mocker (MockerFixture): A built-in fixture from the pytest-mock library to create a Mock object
            results_backend_test_instance (ResultsBackend): A fixture that provides a test instance
                of the `ResultsBackend` class.
        """
        mock_entity = mocker.Mock()
        with pytest.raises(NotImplementedError):
            results_backend_test_instance.save(mock_entity)

    def test_retrieve_raises_exception_if_not_implemented(self, results_backend_test_instance: ResultsBackend):
        """
        Test that the `retrieve` abstract method raises an exception if it's not implemented.

        Args:
            results_backend_test_instance (ResultsBackend): A fixture that provides a test instance
                of the `ResultsBackend` class.
        """
        with pytest.raises(NotImplementedError):
            results_backend_test_instance.retrieve("mock_id", "study")

    def test_retrieve_all_raises_exception_if_not_implemented(self, results_backend_test_instance: ResultsBackend):
        """
        Test that the `retrieve_all` abstract method raises an exception if it's not implemented.

        Args:
            results_backend_test_instance (ResultsBackend): A fixture that provides a test instance
                of the `ResultsBackend` class.
        """
        with pytest.raises(NotImplementedError):
            results_backend_test_instance.retrieve_all("study")

    def test_delete_raises_exception_if_not_implemented(self, results_backend_test_instance: ResultsBackend):
        """
        Test that the `delete` abstract method raises an exception if it's not implemented.

        Args:
            results_backend_test_instance (ResultsBackend): A fixture that provides a test instance
                of the `ResultsBackend` class.
        """
        with pytest.raises(NotImplementedError):
            results_backend_test_instance.delete("mock_id", "study")
