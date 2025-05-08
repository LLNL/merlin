"""
Fixtures for the `results_backend.py` module.
"""

import pytest

from merlin.backends.results_backend import ResultsBackend
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
def results_backend_test_instance(results_backend_test_name: FixtureStr) -> ResultsBackend:
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
        pass

    Test.__abstractmethods__ = frozenset()
    return Test(results_backend_test_name)
