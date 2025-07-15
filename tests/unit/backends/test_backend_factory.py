##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `backend_factory.py` module.
"""

import pytest

from pytest_mock import MockerFixture

from merlin.backends.backend_factory import MerlinBackendFactory
from merlin.backends.results_backend import ResultsBackend
from merlin.exceptions import BackendNotSupportedError


class DummyRedisBackend(ResultsBackend):
    def __init__(self, *args, **kwargs):
        pass

    def get_version(self):
        pass

    def flush_database(self):
        pass


class DummySQLiteBackend(ResultsBackend):
    def __init__(self, *args, **kwargs):
        pass

    def get_version(self):
        pass

    def flush_database(self):
        pass


class TestMerlinBackendFactory:
    """
    Test suite for the `MerlinBackendFactory`.

    This class tests that the backend factory correctly registers, resolves, instantiates,
    and reports supported Merlin backends. It uses mocking to isolate backend behavior
    and focuses on the factory's interface and logic.
    """

    @pytest.fixture
    def backend_factory(self, mocker: MockerFixture) -> MerlinBackendFactory:
        """
        An instance of the `MerlinBackendFactory` class. Resets on each test.

        Args:
            mocker: PyTest mocker fixture.

        Returns:
            An instance of the `MerlinBackendFactory` class for testing.
        """
        mocker.patch("merlin.backends.backend_factory.RedisBackend", DummyRedisBackend)
        mocker.patch("merlin.backends.backend_factory.SQLiteBackend", DummySQLiteBackend)

        return MerlinBackendFactory()

    def test_list_available_backends(self, backend_factory: MerlinBackendFactory):
        """
        Test that `list_available` returns the correct set of built-in backends.

        Args:
            backend_factory: An instance of the `MerlinBackendFactory` class for testing.
        """
        available = backend_factory.list_available()
        assert set(available) == {"redis", "sqlite"}

    @pytest.mark.parametrize("backend_type, expected_cls", [("redis", DummyRedisBackend), ("sqlite", DummySQLiteBackend)])
    def test_create_valid_backend(
        self, backend_factory: MerlinBackendFactory, backend_type: str, expected_cls: ResultsBackend
    ):
        """
        Test that `create` returns a valid backend instance for a registered name.

        Args:
            backend_factory: An instance of the `MerlinBackendFactory` class for testing.
            backend_type: The type of backend to create.
            expected_cls: The class that we're expecting `backend_factory` to create.
        """
        instance = backend_factory.create(backend_type)
        assert isinstance(instance, expected_cls)

    def test_create_valid_backend_with_alias(self, backend_factory: MerlinBackendFactory):
        """
        Test that aliases (e.g. 'rediss') are resolved to canonical backend names.

        Args:
            backend_factory: An instance of the `MerlinBackendFactory` class for testing.
        """
        instance = backend_factory.create("rediss")
        assert isinstance(instance, DummyRedisBackend)

    def test_create_invalid_backend_raises(self, backend_factory: MerlinBackendFactory):
        """
        Test that `create` raises `BackendNotSupportedError` for unknown backends.

        Args:
            backend_factory: An instance of the `MerlinBackendFactory` class for testing.
        """
        with pytest.raises(BackendNotSupportedError, match="unsupported_backend"):
            backend_factory.create("unsupported_backend")

    def test_invalid_registration_type_error(self, backend_factory: MerlinBackendFactory):
        """
        Test that trying to register a non-ResultsBackend raises TypeError.

        Args:
            backend_factory: An instance of the `MerlinBackendFactory` class for testing.
        """

        class NotAResultsBackend:
            pass

        with pytest.raises(TypeError, match="must inherit from ResultsBackend"):
            backend_factory.register("fake", NotAResultsBackend)
