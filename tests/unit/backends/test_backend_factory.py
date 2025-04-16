"""
Tests for the `backend_factory.py` module.
"""

import pytest
from pytest_mock import MockerFixture

from merlin.backends.backend_factory import backend_factory
from merlin.backends.redis.redis_backend import RedisBackend
from merlin.exceptions import BackendNotSupportedError


class TestBackendFactory:
    """
    Test suite for the `backend_factory` module.

    This class contains unit tests to validate the functionality of the `backend_factory`, which is responsible 
    for managing backend instances and providing an interface for retrieving supported backends and resolving 
    backend aliases.

    Fixtures and mocking are used to isolate the tests from the actual backend implementations, ensuring that 
    the tests focus on the behavior of the `backend_factory` module.

    These tests ensure the robustness and correctness of the `backend_factory` module, which is critical for 
    backend management in the Merlin framework.
    """

    def test_get_supported_backends(self):
        """
        Test that `get_supported_backends` returns the correct list of supported backends.
        """
        supported_backends = backend_factory.get_supported_backends()
        assert supported_backends == ["redis"], "Supported backends should only include 'redis'."

    def test_get_backend_with_valid_backend(self, mocker: MockerFixture):
        """
        Test that `get_backend` returns the correct backend instance for a valid backend.

        Args:
            mocker (MockerFixture): A built-in fixture from the pytest-mock library to create a Mock object.
        """
        backend_name = "redis"

        # Mock the RedisBackend class to avoid instantiation issues
        RedisBackendMock = mocker.MagicMock(spec=RedisBackend)
        backend_factory._backends["redis"] = RedisBackendMock

        backend_instance = backend_factory.get_backend(backend_name)

        # Verify the backend instance is created correctly
        RedisBackendMock.assert_called_once_with(backend_name)
        assert backend_instance == RedisBackendMock(backend_name), "Backend instance should match the mocked backend."

    def test_get_backend_with_alias(self, mocker: MockerFixture):
        """
        Test that `get_backend` correctly resolves aliases to canonical backend names.

        Args:
            mocker (MockerFixture): A built-in fixture from the pytest-mock library to create a Mock object.
        """
        alias_name = "rediss"

        # Mock the RedisBackend class to avoid instantiation issues
        RedisBackendMock = mocker.MagicMock(spec=RedisBackend)
        backend_factory._backends["redis"] = RedisBackendMock

        backend_instance = backend_factory.get_backend(alias_name)

        # Verify the alias resolves and the backend instance is created correctly
        RedisBackendMock.assert_called_once_with("redis")
        assert backend_instance == RedisBackendMock("redis"), "Backend instance should match the mocked backend."

    def test_get_backend_with_invalid_backend(self):
        """
        Test that get_backend raises BackendNotSupportedError for an unsupported backend.
        """
        invalid_backend_name = "unsupported_backend"

        with pytest.raises(BackendNotSupportedError, match=f"Backend unsupported by Merlin: {invalid_backend_name}."):
            backend_factory.get_backend(invalid_backend_name)
