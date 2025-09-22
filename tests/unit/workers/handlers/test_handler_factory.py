##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `merlin/workers/handlers/handler_factory.py` module.
"""

import pytest
from pytest_mock import MockerFixture

from merlin.exceptions import MerlinWorkerHandlerNotSupportedError
from merlin.workers.handlers.handler_factory import WorkerHandlerFactory
from merlin.workers.handlers.worker_handler import MerlinWorkerHandler


class DummyCeleryWorkerHandler(MerlinWorkerHandler):
    def __init__(self, *args, **kwargs):
        pass

    def start_workers(self):
        pass

    def stop_workers(self):
        pass

    def query_workers(self):
        pass


class DummyKafkaWorkerHandler(MerlinWorkerHandler):
    def __init__(self, *args, **kwargs):
        pass

    def start_workers(self):
        pass

    def stop_workers(self):
        pass

    def query_workers(self):
        pass


class TestWorkerHandlerFactory:
    """
    Test suite for the `WorkerHandlerFactory`.

    This class verifies that the factory properly registers, validates, instantiates,
    and handles Merlin worker handlers. It mocks built-ins for test isolation.
    """

    @pytest.fixture
    def handler_factory(self, mocker: MockerFixture) -> WorkerHandlerFactory:
        """
        A fixture that returns a fresh instance of `WorkerHandlerFactory` with built-in handlers patched.

        Args:
            mocker: PyTest mocker fixture.

        Returns:
            A factory instance with mocked handler classes.
        """
        mocker.patch("merlin.workers.handlers.handler_factory.CeleryWorkerHandler", DummyCeleryWorkerHandler)
        return WorkerHandlerFactory()

    def test_list_available_handlers(self, handler_factory: WorkerHandlerFactory):
        """
        Test that `list_available` returns the expected built-in handler names.

        Args:
            handler_factory: Instance of the `WorkerHandlerFactory` for testing.
        """
        available = handler_factory.list_available()
        assert set(available) == {"celery"}

    def test_create_valid_handler(self, handler_factory: WorkerHandlerFactory):
        """
        Test that `create` returns a valid handler instance for a registered name.

        Args:
            handler_factory: Instance of the `WorkerHandlerFactory` for testing.
        """
        instance = handler_factory.create("celery")
        assert isinstance(instance, DummyCeleryWorkerHandler)

    def test_create_valid_handler_with_alias(self, handler_factory: WorkerHandlerFactory):
        """
        Test that aliases are resolved to canonical handler names.

        Args:
            handler_factory: Instance of the `WorkerHandlerFactory` for testing.
        """
        handler_factory.register("kafka", DummyKafkaWorkerHandler, aliases=["kfk", "legacy-kafka"])
        instance = handler_factory.create("legacy-kafka")
        assert isinstance(instance, DummyKafkaWorkerHandler)

    def test_create_invalid_handler_raises(self, handler_factory: WorkerHandlerFactory):
        """
        Test that `create` raises `MerlinWorkerHandlerNotSupportedError` for unknown handler types.

        Args:
            handler_factory: Instance of the `WorkerHandlerFactory` for testing.
        """
        with pytest.raises(MerlinWorkerHandlerNotSupportedError, match="unknown_handler"):
            handler_factory.create("unknown_handler")

    def test_invalid_registration_type_error(self, handler_factory: WorkerHandlerFactory):
        """
        Test that trying to register a non-MerlinWorkerHandler raises TypeError.

        Args:
            handler_factory: Instance of the `WorkerHandlerFactory` for testing.
        """

        class NotAWorkerHandler:
            pass

        with pytest.raises(TypeError, match="must inherit from MerlinWorkerHandler"):
            handler_factory.register("fake_handler", NotAWorkerHandler)
