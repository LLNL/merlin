##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `merlin/workers/worker_factory.py` module.
"""

import pytest
from pytest_mock import MockerFixture

from merlin.exceptions import MerlinWorkerNotSupportedError
from merlin.workers.worker import MerlinWorker
from merlin.workers.worker_factory import WorkerFactory


class DummyCeleryWorker(MerlinWorker):
    def __init__(self, *args, **kwargs):
        pass

    def get_launch_command(self):
        pass

    def launch_worker(self):
        pass

    def get_metadata(self):
        pass


class DummyOtherWorker(MerlinWorker):
    def __init__(self, *args, **kwargs):
        pass

    def get_launch_command(self):
        pass

    def launch_worker(self):
        pass

    def get_metadata(self):
        pass


class TestWorkerFactory:
    """
    Test suite for the `WorkerFactory`.

    This class tests that the worker factory correctly registers, resolves, instantiates,
    and reports supported Merlin workers. It uses mocking to isolate worker behavior
    and focuses on the factory's interface and logic.
    """

    @pytest.fixture
    def worker_factory(self, mocker: MockerFixture) -> WorkerFactory:
        """
        An instance of the `WorkerFactory` class. Resets on each test.

        Args:
            mocker: PyTest mocker fixture.

        Returns:
            An instance of the `WorkerFactory` class for testing.
        """
        mocker.patch("merlin.workers.worker_factory.CeleryWorker", DummyCeleryWorker)
        return WorkerFactory()

    def test_list_available_workers(self, worker_factory: WorkerFactory):
        """
        Test that `list_available` returns the correct set of built-in workers.

        Args:
            worker_factory: An instance of the `WorkerFactory` class for testing.
        """
        available = worker_factory.list_available()
        assert set(available) == {"celery"}

    def test_create_valid_worker(self, worker_factory: WorkerFactory):
        """
        Test that `create` returns a valid worker instance for a registered name.

        Args:
            worker_factory: An instance of the `WorkerFactory` class for testing.
        """
        instance = worker_factory.create("celery")
        assert isinstance(instance, DummyCeleryWorker)

    def test_create_invalid_worker_raises(self, worker_factory: WorkerFactory):
        """
        Test that `create` raises `MerlinWorkerNotSupportedError` for unknown workers.

        Args:
            worker_factory: An instance of the `WorkerFactory` class for testing.
        """
        with pytest.raises(MerlinWorkerNotSupportedError, match="unknown_worker"):
            worker_factory.create("unknown_worker")

    def test_invalid_registration_type_error(self, worker_factory: WorkerFactory):
        """
        Test that trying to register a non-MerlinWorker raises TypeError.

        Args:
            worker_factory: An instance of the `WorkerFactory` class for testing.
        """

        class NotAWorker:
            pass

        with pytest.raises(TypeError, match="must inherit from MerlinWorker"):
            worker_factory.register("fake_worker", NotAWorker)

    def test_create_valid_worker_with_alias(self, worker_factory: WorkerFactory):
        """
        Test that aliases are resolved to canonical worker names.

        Args:
            worker_factory: An instance of the `WorkerFactory` class for testing.
        """
        worker_factory.register("other", DummyOtherWorker, aliases=["alt", "legacy"])
        instance = worker_factory.create("alt")
        assert isinstance(instance, DummyOtherWorker)
