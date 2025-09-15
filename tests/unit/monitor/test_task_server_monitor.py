##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `task_server_monitor.py` module.
"""

from typing import List
from unittest.mock import MagicMock

import pytest

from merlin.db_scripts.entities.run_entity import RunEntity
from merlin.monitor.task_server_monitor import TaskServerMonitor  # Update with actual import path


# Create a concrete subclass of TaskServerMonitor for testing
class DummyTaskServerMonitor(TaskServerMonitor):
    """
    A concrete test implementation of the TaskServerMonitor abstract base class.

    This class is used solely for unit testing and tracks calls made to its methods
    without performing any real task monitoring logic.
    """

    def wait_for_workers(self, workers: List[str], sleep: int):
        """
        Simulate waiting for workers by recording the input arguments.

        Args:
            workers: List of worker names or IDs.
            sleep: Number of seconds to wait between checks.
        """
        self.wait_called = (workers, sleep)

    def check_workers_processing(self, queues: List[str]) -> bool:
        """
        Simulate checking if workers are processing tasks by recording the queues.

        Args:
            queues: List of queue names.

        Returns:
            Always returns True to simulate active processing.
        """
        self.check_processing_called = queues
        return True

    def run_worker_health_check(self, workers: List[str]):
        """
        Simulate a health check by recording the list of workers.

        Args:
            workers: List of worker names or IDs to check.
        """
        self.health_check_called = workers

    def check_tasks(self, run: RunEntity) -> bool:
        """
        Simulate checking task status by recording the `RunEntity` instance.

        Args:
            run: A mocked `RunEntity` instance.

        Returns:
            Always returns False to simulate no active tasks.
        """
        self.check_tasks_called = run
        return False


@pytest.fixture
def monitor() -> DummyTaskServerMonitor:
    """
    Pytest fixture that provides a `DummyTaskServerMonitor` instance for use in tests.

    Returns:
        A dummy instantiation of a `TaskServerMonitor` subclass.
    """
    return DummyTaskServerMonitor()


def test_wait_for_workers(monitor: DummyTaskServerMonitor):
    """
    Test that `wait_for_workers` correctly stores the passed arguments.

    Args:
        monitor: A dummy instantiation of a `TaskServerMonitor` subclass.
    """
    monitor.wait_for_workers(["worker1", "worker2"], sleep=5)
    assert monitor.wait_called == (["worker1", "worker2"], 5)


def test_check_workers_processing(monitor: DummyTaskServerMonitor):
    """
    Test that `check_workers_processing` returns True and stores the input queues.

    Args:
        monitor: A dummy instantiation of a `TaskServerMonitor` subclass.
    """
    result = monitor.check_workers_processing(["queue1", "queue2"])
    assert result is True
    assert monitor.check_processing_called == ["queue1", "queue2"]


def test_run_worker_health_check(monitor: DummyTaskServerMonitor):
    """
    Test that `run_worker_health_check` correctly records the provided worker list.

    Args:
        monitor: A dummy instantiation of a `TaskServerMonitor` subclass.
    """
    monitor.run_worker_health_check(["worker1"])
    assert monitor.health_check_called == ["worker1"]


def test_check_tasks(monitor: DummyTaskServerMonitor):
    """
    Test that `check_tasks` returns False and stores the RunEntity instance.

    Args:
        monitor: A dummy instantiation of a `TaskServerMonitor` subclass.
    """
    dummy_run = MagicMock(spec=RunEntity)
    result = monitor.check_tasks(dummy_run)
    assert result is False
    assert monitor.check_tasks_called == dummy_run
