##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `celery_monitor.py` module.
"""

from unittest.mock import MagicMock

import pytest
from _pytest.capture import CaptureFixture
from pytest_mock import MockerFixture

from merlin.exceptions import NoWorkersException
from merlin.monitor.celery_monitor import CeleryMonitor


@pytest.fixture
def monitor() -> CeleryMonitor:
    """
    Fixture to provide a CeleryMonitor instance.

    Returns:
        An instance of the `CeleryMonitor` object.
    """
    return CeleryMonitor()


def test_wait_for_workers_success(mocker: MockerFixture, monitor: CeleryMonitor):
    """
    Test `wait_for_workers` succeeds when a worker is found.

    Args:
        mocker: PyTest mocker fixture.
        monitor: An instance of the `CeleryMonitor` object.
    """
    mock_get_workers = mocker.patch("merlin.monitor.celery_monitor.get_workers_from_app", return_value=["worker1@node"])

    monitor.wait_for_workers(["worker1"], sleep=1)

    assert mock_get_workers.call_count <= 10


def test_wait_for_workers_timeout(mocker: MockerFixture, monitor: CeleryMonitor):
    """
    Test `wait_for_workers` raises exception if workers never appear.

    Args:
        mocker: PyTest mocker fixture.
        monitor: An instance of the `CeleryMonitor` object.
    """
    mocker.patch("merlin.monitor.celery_monitor.get_workers_from_app", return_value=[])
    mocker.patch("time.sleep")

    with pytest.raises(NoWorkersException):
        monitor.wait_for_workers(["worker1"], sleep=0)


def test_check_workers_processing_active(mocker: MockerFixture, monitor: CeleryMonitor):
    """
    Test `check_workers_processing` returns True if matching task is active.

    Args:
        mocker: PyTest mocker fixture.
        monitor: An instance of the `CeleryMonitor` object.
    """
    mock_inspect = MagicMock()
    mock_inspect.active.return_value = {"worker1": [{"delivery_info": {"routing_key": "queue1"}}]}
    mocker.patch("merlin.celery.app.control.inspect", return_value=mock_inspect)

    result = monitor.check_workers_processing(["queue1"])
    assert result is True


def test_check_workers_processing_inactive(mocker: MockerFixture, monitor: CeleryMonitor):
    """
    Test `check_workers_processing` returns False if no tasks match queues.

    Args:
        mocker: PyTest mocker fixture.
        monitor: An instance of the `CeleryMonitor` object.
    """
    mock_inspect = MagicMock()
    mock_inspect.active.return_value = {"worker1": [{"delivery_info": {"routing_key": "other_queue"}}]}
    mocker.patch("merlin.celery.app.control.inspect", return_value=mock_inspect)

    result = monitor.check_workers_processing(["queue1"])
    assert result is False


def test_check_workers_processing_none(mocker: MockerFixture, monitor: CeleryMonitor):
    """
    Test `check_workers_processing` returns False if active() returns None.

    Args:
        mocker: PyTest mocker fixture.
        monitor: An instance of the `CeleryMonitor` object.
    """
    mock_inspect = MagicMock()
    mock_inspect.active.return_value = None
    mocker.patch("merlin.celery.app.control.inspect", return_value=mock_inspect)

    result = monitor.check_workers_processing(["queue1"])
    assert result is False


# TODO will need to update this once the functionality is flushed out
def test_restart_workers_logs(monitor: MockerFixture, caplog: CaptureFixture):
    """
    Test `_restart_workers` logs restart attempts.

    Args:
        mocker: PyTest mocker fixture.
        caplog: PyTest caplog fixture.
    """
    caplog.set_level("INFO")
    monitor._restart_workers(["worker1"])
    assert "Attempting to restart" in caplog.text


def test_get_dead_workers_some_unresponsive(mocker: MockerFixture, monitor: CeleryMonitor):
    """
    Test `_get_dead_workers` returns only unresponsive workers.

    Args:
        mocker: PyTest mocker fixture.
        monitor: An instance of the `CeleryMonitor` object.
    """
    mock_ping_response = [{"worker1": {"ok": "pong"}}, {"worker2": {"ok": "not_pong"}}]
    mocker.patch("merlin.celery.app.control.ping", return_value=mock_ping_response)

    dead = monitor._get_dead_workers(["worker1", "worker2"])
    assert dead == {"worker2"}


def test_run_worker_health_check_triggers_restart(mocker: MockerFixture, monitor: CeleryMonitor):
    """
    Test `run_worker_health_check` calls _restart_workers for dead workers.

    Args:
        mocker: PyTest mocker fixture.
        monitor: An instance of the `CeleryMonitor` object.
    """
    mocker.patch.object(monitor, "_get_dead_workers", return_value={"worker1"})
    mock_restart = mocker.patch.object(monitor, "_restart_workers")

    monitor.run_worker_health_check(["worker1", "worker2"])
    mock_restart.assert_called_once_with({"worker1"})


def test_run_worker_health_check_all_healthy(mocker: MockerFixture, monitor: CeleryMonitor):
    """
    Test `run_worker_health_check` skips restart if all workers are healthy.

    Args:
        mocker: PyTest mocker fixture.
        monitor: An instance of the `CeleryMonitor` object.
    """
    mocker.patch.object(monitor, "_get_dead_workers", return_value=set())
    mock_restart = mocker.patch.object(monitor, "_restart_workers")

    monitor.run_worker_health_check(["worker1", "worker2"])
    mock_restart.assert_not_called()


def test_check_tasks_active(mocker: MockerFixture, monitor: CeleryMonitor):
    """
    Test `check_tasks` returns True if there are jobs in the queues.

    Args:
        mocker: PyTest mocker fixture.
        monitor: An instance of the `CeleryMonitor` object.
    """
    mock_run = MagicMock()
    mock_run.get_queues.return_value = ["queue1"]
    mocker.patch("merlin.monitor.celery_monitor.query_celery_queues", return_value={"queue1": {"jobs": 5}})

    assert monitor.check_tasks(mock_run) is True


def test_check_tasks_inactive(mocker: MockerFixture, monitor: CeleryMonitor):
    """
    Test `check_tasks` returns False if no jobs are found.

    Args:
        mocker: PyTest mocker fixture.
        monitor: An instance of the `CeleryMonitor` object.
    """
    mock_run = MagicMock()
    mock_run.get_queues.return_value = ["queue1"]
    mocker.patch("merlin.monitor.celery_monitor.query_celery_queues", return_value={"queue1": {"jobs": 0}})

    assert monitor.check_tasks(mock_run) is False
