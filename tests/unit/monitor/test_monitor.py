##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `monitor.py` module.
"""

from unittest.mock import MagicMock

import pytest
from _pytest.capture import CaptureFixture
from pytest_mock import MockerFixture
from redis.exceptions import TimeoutError as RedisTimeoutError

from merlin.exceptions import RestartException
from merlin.monitor.monitor import Monitor


@pytest.fixture
def monitor(mocker: MockerFixture) -> Monitor:
    """
    Fixture for `Monitor` with patched `MerlinDatabase` and `task_server_monitor`.

    Args:
        mocker: PyTest mocker fixture.

    Returns:
        A `Monitor` object with mocked properties.
    """
    mock_spec = MagicMock(name="MockSpec")
    mocker.patch("merlin.monitor.monitor.MerlinDatabase", autospec=True)
    mock_monitor = Monitor(spec=mock_spec, sleep=1, task_server="celery", no_restart=False)
    mock_monitor.task_server_monitor = mocker.MagicMock(name="MockTaskServerMonitor")
    return mock_monitor


def test_monitor_all_runs_handles_completed_and_incomplete_runs(mocker: MockerFixture, monitor: Monitor):
    """
    Test `monitor_all_runs` correctly handles a mix of completed and incomplete runs.

    Args:
        mocker: PyTest mocker fixture.
        monitor: A mocked Monitor instance.
    """

    # Set up two mock run objects
    mock_run_1 = mocker.MagicMock()
    mock_run_1.run_complete = True
    mock_run_1.get_workspace.return_value = "ws1"

    mock_run_2 = mocker.MagicMock()
    mock_run_2.run_complete = False
    mock_run_2.get_workspace.return_value = "ws2"

    # Mock study that returns a list of run IDs
    mock_study = mocker.MagicMock()
    mock_study.get_runs.return_value = ["run1", "run2"]

    # Patch monitor_single_run so it doesn't run real logic
    monitor.monitor_single_run = mocker.MagicMock()

    # Patch monitor.merlin_db.get so it returns appropriate values depending on the arguments
    def mock_get(model, *args, **kwargs):
        if model == "study":
            return mock_study
        elif model == "run":
            run_id = args[0]
            return {"run1": mock_run_1, "run2": mock_run_2}[run_id]
        return mocker.MagicMock()

    monitor.merlin_db.get.side_effect = mock_get

    monitor.monitor_all_runs()

    monitor.monitor_single_run.assert_called_once_with(mock_run_2)


def test_check_task_activity_tasks_in_queue(mocker: MockerFixture, monitor: Monitor):
    """
    Test that `_check_task_activity` returns True when there are tasks in the queues.

    Args:
        mocker: PyTest mocker fixture.
        monitor: A mocked Monitor instance.
    """
    run = mocker.MagicMock()
    monitor.task_server_monitor.check_tasks.return_value = True
    result = monitor._check_task_activity(run)
    assert result is True


def test_check_task_activity_workers_processing(mocker: MockerFixture, monitor: Monitor):
    """
    Test that `_check_task_activity` returns True when workers are processing tasks.

    Args:
        mocker: PyTest mocker fixture.
        monitor: A mocked Monitor instance.
    """
    run = mocker.MagicMock()
    monitor.task_server_monitor.check_tasks.return_value = False
    monitor.task_server_monitor.check_workers_processing.return_value = True
    run.get_queues.return_value = ["queue1"]
    result = monitor._check_task_activity(run)
    assert result is True


def test_check_task_activity_inactive(mocker: MockerFixture, monitor: Monitor):
    """
    Test that `_check_task_activity` returns False when no tasks are in the queue and no workers are active.

    Args:
        mocker: PyTest mocker fixture.
        monitor: A mocked Monitor instance.
    """
    run = mocker.MagicMock()
    monitor.task_server_monitor.check_tasks.return_value = False
    monitor.task_server_monitor.check_workers_processing.return_value = False
    result = monitor._check_task_activity(run)
    assert result is False


def test_handle_transient_exception_logs_and_sleeps(mocker: MockerFixture, monitor: Monitor):
    """
    Test that `_handle_transient_exception` logs the exception and sleeps for the specified interval.

    Args:
        mocker: PyTest mocker fixture.
        monitor: A mocked Monitor instance.
    """
    mock_sleep = mocker.patch("time.sleep")
    mock_exception = RedisTimeoutError("redis timed out")
    monitor._handle_transient_exception(mock_exception)
    mock_sleep.assert_called_once_with(monitor.sleep)


def test_monitor_single_run_completes_successfully(mocker: MockerFixture, monitor: Monitor):
    """
    Test `monitor_single_run` completes without restarting when the run finishes
    after one monitoring loop and there are no active tasks.

    Args:
        mocker: PyTest mocker fixture.
        monitor: A mocked Monitor instance.
    """
    run = mocker.MagicMock()
    run.get_workspace.return_value = "workspace1"
    run.get_workers.return_value = ["w1"]
    run.get_queues.return_value = ["q1"]
    run.run_complete = False

    # run_complete toggles to True after one loop iteration
    type(run).run_complete = mocker.PropertyMock(side_effect=[False, True])

    monitor.task_server_monitor.check_tasks.return_value = False
    monitor.task_server_monitor.check_workers_processing.return_value = False
    monitor.restart_workflow = mocker.MagicMock()
    monitor.task_server_monitor.run_worker_health_check = mocker.MagicMock()
    monitor.task_server_monitor.wait_for_workers = mocker.MagicMock()

    mock_worker = mocker.MagicMock()
    mock_worker.get_name.return_value = "worker-name"
    monitor.merlin_db.get.return_value = mock_worker

    monitor.monitor_single_run(run)

    monitor.task_server_monitor.wait_for_workers.assert_called_once()
    monitor.task_server_monitor.run_worker_health_check.assert_called_once()
    monitor.restart_workflow.assert_not_called()


def test_restart_workflow_success(mocker: MockerFixture, monitor: Monitor):
    """
    Test that `restart_workflow` successfully restarts a workflow when the subprocess call returns a zero exit code.

    Args:
        mocker: PyTest mocker fixture.
        monitor: A mocked Monitor instance.
    """
    run = mocker.MagicMock()
    run.get_workspace.return_value = "workspace"

    mocker.patch("merlin.monitor.monitor.verify_dirpath", return_value="workspace")
    mock_subproc = mocker.patch("subprocess.run", return_value=mocker.Mock(returncode=0, stdout="ok", stderr=""))

    monitor.restart_workflow(run)
    mock_subproc.assert_called_once()


def test_restart_workflow_failure(mocker: MockerFixture, monitor: Monitor):
    """
    Test that `restart_workflow` raises a `RestartException` when the subprocess call fails.

    Args:
        mocker: PyTest mocker fixture.
        monitor: A mocked Monitor instance.
    """
    run = mocker.MagicMock()
    run.get_workspace.return_value = "workspace"

    mocker.patch("merlin.monitor.monitor.verify_dirpath", return_value="workspace")
    mocker.patch("subprocess.run", return_value=mocker.Mock(returncode=1, stderr="fail", stdout=""))

    with pytest.raises(RestartException):
        monitor.restart_workflow(run)


def test_restart_workflow_path_invalid(mocker: MockerFixture, monitor: Monitor, caplog: CaptureFixture):
    """
    Test that `restart_workflow` logs a warning when the run's workspace path is invalid.

    Args:
        mocker: PyTest mocker fixture.
        monitor: A mocked Monitor instance.
        caplog: PyTest caplog fixture.
    """
    run = mocker.MagicMock()
    run.get_workspace.return_value = "workspace"

    mocker.patch("merlin.monitor.monitor.verify_dirpath", side_effect=ValueError("bad path"))

    monitor.restart_workflow(run)

    assert "was not found. Ignoring the restart" in caplog.text
