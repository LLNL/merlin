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
    mock_spec.name = "test_study"
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
    # Set up two mock run objects (one complete, one incomplete)
    mock_run_1 = mocker.MagicMock()
    mock_run_1.run_complete = True
    mock_run_1.get_workspace.return_value = "ws1"
    mock_run_1.get_workers.return_value = ["worker1"]

    mock_run_2 = mocker.MagicMock()
    mock_run_2.run_complete = False
    mock_run_2.get_workspace.return_value = "ws2"
    mock_run_2.get_workers.return_value = ["worker2"]

    # Mock study that returns a list of run IDs
    mock_study = mocker.MagicMock()
    mock_study.get_runs.return_value = ["run1", "run2"]

    # Mock the database get method
    def mock_get(model, *args, **kwargs):
        if model == "study":
            return mock_study
        elif model == "run":
            run_id = args[0]
            return {"run1": mock_run_1, "run2": mock_run_2}[run_id]
        elif model == "logical_worker":
            mock_worker = mocker.MagicMock()
            mock_worker.get_name.return_value = "test_worker"
            return mock_worker
        return mocker.MagicMock()

    monitor.merlin_db.get.side_effect = mock_get

    # Mock monitoring methods
    monitor.wait_for_workers = mocker.MagicMock()
    monitor.check_run_health = mocker.MagicMock()
    
    # Use sleep side effect to mark run as complete after first cycle
    def sleep_side_effect(duration):
        # After first cycle, mark run 2 as complete to exit the loop
        mock_run_2.run_complete = True
    
    mocker.patch("time.sleep", side_effect=sleep_side_effect)

    monitor.monitor_all_runs()

    # Should have called wait_for_workers and check_run_health for the incomplete run
    monitor.wait_for_workers.assert_called_once_with(mock_run_2)
    monitor.check_run_health.assert_called_once_with(mock_run_2)


def test_monitor_all_runs_exits_when_all_complete(mocker: MockerFixture, monitor: Monitor):
    """
    Test `monitor_all_runs` exits immediately when all runs are complete.

    Args:
        mocker: PyTest mocker fixture.
        monitor: A mocked Monitor instance.
    """
    # Set up two mock run objects that are both complete
    mock_run_1 = mocker.MagicMock()
    mock_run_1.run_complete = True
    mock_run_1.get_workspace.return_value = "ws1"

    mock_run_2 = mocker.MagicMock()
    mock_run_2.run_complete = True
    mock_run_2.get_workspace.return_value = "ws2"

    # Mock study
    mock_study = mocker.MagicMock()
    mock_study.get_runs.return_value = ["run1", "run2"]

    def mock_get(model, *args, **kwargs):
        if model == "study":
            return mock_study
        elif model == "run":
            run_id = args[0]
            return {"run1": mock_run_1, "run2": mock_run_2}[run_id]
        return mocker.MagicMock()

    monitor.merlin_db.get.side_effect = mock_get

    # Mock monitoring methods
    monitor.wait_for_workers = mocker.MagicMock()
    monitor.check_run_health = mocker.MagicMock()

    monitor.monitor_all_runs()

    # Should not have called wait_for_workers or check_run_health
    monitor.wait_for_workers.assert_not_called()
    monitor.check_run_health.assert_not_called()


def test_monitor_all_runs_monitors_multiple_active_runs(mocker: MockerFixture, monitor: Monitor):
    """
    Test `monitor_all_runs` performs health checks on all active runs in a single cycle.

    Args:
        mocker: PyTest mocker fixture.
        monitor: A mocked Monitor instance.
    """
    # Set up three mock runs, all incomplete
    mock_runs = []
    for i in range(3):
        mock_run = mocker.MagicMock()
        mock_run.run_complete = False
        mock_run.get_workspace.return_value = f"ws{i}"
        mock_run.get_workers.return_value = [f"worker{i}"]
        mock_runs.append(mock_run)

    # Mock study
    mock_study = mocker.MagicMock()
    mock_study.get_runs.return_value = ["run0", "run1", "run2"]

    call_count = 0
    
    def mock_get(model, *args, **kwargs):
        nonlocal call_count
        if model == "study":
            return mock_study
        elif model == "run":
            run_id = args[0]
            run_idx = int(run_id.replace("run", ""))
            # On second iteration, mark all runs complete to exit
            if call_count >= 3:
                mock_runs[run_idx].run_complete = True
            return mock_runs[run_idx]
        elif model == "logical_worker":
            mock_worker = mocker.MagicMock()
            mock_worker.get_name.return_value = "test_worker"
            return mock_worker
        return mocker.MagicMock()

    monitor.merlin_db.get.side_effect = mock_get

    # Mock monitoring methods
    monitor.wait_for_workers = mocker.MagicMock()
    
    def check_health_side_effect(run):
        nonlocal call_count
        call_count += 1
    
    monitor.check_run_health = mocker.MagicMock(side_effect=check_health_side_effect)

    # Mock sleep to avoid delays
    mocker.patch("time.sleep")

    monitor.monitor_all_runs()

    # Should have been called once for each run in the first cycle (3 runs)
    assert monitor.check_run_health.call_count == 3
    assert monitor.wait_for_workers.call_count == 3


def test_monitor_all_runs_detects_new_runs_dynamically(mocker: MockerFixture, monitor: Monitor):
    """
    Test `monitor_all_runs` detects new runs added during monitoring (iterative workflows).

    Args:
        mocker: PyTest mocker fixture.
        monitor: A mocked Monitor instance.
    """
    # Set up initial run
    mock_run_1 = mocker.MagicMock()
    mock_run_1.run_complete = False
    mock_run_1.get_workspace.return_value = "ws1"
    mock_run_1.get_workers.return_value = ["worker1"]

    # New run that will be added
    mock_run_2 = mocker.MagicMock()
    mock_run_2.run_complete = False
    mock_run_2.get_workspace.return_value = "ws2"
    mock_run_2.get_workers.return_value = ["worker2"]

    # Mock study
    mock_study = mocker.MagicMock()
    
    # First call returns one run, second call returns two runs, third returns two complete runs
    mock_study.get_runs.side_effect = [
        ["run1"],           # First cycle: 1 run
        ["run1", "run2"],   # Second cycle: 2 runs (new run added)
        ["run1", "run2"]    # Third cycle: both complete
    ]

    cycle_count = 0
    
    def mock_get(model, *args, **kwargs):
        nonlocal cycle_count
        if model == "study":
            return mock_study
        elif model == "run":
            run_id = args[0]
            # On third cycle, mark all runs complete
            if cycle_count >= 2:
                mock_run_1.run_complete = True
                mock_run_2.run_complete = True
            return {"run1": mock_run_1, "run2": mock_run_2}[run_id]
        elif model == "logical_worker":
            mock_worker = mocker.MagicMock()
            mock_worker.get_name.return_value = "test_worker"
            return mock_worker
        return mocker.MagicMock()

    monitor.merlin_db.get.side_effect = mock_get

    # Mock monitoring methods
    monitor.wait_for_workers = mocker.MagicMock()
    
    def check_health_side_effect(run):
        nonlocal cycle_count
        cycle_count += 1
    
    monitor.check_run_health = mocker.MagicMock(side_effect=check_health_side_effect)

    # Mock sleep
    mocker.patch("time.sleep")

    monitor.monitor_all_runs()

    # Should have monitored: 1 run in cycle 1, 2 runs in cycle 2 = 3 total health checks
    assert monitor.check_run_health.call_count == 3


def test_wait_for_workers(mocker: MockerFixture, monitor: Monitor):
    """
    Test `wait_for_workers` retrieves worker names and waits for them to start.

    Args:
        mocker: PyTest mocker fixture.
        monitor: A mocked Monitor instance.
    """
    run = mocker.MagicMock()
    run.get_workers.return_value = ["worker_id_1", "worker_id_2"]

    mock_worker_1 = mocker.MagicMock()
    mock_worker_1.get_name.return_value = "worker_1"
    
    mock_worker_2 = mocker.MagicMock()
    mock_worker_2.get_name.return_value = "worker_2"

    def mock_get(model, *args, **kwargs):
        worker_id = kwargs.get("worker_id")
        return {"worker_id_1": mock_worker_1, "worker_id_2": mock_worker_2}[worker_id]

    monitor.merlin_db.get.side_effect = mock_get

    monitor.wait_for_workers(run)

    monitor.task_server_monitor.wait_for_workers.assert_called_once_with(
        ["worker_1", "worker_2"], monitor.sleep
    )


def test_check_task_activity_tasks_in_queue(mocker: MockerFixture, monitor: Monitor):
    """
    Test that `check_task_activity` returns True when there are tasks in the queues.

    Args:
        mocker: PyTest mocker fixture.
        monitor: A mocked Monitor instance.
    """
    run = mocker.MagicMock()
    monitor.task_server_monitor.check_tasks.return_value = True
    result = monitor.check_task_activity(run)
    assert result is True


def test_check_task_activity_workers_processing(mocker: MockerFixture, monitor: Monitor):
    """
    Test that `check_task_activity` returns True when workers are processing tasks.

    Args:
        mocker: PyTest mocker fixture.
        monitor: A mocked Monitor instance.
    """
    run = mocker.MagicMock()
    monitor.task_server_monitor.check_tasks.return_value = False
    monitor.task_server_monitor.check_workers_processing.return_value = True
    run.get_queues.return_value = ["queue1"]
    result = monitor.check_task_activity(run)
    assert result is True


def test_check_task_activity_inactive(mocker: MockerFixture, monitor: Monitor):
    """
    Test that `check_task_activity` returns False when no tasks are in the queue and no workers are active.

    Args:
        mocker: PyTest mocker fixture.
        monitor: A mocked Monitor instance.
    """
    run = mocker.MagicMock()
    monitor.task_server_monitor.check_tasks.return_value = False
    monitor.task_server_monitor.check_workers_processing.return_value = False
    result = monitor.check_task_activity(run)
    assert result is False


def test_check_run_health_performs_health_check(mocker: MockerFixture, monitor: Monitor):
    """
    Test that `check_run_health` performs worker health checks and task activity checks.

    Args:
        mocker: PyTest mocker fixture.
        monitor: A mocked Monitor instance.
    """
    run = mocker.MagicMock()
    run.run_complete = False
    run.get_workspace.return_value = "workspace"
    run.get_workers.return_value = ["worker1"]

    monitor.check_task_activity = mocker.MagicMock(return_value=True)
    monitor.restart_workflow = mocker.MagicMock()

    monitor.check_run_health(run)

    monitor.task_server_monitor.run_worker_health_check.assert_called_once_with(["worker1"])
    monitor.check_task_activity.assert_called_once_with(run)
    monitor.restart_workflow.assert_not_called()


def test_check_run_health_restarts_stalled_workflow(mocker: MockerFixture, monitor: Monitor):
    """
    Test that `check_run_health` restarts a workflow when it's stalled (no activity, not complete).

    Args:
        mocker: PyTest mocker fixture.
        monitor: A mocked Monitor instance.
    """
    run = mocker.MagicMock()
    run.run_complete = False
    run.get_workspace.return_value = "workspace"
    run.get_workers.return_value = ["worker1"]

    monitor.check_task_activity = mocker.MagicMock(return_value=False)
    monitor.restart_workflow = mocker.MagicMock()

    monitor.check_run_health(run)

    monitor.restart_workflow.assert_called_once_with(run)


def test_check_run_health_no_restart_when_disabled(mocker: MockerFixture, monitor: Monitor):
    """
    Test that `check_run_health` does not restart when no_restart flag is True.

    Args:
        mocker: PyTest mocker fixture.
        monitor: A mocked Monitor instance.
    """
    monitor.no_restart = True
    
    run = mocker.MagicMock()
    run.run_complete = False
    run.get_workspace.return_value = "workspace"
    run.get_workers.return_value = ["worker1"]

    monitor.check_task_activity = mocker.MagicMock(return_value=False)
    monitor.restart_workflow = mocker.MagicMock()

    monitor.check_run_health(run)

    monitor.restart_workflow.assert_not_called()


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

    def sleep_side_effect(duration):
        # After first cycle, mark run as complete to exit the loop
        run.run_complete = True
    
    mocker.patch("time.sleep", side_effect=sleep_side_effect)

    monitor.wait_for_workers = mocker.MagicMock()
    monitor.check_run_health = mocker.MagicMock()

    monitor.monitor_single_run(run)

    monitor.wait_for_workers.assert_called_once_with(run)
    monitor.check_run_health.assert_called_once_with(run)


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
