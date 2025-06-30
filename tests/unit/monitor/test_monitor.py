from unittest.mock import MagicMock, call

import pytest
from _pytest.capture import CaptureFixture
from pytest_mock import MockerFixture
from redis.exceptions import TimeoutError as RedisTimeoutError


from merlin.exceptions import RestartException
from merlin.monitor.monitor import Monitor


@pytest.fixture
def mock_spec():
    """Mock MerlinSpec object."""
    return MagicMock(name="MockSpec")


@pytest.fixture
def monitor(mocker, mock_spec):
    """Fixture for `Monitor` with patched `MerlinDatabase` and `task_server_monitor`."""
    mock_monitor = Monitor(spec=mock_spec, sleep=1, task_server="celery")
    mock_monitor.merlin_db = mocker.MagicMock(name="MockMerlinDB")
    mock_monitor.task_server_monitor = mocker.MagicMock(name="MockTaskServerMonitor")
    return mock_monitor


def test_monitor_all_runs_handles_completed_and_incomplete_runs(mocker, monitor):
    """Test `monitor_all_runs` correctly handles a mix of completed and incomplete runs."""

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


def test_check_task_activity_tasks_in_queue(mocker, monitor):
    run = mocker.MagicMock()
    monitor.task_server_monitor.check_tasks.return_value = True
    result = monitor._check_task_activity(run)
    assert result is True


def test_check_task_activity_workers_processing(mocker, monitor):
    run = mocker.MagicMock()
    monitor.task_server_monitor.check_tasks.return_value = False
    monitor.task_server_monitor.check_workers_processing.return_value = True
    run.get_queues.return_value = ["queue1"]
    result = monitor._check_task_activity(run)
    assert result is True


def test_check_task_activity_inactive(mocker, monitor):
    run = mocker.MagicMock()
    monitor.task_server_monitor.check_tasks.return_value = False
    monitor.task_server_monitor.check_workers_processing.return_value = False
    result = monitor._check_task_activity(run)
    assert result is False


def test_handle_transient_exception_logs_and_sleeps(mocker, monitor):
    mock_sleep = mocker.patch("time.sleep")
    mock_exception = RedisTimeoutError("redis timed out")
    monitor._handle_transient_exception(mock_exception)
    mock_sleep.assert_called_once_with(monitor.sleep)


def test_monitor_single_run_completes_successfully(mocker, monitor):
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


def test_restart_workflow_success(mocker, monitor):
    run = mocker.MagicMock()
    run.get_workspace.return_value = "workspace"

    mocker.patch("merlin.monitor.monitor.verify_dirpath", return_value="workspace")
    mock_subproc = mocker.patch("subprocess.run", return_value=mocker.Mock(returncode=0, stdout="ok", stderr=""))

    monitor.restart_workflow(run)
    mock_subproc.assert_called_once()


def test_restart_workflow_failure(mocker, monitor):
    run = mocker.MagicMock()
    run.get_workspace.return_value = "workspace"

    mocker.patch("merlin.monitor.monitor.verify_dirpath", return_value="workspace")
    mocker.patch("subprocess.run", return_value=mocker.Mock(returncode=1, stderr="fail", stdout=""))

    with pytest.raises(RestartException):
        monitor.restart_workflow(run)


def test_restart_workflow_path_invalid(mocker, monitor, caplog):
    run = mocker.MagicMock()
    run.get_workspace.return_value = "workspace"

    mocker.patch("merlin.monitor.monitor.verify_dirpath", side_effect=ValueError("bad path"))

    monitor.restart_workflow(run)

    assert "was not found. Ignoring the restart" in caplog.text
