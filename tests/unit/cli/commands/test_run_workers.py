##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `run_workers.py` file of the `cli/` folder.
"""

from argparse import Namespace

from _pytest.capture import CaptureFixture
from pytest_mock import MockerFixture

from merlin.cli.commands.run_workers import RunWorkersCommand
from tests.fixture_types import FixtureCallable


def test_add_parser_sets_up_run_workers_command(create_parser: FixtureCallable):
    """
    Test that the `run-workers` command registers all expected arguments and sets the correct defaults.

    Args:
        create_parser: A fixture to help create a parser.
    """
    command = RunWorkersCommand()
    parser = create_parser(command)
    args = parser.parse_args(["run-workers", "workflow.yaml"])
    assert hasattr(args, "func")
    assert args.func.__name__ == command.process_command.__name__
    assert args.specification == "workflow.yaml"
    assert args.worker_args == ""
    assert args.worker_steps == ["all"]
    assert args.worker_echo_only is False
    assert args.variables is None
    assert args.disable_logs is False


def test_process_command_launches_workers_and_creates_logical_workers(mocker: MockerFixture):
    """
    Test `process_command` launches workers and creates logical worker entries in normal mode.

    Args:
        mocker: PyTest mocker fixture.
    """
    mock_spec = mocker.Mock()
    mock_spec.get_task_queues.return_value = {"step1": "queue1", "step2": "queue2"}
    mock_spec.get_worker_step_map.return_value = {"workerA": ["step1", "step2"]}

    mock_get_spec = mocker.patch(
        "merlin.cli.commands.run_workers.get_merlin_spec_with_override", return_value=(mock_spec, "workflow.yaml")
    )
    mock_launch = mocker.patch("merlin.cli.commands.run_workers.launch_workers", return_value="launched")
    mock_db = mocker.patch("merlin.cli.commands.run_workers.MerlinDatabase")
    mock_log = mocker.patch("merlin.cli.commands.run_workers.LOG")

    args = Namespace(
        specification="workflow.yaml",
        worker_args="--concurrency=4",
        worker_steps=["step1"],
        worker_echo_only=False,
        variables=None,
        disable_logs=False,
    )

    RunWorkersCommand().process_command(args)

    mock_get_spec.assert_called_once_with(args)
    mock_db.return_value.create.assert_called_once_with("logical_worker", "workerA", {"queue1", "queue2"})
    mock_launch.assert_called_once_with(mock_spec, ["step1"], "--concurrency=4", False, False)
    mock_log.info.assert_called_once_with("Launching workers from 'workflow.yaml'")
    mock_log.debug.assert_called_once_with("celery command: launched")


def test_process_command_echo_only_mode_prints_command(mocker: MockerFixture, capsys: CaptureFixture):
    """
    Test `process_command` prints the launch command and initializes config in echo-only mode.

    Args:
        mocker: PyTest mocker fixture.
        capsys: PyTest capsys fixture.
    """
    mock_spec = mocker.Mock()
    mock_spec.get_task_queues.return_value = {}
    mock_spec.get_worker_step_map.return_value = {}

    mocker.patch("merlin.cli.commands.run_workers.get_merlin_spec_with_override", return_value=(mock_spec, "file.yaml"))
    mocker.patch("merlin.cli.commands.run_workers.initialize_config")
    mocker.patch("merlin.cli.commands.run_workers.MerlinDatabase")
    mock_launch = mocker.patch("merlin.cli.commands.run_workers.launch_workers", return_value="echo-cmd")

    args = Namespace(
        specification="spec.yaml",
        worker_args="--autoscale=2,10",
        worker_steps=["all"],
        worker_echo_only=True,
        variables=None,
        disable_logs=False,
    )

    RunWorkersCommand().process_command(args)

    captured = capsys.readouterr()
    assert "echo-cmd" in captured.out
    mock_launch.assert_called_once_with(mock_spec, ["all"], "--autoscale=2,10", False, True)
