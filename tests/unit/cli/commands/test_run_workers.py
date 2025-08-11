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
from merlin.workers.handlers import CeleryWorkerHandler
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


def test_process_command_launches_workers(mocker: MockerFixture):
    """
    Test `process_command` launches workers and creates logical worker entries in normal mode.

    Args:
        mocker: PyTest mocker fixture.
    """
    mock_spec = mocker.Mock()
    mock_spec.get_workers_to_start.return_value = ["workerA"]
    mock_spec.build_worker_list.return_value = ["worker-instance"]
    mock_spec.merlin = {"resources": {"task_server": "celery"}}

    mock_get_spec = mocker.patch(
        "merlin.cli.commands.run_workers.get_merlin_spec_with_override", return_value=(mock_spec, "workflow.yaml")
    )
    mock_handler = mocker.Mock()
    mock_factory = mocker.patch("merlin.cli.commands.run_workers.worker_handler_factory.create", return_value=mock_handler)
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
    mock_spec.get_workers_to_start.assert_called_once_with(["step1"])
    mock_spec.build_worker_list.assert_called_once_with(["workerA"])
    mock_factory.assert_called_once_with("celery")
    mock_handler.launch_workers.assert_called_once_with(
        ["worker-instance"], echo_only=False, override_args="--concurrency=4", disable_logs=False
    )
    mock_log.info.assert_called_once_with("Launching workers from 'workflow.yaml'")


def test_process_command_echo_only_mode_prints_command(mocker: MockerFixture, capsys: CaptureFixture):
    """
    Test `process_command` prints the launch command and initializes config in echo-only mode.

    Args:
        mocker: PyTest mocker fixture.
        capsys: PyTest capsys fixture.
    """
    mock_spec = mocker.Mock()
    mock_spec.get_workers_to_start.return_value = ["workerB"]
    mock_spec.merlin = {"resources": {"task_server": "celery"}}

    mock_worker = mocker.Mock()
    mock_worker.name = "workerB"
    mock_worker.get_launch_command.return_value = "echo-launch-cmd"
    mock_spec.build_worker_list.return_value = [mock_worker]

    mocker.patch("merlin.cli.commands.run_workers.get_merlin_spec_with_override", return_value=(mock_spec, "file.yaml"))
    mocker.patch("merlin.cli.commands.run_workers.initialize_config")
    mocker.patch("merlin.cli.commands.run_workers.worker_handler_factory.create", wraps=lambda _: CeleryWorkerHandler())

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
    assert "echo-launch-cmd" in captured.out
