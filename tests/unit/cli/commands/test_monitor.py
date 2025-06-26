##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `monitor.py` file of the `cli/` folder.
"""

import logging
from argparse import Namespace

from _pytest.capture import CaptureFixture
from pytest_mock import MockerFixture

from merlin.cli.commands.monitor import MonitorCommand
from tests.fixture_types import FixtureCallable


def test_add_parser_sets_up_monitor_command(create_parser: FixtureCallable):
    """
    Ensure the `monitor` command sets the correct default function.

    Args:
        create_parser: A fixture to help create a parser.
    """
    command = MonitorCommand()
    parser = create_parser(command)
    args = parser.parse_args(["monitor", "spec.yaml"])
    assert hasattr(args, "func")
    assert args.func.__name__ == command.process_command.__name__
    assert args.specification == "spec.yaml"
    assert args.steps == ["all"]
    assert args.variables is None
    assert args.task_server == "celery"
    assert args.sleep == 60
    assert not args.no_restart


def test_process_command_all_steps(mocker: MockerFixture):
    """
    Test the case when `args.steps == ['all']` -> uses Monitor.monitor_all_runs().

    Args:
        mocker: PyTest mocker fixture.
    """
    mock_spec = mocker.Mock()
    mocker.patch("merlin.cli.commands.monitor.get_merlin_spec_with_override", return_value=(mock_spec, None))
    mocker.patch("time.sleep")

    mock_monitor = mocker.Mock()
    monitor_class = mocker.patch("merlin.cli.commands.monitor.Monitor", return_value=mock_monitor)

    command = MonitorCommand()
    args = Namespace(
        specification="spec.yaml",
        steps=["all"],
        variables=None,
        task_server="celery",
        sleep=5,
        no_restart=False,
    )
    command.process_command(args)

    monitor_class.assert_called_once_with(mock_spec, 5, "celery", False)
    mock_monitor.monitor_all_runs.assert_called_once()


def test_monitor_process_command_with_specific_steps(mocker: MockerFixture, caplog: CaptureFixture):
    """
    Test the case when `args.steps != ['all']` -> uses `check_merlin_status()` in a loop.

    Args:
        mocker: PyTest mocker fixture.
        caplog: PyTest caplog fixture.
    """
    caplog.set_level(logging.INFO)

    mock_spec = mocker.Mock()
    mock_get_spec = mocker.patch("merlin.cli.commands.monitor.get_merlin_spec_with_override", return_value=(mock_spec, None))
    mock_sleep = mocker.patch("time.sleep")

    # simulate 2 iterations
    mock_check_status = mocker.patch("merlin.cli.commands.monitor.check_merlin_status", side_effect=[True, True, False])

    command = MonitorCommand()
    args = Namespace(
        specification="workflow.yaml",
        steps=["step1"],
        variables=None,
        task_server="celery",
        sleep=5,
        no_restart=False,
    )
    command.process_command(args)

    mock_get_spec.assert_called_once_with(args)
    assert mock_sleep.call_count == 3  # 1 before loop, 2 in loop
    assert mock_check_status.call_count == 3
    assert "Monitor: found tasks in queues and/or tasks being processed" in caplog.text
    assert "Monitor: ... stop condition met" in caplog.text
