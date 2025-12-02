##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `monitor.py` file of the `cli/` folder.
"""

from argparse import Namespace

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
        variables=None,
        task_server="celery",
        sleep=5,
        no_restart=False,
        disable_gc=True,
    )
    command.process_command(args)

    monitor_class.assert_called_once_with(mock_spec, 5, "celery", no_restart=False, auto_cleanup=False)
    mock_monitor.monitor_all_runs.assert_called_once()
