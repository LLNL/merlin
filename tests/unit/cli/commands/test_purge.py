##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `purge.py` file of the `cli/` folder.
"""

import logging
from argparse import Namespace

from _pytest.capture import CaptureFixture
from pytest_mock import MockerFixture

from merlin.cli.commands.purge import PurgeCommand
from tests.fixture_types import FixtureCallable


def test_add_parser_sets_up_purge_command(create_parser: FixtureCallable):
    """
    Ensure the `purge` command sets the correct default function.

    Args:
        create_parser: A fixture to help create a parser.
    """
    command = PurgeCommand()
    parser = create_parser(command)
    args = parser.parse_args(["purge", "workflow.yaml"])
    assert hasattr(args, "func")
    assert args.func.__name__ == command.process_command.__name__
    assert args.specification == "workflow.yaml"
    assert args.purge_force is False
    assert args.purge_steps == ["all"]
    assert args.variables is None


def test_process_command_executes_purge(mocker: MockerFixture, caplog: CaptureFixture):
    """
    Ensure `process_command` calls `purge_tasks` with expected args when using --force and specific steps.

    Args:
        mocker: PyTest mocker fixture.
        caplog: PyTest caplog fixture.
    """
    caplog.set_level(logging.INFO)

    mock_spec = mocker.Mock()
    mock_spec.merlin = {"resources": {"task_server": "celery"}}

    mocker.patch("merlin.cli.commands.purge.get_merlin_spec_with_override", return_value=(mock_spec, None))
    purge_tasks_mock = mocker.patch("merlin.cli.commands.purge.purge_tasks", return_value="mock_return")

    args = Namespace(
        specification="workflow.yaml",
        purge_force=True,
        purge_steps=["step1", "step2"],
        variables=None,
    )

    command = PurgeCommand()
    command.process_command(args)

    purge_tasks_mock.assert_called_once_with("celery", mock_spec, True, ["step1", "step2"])
    assert "Purge return = mock_return" in caplog.text


def test_process_command_with_defaults(mocker: MockerFixture, caplog: CaptureFixture):
    """
    Ensure `process_command` uses default values and purges correctly without --force or custom steps.

    Args:
        mocker: PyTest mocker fixture.
        caplog: PyTest caplog fixture.
    """
    caplog.set_level(logging.INFO)

    mock_spec = mocker.Mock()
    mock_spec.merlin = {"resources": {"task_server": "celery"}}

    mocker.patch("merlin.cli.commands.purge.get_merlin_spec_with_override", return_value=(mock_spec, None))
    purge_tasks_mock = mocker.patch("merlin.cli.commands.purge.purge_tasks", return_value="ok")

    args = Namespace(
        specification="spec.yaml",
        purge_force=False,
        purge_steps=["all"],
        variables=None,
    )

    command = PurgeCommand()
    command.process_command(args)

    purge_tasks_mock.assert_called_once_with("celery", mock_spec, False, ["all"])
    assert "Purge return = ok" in caplog.text
