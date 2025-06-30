##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `info.py` file of the `cli/` folder.
"""

from argparse import Namespace

from pytest_mock import MockerFixture

from merlin.cli.commands.info import InfoCommand
from tests.fixture_types import FixtureCallable


def test_info_parser_sets_func(create_parser: FixtureCallable):
    """
    Ensure the `info` command sets the correct default function.

    Args:
        parser: Parser with the `info` command and its subcommands registered.
    """
    command = InfoCommand()
    parser = create_parser(command)
    args = parser.parse_args(["info"])
    assert hasattr(args, "func")
    assert args.func.__name__ == command.process_command.__name__


def test_info_process_command_calls_display(mocker: MockerFixture):
    """
    Ensure that `process_command` calls `display.print_info` with the given args.

    Args:
        mocker: PyTest mocker fixture.
    """
    mock_print_info = mocker.patch("merlin.display.print_info")
    cmd = InfoCommand()
    dummy_args = Namespace(foo="bar")
    cmd.process_command(dummy_args)
    mock_print_info.assert_called_once_with(dummy_args)
