##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `database.py` file of the `cli/` folder.
"""

from argparse import ArgumentParser, Namespace

import pytest
from pytest_mock import MockerFixture

from merlin.cli.commands.database import DatabaseCommand
from tests.fixture_types import FixtureCallable


@pytest.fixture
def parser(create_parser: FixtureCallable) -> ArgumentParser:
    """
    Returns an `ArgumentParser` configured with the `database` command and its subcommands.

    Args:
        create_parser: A fixture to help create a parser.

    Returns:
        Parser with the `database` command and its subcommands registered.
    """
    return create_parser(DatabaseCommand())


def test_process_command_info_calls_info(mocker: MockerFixture):
    """
    Ensure that when `commands` is `info`, database_info() is invoked.

    Args:
        mocker: PyTest mocker fixture.
    """
    mock_info = mocker.patch("merlin.cli.commands.database.database_info")
    cmd = DatabaseCommand()
    args = Namespace(commands="info", local=False)
    cmd.process_command(args)
    mock_info.assert_called_once()


def test_process_command_get_calls_get(mocker: MockerFixture):
    """
    Ensure that when `commands` is `get`, database_get(args) is invoked.

    Args:
        mocker: PyTest mocker fixture.
    """
    mock_get = mocker.patch("merlin.cli.commands.database.database_get")
    cmd = DatabaseCommand()
    args = Namespace(commands="get", local=False)
    cmd.process_command(args)
    mock_get.assert_called_once_with(args)


def test_process_command_delete_calls_delete(mocker: MockerFixture):
    """
    Ensure that when `commands` is `delete`, database_delete(args) is invoked.

    Args:
        mocker: PyTest mocker fixture.
    """
    mock_delete = mocker.patch("merlin.cli.commands.database.database_delete")
    cmd = DatabaseCommand()
    args = Namespace(commands="delete", local=False)
    cmd.process_command(args)
    mock_delete.assert_called_once_with(args)


def test_process_command_local_initializes_config(mocker: MockerFixture):
    """
    Verify that the local flag triggers initialize_config(local_mode=True) before calling the info command.

    Args:
        mocker: PyTest mocker fixture.
    """
    mock_init_config = mocker.patch("merlin.cli.commands.database.initialize_config")
    mock_info = mocker.patch("merlin.cli.commands.database.database_info")

    cmd = DatabaseCommand()
    args = Namespace(commands="info", local=True)
    cmd.process_command(args)

    mock_init_config.assert_called_once_with(local_mode=True)
    mock_info.assert_called_once()


@pytest.mark.parametrize(
    "command, args",
    [
        ("info", ["database", "info"]),
        ("get", ["database", "get", "study", "dummy_id"]),
        ("delete", ["database", "delete", "study", "dummy_id"]),
    ],
)
def test_add_parser_creates_expected_commands(parser, command, args):
    """
    Validate that the parser correctly sets `commands` for top-level database subcommands.
    """
    parsed = parser.parse_args(args)
    assert parsed.commands == command


@pytest.mark.parametrize("command", ["get", "delete"])
@pytest.mark.parametrize("subcmd", ["study", "run", "logical-worker", "physical-worker"])
def test_subcommands_with_id(parser, command, subcmd):
    """
    Test that subcommands requiring an ID are parsed correctly.
    """
    args = ["database", command, subcmd, "dummy-id"]
    parsed = parser.parse_args(args)

    assert parsed.commands == command

    if command == "get":
        assert parsed.get_type == subcmd
    elif command == "delete":
        assert parsed.delete_type == subcmd


@pytest.mark.parametrize("command", ["get", "delete"])
@pytest.mark.parametrize("subcmd", [
    "all-studies", "all-runs", "all-logical-workers",
    "all-physical-workers", "everything"
])
def test_subcommands_without_id(parser, command, subcmd):
    """
    Test that subcommands not requiring an ID are parsed correctly.
    """
    args = ["database", command, subcmd]
    parsed = parser.parse_args(args)

    assert parsed.commands == command

    if command == "get":
        assert parsed.get_type == subcmd
    elif command == "delete":
        assert parsed.delete_type == subcmd

