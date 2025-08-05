##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the merlin/cli/commands/database/database.py module.
"""


from argparse import ArgumentParser, Namespace
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from merlin.cli.commands.database.database import DatabaseCommand


@pytest.fixture
def mock_info_command(mocker: MockerFixture) -> MagicMock:
    """
    Fixture to patch the `DatabaseInfoCommand` class with a mock object.

    Args:
        mocker: PyTest mocker fixture.

    Returns:
        Mocked `DatabaseInfoCommand` class.
    """
    return mocker.patch("merlin.cli.commands.database.database.DatabaseInfoCommand")


@pytest.fixture
def mock_get_command(mocker: MockerFixture) -> MagicMock:
    """
    Fixture to patch the `DatabaseGetCommand` class with a mock object.

    Args:
        mocker: PyTest mocker fixture.

    Returns:
        Mocked `DatabaseGetCommand` class.
    """
    return mocker.patch("merlin.cli.commands.database.database.DatabaseGetCommand")


@pytest.fixture
def mock_delete_command(mocker: MockerFixture) -> MagicMock:
    """
    Fixture to patch the `DatabaseDeleteCommand` class with a mock object.

    Returns:
        Mocked `DatabaseDeleteCommand` class.
    """
    return mocker.patch("merlin.cli.commands.database.database.DatabaseDeleteCommand")


@pytest.fixture
def database_command(
    mock_info_command: MagicMock,
    mock_get_command: MagicMock,
    mock_delete_command: MagicMock,
) -> DatabaseCommand:
    """
    Fixture to create a DatabaseCommand instance using the mocked subcommands.

    Args:
        mock_info_command: Mocked `DatabaseInfoCommand` class.
        mock_get_command: Mocked `DatabaseGetCommand` class.
        mock_delete_command: Mocked `DatabaseDeleteCommand` class.

    Returns:
        Instance of DatabaseCommand.
    """
    return DatabaseCommand()


def test_add_parser_calls_subcommand_add_parser_methods(
    mock_info_command: MagicMock,
    mock_get_command: MagicMock,
    mock_delete_command: MagicMock,
    database_command: DatabaseCommand,
):
    """
    Test that the `add_parser` method on each subcommand (info, get, delete)
    is called exactly once when `DatabaseCommand.add_parser` is invoked.

    Args:
        mock_info_command: Mocked `DatabaseInfoCommand` class.
        mock_get_command: Mocked `DatabaseGetCommand` class.
        mock_delete_command: Mocked `DatabaseDeleteCommand` class.
        database_command: Instance of `DatabaseCommand`.
    """
    parser = ArgumentParser()
    subparsers = parser.add_subparsers(dest="main", required=True)

    database_command.add_parser(subparsers)

    # Ensure subcommand `add_parser` methods are called with subparsers
    mock_info_command.return_value.add_parser.assert_called_once()
    mock_get_command.return_value.add_parser.assert_called_once()
    mock_delete_command.return_value.add_parser.assert_called_once()


def test_process_command_noop(database_command: DatabaseCommand):
    """
    Test that the process_command method exists and is callable,
    even though it performs no operation by design.

    Args:
        database_command: Instance of `DatabaseCommand`.
    """
    # process_command is a no-op, but should still be callable
    args = Namespace()
    assert database_command.process_command(args) is None
