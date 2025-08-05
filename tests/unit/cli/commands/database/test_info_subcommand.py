##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the merlin/cli/commands/database/info.py module.
"""

from argparse import ArgumentParser, Namespace
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from merlin.cli.commands.database.info import DatabaseInfoCommand


@pytest.fixture
def mock_initialize_config(mocker: MockerFixture) -> MagicMock:
    """
    Fixture that mocks the `initialize_config` function in the `database.info` module.

    Prevents actual configuration initialization during tests and allows verification
    that it is called with the correct parameters.

    Args:
        mocker: PyTest mocker fixture.

    Returns:
        The mocked `initialize_config` function.
    """
    return mocker.patch("merlin.cli.commands.database.info.initialize_config")


@pytest.fixture
def mock_merlin_db(mocker: MockerFixture) -> MagicMock:
    """
    Fixture that mocks the `MerlinDatabase` class in the `database.info` module.

    Prevents real database connections during tests and enables control over
    return values and method call assertions.

    Args:
        mocker: PyTest mocker fixture.

    Returns:
        The mocked `MerlinDatabase` class.
    """
    return mocker.patch("merlin.cli.commands.database.info.MerlinDatabase")


@pytest.fixture
def command() -> DatabaseInfoCommand:
    """
    Fixture that returns a fresh instance of the `DatabaseInfoCommand` class.

    Useful for testing `add_parser` and `process_command` methods in isolation.

    Returns:
        A new instance of the command.
    """
    return DatabaseInfoCommand()


def test_add_parser_registers_info_command(command: DatabaseInfoCommand):
    """
    Test that `add_parser` correctly registers the `info` subcommand and its arguments.

    Verifies that:
    - The `info` subcommand sets `process_command` as the handler function.
    - The `--max-preview` optional argument is parsed correctly.

    Args:
        command: Instance of the `DatabaseInfoCommand` under test.
    """
    parser = ArgumentParser()
    subparsers = parser.add_subparsers(dest="subcmd", required=True)
    command.add_parser(subparsers)

    args = parser.parse_args(["info"])
    assert args.func == command.process_command

    args_with_flag = parser.parse_args(["info", "--max-preview", "5"])
    assert args_with_flag.max_preview == 5


def test_process_command_with_local_flag_calls_initialize_config(
    command: DatabaseInfoCommand, mock_initialize_config: MagicMock, mock_merlin_db: MagicMock
):
    """
    Test that `process_command` calls `initialize_config(local_mode=True)` when the `--local` flag is set.

    Also verifies that `MerlinDatabase.info()` is called with the provided `max_preview` value.

    Args:
        command: Instance of the `DatabaseInfoCommand` under test.
        mock_initialize_config: Mocked `initialize_config` function.
        mock_merlin_db: Mocked `MerlinDatabase` class.
    """
    args = Namespace(local=True, max_preview=2)
    command.process_command(args)

    mock_initialize_config.assert_called_once_with(local_mode=True)
    mock_merlin_db.return_value.info.assert_called_once_with(max_preview=2)


def test_process_command_without_local_flag_does_not_call_initialize_config(
    command: DatabaseInfoCommand, mock_initialize_config: MagicMock, mock_merlin_db: MagicMock
):
    """
    Test that `process_command` does not call `initialize_config` when `--local` is not set.

    Verifies that `MerlinDatabase.info()` is still called with the expected `max_preview` value.

    Args:
        command: Instance of the `DatabaseInfoCommand` under test.
        mock_initialize_config: Mocked `initialize_config` function.
        mock_merlin_db: Mocked `MerlinDatabase` class.
    """
    args = Namespace(local=False, max_preview=4)
    command.process_command(args)

    mock_initialize_config.assert_not_called()
    mock_merlin_db.return_value.info.assert_called_once_with(max_preview=4)
