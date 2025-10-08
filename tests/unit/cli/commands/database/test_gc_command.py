##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the merlin/cli/commands/database/garbage_collection.py module.
"""

from argparse import ArgumentParser, Namespace
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from merlin.cli.commands.database.garbage_collection import DatabaseGarbageCollectionCommand


@pytest.fixture
def mock_garbage_collector(mocker: MockerFixture) -> MagicMock:
    """
    Fixture that mocks the `DatabaseGarbageCollector` class.

    Prevents actual database operations during tests and allows verification
    of method calls and arguments.

    Args:
        mocker: PyTest mocker fixture.

    Returns:
        The mocked `DatabaseGarbageCollector` class.
    """
    return mocker.patch("merlin.cli.commands.database.garbage_collection.DatabaseGarbageCollector")


@pytest.fixture
def command() -> DatabaseGarbageCollectionCommand:
    """
    Fixture that returns a fresh instance of the `DatabaseGarbageCollectionCommand` class.

    Useful for testing `add_parser` and `process_command` methods in isolation.

    Returns:
        A new instance of the command.
    """
    return DatabaseGarbageCollectionCommand()


def test_add_parser_registers_gc_command_with_defaults(command: DatabaseGarbageCollectionCommand):
    """
    Test that `add_parser` correctly registers the `gc` subcommand with proper defaults.

    Verifies that:
    - The `gc` subcommand sets `process_command` as the handler function.
    - All optional arguments have correct default values.

    Args:
        command: Instance of the `DatabaseGarbageCollectionCommand` under test.
    """
    parser = ArgumentParser()
    subparsers = parser.add_subparsers(dest="subcmd", required=True)
    command.add_parser(subparsers)

    args = parser.parse_args(["gc"])
    assert args.func == command.process_command
    assert args.dry_run is False
    assert args.skip_runs is False
    assert args.skip_workers is False
    assert args.skip_studies is False
    assert args.force is False


def test_add_parser_dry_run_flag(command: DatabaseGarbageCollectionCommand):
    """
    Test that the `--dry-run` flag is parsed correctly.

    Args:
        command: Instance of the `DatabaseGarbageCollectionCommand` under test.
    """
    parser = ArgumentParser()
    subparsers = parser.add_subparsers(dest="subcmd", required=True)
    command.add_parser(subparsers)

    args = parser.parse_args(["gc", "--dry-run"])
    assert args.dry_run is True


def test_add_parser_skip_flags(command: DatabaseGarbageCollectionCommand):
    """
    Test that skip flags (`--skip-runs`, `--skip-workers`, `--skip-studies`) are parsed correctly.

    Args:
        command: Instance of the `DatabaseGarbageCollectionCommand` under test.
    """
    parser = ArgumentParser()
    subparsers = parser.add_subparsers(dest="subcmd", required=True)
    command.add_parser(subparsers)

    args = parser.parse_args(["gc", "--skip-runs", "--skip-workers", "--skip-studies"])
    assert args.skip_runs is True
    assert args.skip_workers is True
    assert args.skip_studies is True


def test_add_parser_force_flag(command: DatabaseGarbageCollectionCommand):
    """
    Test that the `--force` / `-f` flag is parsed correctly.

    Args:
        command: Instance of the `DatabaseGarbageCollectionCommand` under test.
    """
    parser = ArgumentParser()
    subparsers = parser.add_subparsers(dest="subcmd", required=True)
    command.add_parser(subparsers)

    # Test long form
    args_long = parser.parse_args(["gc", "--force"])
    assert args_long.force is True

    # Test short form
    args_short = parser.parse_args(["gc", "-f"])
    assert args_short.force is True


@pytest.mark.parametrize("alias", ["garbage-collect", "cleanup"])
def test_add_parser_aliases(command: DatabaseGarbageCollectionCommand, alias: str):
    """
    Test that the aliases for the `gc` command are properly registered.

    Args:
        command: Instance of the `DatabaseGarbageCollectionCommand` under test.
        alias: The alias to test.
    """
    parser = ArgumentParser()
    subparsers = parser.add_subparsers(dest="subcmd", required=True)
    command.add_parser(subparsers)

    args = parser.parse_args([alias])
    assert args.func == command.process_command


def test_process_command_dry_run_calls_scan_only(
    command: DatabaseGarbageCollectionCommand,
    mock_garbage_collector: MagicMock,
):
    """
    Test that `process_command` calls only `scan()` when `--dry-run` is set.

    Verifies that:
    - `scan()` is called with correct parameters.
    - `scan_and_clean()` is NOT called.

    Args:
        command: Instance of the `DatabaseGarbageCollectionCommand` under test.
        mock_garbage_collector: Mocked `DatabaseGarbageCollector` class.
    """
    args = Namespace(
        dry_run=True,
        skip_runs=False,
        skip_workers=False,
        skip_studies=False,
        force=False,
    )
    command.process_command(args)

    collector_instance = mock_garbage_collector.return_value
    collector_instance.scan.assert_called_once_with(
        check_runs=True,
        check_workers=True,
        check_studies=True,
    )
    collector_instance.scan_and_clean.assert_not_called()


def test_process_command_normal_mode_calls_scan_and_clean(
    command: DatabaseGarbageCollectionCommand,
    mock_garbage_collector: MagicMock,
):
    """
    Test that `process_command` calls `scan_and_clean()` when not in dry-run mode.

    Verifies that:
    - `scan_and_clean()` is called with correct parameters.
    - `scan()` is NOT called.

    Args:
        command: Instance of the `DatabaseGarbageCollectionCommand` under test.
        mock_garbage_collector: Mocked `DatabaseGarbageCollector` class.
    """
    args = Namespace(
        dry_run=False,
        skip_runs=False,
        skip_workers=False,
        skip_studies=False,
        force=False,
    )
    command.process_command(args)

    collector_instance = mock_garbage_collector.return_value
    collector_instance.scan_and_clean.assert_called_once_with(
        check_runs=True,
        check_workers=True,
        check_studies=True,
        force=False,
    )
    collector_instance.scan.assert_not_called()


def test_process_command_skip_runs_flag_passed_correctly(
    command: DatabaseGarbageCollectionCommand,
    mock_garbage_collector: MagicMock,
):
    """
    Test that `--skip-runs` flag correctly sets `check_runs=False`.

    Args:
        command: Instance of the `DatabaseGarbageCollectionCommand` under test.
        mock_garbage_collector: Mocked `DatabaseGarbageCollector` class.
    """
    args = Namespace(
        dry_run=True,
        skip_runs=True,
        skip_workers=False,
        skip_studies=False,
        force=False,
    )
    command.process_command(args)

    collector_instance = mock_garbage_collector.return_value
    collector_instance.scan.assert_called_once_with(
        check_runs=False,
        check_workers=True,
        check_studies=True,
    )


def test_process_command_skip_workers_flag_passed_correctly(
    command: DatabaseGarbageCollectionCommand,
    mock_garbage_collector: MagicMock,
):
    """
    Test that `--skip-workers` flag correctly sets `check_workers=False`.

    Args:
        command: Instance of the `DatabaseGarbageCollectionCommand` under test.
        mock_garbage_collector: Mocked `DatabaseGarbageCollector` class.
    """
    args = Namespace(
        dry_run=True,
        skip_runs=False,
        skip_workers=True,
        skip_studies=False,
        force=False,
    )
    command.process_command(args)

    collector_instance = mock_garbage_collector.return_value
    collector_instance.scan.assert_called_once_with(
        check_runs=True,
        check_workers=False,
        check_studies=True,
    )


def test_process_command_skip_studies_flag_passed_correctly(
    command: DatabaseGarbageCollectionCommand,
    mock_garbage_collector: MagicMock,
):
    """
    Test that `--skip-studies` flag correctly sets `check_studies=False`.

    Args:
        command: Instance of the `DatabaseGarbageCollectionCommand` under test.
        mock_garbage_collector: Mocked `DatabaseGarbageCollector` class.
    """
    args = Namespace(
        dry_run=True,
        skip_runs=False,
        skip_workers=False,
        skip_studies=True,
        force=False,
    )
    command.process_command(args)

    collector_instance = mock_garbage_collector.return_value
    collector_instance.scan.assert_called_once_with(
        check_runs=True,
        check_workers=True,
        check_studies=False,
    )


def test_process_command_all_skip_flags_combined(
    command: DatabaseGarbageCollectionCommand,
    mock_garbage_collector: MagicMock,
):
    """
    Test that all skip flags can be used together.

    Args:
        command: Instance of the `DatabaseGarbageCollectionCommand` under test.
        mock_garbage_collector: Mocked `DatabaseGarbageCollector` class.
    """
    args = Namespace(
        dry_run=True,
        skip_runs=True,
        skip_workers=True,
        skip_studies=True,
        force=False,
    )
    command.process_command(args)

    collector_instance = mock_garbage_collector.return_value
    collector_instance.scan.assert_called_once_with(
        check_runs=False,
        check_workers=False,
        check_studies=False,
    )


def test_process_command_force_flag_passed_correctly(
    command: DatabaseGarbageCollectionCommand,
    mock_garbage_collector: MagicMock,
):
    """
    Test that `--force` flag is correctly passed to `scan_and_clean()`.

    Args:
        command: Instance of the `DatabaseGarbageCollectionCommand` under test.
        mock_garbage_collector: Mocked `DatabaseGarbageCollector` class.
    """
    args = Namespace(
        dry_run=False,
        skip_runs=False,
        skip_workers=False,
        skip_studies=False,
        force=True,
    )
    command.process_command(args)

    collector_instance = mock_garbage_collector.return_value
    collector_instance.scan_and_clean.assert_called_once_with(
        check_runs=True,
        check_workers=True,
        check_studies=True,
        force=True,
    )


def test_process_command_force_flag_not_passed_to_scan(
    command: DatabaseGarbageCollectionCommand,
    mock_garbage_collector: MagicMock,
):
    """
    Test that `--force` flag does not affect dry-run mode (scan only).

    The `force` flag should only affect `scan_and_clean()`, not `scan()`.

    Args:
        command: Instance of the `DatabaseGarbageCollectionCommand` under test.
        mock_garbage_collector: Mocked `DatabaseGarbageCollector` class.
    """
    args = Namespace(
        dry_run=True,
        skip_runs=False,
        skip_workers=False,
        skip_studies=False,
        force=True,  # Should be ignored in dry-run mode
    )
    command.process_command(args)

    collector_instance = mock_garbage_collector.return_value
    collector_instance.scan.assert_called_once_with(
        check_runs=True,
        check_workers=True,
        check_studies=True,
    )
    # Verify force wasn't passed to scan (it doesn't accept that parameter)
    assert "force" not in collector_instance.scan.call_args[1]


def test_process_command_complex_flag_combination(
    command: DatabaseGarbageCollectionCommand,
    mock_garbage_collector: MagicMock,
):
    """
    Test a complex combination of flags to ensure proper interaction.

    Args:
        command: Instance of the `DatabaseGarbageCollectionCommand` under test.
        mock_garbage_collector: Mocked `DatabaseGarbageCollector` class.
    """
    args = Namespace(
        dry_run=False,
        skip_runs=True,
        skip_workers=False,
        skip_studies=True,
        force=True,
    )
    command.process_command(args)

    collector_instance = mock_garbage_collector.return_value
    collector_instance.scan_and_clean.assert_called_once_with(
        check_runs=False,
        check_workers=True,
        check_studies=False,
        force=True,
    )


def test_garbage_collector_instantiation(
    command: DatabaseGarbageCollectionCommand,
    mock_garbage_collector: MagicMock,
):
    """
    Test that `DatabaseGarbageCollector` is instantiated exactly once per command execution.

    Args:
        command: Instance of the `DatabaseGarbageCollectionCommand` under test.
        mock_garbage_collector: Mocked `DatabaseGarbageCollector` class.
    """
    args = Namespace(
        dry_run=True,
        skip_runs=False,
        skip_workers=False,
        skip_studies=False,
        force=False,
    )
    command.process_command(args)

    mock_garbage_collector.assert_called_once_with()
