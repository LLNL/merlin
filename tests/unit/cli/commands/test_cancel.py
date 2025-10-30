##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `merlin/cli/commands/cancel.py` module.
"""

import logging
from argparse import ArgumentParser, Namespace
from unittest.mock import MagicMock, patch

import pytest
from pytest_mock import MockerFixture

from merlin.cli.commands.cancel import CancelCommand


@pytest.fixture
def mock_get_spec(mocker: MockerFixture) -> MagicMock:
    """
    Fixture that mocks the `get_merlin_spec_with_override` function.

    Prevents actual spec file loading during tests and allows verification
    of method calls and arguments.

    Args:
        mocker: PyTest mocker fixture.

    Returns:
        The mocked `get_merlin_spec_with_override` function.
    """
    mock_spec = MagicMock()
    mock_spec.name = "test_study"
    return mocker.patch(
        "merlin.cli.commands.cancel.get_merlin_spec_with_override",
        return_value=(mock_spec, "test_spec.yaml"),
    )


@pytest.fixture
def mock_study_manager(mocker: MockerFixture) -> MagicMock:
    """
    Fixture that mocks the `StudyManager` class.

    Prevents actual study management operations during tests and allows
    verification of method calls and arguments.

    Args:
        mocker: PyTest mocker fixture.

    Returns:
        The mocked `StudyManager` class.
    """
    mock_manager = mocker.patch("merlin.cli.commands.cancel.StudyManager")
    mock_instance = mock_manager.return_value
    mock_instance.cancel.return_value = {
        "study_name": "test_study",
        "runs_cancelled": 3,
        "queues_purged": ["queue1", "queue2"],
        "workers_stopped": ["worker1", "worker2"],
    }
    return mock_manager


@pytest.fixture
def command() -> CancelCommand:
    """
    Fixture that returns a fresh instance of the `CancelCommand` class.

    Useful for testing `add_parser` and `process_command` methods in isolation.

    Returns:
        A new instance of the command.
    """
    return CancelCommand()


def test_add_parser_registers_cancel_command_with_defaults(command: CancelCommand):
    """
    Test that `add_parser` correctly registers the `cancel` subcommand with proper defaults.

    Verifies that:
    - The `cancel` subcommand sets `process_command` as the handler function.
    - All optional arguments have correct default values.

    Args:
        command: Instance of the `CancelCommand` under test.
    """
    parser = ArgumentParser()
    subparsers = parser.add_subparsers(dest="subcmd", required=True)
    command.add_parser(subparsers)

    args = parser.parse_args(["cancel", "test_spec.yaml"])
    assert args.func == command.process_command
    assert args.specification == "test_spec.yaml"
    assert args.no_purge is False
    assert args.no_stop_workers is False
    assert args.no_mark_cancelled is False
    assert args.variables is None


def test_add_parser_no_purge_flag(command: CancelCommand):
    """
    Test that the `--no-purge` flag is parsed correctly.

    Args:
        command: Instance of the `CancelCommand` under test.
    """
    parser = ArgumentParser()
    subparsers = parser.add_subparsers(dest="subcmd", required=True)
    command.add_parser(subparsers)

    args = parser.parse_args(["cancel", "test_spec.yaml", "--no-purge"])
    assert args.no_purge is True


def test_add_parser_no_stop_workers_flag(command: CancelCommand):
    """
    Test that the `--no-stop-workers` flag is parsed correctly.

    Args:
        command: Instance of the `CancelCommand` under test.
    """
    parser = ArgumentParser()
    subparsers = parser.add_subparsers(dest="subcmd", required=True)
    command.add_parser(subparsers)

    args = parser.parse_args(["cancel", "test_spec.yaml", "--no-stop-workers"])
    assert args.no_stop_workers is True


def test_add_parser_no_mark_cancelled_flag(command: CancelCommand):
    """
    Test that the `--no-mark-cancelled` flag is parsed correctly.

    Args:
        command: Instance of the `CancelCommand` under test.
    """
    parser = ArgumentParser()
    subparsers = parser.add_subparsers(dest="subcmd", required=True)
    command.add_parser(subparsers)

    args = parser.parse_args(["cancel", "test_spec.yaml", "--no-mark-cancelled"])
    assert args.no_mark_cancelled is True


def test_add_parser_vars_flag_single_variable(command: CancelCommand):
    """
    Test that the `--vars` flag is parsed correctly with a single variable.

    Args:
        command: Instance of the `CancelCommand` under test.
    """
    parser = ArgumentParser()
    subparsers = parser.add_subparsers(dest="subcmd", required=True)
    command.add_parser(subparsers)

    args = parser.parse_args(["cancel", "test_spec.yaml", "--vars", "QUEUE=custom_queue"])
    assert args.variables == ["QUEUE=custom_queue"]


def test_add_parser_vars_flag_multiple_variables(command: CancelCommand):
    """
    Test that the `--vars` flag is parsed correctly with multiple variables.

    Args:
        command: Instance of the `CancelCommand` under test.
    """
    parser = ArgumentParser()
    subparsers = parser.add_subparsers(dest="subcmd", required=True)
    command.add_parser(subparsers)

    args = parser.parse_args(
        ["cancel", "test_spec.yaml", "--vars", "QUEUE=custom_queue", "WORKER=special_worker"]
    )
    assert args.variables == ["QUEUE=custom_queue", "WORKER=special_worker"]


def test_add_parser_all_flags_combined(command: CancelCommand):
    """
    Test that all flags can be used together.

    Args:
        command: Instance of the `CancelCommand` under test.
    """
    parser = ArgumentParser()
    subparsers = parser.add_subparsers(dest="subcmd", required=True)
    command.add_parser(subparsers)

    args = parser.parse_args([
        "cancel",
        "test_spec.yaml",
        "--no-purge",
        "--no-stop-workers",
        "--no-mark-cancelled",
        "--vars",
        "VAR1=value1",
        "VAR2=value2",
    ])
    assert args.no_purge is True
    assert args.no_stop_workers is True
    assert args.no_mark_cancelled is True
    assert args.variables == ["VAR1=value1", "VAR2=value2"]


def test_process_command_full_cancellation(
    command: CancelCommand,
    mock_get_spec: MagicMock,
    mock_study_manager: MagicMock,
):
    """
    Test that `process_command` performs full cancellation by default.

    Verifies that:
    - All cancellation steps are enabled (purge, stop workers, mark cancelled).
    - The correct spec is loaded.
    - Summary is logged.

    Args:
        command: Instance of the `CancelCommand` under test.
        mock_get_spec: Mocked `get_merlin_spec_with_override` function.
        mock_study_manager: Mocked `StudyManager` class.
    """
    args = Namespace(
        specification="test_spec.yaml",
        no_purge=False,
        no_stop_workers=False,
        no_mark_cancelled=False,
        variables=None,
    )

    command.process_command(args)

    # Verify spec was loaded
    mock_get_spec.assert_called_once_with(args)

    # Verify StudyManager was instantiated
    mock_study_manager.assert_called_once_with()

    # Verify cancel was called with correct parameters
    manager_instance = mock_study_manager.return_value
    manager_instance.cancel.assert_called_once_with(
        spec=mock_get_spec.return_value[0],
        purge_queues=True,
        stop_workers=True,
        mark_runs_cancelled=True,
    )


def test_process_command_no_purge_flag_passed_correctly(
    command: CancelCommand,
    mock_get_spec: MagicMock,
    mock_study_manager: MagicMock,
):
    """
    Test that `--no-purge` flag correctly sets `purge_queues=False`.

    Args:
        command: Instance of the `CancelCommand` under test.
        mock_get_spec: Mocked `get_merlin_spec_with_override` function.
        mock_study_manager: Mocked `StudyManager` class.
    """
    args = Namespace(
        specification="test_spec.yaml",
        no_purge=True,
        no_stop_workers=False,
        no_mark_cancelled=False,
        variables=None,
    )

    command.process_command(args)

    manager_instance = mock_study_manager.return_value
    manager_instance.cancel.assert_called_once_with(
        spec=mock_get_spec.return_value[0],
        purge_queues=False,
        stop_workers=True,
        mark_runs_cancelled=True,
    )


def test_process_command_no_stop_workers_flag_passed_correctly(
    command: CancelCommand,
    mock_get_spec: MagicMock,
    mock_study_manager: MagicMock,
):
    """
    Test that `--no-stop-workers` flag correctly sets `stop_workers=False`.

    Args:
        command: Instance of the `CancelCommand` under test.
        mock_get_spec: Mocked `get_merlin_spec_with_override` function.
        mock_study_manager: Mocked `StudyManager` class.
    """
    args = Namespace(
        specification="test_spec.yaml",
        no_purge=False,
        no_stop_workers=True,
        no_mark_cancelled=False,
        variables=None,
    )

    command.process_command(args)

    manager_instance = mock_study_manager.return_value
    manager_instance.cancel.assert_called_once_with(
        spec=mock_get_spec.return_value[0],
        purge_queues=True,
        stop_workers=False,
        mark_runs_cancelled=True,
    )


def test_process_command_no_mark_cancelled_flag_passed_correctly(
    command: CancelCommand,
    mock_get_spec: MagicMock,
    mock_study_manager: MagicMock,
):
    """
    Test that `--no-mark-cancelled` flag correctly sets `mark_runs_cancelled=False`.

    Args:
        command: Instance of the `CancelCommand` under test.
        mock_get_spec: Mocked `get_merlin_spec_with_override` function.
        mock_study_manager: Mocked `StudyManager` class.
    """
    args = Namespace(
        specification="test_spec.yaml",
        no_purge=False,
        no_stop_workers=False,
        no_mark_cancelled=True,
        variables=None,
    )

    command.process_command(args)

    manager_instance = mock_study_manager.return_value
    manager_instance.cancel.assert_called_once_with(
        spec=mock_get_spec.return_value[0],
        purge_queues=True,
        stop_workers=True,
        mark_runs_cancelled=False,
    )


def test_process_command_all_skip_flags_combined(
    command: CancelCommand,
    mock_get_spec: MagicMock,
    mock_study_manager: MagicMock,
):
    """
    Test that all skip flags can be used together.

    Args:
        command: Instance of the `CancelCommand` under test.
        mock_get_spec: Mocked `get_merlin_spec_with_override` function.
        mock_study_manager: Mocked `StudyManager` class.
    """
    args = Namespace(
        specification="test_spec.yaml",
        no_purge=True,
        no_stop_workers=True,
        no_mark_cancelled=True,
        variables=None,
    )

    command.process_command(args)

    manager_instance = mock_study_manager.return_value
    manager_instance.cancel.assert_called_once_with(
        spec=mock_get_spec.return_value[0],
        purge_queues=False,
        stop_workers=False,
        mark_runs_cancelled=False,
    )


def test_process_command_complex_flag_combination(
    command: CancelCommand,
    mock_get_spec: MagicMock,
    mock_study_manager: MagicMock,
):
    """
    Test a complex combination of flags to ensure proper interaction.

    Args:
        command: Instance of the `CancelCommand` under test.
        mock_get_spec: Mocked `get_merlin_spec_with_override` function.
        mock_study_manager: Mocked `StudyManager` class.
    """
    args = Namespace(
        specification="test_spec.yaml",
        no_purge=True,
        no_stop_workers=False,
        no_mark_cancelled=True,
        variables=["ENV=production"],
    )

    command.process_command(args)

    manager_instance = mock_study_manager.return_value
    manager_instance.cancel.assert_called_once_with(
        spec=mock_get_spec.return_value[0],
        purge_queues=False,
        stop_workers=True,
        mark_runs_cancelled=False,
    )


def test_process_command_logs_summary(
    command: CancelCommand,
    mock_get_spec: MagicMock,
    mock_study_manager: MagicMock,
    caplog,
):
    """
    Test that the cancellation summary is logged.

    Args:
        command: Instance of the `CancelCommand` under test.
        mock_get_spec: Mocked `get_merlin_spec_with_override` function.
        mock_study_manager: Mocked `StudyManager` class.
        caplog: PyTest fixture for capturing log output.
    """
    args = Namespace(
        specification="test_spec.yaml",
        no_purge=False,
        no_stop_workers=False,
        no_mark_cancelled=False,
        variables=None,
    )

    with caplog.at_level(logging.INFO):
        command.process_command(args)

    # Verify summary contains expected information
    log_output = caplog.text
    assert "Cancellation Summary:" in log_output
    assert "Study: test_study" in log_output
    assert "Runs cancelled: 3" in log_output
    assert "Queues purged: 2" in log_output
    assert "Workers stopped: 2" in log_output
