##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the merlin/cli/commands/database/get.py module.
"""

import pytest
from argparse import Namespace
from unittest.mock import MagicMock

from pytest_mock import MockerFixture

from merlin.cli.commands.database.get import DatabaseGetCommand

# TODO write docstrings/type hints for this file and test_info_subcommand.py
# TODO fix the remainder of the broken tests
# TODO update the documentation for the database command

@pytest.fixture
def mock_merlin_db(mocker: MockerFixture) -> MagicMock:
    """
    Fixture that mocks the MerlinDatabase class used in the `database.get` module.

    This prevents real database connections and allows inspection of database method calls.
    
    Args:
        mocker: PyTest mocker fixture.

    Returns:
        The mocked MerlinDatabase class.
    """
    return mocker.patch("merlin.cli.commands.database.get.MerlinDatabase")


@pytest.fixture
def mock_initialize_config(mocker: MockerFixture) -> MagicMock:
    """
    Fixture that mocks the `initialize_config` function in the `database.get` module.

    This prevents configuration initialization from affecting test environments.
    
    Args:
        mocker: PyTest mocker fixture.
    
    Returns:
        The mocked initialize_config function.
    """
    return mocker.patch("merlin.cli.commands.database.get.initialize_config")


@pytest.fixture
def mock_entity_registry(mocker: MockerFixture) -> MagicMock:
    """
    Fixture that replaces ENTITY_REGISTRY in the `database.get` module with a mock registry.

    This allows tests to simulate entity-specific behavior (e.g., "study", "run")
    without relying on the actual registry.
    
    Args:
        mocker: PyTest mocker fixture.

    Returns:
        The mocked entity registry dictionary.
    """
    registry = {"study": mocker.Mock(), "run": mocker.Mock()}
    mocker.patch("merlin.cli.commands.database.get.ENTITY_REGISTRY", registry)
    return registry


@pytest.fixture
def command() -> DatabaseGetCommand:
    """
    Fixture that returns a fresh instance of the `DatabaseGetCommand` class.

    Useful for testing `add_parser` and `process_command` methods in isolation.

    Returns:
        A new instance of the command.
    """
    return DatabaseGetCommand()


def test_process_command_local_triggers_initialize_config(
    command: DatabaseGetCommand,
    mock_initialize_config: MagicMock,
    mock_merlin_db: MagicMock,
):
    """
    Test that `initialize_config` is called with `local_mode=True` when the `--local` flag is set.

    Args:
        command: Instance of the `DatabaseGetCommand` under test.
        mock_initialize_config: Mocked `initialize_config` function.
        mock_merlin_db: Mocked `MerlinDatabase` class.
    """
    args = Namespace(get_type="everything", local=True)
    command.process_command(args)
    mock_initialize_config.assert_called_once_with(local_mode=True)


def test_process_command_get_everything(command: DatabaseGetCommand, mock_merlin_db: MagicMock, mocker: MockerFixture):
    """
    Test that calling `process_command` with `get_type='everything'` retrieves all items
    and passes them to `_print_items`.

    Args:
        command: Instance of the `DatabaseGetCommand` under test.
        mock_merlin_db: Mocked `MerlinDatabase` class.
        mocker: PyTest mocker fixture..
    """
    mock_print_items = mocker.patch.object(command, "_print_items")
    args = Namespace(get_type="everything", local=False)
    command.process_command(args)
    mock_print_items.assert_called_once_with(mock_merlin_db.return_value.get_everything.return_value, "Nothing found in the database.")


def test_process_command_get_specific_entities(
    command: DatabaseGetCommand,
    mock_merlin_db: MagicMock,
    mock_entity_registry: MagicMock,
    mocker: MockerFixture,
):
    """
    Test that `process_command` retrieves specific entities and calls
    `_print_items` with the results.

    Args:
        command: Instance of the `DatabaseGetCommand` under test.
        mock_merlin_db: Mocked `MerlinDatabase` class.
        mock_entity_registry: Mocked ENTITY_REGISTRY mapping entity types to handler classes.
        mocker: PyTest mocker fixture.
    """
    mock_print_items = mocker.patch.object(command, "_print_items")
    args = Namespace(get_type="study", entity=["s1", "s2"], local=False)
    command.process_command(args)

    merlin_db_instance = mock_merlin_db.return_value
    merlin_db_instance.get.assert_any_call("study", "s1")
    merlin_db_instance.get.assert_any_call("study", "s2")
    assert merlin_db_instance.get.call_count == 2
    mock_print_items.assert_called_once()


def test_process_command_get_all_entities_with_filters(
    command: DatabaseGetCommand,
    mock_merlin_db: MagicMock,
    mocker: MockerFixture,
):
    """
    Test that `process_command` retrieves all entities of a given type with applied filters.

    Mocks the helper functions `get_singular_of_entity` and `get_filters_for_entity` to return
    a valid entity type and a non-empty filter dictionary, then verifies that `get_all` is called
    with the correct arguments and `_print_items` is invoked.

    Args:
        command: Instance of the `DatabaseGetCommand` under test.
        mock_merlin_db: Mocked `MerlinDatabase` class.
        mocker: PyTest mocker fixture.
    """
    mocker.patch("merlin.cli.commands.database.get.get_singular_of_entity", return_value="run")
    mocker.patch("merlin.cli.commands.database.get.get_filters_for_entity", return_value={"status": "complete"})
    mock_print_items = mocker.patch.object(command, "_print_items")

    args = Namespace(get_type="all-runs", local=False)
    command.process_command(args)

    mock_merlin_db.return_value.get_all.assert_called_once_with("run", filters={"status": "complete"})
    mock_print_items.assert_called_once()


def test_process_command_get_all_entities_without_filters(
    command: DatabaseGetCommand,
    mock_merlin_db: MagicMock,
    mocker: MockerFixture,
):
    """
    Test that `process_command` retrieves all entities of a given type without filters.

    Mocks the helper functions `get_singular_of_entity` and `get_filters_for_entity` to return
    a valid entity type and an empty filter dictionary, then verifies that `get_all` is called
    with the expected arguments.

    Args:
        command: Instance of the `DatabaseGetCommand` under test.
        mock_merlin_db: Mocked `MerlinDatabase` class.
        mocker: PyTest mocker fixture.
    """
    mocker.patch("merlin.cli.commands.database.get.get_singular_of_entity", return_value="run")
    mocker.patch("merlin.cli.commands.database.get.get_filters_for_entity", return_value={})
    mocker.patch.object(command, "_print_items")

    args = Namespace(get_type="all-runs", local=False)
    command.process_command(args)

    mock_merlin_db.return_value.get_al
