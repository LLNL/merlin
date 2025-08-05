##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the merlin/cli/commands/database/delete.py module.
"""

import pytest
from argparse import Namespace
from pytest_mock import MockerFixture
from unittest.mock import MagicMock

from merlin.cli.commands.database.delete import DatabaseDeleteCommand
from tests.fixture_types import FixtureDict


@pytest.fixture
def mock_merlin_db(mocker: MockerFixture) -> MagicMock:
    """
    Fixture to patch the `MerlinDatabase` class with a mock object.

    Args:
        mocker: PyTest mocker fixture.

    Returns:
        Mocked `MerlinDatabase` class.
    """
    return mocker.patch("merlin.cli.commands.database.delete.MerlinDatabase")


@pytest.fixture
def mock_initialize_config(mocker: MockerFixture) -> MagicMock:
    """
    Fixture to patch the `initialize_config` function.

    Args:
        mocker: PyTest mocker fixture.

    Returns:
        Mocked `initialize_config` function.
    """
    return mocker.patch("merlin.cli.commands.database.delete.initialize_config")


@pytest.fixture
def mock_entity_registry(mocker: MockerFixture) -> FixtureDict[str, MagicMock]:
    """
    Fixture to patch the `ENTITY_REGISTRY` with mocked entity managers.

    Args:
        mocker: PyTest mocker fixture.

    Returns:
        A dictionary mapping entity names to mock managers.
    """
    registry = {"study": mocker.Mock(), "run": mocker.Mock()}
    mocker.patch("merlin.cli.commands.database.delete.ENTITY_REGISTRY", registry)
    return registry


@pytest.fixture
def command() -> DatabaseDeleteCommand:
    """
    Fixture to create an instance of `DatabaseDeleteCommand`.

    Returns:
        Instance under test.
    """
    return DatabaseDeleteCommand()


def test_process_command_local_triggers_initialize_config(
    command: DatabaseDeleteCommand,
    mocker: MockerFixture,
    mock_initialize_config: MagicMock,
    mock_merlin_db: MagicMock,
):
    """
    Test that `initialize_config` is called with `local_mode=True` when the local flag is set.

    Args:
        command: Instance of `DatabaseDeleteCommand`.
        mocker: Pytest mocker fixture.
        mock_initialize_config: Mocked `initialize_config` function.
        mock_merlin_db: Mocked `MerlinDatabase` class.
    """
    args = Namespace(delete_type="everything", force=True, local=True)
    command.process_command(args)
    mock_initialize_config.assert_called_once_with(local_mode=True)


def test_process_command_delete_everything(command: DatabaseDeleteCommand, mock_merlin_db: MagicMock):
    """
    Test that `delete_everything` is called when `delete_type` is 'everything'.

    Args:
        command: Instance of `DatabaseDeleteCommand`.
        mock_merlin_db: Mocked `MerlinDatabase` class.
    """
    args = Namespace(delete_type="everything", force=True, local=False)
    command.process_command(args)
    mock_merlin_db.return_value.delete_everything.assert_called_once_with(force=True)


def test_process_command_delete_specific_entities(
    command: DatabaseDeleteCommand,
    mocker: MockerFixture,
    mock_merlin_db: MagicMock,
    mock_entity_registry: MagicMock,
):
    """
    Test deletion of specific entities.

    Args:
        command: Instance of `DatabaseDeleteCommand`.
        mocker: Pytest mocker fixture.
        mock_merlin_db: Mocked `MerlinDatabase` class.
        mock_entity_registry: Patched `ENTITY_REGISTRY`.
    """
    args = Namespace(delete_type="study", entity=["abc", "def"], keep_associated_runs=False, local=False)
    command.process_command(args)

    merlin_db_instance = mock_merlin_db.return_value
    merlin_db_instance.delete.assert_any_call("study", "abc", remove_associated_runs=True)
    merlin_db_instance.delete.assert_any_call("study", "def", remove_associated_runs=True)
    assert merlin_db_instance.delete.call_count == 2


def test_process_command_delete_all_entities_with_filters(
    command: DatabaseDeleteCommand,
    mocker: MockerFixture,
    mock_merlin_db: MagicMock,
):
    """
    Test deletion of all entities of a type using filters.

    Args:
        command: Instance of `DatabaseDeleteCommand`.
        mocker: Pytest mocker fixture.
        mock_merlin_db: Mocked `MerlinDatabase` class.
    """
    mocker.patch("merlin.cli.commands.database.delete.get_singular_of_entity", return_value="run")
    mocker.patch("merlin.cli.commands.database.delete.get_filters_for_entity", return_value={"status": "complete"})

    dummy_entity = mocker.Mock()
    dummy_entity.id = "r1"
    mock_merlin_db.return_value.get_all.return_value = [dummy_entity]

    args = Namespace(delete_type="all-runs", local=False)
    command.process_command(args)

    merlin_db_instance = mock_merlin_db.return_value
    merlin_db_instance.get_all.assert_called_once_with("run", filters={"status": "complete"})
    merlin_db_instance.delete.assert_called_once_with("run", "r1")


def test_process_command_delete_all_entities_no_filters(
    command: DatabaseDeleteCommand,
    mocker: MockerFixture,
    mock_merlin_db: MagicMock,
):
    """
    Test deletion of all entities of a type when no filters are provided.

    Args:
        command: Instance of `DatabaseDeleteCommand`.
        mocker: Pytest mocker fixture.
        mock_merlin_db: Mocked `MerlinDatabase` class.
    """
    mocker.patch("merlin.cli.commands.database.delete.get_singular_of_entity", return_value="run")
    mocker.patch("merlin.cli.commands.database.delete.get_filters_for_entity", return_value={})

    args = Namespace(delete_type="all-runs", local=False)
    command.process_command(args)

    mock_merlin_db.return_value.delete_all.assert_called_once_with("run")


def test_process_command_unrecognized_type_logs_error(command: DatabaseDeleteCommand, mocker: MockerFixture):
    """
    Test that an error is logged when `delete_type` is unrecognized.

    Args:
        command: Instance of `DatabaseDeleteCommand`.
        mocker: Pytest mocker fixture.
    """
    mock_log = mocker.patch("merlin.cli.commands.database.delete.LOG")
    args = Namespace(delete_type="bad-type", local=False)
    command.process_command(args)
    mock_log.error.assert_called_once_with("Unrecognized delete_type: bad-type")


def test_extract_entity_kwargs_study_flag(command: DatabaseDeleteCommand):
    """
    Test `_extract_entity_kwargs` returns correct kwargs for study entities.

    Args:
        command: Instance of `DatabaseDeleteCommand`.
    """
    args = Namespace(keep_associated_runs=False)
    result = command._extract_entity_kwargs(args, "study")
    assert result == {"remove_associated_runs": True}


def test_extract_entity_kwargs_unrecognized_type(command: DatabaseDeleteCommand):
    """
    Test `_extract_entity_kwargs` returns an empty dict for unrecognized entity types.

    Args:
        command: Instance of `DatabaseDeleteCommand`.
    """
    args = Namespace()
    result = command._extract_entity_kwargs(args, "run")
    assert result == {}
