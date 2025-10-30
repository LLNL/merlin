##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the merlin/cli/commands/database/utils.py module.
"""

from argparse import ArgumentParser, Namespace
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from merlin.cli.commands.database.utils import get_filters_for_entity, setup_db_entity_subcommands


@pytest.fixture
def patched_registry(mocker: MockerFixture) -> MagicMock:
    entity_registry = {
        "study": {
            "identifiers": "study_id",
            "ident_help": "Study ID(s) to {verb}.",
            "filters": [
                {"name": "user", "type": str},
                {"name": "status", "type": str, "nargs": "+"},
            ],
        },
        "run": {
            "identifiers": "run_id",
            "ident_help": "Run ID(s) to {verb}.",
            "filters": [],
        },
    }
    return mocker.patch("merlin.cli.commands.database.utils.ENTITY_REGISTRY", entity_registry)


class TestSetupDbEntitySubcommands:
    """
    Unit tests for the `setup_db_entity_subcommands` function in `cli/utils.py`.
    """

    def test_creates_expected_subcommands(self, patched_registry: MagicMock):
        """
        Test that both singular and all-entity subcommands are added for each registered entity.

        Args:
            patched_registry: Mocked ENTITY_REGISTRY.
        """
        parser = ArgumentParser()
        subparsers = parser.add_subparsers(dest="entity")

        result = setup_db_entity_subcommands(subparsers, "delete")
        assert "study" in result
        assert "all-studies" in result

        study_args = result["study"].parse_args(["study123"])
        assert study_args.entity == ["study123"]

        all_args = result["all-studies"].parse_args(["--status", "running", "paused"])
        assert all_args.status == ["running", "paused"]


class TestGetFiltersForEntity:
    """
    Unit tests for the get_filters_for_entity utility function.
    """

    def test_returns_correct_filters(self, patched_registry: MagicMock):
        """
        Test that get_filters_for_entity returns only non-None filter values.

        Args:
            patched_registry: Mocked ENTITY_REGISTRY.
        """
        args = Namespace(user="alice", status=["complete", "failed"])
        filters = get_filters_for_entity(args, "study")
        assert filters == {"user": "alice", "status": ["complete", "failed"]}

    def test_ignores_none_values(self, patched_registry: MagicMock):
        """
        Test that filters with None values are excluded.

        Args:
            patched_registry: Mocked ENTITY_REGISTRY.
        """
        args = Namespace(user=None, status=["running"])
        filters = get_filters_for_entity(args, "study")
        assert filters == {"status": ["running"]}

    def test_invalid_entity_returns_empty_dict(self, patched_registry: MagicMock, mocker: MockerFixture):
        """
        Test that invalid entity types return an empty dict and log an error.

        Args:
            patched_registry: Mocked ENTITY_REGISTRY.
            mocker: Pytest mocker fixture for capturing logs.
        """
        mock_logger = mocker.patch("merlin.cli.commands.database.utils.LOG")
        args = Namespace()
        filters = get_filters_for_entity(args, "invalid")
        assert filters == {}
        mock_logger.error.assert_called_once_with("Invalid entity: 'invalid'.")

    def test_no_filters_defined_returns_empty_dict(self, patched_registry: MagicMock, mocker: MockerFixture):
        """
        Test that an entity with no filter config logs and returns an empty dict.

        Args:
            patched_registry: Mocked ENTITY_REGISTRY.
            mocker: Pytest mocker fixture for capturing logs.
        """
        mock_logger = mocker.patch("merlin.cli.commands.database.utils.LOG")
        args = Namespace()
        filters = get_filters_for_entity(args, "run")
        assert filters == {}
        mock_logger.error.assert_called_once_with("No filters supported for 'run'.")
