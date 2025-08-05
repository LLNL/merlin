##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `utils.py` file of the `cli/` folder.
"""

from argparse import ArgumentParser, Namespace
from unittest.mock import MagicMock

import pytest
from pytest_mock import MockerFixture

from merlin.cli.utils import (
    get_filters_for_entity,
    get_merlin_spec_with_override,
    parse_override_vars,
    setup_db_entity_subcommands,
)


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
    return mocker.patch("merlin.cli.utils.ENTITY_REGISTRY", entity_registry)


class TestParseOverrideVars:
    def test_returns_none_if_input_is_none(self):
        """Should return None when the input variable list is None."""
        assert parse_override_vars(None) is None

    def test_parses_valid_string_and_int_values(self):
        """Should parse valid KEY=value strings into a dictionary with proper types."""
        input_vars = ["FOO=bar", "COUNT=42"]
        expected = {"FOO": "bar", "COUNT": 42}
        assert parse_override_vars(input_vars) == expected

    def test_raises_if_missing_equal_sign(self):
        """Should raise ValueError if '=' is missing in a variable assignment."""
        with pytest.raises(ValueError, match="requires '=' operator"):
            parse_override_vars(["FOO42"])

    def test_raises_if_multiple_equal_signs(self):
        """Should raise ValueError if multiple '=' characters are present in an assignment."""
        with pytest.raises(ValueError, match="ONE '=' operator"):
            parse_override_vars(["FOO=bar=baz"])

    def test_raises_if_invalid_key(self):
        """Should raise ValueError if the variable name is invalid (e.g., includes '$')."""
        with pytest.raises(ValueError, match="valid variable names"):
            parse_override_vars(["$FOO=bar"])

    def test_raises_if_reserved_key(self, mocker: MockerFixture):
        """
        Should raise ValueError if the key is in the set of reserved variable names.

        Args:
            mocker: PyTest mocker fixture.
        """
        mocker.patch("merlin.cli.utils.RESERVED", {"FOO"})
        with pytest.raises(ValueError, match="Cannot override reserved word"):
            parse_override_vars(["FOO=bar"])

    def test_leaves_string_if_not_int(self):
        """Should keep string values as-is if they are not integers."""
        input_vars = ["FOO=bar"]
        assert parse_override_vars(input_vars)["FOO"] == "bar"

    def test_converts_string_number_to_int(self):
        """Should convert string values that represent integers into actual int type."""
        input_vars = ["COUNT=123"]
        result = parse_override_vars(input_vars)
        assert isinstance(result["COUNT"], int)
        assert result["COUNT"] == 123


class TestGetMerlinSpecWithOverride:
    def test_returns_spec_and_filepath(self, mocker: MockerFixture):
        """
        Should return a parsed MerlinSpec and verified filepath, using all helper functions.

        Args:
            mocker: PyTest mocker fixture.
        """
        fake_args = Namespace(specification="path/to/spec.yaml", variables=["FOO=bar"])
        fake_filepath = "expanded/path/to/spec.yaml"
        fake_spec = mocker.Mock(name="MerlinSpec")

        mock_verify = mocker.patch("merlin.cli.utils.verify_filepath", return_value=fake_filepath)
        mock_override = mocker.patch("merlin.cli.utils.parse_override_vars", return_value={"FOO": "bar"})
        mock_get_spec = mocker.patch("merlin.cli.utils.get_spec_with_expansion", return_value=fake_spec)

        spec, path = get_merlin_spec_with_override(fake_args)

        mock_verify.assert_called_once_with("path/to/spec.yaml")
        mock_override.assert_called_once_with(["FOO=bar"])
        mock_get_spec.assert_called_once_with(fake_filepath, override_vars={"FOO": "bar"})

        assert spec is fake_spec
        assert path == fake_filepath


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
        mock_logger = mocker.patch("merlin.cli.utils.LOG")
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
        mock_logger = mocker.patch("merlin.cli.utils.LOG")
        args = Namespace()
        filters = get_filters_for_entity(args, "run")
        assert filters == {}
        mock_logger.error.assert_called_once_with("No filters supported for 'run'.")
