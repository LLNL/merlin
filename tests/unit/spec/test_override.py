##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `override.py` file of the `spec/` folder.
"""

from copy import deepcopy

import pytest
from pytest_mock import MockerFixture

from merlin.spec.expansion import error_override_vars, replace_override_vars


def test_error_override_vars_none():
    """
    Test that the function returns silently when `override_vars` is None.
    """
    # Should do nothing
    error_override_vars(None, "fake/path.yaml")  # No exception expected


def test_error_override_vars_all_vars_present(mocker: MockerFixture):
    """
    Test that the function returns silently when all override vars exist in the spec file.

    Args:
        mocker: PyTest mocker fixture.
    """
    fake_file_content = "FOO: value\nBAR: other"
    mocker.patch("builtins.open", mocker.mock_open(read_data=fake_file_content))

    override_vars = {"FOO": "new_val", "BAR": "another_val"}

    # Should not raise
    error_override_vars(override_vars, "fake/path.yaml")


def test_error_override_vars_raises_on_missing_var(mocker: MockerFixture):
    """
    Test that the function raises a ValueError if a variable is missing from the spec.

    Args:
        mocker: PyTest mocker fixture.
    """
    fake_file_content = "FOO: something"
    mocker.patch("builtins.open", mocker.mock_open(read_data=fake_file_content))

    override_vars = {"FOO": "new_val", "MISSING": "oops"}

    with pytest.raises(ValueError, match=r"override variable 'MISSING' not found"):
        error_override_vars(override_vars, "fake/path.yaml")


def test_replace_override_vars_none():
    """
    If `override_vars` is None, the function should return the original environment as-is.
    """
    env = {"variables": {"FOO": "bar"}}
    result = replace_override_vars(env, None)
    assert result == env
    assert result is env  # same object returned


def test_replace_override_vars_applies_overrides():
    """
    Overrides should be applied to matching keys in nested env dictionary.
    """
    env = {"variables": {"FOO": "original_val", "BAR": "something $(BAZ)"}, "labels": {"LABEL1": "use $(FOO) and $(BAZ)"}}
    override_vars = {"FOO": "override_foo", "BAZ": "override_baz"}

    result = replace_override_vars(env, override_vars)

    # Should not modify original
    assert result is not env
    assert result["variables"]["FOO"] == "override_foo"  # overridden
    assert result["variables"]["BAR"] == "something $(BAZ)"  # not overridden


def test_replace_override_vars_ignores_unmatched_keys():
    """
    Overrides for non-existent variables should be ignored without error.
    """
    env = {"variables": {"FOO": "bar"}}
    override_vars = {"UNUSED": "noop"}

    result = replace_override_vars(env, override_vars)
    assert result["variables"]["FOO"] == "bar"
    assert "UNUSED" not in result["variables"]


def test_replace_override_vars_does_not_mutate_input():
    """
    The original env should not be mutated.
    """
    env = {"variables": {"FOO": "bar"}}
    env_copy = deepcopy(env)

    override_vars = {"FOO": "new_value"}
    _ = replace_override_vars(env, override_vars)

    assert env == env_copy
