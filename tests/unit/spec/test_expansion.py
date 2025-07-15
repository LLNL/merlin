##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `expansion.py` file of the `spec/` folder.
"""

import logging
import os
from copy import deepcopy
from unittest.mock import MagicMock

import pytest
from _pytest.capture import CaptureFixture
from pytest_mock import MockerFixture

from merlin.common.enums import ReturnCode
from merlin.spec.expansion import (
    determine_user_variables,
    expand_by_line,
    expand_env_vars,
    expand_line,
    expand_spec_no_study,
    get_spec_with_expansion,
    parameter_substitutions_for_cmd,
    parameter_substitutions_for_sample,
    var_ref,
)


@pytest.fixture
def mock_contains_token(mocker: MockerFixture) -> MagicMock:
    """
    Fixture to mock the `contains_token` function.

    Args:
        mocker: PyTest mocker fixture.

    Returns:
        A mock object for `contains_token`.
    """
    return mocker.patch("merlin.spec.expansion.contains_token")


@pytest.fixture
def mock_contains_shell_ref(mocker: MockerFixture) -> MagicMock:
    """
    Fixture to mock the `contains_shell_ref` function.

    Args:
        mocker: PyTest mocker fixture.

    Returns:
        A mock object for `contains_shell_ref`.
    """
    return mocker.patch("merlin.spec.expansion.contains_shell_ref")


@pytest.fixture
def mock_var_ref(mocker: MockerFixture) -> MagicMock:
    """
    Fixture to mock the `var_ref` function.

    Args:
        mocker: PyTest mocker fixture.

    Returns:
        A mock object for `var_ref`.
    """
    return mocker.patch("merlin.spec.expansion.var_ref")


@pytest.fixture
def mock_expand_line(mocker: MockerFixture) -> MagicMock:
    """
    Fixture to mock the `expand_line` function.

    Args:
        mocker: PyTest mocker fixture.

    Returns:
        MagicMock: The mocked version of `expand_line`.
    """
    return mocker.patch("merlin.spec.expansion.expand_line")


def test_var_ref_returns_formatted_string(mock_contains_token: MagicMock):
    """
    Test that `var_ref` returns a correctly formatted variable reference
    when the input does not already contain a token.

    Args:
        mock_contains_token: A mock object for `contains_token`.
    """
    mock_contains_token.return_value = False
    result = var_ref("example")
    assert result == "$(EXAMPLE)"


def test_var_ref_returns_original_if_token_found(caplog: CaptureFixture, mock_contains_token: MagicMock):
    """
    Test that `var_ref` returns the original uppercased string and logs a warning
    when the input already contains a token.

    Args:
        caplog: PyTest caplog fixture.
        mock_contains_token: A mock object for `contains_token`.
    """
    caplog.set_level(logging.WARNING)
    mock_contains_token.return_value = True
    result = var_ref("already_token")
    assert result == "ALREADY_TOKEN"
    assert "Bad var_ref usage on string 'ALREADY_TOKEN'." in caplog.text


def test_var_ref_uppercases_input(mock_contains_token: MagicMock):
    """
    Test that `var_ref` uppercases the input string before formatting it
    as a variable reference.

    Args:
        mock_contains_token: A mock object for `contains_token`.
    """
    mock_contains_token.return_value = False
    result = var_ref("lowerCase123")
    assert result == "$(LOWERCASE123)"


def test_var_ref_empty_string(mock_contains_token: MagicMock):
    """
    Test that `var_ref` correctly handles an empty string input.
    It should return the string formatted as `$()`.

    Args:
        mock_contains_token: A mock object for `contains_token`.
    """
    mock_contains_token.return_value = False
    result = var_ref("")
    assert result == "$()"


def test_expand_line_substitutes_variables(
    mock_contains_token: MagicMock,
    mock_contains_shell_ref: MagicMock,
    mock_var_ref: MagicMock,
):
    """
    Test that `expand_line` correctly substitutes `$(VAR)` tokens with values from `var_dict`.

    Args:
        mock_contains_token: A mock object for `contains_token`.
        mock_contains_shell_ref: A mock object for `contains_shell_ref`.
        mock_var_ref: A mock object for `var_ref`.
    """
    mock_contains_token.return_value = True
    mock_contains_shell_ref.return_value = False
    mock_var_ref.side_effect = lambda s: f"$({s})"

    line = "Path: $(VAR1) and $(VAR2)"
    var_dict = {"VAR1": "/path/to/dir", "VAR2": "/another/path"}

    result = expand_line(line, var_dict)
    assert result == "Path: /path/to/dir and /another/path"


def test_expand_line_with_env_vars(mock_contains_token: MagicMock, mock_contains_shell_ref: MagicMock):
    """
    Test that `expand_line` expands environment variables and `~` when `env_vars=True`.

    Args:
        mock_contains_token: A mock object for `contains_token`.
        mock_contains_shell_ref: A mock object for `contains_shell_ref`.
    """
    mock_contains_token.return_value = False
    mock_contains_shell_ref.return_value = True

    line = "Home is ~$USER"
    expected = os.path.expandvars(os.path.expanduser(line))

    result = expand_line(line, {}, env_vars=True)
    assert result == expected


def test_expand_line_expands_tilde_only(mock_contains_token: MagicMock, mock_contains_shell_ref: MagicMock):
    """
    Test that `expand_line` expands `~` to the home directory when `env_vars=True`.

    Args:
        mock_contains_token: A mock object for `contains_token`.
        mock_contains_shell_ref: A mock object for `contains_shell_ref`.
    """
    mock_contains_token.return_value = False
    mock_contains_shell_ref.return_value = False

    line = "~/mydir"
    expected = os.path.expanduser(line)

    result = expand_line(line, {}, env_vars=True)
    assert result == expected


def test_expand_line_no_token_or_env_ref(mock_contains_token: MagicMock, mock_contains_shell_ref: MagicMock):
    """
    Test that `expand_line` returns the line unchanged if it contains no tokens or `~`.

    Args:
        mock_contains_token: A mock object for `contains_token`.
        mock_contains_shell_ref: A mock object for `contains_shell_ref`.
    """
    mock_contains_token.return_value = False
    mock_contains_shell_ref.return_value = False

    line = "no expansion needed"
    result = expand_line(line, {})
    assert result == "no expansion needed"


def test_expand_line_combined_user_and_env_and_dict(
    mock_contains_token: MagicMock,
    mock_contains_shell_ref: MagicMock,
    mock_var_ref: MagicMock,
):
    """
    Test that `expand_line` handles both variable substitutions and environment/home expansion.

    Args:
        mock_contains_token: A mock object for `contains_token`.
        mock_contains_shell_ref: A mock object for `contains_shell_ref`.
        mock_var_ref: A mock object for `var_ref`.
    """
    mock_contains_token.return_value = True
    mock_contains_shell_ref.return_value = True
    mock_var_ref.side_effect = lambda s: f"$({s})"

    line = "File: $(VAR1) at ~$USER"
    var_dict = {"VAR1": "/tmp/data"}
    intermediate = "File: /tmp/data at ~$USER"
    expected = os.path.expandvars(os.path.expanduser(intermediate))

    result = expand_line(line, var_dict, env_vars=True)
    assert result == expected


def test_expand_by_line_calls_expand_line_per_line(mock_expand_line: MagicMock):
    """
    Test that `expand_by_line` calls `expand_line` once per input line.

    Args:
        mock_expand_line: A mock object for `expand_line`.
    """
    mock_expand_line.side_effect = lambda line, _: f"expanded({line})"
    text = "line1\nline2\nline3"
    var_dict = {"VAR": "value"}

    result = expand_by_line(text, var_dict)
    expected = "expanded(line1)\nexpanded(line2)\nexpanded(line3)\n"

    assert result == expected
    assert mock_expand_line.call_count == 3
    mock_expand_line.assert_any_call("line1", var_dict)
    mock_expand_line.assert_any_call("line2", var_dict)
    mock_expand_line.assert_any_call("line3", var_dict)


def test_expand_by_line_handles_empty_text(mock_expand_line: MagicMock):
    """
    Test that `expand_by_line` returns just a newline when input is an empty string.

    Args:
        mock_expand_line: A mock object for `expand_line`.
    """
    mock_expand_line.return_value = ""
    result = expand_by_line("", {})
    assert result == ""
    mock_expand_line.assert_not_called()


def test_expand_by_line_preserves_line_structure(mock_expand_line: MagicMock):
    """
    Test that `expand_by_line` preserves the number and order of lines in the input.

    Args:
        mock_expand_line: A mock object for `expand_line`.
    """
    mock_expand_line.side_effect = lambda line, _: f"[{line}]"
    text = "a\nb\nc"
    expected = "[a]\n[b]\n[c]\n"

    result = expand_by_line(text, {})
    assert result == expected


def test_expand_by_line_handles_single_line_input(mock_expand_line: MagicMock):
    """
    Test that `expand_by_line` correctly processes single-line input.

    Args:
        mock_expand_line: A mock object for `expand_line`.
    """
    mock_expand_line.return_value = "processed"
    result = expand_by_line("just one line", {"VAR": "value"})
    assert result == "processed\n"
    mock_expand_line.assert_called_once_with("just one line", {"VAR": "value"})


class DummyMerlinSpec:
    """
    A dummy spec object used for testing `expand_env_vars`.
    Mimics `MerlinSpec` behavior by keeping a `.sections` attribute
    and updating instance attributes for each top-level section.
    """

    def __init__(self, sections):
        self.sections = sections
        for name, section in sections.items():
            setattr(self, name, section)


def test_expand_env_vars_expands_strings():
    """
    Test that string values with environment variables or `~` are expanded.
    """
    home = os.path.expanduser("~")
    spec_dict = {"batch": {"workdir": "~/workspace", "logfile": "$HOME/logs/output.log"}}
    spec = DummyMerlinSpec(deepcopy(spec_dict))

    expanded_spec = expand_env_vars(spec)

    assert expanded_spec.batch["workdir"] == home + "/workspace"
    assert expanded_spec.batch["logfile"] == os.path.expandvars("$HOME/logs/output.log")


def test_expand_env_vars_skips_cmd_and_restart():
    """
    Test that `cmd` and `restart` values are not expanded.
    """
    spec_dict = {"tasks": {"cmd": "echo $HOME", "restart": "~/restart.sh", "input": "$HOME/input.txt"}}
    spec = DummyMerlinSpec(deepcopy(spec_dict))

    expanded = expand_env_vars(spec)

    assert expanded.tasks["cmd"] == "echo $HOME"
    assert expanded.tasks["restart"] == "~/restart.sh"
    assert expanded.tasks["input"] == os.path.expandvars("$HOME/input.txt")


def test_expand_env_vars_recursively_handles_lists_and_dicts():
    """
    Test that lists and nested structures are handled recursively.
    """
    spec_dict = {"env": {"PATHS": ["~/bin", "$HOME/tools"], "nested": {"config": "$HOME/config.yml"}}}
    spec = DummyMerlinSpec(deepcopy(spec_dict))

    expanded = expand_env_vars(spec)

    assert expanded.env["PATHS"][0] == os.path.expanduser("~/bin")
    assert expanded.env["PATHS"][1] == os.path.expandvars("$HOME/tools")
    assert expanded.env["nested"]["config"] == os.path.expandvars("$HOME/config.yml")


def test_expand_env_vars_handles_none_and_non_strings():
    """
    Test that None and non-string values are returned as-is.
    """
    spec_dict = {
        "params": {
            "some_value": None,
            "numeric": 123,
            "boolean": True,
        }
    }
    spec = DummyMerlinSpec(deepcopy(spec_dict))

    expanded = expand_env_vars(spec)

    assert expanded.params["some_value"] is None
    assert expanded.params["numeric"] == 123
    assert expanded.params["boolean"] is True


def test_expand_env_vars_sets_attributes_from_sections():
    """
    Test that expanded sections are assigned as attributes of the spec.
    """
    spec_dict = {"resources": {"scratch": "~/scratch"}}
    spec = DummyMerlinSpec(deepcopy(spec_dict))

    expanded = expand_env_vars(spec)

    assert hasattr(expanded, "resources")
    assert expanded.resources["scratch"] == os.path.expanduser("~/scratch")


def test_single_dict_without_refs():
    """
    Test that `determine_user_variables` handles a single dictionary without references.
    """
    user_vars = {"n_samples": 5, "output": "./output"}
    result = determine_user_variables(user_vars)
    assert result == {"N_SAMPLES": "5", "OUTPUT": "./output"}


def test_multiple_dicts_with_overlapping_keys():
    """
    Test that later dictionaries override earlier ones.
    """
    vars1 = {"x": 1, "y": "hello"}
    vars2 = {"y": "world", "z": 3}
    result = determine_user_variables(vars1, vars2)
    assert result == {"X": "1", "Y": "world", "Z": "3"}


def test_variable_reference_resolution():
    """
    Test that variable references like `$(VAR)` are correctly resolved from prior keys.
    """
    user_vars = [{"TARGET": "subdir"}, {"RESULT_PATH": "$(TARGET)/results"}]
    result = determine_user_variables(*user_vars)
    assert result == {"TARGET": "subdir", "RESULT_PATH": "subdir/results"}


def test_expand_env_and_user_paths(mocker: MockerFixture):
    """
    Test that environment variables and user (~) paths are expanded.

    Args:
        mocker: PyTest mocker fixture.
    """
    mocker.patch.dict(os.environ, {"MYENV": "envval"})
    user_vars = {"PATH": "~/workspace/${MYENV}"}
    result = determine_user_variables(user_vars)
    expected = os.path.expanduser("~/workspace/envval")
    assert result == {"PATH": expected}


def test_variable_reference_chain_resolution():
    """
    Test multiple chained variable references are resolved in order.
    """
    user_vars = [{"BASE": "/root"}, {"MID": "$(BASE)/data"}, {"END": "$(MID)/output"}]
    result = determine_user_variables(*user_vars)
    assert result == {"BASE": "/root", "MID": "/root/data", "END": "/root/data/output"}


def test_raises_on_reserved_word_reassignment():
    """
    Test that attempting to redefine a reserved word raises a ValueError.
    """
    with pytest.raises(ValueError, match="Cannot reassign value of reserved word 'SPECROOT'"):
        determine_user_variables({"SPECROOT": "/forbidden"})


def test_reference_to_undefined_variable_remains_unchanged():
    """
    Test that unresolved variable references remain unchanged.
    """
    user_vars = {"RESULT_PATH": "$(UNDEFINED_VAR)/out"}
    result = determine_user_variables(user_vars)
    assert result == {"RESULT_PATH": "$(UNDEFINED_VAR)/out"}


def test_parameter_substitutions_basic():
    """
    Test basic substitution mapping of labels to values and inclusion of sample metadata.
    """
    sample = [10, 20]
    labels = ["X", "Y"]
    sample_id = 0
    path = "/0/3/4/8/9/"
    expected = [("$(X)", "10"), ("$(Y)", "20"), ("$(MERLIN_SAMPLE_ID)", "0"), ("$(MERLIN_SAMPLE_PATH)", path)]
    result = parameter_substitutions_for_sample(sample, labels, sample_id, path)
    assert result == expected


def test_parameter_substitutions_empty_sample():
    """
    Test that only metadata substitutions are returned for an empty sample.
    """
    sample = []
    labels = []
    sample_id = 5
    path = "/5/2/"
    expected = [("$(MERLIN_SAMPLE_ID)", "5"), ("$(MERLIN_SAMPLE_PATH)", path)]
    result = parameter_substitutions_for_sample(sample, labels, sample_id, path)
    assert result == expected


def test_parameter_substitutions_mismatched_labels():
    """
    Test that mismatched sample and label lengths only zips matching pairs.
    """
    sample = [42]
    labels = ["A", "B", "C"]
    sample_id = 3
    path = "/3/3/3/"
    expected = [("$(A)", "42"), ("$(MERLIN_SAMPLE_ID)", "3"), ("$(MERLIN_SAMPLE_PATH)", path)]
    result = parameter_substitutions_for_sample(sample, labels, sample_id, path)
    assert result == expected


def test_parameter_substitutions_non_string_values():
    """
    Test that values of different types (e.g., float, bool) are stringified properly.
    """
    sample = [3.14, True, None]
    labels = ["PI", "FLAG", "MISSING"]
    sample_id = 9
    path = "/sample/path"
    expected = [
        ("$(PI)", "3.14"),
        ("$(FLAG)", "True"),
        ("$(MISSING)", "None"),
        ("$(MERLIN_SAMPLE_ID)", "9"),
        ("$(MERLIN_SAMPLE_PATH)", path),
    ]
    result = parameter_substitutions_for_sample(sample, labels, sample_id, path)
    assert result == expected


def test_parameter_substitutions_for_cmd_basic():
    """
    Test standard substitution of glob path, sample paths, and return codes.
    """
    glob_path = "/samples/*"
    sample_paths = "/samples/run1:/samples/run2"
    result = parameter_substitutions_for_cmd(glob_path, sample_paths)

    expected = [
        ("$(MERLIN_GLOB_PATH)", glob_path),
        ("$(MERLIN_PATHS_ALL)", sample_paths),
        ("$(MERLIN_SUCCESS)", str(int(ReturnCode.OK))),
        ("$(MERLIN_RESTART)", str(int(ReturnCode.RESTART))),
        ("$(MERLIN_SOFT_FAIL)", str(int(ReturnCode.SOFT_FAIL))),
        ("$(MERLIN_HARD_FAIL)", str(int(ReturnCode.HARD_FAIL))),
        ("$(MERLIN_RETRY)", str(int(ReturnCode.RETRY))),
        ("$(MERLIN_STOP_WORKERS)", str(int(ReturnCode.STOP_WORKERS))),
        ("$(MERLIN_RAISE_ERROR)", str(int(ReturnCode.RAISE_ERROR))),
    ]

    assert result == expected


def test_parameter_substitutions_for_cmd_empty_inputs():
    """
    Test that the function still returns return codes even if inputs are empty.
    """
    result = parameter_substitutions_for_cmd("", "")
    assert ("$(MERLIN_GLOB_PATH)", "") in result
    assert ("$(MERLIN_PATHS_ALL)", "") in result

    return_code_vars = [
        "$(MERLIN_SUCCESS)",
        "$(MERLIN_RESTART)",
        "$(MERLIN_SOFT_FAIL)",
        "$(MERLIN_HARD_FAIL)",
        "$(MERLIN_RETRY)",
        "$(MERLIN_STOP_WORKERS)",
        "$(MERLIN_RAISE_ERROR)",
    ]
    for var in return_code_vars:
        assert any(key == var for key, _ in result)


def test_parameter_substitutions_for_cmd_correct_return_codes():
    """
    Test that each return code maps to the correct integer value from ReturnCode.
    """
    result = dict(parameter_substitutions_for_cmd("glob", "paths"))
    assert result["$(MERLIN_SUCCESS)"] == str(int(ReturnCode.OK))
    assert result["$(MERLIN_RESTART)"] == str(int(ReturnCode.RESTART))
    assert result["$(MERLIN_SOFT_FAIL)"] == str(int(ReturnCode.SOFT_FAIL))
    assert result["$(MERLIN_HARD_FAIL)"] == str(int(ReturnCode.HARD_FAIL))
    assert result["$(MERLIN_RETRY)"] == str(int(ReturnCode.RETRY))
    assert result["$(MERLIN_STOP_WORKERS)"] == str(int(ReturnCode.STOP_WORKERS))
    assert result["$(MERLIN_RAISE_ERROR)"] == str(int(ReturnCode.RAISE_ERROR))


def test_expand_spec_no_study_basic(mocker: MockerFixture):
    """
    Test that the function expands a spec using merged environment variables and override vars.

    Args:
        mocker: PyTest mocker fixture.
    """
    filepath = "fake/path/spec.yaml"
    override_vars = {"TARGET": "override_val"}

    # Mock dependencies
    mock_error_override_vars = mocker.patch("merlin.spec.expansion.error_override_vars")
    mock_replace_override_vars = mocker.patch("merlin.spec.expansion.replace_override_vars")
    mock_determine_user_variables = mocker.patch("merlin.spec.expansion.determine_user_variables")
    mock_expand_by_line = mocker.patch("merlin.spec.expansion.expand_by_line")
    mock_merlin_spec = mocker.patch("merlin.spec.expansion.MerlinSpec")

    # Mock spec instance
    mock_spec_instance = mocker.MagicMock()
    original_env = {"variables": {"TARGET": "default_val"}, "labels": {"RESULT_PATH": "$(TARGET)/results"}}
    mock_spec_instance.environment = original_env.copy()
    mock_spec_instance.dump.return_value = "expanded yaml content"
    mock_merlin_spec.load_specification.return_value = mock_spec_instance

    mock_replace_override_vars.return_value = original_env
    mock_determine_user_variables.return_value = {"TARGET": "override_val", "RESULT_PATH": "override_val/results"}
    mock_expand_by_line.return_value = "final expanded output"

    # Run function
    result = expand_spec_no_study(filepath, override_vars)

    # Assertions
    mock_error_override_vars.assert_called_once_with(override_vars, filepath)
    mock_replace_override_vars.assert_called_once_with(original_env, override_vars)
    mock_determine_user_variables.assert_called_once_with(
        mock_spec_instance.environment["variables"], mock_spec_instance.environment["labels"]
    )
    mock_expand_by_line.assert_called_once_with(
        "expanded yaml content", {"TARGET": "override_val", "RESULT_PATH": "override_val/results"}
    )
    assert result == "final expanded output"


def test_expand_spec_no_study_no_vars_section(mocker: MockerFixture):
    """
    Test when the spec has no 'variables' or 'labels' section.

    Args:
        mocker: PyTest mocker fixture.
    """
    filepath = "fake/path/spec.yaml"
    override_vars = {}

    # Mock dependencies
    mock_error_override_vars = mocker.patch("merlin.spec.expansion.error_override_vars")
    mock_replace_override_vars = mocker.patch("merlin.spec.expansion.replace_override_vars")
    mock_determine_user_variables = mocker.patch("merlin.spec.expansion.determine_user_variables")
    mock_expand_by_line = mocker.patch("merlin.spec.expansion.expand_by_line")
    mock_merlin_spec = mocker.patch("merlin.spec.expansion.MerlinSpec")

    mock_spec_instance = mocker.MagicMock()
    mock_spec_instance.environment = {}
    mock_spec_instance.dump.return_value = "raw yaml content"
    mock_merlin_spec.load_specification.return_value = mock_spec_instance

    mock_determine_user_variables.return_value = {}
    mock_expand_by_line.return_value = "unchanged output"

    result = expand_spec_no_study(filepath, override_vars)

    mock_error_override_vars.assert_called_once()
    mock_replace_override_vars.assert_called_once()
    mock_determine_user_variables.assert_called_once_with()
    mock_expand_by_line.assert_called_once_with("raw yaml content", {})
    assert result == "unchanged output"


def test_get_spec_with_expansion_basic(mocker: MockerFixture):
    """
    Test that the function returns a MerlinSpec object with verified path and expanded text.

    Args:
        mocker: PyTest mocker fixture.
    """
    filepath = "specs/sample.yaml"
    verified_path = "/abs/specs/sample.yaml"
    override_vars = {"FOO": "bar"}
    expanded_text = "expanded: true"

    # Mock dependencies
    mock_verify_filepath = mocker.patch("merlin.spec.expansion.verify_filepath", return_value=verified_path)
    mock_expand_spec = mocker.patch("merlin.spec.expansion.expand_spec_no_study", return_value=expanded_text)
    mock_merlin_spec = mocker.patch("merlin.spec.expansion.MerlinSpec")
    mock_spec_instance = mocker.MagicMock()
    mock_merlin_spec.load_spec_from_string.return_value = mock_spec_instance

    # Run function
    result = get_spec_with_expansion(filepath, override_vars)

    # Assertions
    mock_verify_filepath.assert_called_once_with(filepath)
    mock_expand_spec.assert_called_once_with(verified_path, override_vars)
    mock_merlin_spec.load_spec_from_string.assert_called_once_with(expanded_text)
    assert result == mock_spec_instance


def test_get_spec_with_expansion_no_overrides(mocker: MockerFixture):
    """
    Test the function behavior when no overrides are passed.

    Args:
        mocker: PyTest mocker fixture.
    """
    filepath = "specs/no_overrides.yaml"
    expanded_text = "no_overrides_expanded"

    mock_verify_filepath = mocker.patch("merlin.spec.expansion.verify_filepath", return_value=filepath)
    mock_expand_spec = mocker.patch("merlin.spec.expansion.expand_spec_no_study", return_value=expanded_text)

    mock_merlin_spec = mocker.patch("merlin.spec.expansion.MerlinSpec")
    mock_spec_instance = mocker.MagicMock()
    mock_merlin_spec.load_spec_from_string.return_value = mock_spec_instance

    result = get_spec_with_expansion(filepath)

    mock_verify_filepath.assert_called_once_with(filepath)
    mock_expand_spec.assert_called_once_with(filepath, None)
    mock_merlin_spec.load_spec_from_string.assert_called_once_with(expanded_text)
    assert result == mock_spec_instance
