##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module handles the expansion of variables in a Merlin spec file.

It provides functionality to expand user-defined variables, environment variables, and reserved variables
within a spec file. The module also supports variable substitution for specific use cases, such as parameter
substitutions for samples and commands, and allows for the processing of override variables provided via
the command-line interface.
"""

import logging
from collections import ChainMap
from copy import deepcopy
from os.path import expanduser, expandvars
from typing import Dict, List, Tuple

from merlin.common.enums import ReturnCode
from merlin.spec.override import error_override_vars, replace_override_vars
from merlin.spec.specification import MerlinSpec
from merlin.utils import contains_shell_ref, contains_token, verify_filepath


MAESTRO_RESERVED = {"SPECROOT", "WORKSPACE", "LAUNCHER"}
STEP_AWARE = {
    "MERLIN_GLOB_PATH",
    "MERLIN_PATHS_ALL",
    "MERLIN_SAMPLE_ID",
    "MERLIN_SAMPLE_PATH",
}
PROVENANCE_REPLACE = {
    "MERLIN_WORKSPACE",
    "MERLIN_TIMESTAMP",
    "MERLIN_INFO",
    "MERLIN_SUCCESS",
    "MERLIN_RESTART",
    "MERLIN_SOFT_FAIL",
    "MERLIN_HARD_FAIL",
    "MERLIN_RETRY",
    "MERLIN_STOP_WORKERS",
    "MERLIN_RAISE_ERROR",
}
MERLIN_RESERVED = STEP_AWARE | PROVENANCE_REPLACE
RESERVED = MAESTRO_RESERVED | MERLIN_RESERVED


LOG = logging.getLogger(__name__)


def var_ref(string: str) -> str:
    """
    Format a string as a variable reference.

    This function takes a string, converts it to uppercase, and returns it
    wrapped in the format `$(<string>)`. If the string already contains
    a token (e.g., it is already formatted as a variable reference), a warning
    is logged and the original string is returned unchanged.

    Args:
        string: The input string to format as a variable reference.

    Returns:
        The formatted variable reference, or the original string if it
            already contains a token.

    Example:
        ```python
        >>> var_ref("example")
        '$(EXAMPLE)'
        ```
    """
    string = string.upper()
    if contains_token(string):
        LOG.warning(f"Bad var_ref usage on string '{string}'.")
        return string
    return f"$({string})"


def expand_line(line: str, var_dict: Dict[str, str], env_vars: bool = False) -> str:
    """
    Expand a single line of text by substituting variables.

    This function replaces variable references in a given line of text with
    their corresponding values from a provided dictionary. Optionally, it can
    also expand environment variables and user home directory shortcuts (e.g., `~`).

    Args:
        line: The input line of text to expand.
        var_dict: A dictionary of variable names and their corresponding values
            to substitute in the line.
        env_vars: If True, environment variables and home directory shortcuts
            will also be expanded.

    Returns:
        The expanded line of text with all applicable substitutions applied.

    Example:
        ```python
        >>> line = "Path: $(VAR1) and $(VAR2)"
        >>> var_dict = {"VAR1": "/path/to/dir", "VAR2": "/another/path"}
        >>> expand_line(line, var_dict)
        'Path: /path/to/dir and /another/path'
        ```
    """
    # fmt: off
    if (
        (not contains_token(line))
        and (not contains_shell_ref(line))
        and ("~" not in line)
    ):
        return line
    # fmt: on
    for key, val in var_dict.items():
        if key in line:
            line = line.replace(var_ref(key), str(val))
    if env_vars:
        line = expandvars(expanduser(line))
    return line


def expand_by_line(text: str, var_dict: Dict[str, str]) -> str:
    """
    Expand variables in a text line by line.

    This function processes a multi-line text (e.g., a YAML specification) and
    replaces variable references in each line using a provided dictionary of
    variable names and their corresponding values.

    Args:
        text: The input multi-line text to process.
        var_dict: A dictionary of variable names and their corresponding values
            to substitute in the text.

    Returns:
        The text with all applicable variable substitutions applied, processed
            line by line.

    Example:
        ```python
        >>> text = "Path is $(VAR1) and stores $(VAR2)"
        >>> var_dict = {"VAR1": "/path/to/dir", "VAR2": "value"}
        >>> expand_by_line(text, var_dict)
        'Path is /path/to/dir and stores value'
        ```
    """
    text = text.splitlines()
    result = ""
    for line in text:
        expanded_line = expand_line(line, var_dict)
        result += expanded_line + "\n"
    return result


def expand_env_vars(spec: MerlinSpec) -> MerlinSpec:
    """
    Expand environment variables in all sections of a spec.

    This function processes all sections of a given spec object and expands
    environment variables (e.g., `$HOME` or `~`) in string values. It skips
    expansion for values associated with the keys 'cmd' or 'restart', as these
    are typically shell scripts where environment variable expansion would
    already occur during execution.

    Args:
        spec (spec.specification.MerlinSpec): The spec object
            containing sections to process.

    Returns:
        (spec.specification.MerlinSpec): The updated spec object with environment variables expanded in all
            applicable sections.
    """

    def recurse(section):
        if section is None:
            return section
        if isinstance(section, str):
            return expandvars(expanduser(section))
        if isinstance(section, dict):
            for key, val in section.items():
                if key in ["cmd", "restart"]:
                    continue
                section[key] = recurse(val)
        elif isinstance(section, list):
            for i, elem in enumerate(deepcopy(section)):
                section[i] = recurse(elem)
        return section

    for name, section in spec.sections.items():
        setattr(spec, name, recurse(section))
    return spec


def determine_user_variables(*user_var_dicts: List[Dict]) -> Dict:
    """
    Determine user-defined variables from multiple dictionaries.

    This function takes an arbitrary number of dictionaries containing user-defined
    variables and resolves them in order, handling variable references and expansions
    (e.g., environment variables, user home directory shortcuts). Variable names are
    converted to uppercase, and reserved words cannot be reassigned.

    Args:
        user_var_dicts: One or more dictionaries of user variables. Each dictionary
            contains key-value pairs where the key is the variable name, and the value
            is the variable's definition.

    Returns:
        A dictionary of resolved user variables, with variable names in uppercase
            and all references expanded.

    Raises:
        ValueError: If a reserved word is attempted to be reassigned.

    Example:
        ```python
        >>> user_vars_1 = {'OUTPUT_PATH': './studies', 'N_SAMPLES': 10}
        >>> user_vars_2 = {'TARGET': 'target_dir', 'PATH': '$(SPECROOT)/$(TARGET)'}
        >>> determine_user_variables(user_vars_1, user_vars_2)
        {'OUTPUT_PATH': './studies', 'N_SAMPLES': '10',
         'TARGET': 'target_dir', 'PATH': '$(SPECROOT)/target_dir'}
        ```
    """
    all_var_dicts = dict(ChainMap(*user_var_dicts))
    determined_results = {}
    for key, val in all_var_dicts.items():
        if key in RESERVED:
            raise ValueError(f"Cannot reassign value of reserved word '{key}'! Reserved words are: {RESERVED}.")
        new_val = str(val)
        if contains_token(new_val):
            for determined_key, determined_val in determined_results.items():
                var_determined_key = var_ref(determined_key)
                if var_determined_key in new_val:
                    new_val = new_val.replace(var_determined_key, determined_val)
        new_val = expandvars(expanduser(new_val))
        determined_results[key.upper()] = new_val
    return determined_results


def parameter_substitutions_for_sample(
    sample: List[str], labels: List[str], sample_id: int, relative_path_to_sample: str
) -> List[Tuple[str, str]]:
    """
    Generate parameter substitutions for a specific sample.

    This function creates a list of substitution pairs for a given sample,
    mapping variable references (e.g., `$(LABEL)`) to their corresponding
    values in the sample. It also includes metadata substitutions such as
    the sample ID and the relative path to the sample.

    Args:
        sample: A list of values representing the sample.
        labels: A list of column labels corresponding to the sample values.
        sample_id: The unique integer ID of the sample.
        relative_path_to_sample: The relative path to the sample.

    Returns:
        A list of tuples, where each tuple contains a variable reference
            (e.g., `$(LABEL)`) and its corresponding value.

    Example:
        ```python
        >>> sample = [10, 20]
        >>> labels = ["X", "Y"]
        >>> sample_id = 0
        >>> relative_path_to_sample = "/0/3/4/8/9/"
        >>> parameter_substitutions_for_sample(sample, labels, sample_id, relative_path_to_sample)
        [
            ("$(X)", "10"),
            ("$(Y)", "20"),
            ("$(MERLIN_SAMPLE_ID)", "0"),
            ("$(MERLIN_SAMPLE_PATH)", "/0/3/4/8/9/")
        ]
        ```
    """
    substitutions = []
    for label, axis in zip(labels, sample):
        substitutions.append((f"$({label})", str(axis)))

    # The 0:N (integer) id of a sample
    substitutions.append(("$(MERLIN_SAMPLE_ID)", str(sample_id)))

    # Specific path to the sample, ie /0/3/4/8/9/
    substitutions.append(("$(MERLIN_SAMPLE_PATH)", relative_path_to_sample))

    return substitutions


def parameter_substitutions_for_cmd(glob_path: str, sample_paths: str) -> List[Tuple[str, str]]:
    """
    Generate parameter substitutions for a Merlin command.

    This function creates a list of substitution pairs for a Merlin command,
    mapping variable references to their corresponding values. It also includes
    predefined return codes for various Merlin states.

    Substitutions:
        - `$(MERLIN_GLOB_PATH)`: The provided `glob_path`.
        - `$(MERLIN_PATHS_ALL)`: The provided `sample_paths`.
        - `$(MERLIN_SUCCESS)`: The return code for a successful operation.
        - `$(MERLIN_RESTART)`: The return code for a restart operation.
        - `$(MERLIN_SOFT_FAIL)`: The return code for a soft failure.
        - `$(MERLIN_HARD_FAIL)`: The return code for a hard failure.
        - `$(MERLIN_RETRY)`: The return code for a retry operation.
        - `$(MERLIN_STOP_WORKERS)`: The return code for stopping workers.
        - `$(MERLIN_RAISE_ERROR)`: The return code for raising an error.

    Args:
        glob_path: A glob pattern that yields the paths to all Merlin samples.
        sample_paths: A delimited string containing the paths to all samples.

    Returns:
        A list of tuples, where each tuple contains a variable reference and its
            corresponding value.

    Example:
        ```python
        >>> glob_path = "/path/to/samples/*"
        >>> sample_paths = "/path/to/sample1:/path/to/sample2"
        >>> parameter_substitutions_for_cmd(glob_path, sample_paths)
        [
            ("$(MERLIN_GLOB_PATH)", "/path/to/samples/*"),
            ("$(MERLIN_PATHS_ALL)", "/path/to/sample1:/path/to/sample2"),
            ("$(MERLIN_SUCCESS)", "0"),
            ("$(MERLIN_RESTART)", "100"),
            ("$(MERLIN_SOFT_FAIL)", "101"),
            ("$(MERLIN_HARD_FAIL)", "102"),
            ("$(MERLIN_RETRY)", "104"),
            ("$(MERLIN_STOP_WORKERS)", "105"),
            ("$(MERLIN_RAISE_ERROR)", "106")
        ]
        ```
    """
    substitutions = []
    substitutions.append(("$(MERLIN_GLOB_PATH)", glob_path))
    substitutions.append(("$(MERLIN_PATHS_ALL)", sample_paths))
    # Return codes
    substitutions.append(("$(MERLIN_SUCCESS)", str(int(ReturnCode.OK))))
    substitutions.append(("$(MERLIN_RESTART)", str(int(ReturnCode.RESTART))))
    substitutions.append(("$(MERLIN_SOFT_FAIL)", str(int(ReturnCode.SOFT_FAIL))))
    substitutions.append(("$(MERLIN_HARD_FAIL)", str(int(ReturnCode.HARD_FAIL))))
    substitutions.append(("$(MERLIN_RETRY)", str(int(ReturnCode.RETRY))))
    substitutions.append(("$(MERLIN_STOP_WORKERS)", str(int(ReturnCode.STOP_WORKERS))))
    substitutions.append(("$(MERLIN_RAISE_ERROR)", str(int(ReturnCode.RAISE_ERROR))))
    return substitutions


# There's similar code inside study.py but the whole point of this function is to not use
# the MerlinStudy object so we disable this pylint error
# pylint: disable=duplicate-code
def expand_spec_no_study(filepath: str, override_vars: Dict[str, str] = None) -> str:
    """
    Get the expanded text of a spec without creating
    a MerlinStudy. Expansion is limited to user variables
    (the ones defined inside the yaml spec or at the command
    line).

    Expand a spec without creating a [`MerlinStudy`][study.study.MerlinStudy].

    This function processes a spec file to expand user-defined variables (those defined
    in the YAML spec or provided via `override_vars`) without creating a `MerlinStudy`
    object. It returns the expanded text of the specification.

    Args:
        filepath: The path to the YAML specification file.
        override_vars: A dictionary of variable overrides to apply during the expansion.
            These overrides replace or supplement the variables defined in the spec.

    Returns:
        The expanded YAML specification as a string, with user-defined variables resolved.
    """
    error_override_vars(override_vars, filepath)
    spec = MerlinSpec.load_specification(filepath)
    spec.environment = replace_override_vars(spec.environment, override_vars)
    spec_text = spec.dump()

    uvars = []
    if "variables" in spec.environment:
        uvars.append(spec.environment["variables"])
    if "labels" in spec.environment:
        uvars.append(spec.environment["labels"])
    evaluated_uvars = determine_user_variables(*uvars)

    return expand_by_line(spec_text, evaluated_uvars)


# pylint: enable=duplicate-code


def get_spec_with_expansion(filepath: str, override_vars: Dict[str, str] = None) -> MerlinSpec:
    """
    Load and expand a Merlin YAML specification with overrides, without creating a
    [`MerlinStudy`][study.study.MerlinStudy] object.

    This function returns a [`MerlinSpec`][spec.specification.MerlinSpec] object with
    variables expanded and overrides applied. It processes the YAML specification file
    and resolves user-defined variables without creating a `MerlinStudy` object.

    Args:
        filepath: The path to the YAML specification file.
        override_vars: A dictionary of variable overrides to apply during the expansion.
            These overrides replace or supplement the variables defined in the YAML spec.

    Returns:
        (spec.specification.MerlinSpec): A `MerlinSpec` object with expanded variables and applied overrides.
    """
    filepath = verify_filepath(filepath)
    expanded_spec_text = expand_spec_no_study(filepath, override_vars)
    return MerlinSpec.load_spec_from_string(expanded_spec_text)
