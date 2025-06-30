##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Utility functions to support Merlin CLI command handlers.

This module provides common helper functions used by various CLI commands
in the Merlin application. These utilities focus on parsing and validating
command-line arguments related to specification files and variable overrides,
as well as loading and expanding Merlin YAML specifications.
"""

import logging
from argparse import Namespace
from contextlib import suppress
from typing import Dict, List, Optional, Tuple, Union

from merlin.spec.expansion import RESERVED, get_spec_with_expansion
from merlin.spec.specification import MerlinSpec
from merlin.utils import verify_filepath


LOG = logging.getLogger("merlin")


def parse_override_vars(
    variables_list: Optional[List[str]],
) -> Optional[Dict[str, Union[str, int]]]:
    """
    Parse a list of command-line variables into a dictionary of key-value pairs.

    This function takes an optional list of strings following the syntax
    "KEY=val" and converts them into a dictionary. It validates the format
    of the variables and ensures that keys are valid according to specified rules.

    Args:
        variables_list: An optional list of strings, where each string should be in the
            format "KEY=val", e.g., ["KEY1=value1", "KEY2=42"].

    Returns:
        A dictionary where the keys are variable names (str) and the
            values are either strings or integers. If `variables_list` is
            None or empty, returns None.

    Raises:
        ValueError: If the input format is incorrect, including:\n
            - Missing '=' operator.
            - Excess '=' operators in a variable assignment.
            - Invalid variable names (must be alphanumeric and underscores).
            - Attempting to override reserved variable names.
    """
    if variables_list is None:
        return None
    LOG.debug(f"Command line override variables = {variables_list}")
    result: Dict[str, Union[str, int]] = {}
    arg: str
    for arg in variables_list:
        try:
            if "=" not in arg:
                raise ValueError("--vars requires '=' operator. See 'merlin run --help' for an example.")
            entry: str = arg.split("=")
            if len(entry) != 2:
                raise ValueError("--vars requires ONE '=' operator (without spaces) per variable assignment.")
            key: str = entry[0]
            if key is None or key == "" or "$" in key:
                raise ValueError("--vars requires valid variable names comprised of alphanumeric characters and underscores.")
            if key in RESERVED:
                raise ValueError(f"Cannot override reserved word '{key}'! Reserved words are: {RESERVED}.")

            val: Union[str, int] = entry[1]
            with suppress(ValueError):
                int(val)
                val = int(val)
            result[key] = val

        except Exception as excpt:
            raise ValueError(
                f"{excpt} Bad '--vars' formatting on command line. See 'merlin run --help' for an example."
            ) from excpt
    return result


def get_merlin_spec_with_override(args: Namespace) -> Tuple[MerlinSpec, str]:
    """
    Shared command to retrieve a [`MerlinSpec`][spec.specification.MerlinSpec] object
    and an expanded filepath.

    This function processes parsed command-line interface (CLI) arguments to validate
    and expand the specified filepath and any associated variables. It then constructs
    and returns a [`MerlinSpec`][spec.specification.MerlinSpec] object based on the
    provided specification.

    Args:
        args: Parsed CLI arguments containing:\n
            - `specification`: the path to the specification file
            - `variables`: optional variable overrides to customize the spec.

    Returns:
        spec (spec.specification.MerlinSpec): An instance of the
            [`MerlinSpec`][spec.specification.MerlinSpec] class with the expanded
            configuration based on the provided filepath and variables.
        filepath: The expanded filepath derived from the specification.
    """
    filepath = verify_filepath(args.specification)
    variables_dict = parse_override_vars(args.variables)
    spec = get_spec_with_expansion(filepath, override_vars=variables_dict)
    return spec, filepath
