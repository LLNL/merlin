###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.12.2.
#
# For details, see https://github.com/LLNL/merlin.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################
"""
This module provides functionality to handle overriding variables in a spec file
via the command-line interface. It includes functions to validate and replace variables
in the spec file or environment block based on user-provided overrides.
"""

import logging
from copy import deepcopy
from typing import Dict


LOG = logging.getLogger(__name__)


def error_override_vars(override_vars: Dict[str, str], spec_filepath: str):
    """
    Warns the user if any given variable name in the override list is not found
    in the original spec file.

    Args:
        override_vars: A dictionary of variables to override, where keys are
            variable names and values are their corresponding new values. Can
            be `None` if no overrides are provided.
        spec_filepath: The file path to the original spec file.

    Raises:
        ValueError: If any variable name in `override_vars` is not found in the
            spec file.
    """
    if override_vars is None:
        return
    with open(spec_filepath, "r") as ospec:
        original_text = ospec.read()
    for variable in override_vars.keys():
        if variable not in original_text:
            raise ValueError(f"Command line override variable '{variable}' not found in spec file '{spec_filepath}'.")


def replace_override_vars(env: Dict[str, str], override_vars: Dict[str, str]) -> Dict[str, str]:
    """
    Replaces variables in the given environment block with the provided override
    values.

    Args:
        env: The environment block, represented as a dictionary where keys are
            environment variable names and values are their corresponding values.
        override_vars: A dictionary of variables to override, where keys are
            variable names and values are their corresponding new values. Can be
            `None` if no overrides are provided.

    Returns:
        A new environment block with the override variables replaced.
    """
    if override_vars is None:
        return env
    result = deepcopy(env)
    for key, val in env.items():
        for var_name, var_val in override_vars.items():
            if var_name in val:
                result[key][var_name] = var_val
    return result
