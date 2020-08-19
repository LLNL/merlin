###############################################################################
# Copyright (c) 2019, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.7.3.
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

import logging
from collections import ChainMap
from copy import deepcopy
from os.path import expanduser, expandvars

from merlin.common.abstracts.enums import ReturnCode
from merlin.spec.override import dump_with_overrides, error_override_vars
from merlin.spec.specification import MerlinSpec
from merlin.utils import contains_shell_ref, contains_token


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
}
MERLIN_RESERVED = STEP_AWARE | PROVENANCE_REPLACE
RESERVED = MAESTRO_RESERVED | MERLIN_RESERVED


LOG = logging.getLogger(__name__)


def var_ref(string):
    """
    Given a string <str>, return that string surrounded
    by $(<str>).
    """
    string = string.upper()
    if contains_token(string):
        LOG.warning(f"Bad var_ref usage on string '{string}'.")
        return string
    return f"$({string})"


def expand_line(line, var_dict, env_vars=False):
    """
    Expand one line of text by substituting user variables, 
    optionally environment variables, as well as variables in 'var_dict'.
    """
    if (
        (not contains_token(line))
        and (not contains_shell_ref(line))
        and ("~" not in line)
    ):
        return line
    for key, val in var_dict.items():
        if key in line:
            line = line.replace(var_ref(key), str(val))
    if env_vars:
        line = expandvars(expanduser(line))
    return line


def expand_by_line(text, var_dict):
    """
    Given a text (yaml spec), and a dictionary of variable names
    and values, expand variables in the text line by line.
    """
    text = text.splitlines()
    result = ""
    for line in text:
        expanded_line = expand_line(line, var_dict)
        result += expanded_line + "\n"
    return result


def expand_env_vars(spec):
    """
    Expand environment variables for all sections of a spec, except
    for values with the key 'cmd' or 'restart' (these are executable
    shell scripts, so environment variable expansion would be redundant).
    """

    def recurse(section):
        if section is None:
            return section
        if isinstance(section, str):
            return expandvars(expanduser(section))
        if isinstance(section, dict):
            for k, v in section.items():
                if k in ["cmd", "restart"]:
                    continue
                section[k] = recurse(v)
        elif isinstance(section, list):
            for i, elem in enumerate(deepcopy(section)):
                section[i] = recurse(elem)
        return section

    for name, section in spec.sections.items():
        setattr(spec, name, recurse(section))
    return spec


def determine_user_variables(*user_var_dicts):
    """
    Given an arbitrary number of dictionaries, determine them
    in order.

    param `user_var_dicts`: A list of dictionaries of user variables.
    For example:
        [variables, labels]

    A single user var dict may look like:
        {'OUTPUT_PATH':'./studies', 'N_SAMPLES':10}

    This user var dict:
        {'TARGET': 'target_dir',
        'PATH': '$(SPECROOT)/$(TARGET)'}

    ...would be determined as:
        {'TARGET': 'target_dir',
        'PATH': '$(SPECROOT)/target_dir'}
    """
    # TODO move this logic to specification.py?
    try:
        all_var_dicts = dict(ChainMap(*user_var_dicts))
    except TypeError:
        all_var_dicts = {}
    determined_results = {}
    for key, val in all_var_dicts.items():
        if key in RESERVED:
            raise ValueError(
                f"Cannot reassign value of reserved word '{key}'! Reserved words are: {RESERVED}."
            )
        new_val = str(val)
        if contains_token(new_val):
            for determined_key in determined_results.keys():
                var_determined_key = var_ref(determined_key)
                if var_determined_key in new_val:
                    new_val = new_val.replace(
                        var_determined_key, determined_results[determined_key]
                    )
        new_val = expandvars(expanduser(new_val))
        determined_results[key.upper()] = new_val
    return determined_results


def parameter_substitutions_for_sample(
    sample, labels, sample_id, relative_path_to_sample
):
    """
    :param sample : The sample to do substitution for.
    :param labels : The column labels of the sample.
    :param sample_id : The merlin sample id for this sample.
    :param relative_path_to_sample : The relative path to this sample.

    :return : list of pairs indicating what needs to be substituted for a
    merlin sample
    """
    substitutions = []
    for label, axis in zip(labels, sample):
        substitutions.append((f"$({label})", str(axis)))

    # The 0:N (integer) id of a sample
    substitutions.append(("$(MERLIN_SAMPLE_ID)", str(sample_id)))

    # Specific path to the sample, ie /0/3/4/8/9/
    substitutions.append(("$(MERLIN_SAMPLE_PATH)", relative_path_to_sample))

    return substitutions


def parameter_substitutions_for_cmd(glob_path, sample_paths):
    """
    :param glob_path: a glob that should yield the paths to all merlin samples
    :param sample_paths: a delimited list of all of the samples

    :return : list of pairs indicating what needs to be substituted for a
        merlin cmd
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
    return substitutions


def expand_spec_no_study(filepath, override_vars=None):
    """
    Get the expanded text of a spec without creating
    a MerlinStudy. Expansion is limited to user variables
    (the ones defined inside the yaml spec or at the command
    line).
    """
    error_override_vars(override_vars, filepath)
    spec = MerlinSpec.load_specification(filepath)
    full_spec = dump_with_overrides(spec, override_vars)
    spec = MerlinSpec.load_spec_from_string(full_spec)

    uvars = []
    if "variables" in spec.environment:
        uvars.append(spec.environment["variables"])
    if "labels" in spec.environment:
        uvars.append(spec.environment["labels"])
    evaluated_uvars = determine_user_variables(*uvars)

    return expand_by_line(full_spec, evaluated_uvars)


def get_spec_with_expansion(filepath, override_vars=None):
    """
    Return a MerlinSpec with overrides and expansion, without
    creating a MerlinStudy.
    """
    expanded_spec_text = expand_spec_no_study(filepath, override_vars)
    return MerlinSpec.load_spec_from_string(expanded_spec_text)
