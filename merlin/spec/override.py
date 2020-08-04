import logging
from copy import deepcopy


LOG = logging.getLogger(__name__)


def error_override_vars(override_vars, spec_filepath):
    """
    Warn user if any given variable name isn't found in the original spec file.
    """
    if override_vars is None:
        return
    original_text = open(spec_filepath, "r").read()
    for variable in override_vars.keys():
        if variable not in original_text:
            raise ValueError(
                f"Command line override variable '{variable}' not found in spec file '{spec_filepath}'."
            )


def replace_override_vars(env, override_vars):
    if override_vars is None:
        return env
    result = deepcopy(env)
    for key, val in env.items():
        for var_name, var_val in override_vars.items():
            if var_name in val:
                result[key][var_name] = var_val
    return result
