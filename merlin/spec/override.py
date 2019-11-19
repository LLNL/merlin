import logging
import re

import yaml


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


def replace_override_vars(full_text, env, override_vars):
    """
    Given the full text of a yaml spec, return the full
    text with user variable overrides in the 'env' block.

    The env yaml block looks like:

        env:
            variables:
                .......
                .......

    The regex will find and match to the above yaml block.
    """
    updated_env = dict(env)
    if override_vars is not None:
        for key, val in env.items():
            updated_env[key].update(override_vars)
    updated_env = {"env": updated_env}
    dump = yaml.dump(updated_env, default_flow_style=False, sort_keys=False)
    updated_env_text = f"\n{dump}\n"

    env_block_pattern = r"\nenv\s*:\s*(\n+( |\t)+.*)+\n*"
    regex = re.compile(env_block_pattern)
    updated_full_text = re.sub(regex, updated_env_text, full_text)
    return updated_full_text


def dump_with_overrides(spec, override_vars):
    dumped_text = spec.dump()
    if override_vars is None:
        return dumped_text
    result = replace_override_vars(
        full_text=dumped_text, env=spec.environment, override_vars=override_vars
    )
    return result
