##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module contains a list of examples that can be used when learning to use
Merlin, or for setting up new workflows.

Examples are packaged in directories, with the directory name denoting
the example name. This must match the name of the Merlin specification inside.
"""
import glob
import logging
import os
import shutil
from typing import Dict, List, Union

import tabulate
import yaml


LOG = logging.getLogger(__name__)

EXAMPLES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "workflows")

# TODO modify the example command to eliminate redundancy
# - e.g. running `merlin example flux_local` will produce the same output
#   as running `merlin example flux_par` or `merlin example flux_par_restart`.
#   This should just be `merlin example flux`.
# - restart and restart delay should be one example
# - feature demo and remote feature demo should be one example
# - all openfoam examples should just be under one openfoam label


def gather_example_dirs() -> Dict[str, str]:
    """
    Get all the example directories.

    Returns:
        A dictionary where the keys and values are the names of example directories.
    """
    result = {}
    for directory in sorted(os.listdir(EXAMPLES_DIR)):
        result[directory] = directory
    return result


def gather_all_examples() -> List[str]:
    """
    Get all the example YAML files.

    Returns:
        A list of file paths to all YAML files in the example directories.
    """
    path = os.path.join(os.path.join(EXAMPLES_DIR, ""), os.path.join("*", "*.yaml"))
    return glob.glob(path)


def write_example(src_path: str, dst_path: str):
    """
    Write out the example workflow to a file or directory.

    Args:
        src_path: The path to copy the example from.
        dst_path: The destination path to copy the example to.
    """
    if os.path.isdir(src_path):
        shutil.copytree(src_path, dst_path)
    else:
        shutil.copy(src_path, dst_path)


def list_examples() -> str:
    """
    List all available examples with their descriptions.

    Returns:
        A formatted string table of example names and their descriptions.
    """
    headers = ["name", "description"]
    rows = []
    for example_dir in gather_example_dirs():
        directory = os.path.join(os.path.join(EXAMPLES_DIR, example_dir), "")
        specs = glob.glob(directory + "*.yaml")
        for spec in sorted(specs):
            if "template" in spec:
                continue
            with open(spec) as f:  # pylint: disable=C0103
                try:
                    spec_metadata = yaml.safe_load(f)["description"]
                except KeyError:
                    LOG.warning(f"{spec} lacks required section 'description'")
                    continue
                except TypeError:
                    continue
            name = spec_metadata["name"]
            if name is None:
                continue
            # if there is a variable reference in the workflow name, instead list the filename (minus the yaml extension).
            if "$" in name:
                name = os.path.basename(os.path.normpath(spec)).replace(".yaml", "")
            rows.append([name, spec_metadata["description"]])
    return "\n" + tabulate.tabulate(rows, headers) + "\n"


def setup_example(name: str, outdir: str) -> Union[str, None]:
    """
    Set up the given example by copying it to the specified output directory.

    Args:
        name: The name of the example to set up.
        outdir: The output directory where the example will be copied.

    Returns:
        The name of the example if successful, or None if the example was not found or an error occurred.
    """
    example = None
    spec_paths = gather_all_examples()
    spec_path = None
    for spec_path in spec_paths:
        spec = os.path.basename(os.path.normpath(spec_path)).replace(".yaml", "")
        if name == spec:
            example = os.path.basename(os.path.dirname(spec_path))
            break
    if example is None:
        LOG.error(f"Example '{name}' not found.")
        return None

    # if there is only 1 file in the example, don't bother making a directory for it
    if len(os.listdir(os.path.dirname(spec_path))) == 1:
        src_path = os.path.join(EXAMPLES_DIR, os.path.join(example, example + ".yaml"))

    else:
        src_path = os.path.join(EXAMPLES_DIR, example)

        if outdir:
            outdir = os.path.join(os.getcwd(), outdir)
        else:
            outdir = os.path.join(os.getcwd(), example)

        if os.path.exists(outdir):
            LOG.error(f"File '{outdir}' already exists!")
            return None

    if outdir is None:
        outdir = os.getcwd()

    LOG.info(f"Copying example '{name}' to {outdir}")
    write_example(src_path, outdir)
    return example
