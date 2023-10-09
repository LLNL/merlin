###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.11.0.
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
This module contains a list of examples that can be used when learning to use
Merlin, or for setting up new workflows.

Examples are packaged in directories, with the directory name denoting
the example name.  This must match the name of the merlin specification inside.
"""
import glob
import logging
import os
import shutil

import tabulate
import yaml


LOG = logging.getLogger(__name__)

EXAMPLES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "workflows")


def gather_example_dirs():
    """Get all the example directories"""
    result = {}
    for directory in os.listdir(EXAMPLES_DIR):
        result[directory] = directory
    return result


def gather_all_examples():
    """Get all the example yaml files"""
    path = os.path.join(os.path.join(EXAMPLES_DIR, ""), os.path.join("*", "*.yaml"))
    return glob.glob(path)


def write_example(src_path, dst_path):
    """
    Write out the example workflow to a file.
    :param src_path: The path to copy from.
    :param content: The formatted content to write the file to.
    """
    if os.path.isdir(src_path):
        shutil.copytree(src_path, dst_path)
    else:
        shutil.copy(src_path, dst_path)


def list_examples():
    """List all available examples."""
    headers = ["name", "description"]
    rows = []
    for example_dir in gather_example_dirs():
        directory = os.path.join(os.path.join(EXAMPLES_DIR, example_dir), "")
        specs = glob.glob(directory + "*.yaml")
        for spec in specs:
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


def setup_example(name, outdir):
    """Setup the given example."""
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
