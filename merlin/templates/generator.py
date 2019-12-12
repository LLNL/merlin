###############################################################################
# Copyright (c) 2019, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.0.5.
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
This module contains a list of example templates that can be used for setting
up new workflows.
"""
import logging
import os

import shutil
import tabulate

from merlin.templates import examples


LOG = logging.getLogger("merlin-templates")

TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "workflows")


def gather_templates():
    result = {}
    for d in os.listdir(TEMPLATE_DIR):
        result[d] = d
    return result


def find_template(name):
    for template in examples.TEMPLATES:
        if template["name"] == name:
            return template  # Return only the first template for now.


def write_template(src_path, dst_path):
    """
    Write out the template workflow to a file.

    :param src_path: The path to copy from.
    :param content: The formatted content to write the file to.
    """
    if os.path.isdir(src_path):
        print(f"{src_path}")
        print(f"{dst_path}")
        shutil.copytree(src_path, dst_path) 
    else:
        with open(src_path, "w") as _file:
            _file.write(src_path)


def list_templates():
    """List all available templates."""
    templates = examples.TEMPLATES

    print("")
    headers = ["name", "description"]
    rows = []
    for template in templates:
        rows.append([template["name"], template["description"]])
    print(tabulate.tabulate(rows, headers))
    print("")


def setup_template(name, outdir):
    """Setup the given template."""
    #template = find_template(name)
    template = gather_templates()[name]

    if template is None:
        LOG.error(f"Template '{name}' not found.")
        return None

    if template == "simple_chain":
        src_path = os.path.join(TEMPLATE_DIR, os.path.join("simple_chain", "simple_chain.yaml"))
    else:
        src_path = os.path.join(TEMPLATE_DIR, template)
        if outdir is None:
            outdir = os.path.join(os.getcwd(), template)

        if os.path.exists(outdir):
            LOG.error(f"File '{outdir}' already exists!")
            return None

    LOG.info(f"Copying template '{name}' to {outdir}")
    write_template(src_path, outdir)
    return template
