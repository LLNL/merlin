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

import tabulate

from merlin.templates import examples


LOG = logging.getLogger("merlin-templates")


def find_template(name):
    for template in examples.TEMPLATES:
        if template["name"] == name:
            return template  # Return only the first template for now.


def write_template(filepath, content):
    """
    Write out the template workflow to a file.

    :param filepath: The path to write the template file to.
    :param content: The formatted content to write the file to.
    """
    with open(filepath, "w") as _file:
        _file.write(content)


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


def setup_template(name, outdir=None):
    """Setup the given template."""
    template = find_template(name)

    if template is None:
        LOG.error(f"Template '{name}' not found.")
        return None

    if outdir:
        filepath = os.path.join(outdir, template["filename"])
    else:
        filepath = template["filename"]

    if os.path.isfile(filepath):
        LOG.error(f"Filename '{filepath}' already exists!")
        return None

    LOG.info(f"Copying template '{name}' to {outdir}")
    write_template(filepath, template["content"])
    return template
