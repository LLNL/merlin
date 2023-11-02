###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.11.1.
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
This module handles the CLI for the deprecated `merlin-templates` command.
"""
import argparse
import logging
import sys

from merlin.ascii_art import banner_small
from merlin.log_formatter import setup_logging


LOG = logging.getLogger("merlin-templates")
DEFAULT_LOG_LEVEL = "ERROR"

# We disable all pylint errors in this file since this is deprecated anyways


def process_templates(args):  # pylint: disable=W0613,C0116
    LOG.error("The command `merlin-templates` has been deprecated in favor of `merlin example`.")


def setup_argparse():  # pylint: disable=C0116
    parser = argparse.ArgumentParser(
        prog="Merlin Examples",
        description=banner_small,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.set_defaults(func=process_templates)
    return parser


def main():  # pylint: disable=C0116
    try:
        parser = setup_argparse()
        args = parser.parse_args()
        setup_logging(logger=LOG, log_level=DEFAULT_LOG_LEVEL, colors=True)
        args.func(args)
        sys.exit()
    except Exception as ex:  # pylint: disable=W0718
        print(ex)
        sys.exit(1)


if __name__ == "__main__":
    main()
