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
This module handles the CLI for the merlin-example.
"""
import argparse
import logging
import os
import sys

from merlin.ascii_art import banner_small
from merlin.examples.generator import list_examples, setup_example
from merlin.log_formatter import setup_logging


LOG = logging.getLogger("merlin-example")
DEFAULT_LOG_LEVEL = "INFO"


def process_example(args):
    setup_example(args.example, args.path)


def example_list(args):
    print(banner_small)
    list_examples()


def setup_argparse():
    parser = argparse.ArgumentParser(
        prog="Merlin Examples",
        description=banner_small,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=list_examples()
        + "\nSee merlin-example <command> --help for more info.\n",
    )
    parser.add_argument(
        "-lvl",
        "--level",
        action="store",
        dest="level",
        type=str,
        default=DEFAULT_LOG_LEVEL,
        help="Set the log level. Options: DEBUG, INFO, WARNING, ERROR. "
        "[Default: %(default)s]",
    )
    parser.add_argument(
        "example", action="store", type=str, help="The name of the example to setup."
    )
    parser.add_argument(
        "-p",
        "--path",
        action="store",
        type=str,
        default=None,
        help="Specify a path to write the workflow to. Defaults to current "
        "working directory",
    )
    parser.set_defaults(func=process_example)

    return parser


def main():
    parser = setup_argparse()
    args = parser.parse_args()

    setup_logging(logger=LOG, log_level=args.level.upper(), colors=True)

    args.func(args)


if __name__ == "__main__":
    sys.exit(main())
