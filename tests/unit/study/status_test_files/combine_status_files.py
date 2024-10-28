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
Script to combine all status files from a study into one

Usage: python combine_status_files.py <workspace with statuses> <output filename>.json [-d]
"""
import argparse
import json
import logging
import os
import sys

from merlin.utils import dict_deep_merge, verify_dirpath, verify_filepath


LOG = logging.getLogger(__name__)


def combine_status_files(workspace, outfile, delete):
    """
    Traverse the workspace looking for status files and condense their contents
    into one outfile.

    :param `workspace`: The workspace to search for status files in
    :param `outfile`: The output file that we're dumping statuses to
    :param `delete`: If true, delete status files found in workspace after reading their contents
    """
    condensed_statuses = {}

    # Traverse the step workspace and look for MERLIN_STATUS files
    for root, _, files in os.walk(workspace):
        # We only care about this level of the workspace if there's a MERLIN_STATUS file
        if "MERLIN_STATUS.json" in files:
            # Read in the status
            status_filepath = f"{root}/MERLIN_STATUS.json"
            with open(status_filepath, "r") as status_file:
                status = json.load(status_file)

            # Merge the status with the other statuses (and delete the status file if necessary)
            dict_deep_merge(condensed_statuses, status)
            if delete:
                os.remove(status_filepath)

    # Dump all of the statuses to the outfile
    with open(outfile, "w") as condensed_status_file:
        json.dump(condensed_statuses, condensed_status_file)


def main():
    """
    Build the argparse object, verify the arguments provided by the user, and then
    call combine_status_files
    """
    # Create argument parser and parse the args
    parser = argparse.ArgumentParser(prog="combine-statuses", description="Combine the status files from a workspace")
    parser.add_argument("workspace", action="store", help="the workspace containing status files")
    parser.add_argument("outfile", action="store", help="a json filepath to dump the statuses to")
    parser.add_argument(
        "-d", "--delete", dest="delete", action="store_true", help="delete the status files after reading their information"
    )
    args = parser.parse_args()

    # Verify workspace value provided and get absolute path
    try:
        workspace = verify_dirpath(args.workspace)
    except ValueError:
        LOG.error(f"The directory path {args.workspace} does not exist.")
        sys.exit()

    # Verify outfile path
    if not args.outfile.endswith(".json"):
        LOG.error("The outfile must be json.")
        sys.exit()
    try:
        outfile = verify_filepath(args.outfile)
    except ValueError:
        LOG.error(f"The file path {args.outfile} does not exist.")
        sys.exit()

    # Combine the status files
    combine_status_files(workspace, outfile, args.delete)


if __name__ == "__main__":
    main()
