##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

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
