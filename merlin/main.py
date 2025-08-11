##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Main entry point into Merlin's codebase.
"""

import logging
import sys
import traceback

from merlin.cli.argparse_main import build_main_parser
from merlin.log_formatter import setup_logging


LOG = logging.getLogger("merlin")


def main():
    """
    Entry point for the Merlin command-line interface (CLI) operations.

    This function sets up the argument parser, handles command-line arguments,
    initializes logging, and executes the appropriate function based on the
    provided command. It ensures that the user receives help information if
    no arguments are provided and performs error handling for any exceptions
    that may occur during command execution.
    """
    parser = build_main_parser()
    if len(sys.argv) == 1:
        parser.print_help(sys.stdout)
        return 1
    args = parser.parse_args()

    setup_logging(logger=LOG, log_level=args.level.upper(), colors=True)

    try:
        args.func(args)
        # pylint complains that this exception is too broad - being at the literal top of the program stack, it's ok.
    except Exception as excpt:  # pylint: disable=broad-except
        LOG.debug(traceback.format_exc())
        LOG.error(str(excpt))
        sys.exit(1)

    # All paths in a function ought to return an exit code, or none of them should. Given the
    # distributed nature of Merlin, maybe it doesn't make sense for it to exit 0 until the work is completed, but
    # if the work is dispatched with no errors, that is a 'successful' Merlin run - any other failures are runtime.
    sys.exit()


if __name__ == "__main__":
    main()
