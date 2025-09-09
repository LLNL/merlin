##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Main CLI parser setup for the Merlin command-line interface.

This module defines the primary argument parser for the `merlin` CLI tool,
including custom error handling and integration of all available subcommands.
"""

import sys
from argparse import ArgumentParser, RawDescriptionHelpFormatter

from merlin import VERSION
from merlin.ascii_art import banner_small
from merlin.cli.commands import ALL_COMMANDS


DEFAULT_LOG_LEVEL = "INFO"


class HelpParser(ArgumentParser):
    """
    This class overrides the error message of the argument parser to
    print the help message when an error happens.

    Methods:
        error: Override the error message of the `ArgumentParser` class.
    """

    def error(self, message: str):
        """
        Override the error message of the `ArgumentParser` class.

        Args:
            message: The error message to log.
        """
        sys.stderr.write(f"error: {message}\n")
        self.print_help()
        sys.exit(2)


def build_main_parser() -> ArgumentParser:
    """
    Set up the command-line argument parser for the Merlin package.

    Returns:
        An `ArgumentParser` object with every parser defined in Merlin's codebase.
    """
    parser = HelpParser(
        prog="merlin",
        description=banner_small,
        formatter_class=RawDescriptionHelpFormatter,
        epilog="See merlin <command> --help for more info",
    )
    parser.add_argument("-v", "--version", action="version", version=VERSION)
    parser.add_argument(
        "-lvl",
        "--level",
        type=str,
        default=DEFAULT_LOG_LEVEL,
        help="Set log level: DEBUG, INFO, WARNING, ERROR [Default: %(default)s]",
    )
    subparsers = parser.add_subparsers(dest="subparsers", required=True)

    for command in ALL_COMMANDS:
        command.add_parser(subparsers)

    return parser
