##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
CLI module for displaying configuration and environment information.

This module defines the `InfoCommand` class, which handles the `info` subcommand
of the Merlin CLI. The `info` command is intended to display detailed information
about the current Merlin configuration, Python environment, and other diagnostic
data useful for debugging or verifying setup.
"""

# pylint: disable=duplicate-code

import logging
from argparse import ArgumentParser, Namespace

from merlin.cli.commands.command_entry_point import CommandEntryPoint


LOG = logging.getLogger("merlin")


class InfoCommand(CommandEntryPoint):
    """
    Handles `info` CLI command for viewing information about server connections.

    Methods:
        add_parser: Adds the `info` command to the CLI parser.
        process_command: Processes the CLI input and dispatches the appropriate action.
    """

    def add_parser(self, subparsers: ArgumentParser):
        """
        Add the `info` command parser to the CLI argument parser.

        Parameters:
            subparsers (ArgumentParser): The subparsers object to which the `info` command parser will be added.
        """
        info: ArgumentParser = subparsers.add_parser(
            "info",
            help="display info about the merlin configuration and the python configuration. Useful for debugging.",
        )
        info.set_defaults(func=self.process_command)

    def process_command(self, args: Namespace):
        """
        CLI command to print merlin configuration info.

        Args:
            args: Parsed CLI arguments.
        """
        # if this is moved to the toplevel per standard style, merlin is unable to generate the (needed) default config file
        from merlin import display  # pylint: disable=import-outside-toplevel

        display.print_info(args)
