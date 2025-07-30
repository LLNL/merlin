##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module defines the `DatabaseInfoCommand` class, which implements the
`database info` subcommand for the Merlin CLI.

The `database info` subcommand provides users with summary details about the
currently active database configuration and contents. This includes backend type,
connection information, and a preview of stored entities.

The command is integrated into the broader Merlin CLI infrastructure through
the `CommandEntryPoint` base class and is registered under the top-level
`database` command group.
"""

from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser, Namespace

from merlin.cli.commands.command_entry_point import CommandEntryPoint
from merlin.config.configfile import initialize_config
from merlin.db_scripts.merlin_db import MerlinDatabase


class DatabaseInfoCommand(CommandEntryPoint):
    """
    Handles the `database info` subcommand, which prints configuration
    details about the currently active database backend.

    Methods:
        add_parser: Adds the `database info` command to the CLI parser.
        process_command: Processes the CLI input and dispatches the appropriate action.
    """

    def add_parser(self, subparsers: ArgumentParser):
        """
        Add the `database info` subcommand parser to the CLI argument parser.

        Parameters:
            subparsers (ArgumentParser): The subparsers object to which the `database info`
                subcommand parser will be added.
        """
        parser = subparsers.add_parser(
            "info",
            help="Print information about the database.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )
        parser.set_defaults(func=self.process_command)

        parser.add_argument(
            "-m",
            "--max-preview",
            type=int,
            default=3,
            help="The maximum number of entities to preview in the output.",
        )

    def process_command(self, args: Namespace):
        """
        Print information about the database to the console.

        Args:
            args: An argparse Namespace containing user arguments.
        """
        # TODO figure out a better way to handle configurations so we don't have to
        # check this in each of the database subcommands
        if args.local:
            initialize_config(local_mode=True)

        merlin_db = MerlinDatabase()
        merlin_db.info(max_preview=args.max_preview)
