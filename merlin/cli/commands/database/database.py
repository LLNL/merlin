##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module defines the `DatabaseCommand` class, which provides CLI subcommands
for interacting with the Merlin application's underlying database. It supports
commands for retrieving, deleting, and inspecting database contents, including
entities like studies, runs, and workers.

The commands are registered under the `database` top-level command and integrated
into Merlin's argument parser system.
"""

import logging
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser, Namespace

from merlin.cli.commands.command_entry_point import CommandEntryPoint
from merlin.cli.commands.database.delete import DatabaseDeleteCommand
from merlin.cli.commands.database.get import DatabaseGetCommand
from merlin.cli.commands.database.info import DatabaseInfoCommand


LOG = logging.getLogger("merlin")


class DatabaseCommand(CommandEntryPoint):
    """
    Handles `database` CLI commands for interacting with Merlin's database.

    Attributes:
        info_command (cli.commands.database.info.DatabaseInfoCommand): Handles the `database info` subcommand.
        get_command (cli.commands.database.get.DatabaseGetCommand): Handles the `database get` subcommand.
        delete_command (cli.commands.database.delete.DatabaseDeleteCommand): Handles the `database delete` subcommand.

    Methods:
        add_parser: Adds the `database` command and its subcommands to the CLI parser.
        process_command: Processes the CLI input and dispatches the appropriate action.
    """

    def __init__(self):
        """
        Initialize the `DatabaseCommand` instance and its subcommand handlers.
        """
        self.info_command = DatabaseInfoCommand()
        self.get_command = DatabaseGetCommand()
        self.delete_command = DatabaseDeleteCommand()

    def add_parser(self, subparsers: ArgumentParser):
        """
        Add the `database` command parser to the CLI argument parser.

        Parameters:
            subparsers (ArgumentParser): The subparsers object to which the `database` command parser will be added.
        """
        database: ArgumentParser = subparsers.add_parser(
            "database",
            help="Interact with Merlin's database.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )
        database.set_defaults(func=self.process_command)

        database.add_argument(
            "-l",
            "--local",
            action="store_true",
            help="Use the local SQLite database for this command.",
        )

        database_commands: ArgumentParser = database.add_subparsers(dest="commands", required=True)

        self.info_command.add_parser(database_commands)
        self.get_command.add_parser(database_commands)
        self.delete_command.add_parser(database_commands)

    def process_command(self, args: Namespace):
        """
        This method doesn't do anything as the subcommands each have logic
        for processing their respective commands. This still has to be implemented
        as we inherit from CommandEntryPoint.

        Args:
            args: An argparse Namespace containing user arguments.
        """
