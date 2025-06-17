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
from merlin.config.configfile import initialize_config
from merlin.db_scripts.db_commands import database_delete, database_get, database_info

LOG = logging.getLogger("merlin")


class DatabaseCommand(CommandEntryPoint):
    """
    Handles `database` CLI commands for interacting with Merlin's database.

    Methods:
        add_parser: Adds the `database` command and its subcommands to the CLI parser.
        process_command: Processes the CLI input and dispatches the appropriate action.
    """

    def _add_delete_subcommand(self, database_commands: ArgumentParser):
        """
        Add the `delete` subcommand and its options to remove data from the database.

        Parameters:
            database_commands (ArgumentParser): The parent parser for database subcommands.
        """
        db_delete: ArgumentParser = database_commands.add_parser(
            "delete",
            help="Delete information stored in the database.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )

        # Add subcommands for delete
        delete_subcommands = db_delete.add_subparsers(dest="delete_type", required=True)

        # TODO enable support for deletion of study by passing in spec file
        # Subcommand: delete study
        delete_study = delete_subcommands.add_parser(
            "study",
            help="Delete one or more studies by ID or name.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )
        delete_study.add_argument(
            "study",
            type=str,
            nargs="+",
            help="A space-delimited list of IDs or names of studies to delete.",
        )
        delete_study.add_argument(
            "-k",
            "--keep-associated-runs",
            action="store_true",
            help="Keep runs associated with the studies.",
        )

        # Subcommand: delete run
        delete_run = delete_subcommands.add_parser(
            "run",
            help="Delete one or more runs by ID or workspace.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )
        delete_run.add_argument(
            "run",
            type=str,
            nargs="+",
            help="A space-delimited list of IDs or workspaces of runs to delete.",
        )
        # TODO implement the below option; this removes the output workspace from file system
        # delete_run.add_argument(
        #     "--delete-workspace",
        #     action="store_true",
        #     help="Delete the output workspace for the run.",
        # )

        # Subcommand: delete logical-worker
        delete_logical_worker = delete_subcommands.add_parser(
            "logical-worker",
            help="Delete one or more logical workers by ID.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )
        delete_logical_worker.add_argument(
            "worker",
            type=str,
            nargs="+",
            help="A space-delimited list of IDs of logical workers to delete.",
        )

        # Subcommand: delete physical-worker
        delete_physical_worker = delete_subcommands.add_parser(
            "physical-worker",
            help="Delete one or more physical workers by ID or name.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )
        delete_physical_worker.add_argument(
            "worker",
            type=str,
            nargs="+",
            help="A space-delimited list of IDs of physical workers to delete.",
        )

        # Subcommand: delete all-studies
        delete_all_studies = delete_subcommands.add_parser(
            "all-studies",
            help="Delete all studies from the database.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )
        delete_all_studies.add_argument(
            "-k",
            "--keep-associated-runs",
            action="store_true",
            help="Keep runs associated with the studies.",
        )

        # Subcommand: delete all-runs
        delete_subcommands.add_parser(
            "all-runs",
            help="Delete all runs from the database.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )

        # Subcommand: delete all-logical-workers
        delete_subcommands.add_parser(
            "all-logical-workers",
            help="Delete all logical workers from the database.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )

        # Subcommand: delete all-physical-workers
        delete_subcommands.add_parser(
            "all-physical-workers",
            help="Delete all physical workers from the database.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )

        # Subcommand: delete everything
        delete_everything = delete_subcommands.add_parser(
            "everything",
            help="Delete everything from the database.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )
        delete_everything.add_argument(
            "-f",
            "--force",
            action="store_true",
            help="Delete everything in the database without confirmation.",
        )

    def _add_get_subcommand(self, database_commands: ArgumentParser):
        """
        Add the `get` subcommand and its options to retrieve data from the database.

        Parameters:
            database_commands (ArgumentParser): The parent parser for database subcommands.
        """
        db_get: ArgumentParser = database_commands.add_parser(
            "get",
            help="Get information stored in the database.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )

        # Add subcommands for get
        get_subcommands = db_get.add_subparsers(dest="get_type", required=True)

        # TODO enable support for retrieval of study by passing in spec file
        # Subcommand: get study
        get_study = get_subcommands.add_parser(
            "study",
            help="Get one or more studies by ID or name.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )
        get_study.add_argument(
            "study",
            type=str,
            nargs="+",
            help="A space-delimited list of IDs or names of the studies to get.",
        )

        # Subcommand: get run
        get_run = get_subcommands.add_parser(
            "run",
            help="Get one or more runs by ID or workspace.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )
        get_run.add_argument(
            "run",
            type=str,
            nargs="+",
            help="A space-delimited list of IDs or workspaces of the runs to get.",
        )

        # Subcommand get logical-worker
        get_logical_worker = get_subcommands.add_parser(
            "logical-worker",
            help="Get one or more logical workers by ID.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )
        get_logical_worker.add_argument(
            "worker",
            type=str,
            nargs="+",
            help="A space-delimited list of IDs of the logical workers to get.",
        )

        # Subcommand get physical-worker
        get_physical_worker = get_subcommands.add_parser(
            "physical-worker",
            help="Get one or more physical workers by ID or name.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )
        get_physical_worker.add_argument(
            "worker",
            type=str,
            nargs="+",
            help="A space-delimited list of IDs or names of the physical workers to get.",
        )

        # Subcommand: get all-studies
        get_subcommands.add_parser(
            "all-studies",
            help="Get all studies from the database.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )

        # Subcommand: get all-runs
        get_subcommands.add_parser(
            "all-runs",
            help="Get all runs from the database.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )

        # Subcommand: get all-logical-workers
        get_subcommands.add_parser(
            "all-logical-workers",
            help="Get all logical workers from the database.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )

        # Subcommand: get all-physical-workers
        get_subcommands.add_parser(
            "all-physical-workers",
            help="Get all physical workers from the database.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )

        # Subcommand: get everything
        get_subcommands.add_parser(
            "everything",
            help="Get everything from the database.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )

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

        database_commands: ArgumentParser = database.add_subparsers(dest="commands")

        # Subcommand: database info
        database_commands.add_parser(
            "info",
            help="Print information about the database.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )

        # Subcommand: database delete
        self._add_delete_subcommand(database_commands)

        # Subcommand: database get
        self._add_get_subcommand(database_commands)

    def process_command(self, args: Namespace):
        """
        Process database commands by routing to the correct function.

        Args:
            args: An argparse Namespace containing user arguments.
        """
        if args.local:
            initialize_config(local_mode=True)

        if args.commands == "info":
            database_info()
        elif args.commands == "get":
            database_get(args)
        elif args.commands == "delete":
            database_delete(args)