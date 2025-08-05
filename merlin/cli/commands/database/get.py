##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Implements the `database get` subcommand for the Merlin CLI.

This module defines the `DatabaseGetCommand` class, which enables users to retrieve
data from the Merlin database. Users can query individual entities by identifier,
retrieve all entities of a given type (optionally filtered), or dump the entire
database contents.

The command supports dynamic registration of entity types using the `ENTITY_REGISTRY`
and integrates filter support for more precise queries. It also ensures compatibility
with both local and distributed configurations through the Merlin configuration system.

Main Capabilities:
- `database get <entity> <ids...>`: Retrieve one or more specific entities.
- `database get all-<entity>`: Retrieve all instances of an entity type with optional filters.
- `database get everything`: Retrieve all data from all registered entity types.
"""

import logging
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser, Namespace
from typing import Any, List

from merlin.cli.commands.command_entry_point import CommandEntryPoint
from merlin.cli.commands.database.entity_registry import ENTITY_REGISTRY
from merlin.cli.utils import get_filters_for_entity, setup_db_entity_subcommands
from merlin.config.configfile import initialize_config
from merlin.db_scripts.merlin_db import MerlinDatabase
from merlin.utils import get_plural_of_entity, get_singular_of_entity


LOG = logging.getLogger("merlin")


class DatabaseGetCommand(CommandEntryPoint):
    """
    Handles the `database get` subcommand, which retrieves data from the
    Merlin database based on entity type, identifiers, and filters.

    Methods:
        add_parser: Adds the `database get` parser and its subcommands.
        process_command: Dispatches the appropriate get operation based on CLI args.
        _print_items: Outputs items or logs a fallback message.
        _get_and_print: Fetches and prints specific entities.
        _get_all_and_print: Fetches and prints filtered entities of a type.
    """

    def add_parser(self, database_commands: ArgumentParser):  # pylint: disable=arguments-renamed
        """
        Add the `database get` subcommand parser to the CLI argument parser.

        Parameters:
            database_commands (ArgumentParser): The subparsers object to which the `database get`
                subcommand parser will be added.
        """
        # Subcommand: database get
        db_get_parser = database_commands.add_parser(
            "get",
            help="Get information stored in the database.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )
        db_get_parser.set_defaults(func=self.process_command)

        # Subcommand: database get <entity> or all-<entity>
        get_subcommands_parser = db_get_parser.add_subparsers(dest="get_type", required=True)
        setup_db_entity_subcommands(get_subcommands_parser, "get")

        # Subcommand: database get everything
        get_subcommands_parser.add_parser(
            "everything",
            help="Get everything from the database.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )

    def _print_items(self, items: List[Any], empty_message: str):
        """
        Print each item in a list, or log a message if the list is empty.

        Args:
            items (List[Any]): List of items to print.
            empty_message (str): Message to log if the list is empty.
        """
        if items:
            for item in items:
                print(item)
        else:
            LOG.info(empty_message)

    def _get_and_print(self, entity_type: str, identifiers: List[str], merlin_db: MerlinDatabase):
        """
        Fetch and print specific entities by type and identifier(s).

        Args:
            entity_type (str): The type of entity to retrieve (e.g., "study", "run").
            identifiers (List[str]): List of identifiers to retrieve.
            merlin_db (MerlinDatabase): Interface to the Merlin database.
        """
        items = [merlin_db.get(entity_type.replace("-", "_"), ident) for ident in identifiers]
        plural_name = get_plural_of_entity(entity_type, join_delimiter=" ")
        self._print_items(items, f"No {plural_name} found for the given identifiers.")

    def _get_all_and_print(self, args: Namespace, entity_type: str, merlin_db: MerlinDatabase):
        """
        Fetch and print all entities of a given type, with optional filters.

        Args:
            args (Namespace): Parsed CLI arguments.
            entity_type (str): The type of entity to retrieve (e.g., "run", "logical-worker").
            merlin_db (MerlinDatabase): Interface to the Merlin database.
        """
        filters = get_filters_for_entity(args, entity_type)
        items = merlin_db.get_all(entity_type.replace("-", "_"), filters=filters)
        plural_name = get_plural_of_entity(entity_type, join_delimiter=" ")
        filter_msg = f" with filters {filters}" if filters else ""
        self._print_items(items, f"No {plural_name}{filter_msg} found in the database.")

    def process_command(self, args: Namespace):
        """
        Process the `database get` command using the provided CLI arguments.

        Args:
            args: An argparse Namespace containing user arguments.
        """
        if args.local:
            initialize_config(local_mode=True)

        merlin_db = MerlinDatabase()
        get_type = args.get_type

        # Process `get everything` command
        if get_type == "everything":
            self._print_items(merlin_db.get_everything(), "Nothing found in the database.")
        # Process `get all-<entity>` commands
        elif get_type.startswith("all-"):
            entity_type = get_singular_of_entity(get_type[4:])  # Remove "all-" prefix first
            self._get_all_and_print(args, entity_type, merlin_db)
        # Process `get <entity>` commands
        elif get_type in ENTITY_REGISTRY:
            self._get_and_print(get_type, args.entity, merlin_db)
        # Fallback for unrecognized
        else:
            LOG.error(f"Unrecognized get_type: {get_type}")
