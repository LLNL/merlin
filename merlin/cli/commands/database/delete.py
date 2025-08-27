##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Implements the `database delete` subcommand for the Merlin CLI.

This module defines the `DatabaseDeleteCommand` class, which enables users to
remove entities from the Merlin database through the CLI. It supports targeted
deletion by identifier, bulk deletion of entities with optional filters, and
complete database clearance.

Entity types and their supported operations are dynamically registered via the
`ENTITY_REGISTRY`, and entity-specific flags (e.g., study-related options) are
automatically integrated into the CLI. This design ensures easy extensibility
as new entities or deletion options are introduced.

Main Capabilities:
- `database delete <entity> <ids...>`: Delete specific entities by their IDs.
- `database delete all-<entity>`: Delete all instances of an entity type with optional filters.
- `database delete everything`: Delete the entire database, with optional force flag.
"""

import logging
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser, Namespace
from typing import Dict, List

from merlin.cli.commands.command_entry_point import CommandEntryPoint
from merlin.cli.commands.database.entity_registry import ENTITY_REGISTRY
from merlin.cli.utils import get_filters_for_entity, setup_db_entity_subcommands
from merlin.config.configfile import initialize_config
from merlin.db_scripts.merlin_db import MerlinDatabase
from merlin.utils import get_singular_of_entity


LOG = logging.getLogger("merlin")


class DatabaseDeleteCommand(CommandEntryPoint):
    """
    Handles the `database delete` subcommand, which deletes entities from the
    Merlin database based on type, identifiers, and filters.

    Methods:
        add_parser: Register the `database delete` command and its subcommands with the CLI parser.
        process_command: Dispatch the appropriate database deletion logic based on CLI args.
        _delete_entities: Deletes specific entities.
        _delete_all_entities: Deletes all entities of a type, applying filters if present.
    """

    def add_parser(self, database_commands: ArgumentParser):  # pylint: disable=arguments-renamed
        """
        Add the `database delete` subcommand parser to the CLI argument parser.

        Parameters:
            database_commands (ArgumentParser): The subparsers object to which the `database delete`
                subcommand parser will be added.
        """
        # Subcommand: database delete
        db_delete_parser = database_commands.add_parser(
            "delete",
            help="Delete information stored in the database.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )
        db_delete_parser.set_defaults(func=self.process_command)

        # Subcommand: database delete <entity> or all-<entity>
        delete_subcommands_parser = db_delete_parser.add_subparsers(dest="delete_type", required=True)
        db_delete_subcommands = setup_db_entity_subcommands(delete_subcommands_parser, "delete")

        # Add additional arguments for specific commands
        db_delete_subcommands["study"].add_argument(
            "-k",
            "--keep-associated-runs",
            action="store_true",
            help="Keep runs associated with the studies.",
        )
        db_delete_subcommands["all-studies"].add_argument(
            "-k",
            "--keep-associated-runs",
            action="store_true",
            help="Keep runs associated with the studies.",
        )

        # Subcommand: database delete everything
        delete_everything = delete_subcommands_parser.add_parser(
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

    def _extract_entity_kwargs(self, args: Namespace, entity_type: str) -> Dict:
        """
        Extracts entity-specific keyword arguments from the CLI args for deletion logic.

        Parameters:
            args (Namespace): Parsed CLI arguments.
            entity_type (str): The singular form of the entity type being deleted.

        Returns:
            Keyword arguments to pass to `delete` or `delete_all`.
        """
        kwargs = {}

        if entity_type == "study":
            kwargs["remove_associated_runs"] = not args.keep_associated_runs

        # Add more entity-specific argument extraction here as needed
        return kwargs

    def _delete_entities(self, entity_type: str, identifiers: List[str], merlin_db: MerlinDatabase, **kwargs):
        """
        Delete individual entities of the given type by their identifiers.

        Parameters:
            entity_type (str): The type of entity to delete (e.g., "run", "study").
            identifiers (List[str]): A list of entity identifiers to delete.
            merlin_db (MerlinDatabase): Interface to the Merlin database.
        """
        for identifier in identifiers:
            merlin_db.delete(entity_type.replace("-", "_"), identifier, **kwargs)

    def _delete_all_entities(self, entity_type: str, args: Namespace, merlin_db: MerlinDatabase, **kwargs):
        """
        Delete all entities of the given type, optionally applying filters.

        Parameters:
            entity_type (str): The type of entity to delete (e.g., "run", "study").
            args (Namespace): Parsed CLI arguments containing optional filter values.
            merlin_db (MerlinDatabase): Interface to the Merlin database.
        """
        db_entity_type = entity_type.replace("-", "_")
        filters = get_filters_for_entity(args, entity_type)

        if filters:
            LOG.info(f"Deleting all {entity_type} entities matching filters: {filters}")
            entities = merlin_db.get_all(db_entity_type, filters=filters)
            for entity in entities:
                merlin_db.delete(db_entity_type, entity.id, **kwargs)
        else:
            LOG.info(f"Deleting all {entity_type} entities (no filters applied)")
            merlin_db.delete_all(db_entity_type, **kwargs)

    def process_command(self, args: Namespace):
        """
        Process the `database delete` command using the provided CLI arguments.

        Args:
            args (Namespace): Parsed CLI arguments from the user.
        """
        if args.local:
            initialize_config(local_mode=True)

        merlin_db = MerlinDatabase()
        delete_type = args.delete_type

        # Process `delete everything` command
        if delete_type == "everything":
            merlin_db.delete_everything(force=args.force)
        # Process `delete all-<entity>` commands
        elif delete_type.startswith("all-"):
            entity_type = get_singular_of_entity(delete_type[4:])
            kwargs = self._extract_entity_kwargs(args, entity_type)
            self._delete_all_entities(entity_type, args, merlin_db, **kwargs)
        # Process `delete <entity>` commands
        elif delete_type in ENTITY_REGISTRY:
            entity_type = delete_type
            kwargs = self._extract_entity_kwargs(args, entity_type)
            self._delete_entities(entity_type, args.entity, merlin_db, **kwargs)
        # Fallback for unrecognized
        else:
            LOG.error(f"Unrecognized delete_type: {delete_type}")
