##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Utilities for the database subcommand.
"""

import logging
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser, Namespace
from typing import Dict

from merlin.cli.commands.database.entity_registry import ENTITY_REGISTRY
from merlin.utils import get_plural_of_entity


LOG = logging.getLogger("merlin")


def setup_db_entity_subcommands(subcommand_parser: ArgumentParser, subcommand_name: str) -> Dict[str, ArgumentParser]:
    """
    Dynamically sets up subcommands for each entity type for a given subcommand.

    This function adds both singular (`<entity>`) and plural (`all-<entities>`) variants
    to support direct targeting and filter-based selection, respectively.

    Args:
        subcommand_parser (ArgumentParser): The parser to which entity subcommands should be added.
        subcommand_name (str): The name of the subcommand being configured (e.g., "delete", "get").

    Returns:
        A mapping from subcommand name to the corresponding ArgumentParser instance.
    """
    parser_map = {}

    for entity_key, config in ENTITY_REGISTRY.items():
        ident_help = config["ident_help"].format(verb=subcommand_name)
        plural_name = get_plural_of_entity(entity_key)
        filters = config["filters"]

        # <entity> command
        singular = subcommand_parser.add_parser(
            entity_key,
            help=f"{subcommand_name.capitalize()} one or more {plural_name} by {config['identifiers']}.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )
        singular.add_argument(
            "entity",
            type=str,
            nargs="+",
            help=ident_help,
        )
        parser_map[entity_key] = singular

        # all-<entities> command
        all_name = f"all-{plural_name}"
        all_parser = subcommand_parser.add_parser(
            all_name,
            help=f"{subcommand_name.capitalize()} all {plural_name} (supports filters).",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )
        for filt in filters:
            arg_name = filt["name"]
            arg_type = filt["type"]
            nargs = filt.get("nargs")
            all_parser.add_argument(
                f"--{arg_name.replace('_', '-')}",
                type=arg_type,
                nargs=nargs,
                help=f"Filter by {arg_name.replace('_', ' ')}.",
            )
        parser_map[all_name] = all_parser

    return parser_map


def get_filters_for_entity(args: Namespace, entity_type: str) -> Dict:
    """
    Extracts filter arguments from parsed CLI input for a specific entity type.

    This is used to dynamically build the keyword arguments for querying or deleting
    entities via the database manager.

    Args:
        args (Namespace): Parsed command-line arguments.
        entity_type (str): The entity type whose filter definitions should be used.

    Returns:
        A dictionary of filter argument names to their provided values. Returns an
            empry dictionary if the entity is invalid or has no filters.
    """
    entity_config = ENTITY_REGISTRY.get(entity_type, None)
    if not entity_config:
        LOG.error(f"Invalid entity: '{entity_type}'.")
        return {}

    filter_options = entity_config.get("filters", {})
    if not filter_options:
        LOG.error(f"No filters supported for '{entity_type}'.")
        return {}

    filter_keys = [filter["name"] for filter in filter_options]
    filters = {key: getattr(args, key) for key in filter_keys if getattr(args, key) is not None}
    return filters
