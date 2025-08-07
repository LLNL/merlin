##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
CLI command package for interacting with the Merlin database.

This package defines and implements the `database` command group in the Merlin CLI,
enabling users to inspect, retrieve, and modify entities in the Merlin database,
including studies, runs, logical workers, and physical workers.

The modules within this package work together to provide a dynamic, extensible interface
that adapts to the available entity types and supports filtering, bulk operations,
and safe inspection mechanisms.

Modules:
    database: Entry point for the `database` command group. Registers subcommands like `get`, `delete`, and `info`.
    get: Defines the `database get` subcommand, which allows users to query and retrieve database contents.
    delete: Defines the `database delete` subcommand, which supports deletion of individual entities, filtered bulk
        deletions, or full database wipes.
    info: Defines the `database info` subcommand, which displays configuration details and previews of database contents.
    entity_registry: Maintains a central registry of supported entity types and their CLI metadata, enabling dynamic
        argument generation and command dispatching.

This package is designed for extensibility. Adding support for a new entity type generally requires only registering
it in the `ENTITY_REGISTRY`, with no changes needed to the subcommand logic (unless adding specific arguments to `get`
or `delete` logic).
"""

from merlin.cli.commands.database.database import DatabaseCommand


__all__ = ["DatabaseCommand"]
