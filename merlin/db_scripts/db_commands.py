##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module acts as an interface for users to interact with Merlin's
database.
"""

import logging
from argparse import Namespace
from typing import Any, List

from merlin.db_scripts.merlin_db import MerlinDatabase


LOG = logging.getLogger("merlin")


def database_info():
    """
    Print information about the database to the console.
    """
    merlin_db = MerlinDatabase()
    db_studies = merlin_db.get_all("study")
    db_runs = merlin_db.get_all("run")
    db_logical_workers = merlin_db.get_all("logical_worker")
    db_physical_workers = merlin_db.get_all("physical_worker")

    print("Merlin Database Information")
    print("---------------------------")
    print("General Information:")
    print(f"- Database Type: {merlin_db.get_db_type()}")
    print(f"- Database Version: {merlin_db.get_db_version()}")
    print(f"- Connection String: {merlin_db.get_connection_string()}")

    print()
    print("Studies:")
    print(f"- Total: {len(db_studies)}")
    # TODO add something about recent studies that looks like so:
    # - Recent Studies:
    #     1. Study ID: 123, Name: "Experiment A"
    #     2. Study ID: 124, Name: "Experiment B"
    #     3. Study ID: 125, Name: "Experiment C"
    #     (and 9 more studies)

    print()
    print("Runs:")
    print(f"- Total: {len(db_runs)}")
    # TODO add something about recent runs that looks like so:
    # - Recent Runs:
    #     1. Run ID: 456, Workspace: "/path/to/workspace"
    #     2. Run ID: 457, Workspace: "/path/to/workspace"
    #     3. Run ID: 458, Workspace: "/path/to/workspace"
    #     (and 42 more runs)

    print()
    print("Logical Workers:")
    print(f"- Total: {len(db_logical_workers)}")

    print()
    print("Physical Workers:")
    print(f"- Total: {len(db_physical_workers)}")

    print()


def database_get(args: Namespace):
    """
    Handles the delegation of get operations to Merlin's database.

    Args:
        args: Parsed CLI arguments from the user.
    """
    merlin_db = MerlinDatabase()

    def print_items(items: List[Any], empty_message: str):
        """
        Prints a list of items or logs a message if the list is empty.

        Args:
            items: List of items to print.
            empty_message: Message to log if the list is empty.
        """
        if items:
            for item in items:
                print(item)
        else:
            LOG.info(empty_message)

    def get_and_print(entity_type: str, identifiers: List[str]):
        """
        Get entities by type and identifiers, then print them.

        Args:
            entity_type: The entity type (study, run, logical_worker, etc.).
            identifiers: List of identifiers for fetching.
        """
        items = [merlin_db.get(entity_type, identifier) for identifier in identifiers]
        print_items(items, f"No {entity_type.replace('_', ' ')}s found for the given identifiers.")

    def get_all_and_print(entity_type: str):
        """
        Get all entities of a given type and print them.

        Args:
            entity_type: The entity type.
        """
        items = merlin_db.get_all(entity_type)
        entity_type_str = "studies" if entity_type == "study" else f"{entity_type.replace('_', ' ')}s"
        print_items(items, f"No {entity_type_str} found in the database.")

    operations = {
        "study": lambda: get_and_print("study", args.study),
        "run": lambda: get_and_print("run", args.run),
        "logical-worker": lambda: get_and_print("logical_worker", args.worker),
        "physical-worker": lambda: get_and_print("physical_worker", args.worker),
        "all-studies": lambda: get_all_and_print("study"),
        "all-runs": lambda: get_all_and_print("run"),
        "all-logical-workers": lambda: get_all_and_print("logical_worker"),
        "all-physical-workers": lambda: get_all_and_print("physical_worker"),
        "everything": lambda: print_items(merlin_db.get_everything(), "Nothing found in the database."),
    }

    operation = operations.get(args.get_type)
    if operation:
        operation()
    else:
        LOG.error("No valid get option provided.")


def database_delete(args: Namespace):
    """
    Handles the delegation of delete operations to Merlin's database.

    Args:
        args: Parsed CLI arguments from the user.
    """
    merlin_db = MerlinDatabase()

    def delete_entities(entity_type: str, identifiers: List[str], **kwargs):
        """
        Delete a list of entities by type and identifier.

        Args:
            entity_type: The entity type (study, run, etc.).
            identifiers: The identifiers of the entities to delete.
            kwargs: Additional keyword args for the delete call.
        """
        for identifier in identifiers:
            merlin_db.delete(entity_type, identifier, **kwargs)

    operations = {
        "study": lambda: delete_entities("study", args.study, remove_associated_runs=not args.keep_associated_runs),
        "run": lambda: delete_entities("run", args.run),
        "logical-worker": lambda: delete_entities("logical_worker", args.worker),
        "physical-worker": lambda: delete_entities("physical_worker", args.worker),
        "all-studies": lambda: merlin_db.delete_all("study", remove_associated_runs=not args.keep_associated_runs),
        "all-runs": lambda: merlin_db.delete_all("run"),
        "all-logical-workers": lambda: merlin_db.delete_all("logical_worker"),
        "all-physical-workers": lambda: merlin_db.delete_all("physical_worker"),
        "everything": lambda: merlin_db.delete_everything(force=args.force),
    }

    operation = operations.get(args.delete_type)
    if operation:
        operation()
    else:
        LOG.error("No valid delete option provided.")
