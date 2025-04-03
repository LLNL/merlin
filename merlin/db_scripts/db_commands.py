"""
This module acts as an interface for users to interact with Merlin's
database.
"""

import logging
from argparse import Namespace
from typing import Callable, List

from merlin.db_scripts.db_entity import DatabaseEntity
from merlin.db_scripts.merlin_db import MerlinDatabase


LOG = logging.getLogger("merlin")


def database_info():
    """
    Print information about the database to the console.
    """
    merlin_db = MerlinDatabase()
    db_studies = merlin_db.get_all_studies()
    db_runs = merlin_db.get_all_runs()
    db_logical_workers = merlin_db.get_all_logical_workers()

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
    print()
    print("Logical Workers:")
    print(f"- Total: {len(db_logical_workers)}")

    print()


def database_get(args: Namespace):
    """
    Handles the delegation of get operations to Merlin's database.

    Args:
        args: Parsed CLI arguments from the user.
    """
    merlin_db = MerlinDatabase()

    def build_list_of_entities(get_entity: Callable, identifiers: List[str]) -> List[DatabaseEntity]:
        """
        Builds a list of entities by retrieving them using the provided function.

        Args:
            get_entity: A callable function that takes an identifier as input and retrieves the
                corresponding entity.
            identifiers: A list of identifiers used to retrieve the entities.

        Returns:
            A list of [`DatabaseEntity`][db_scripts.db_entity.DatabaseEntity] objects retrieved using the
                `get_entity` function.
        """
        results = []
        for identifier in identifiers:
            results.append(get_entity(identifier))
        return results

    def print_items(items: List[DatabaseEntity], empty_message: str):
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

    # Dispatch table for get operations
    operations = {
        "study": lambda: print_items(
            build_list_of_entities(merlin_db.get_study, args.study),
            "No studies found for the given identifiers.",
        ),
        "run": lambda: print_items(
            build_list_of_entities(merlin_db.get_run, args.run),
            "No runs found for the given identifiers.",
        ),
        "logical-worker": lambda: print_items(
            build_list_of_entities(merlin_db.get_logical_worker, args.worker),
            "No logical workers found for the given identifiers.",
        ),
        "all-studies": lambda: print_items(merlin_db.get_all_studies(), "No studies found in the database."),
        "all-runs": lambda: print_items(merlin_db.get_all_runs(), "No runs found in the database."),
        "all-logical-workers": lambda: print_items(
            merlin_db.get_all_logical_workers(), "No logical workers found in the database."
        ),
    }

    # Execute the appropriate operation or log an error
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

    def delete_list_of_entities(delete_entity: Callable, identifiers: List[str], **kwargs):
        """
        Deletes a list of entities using the provided delete function.

        Args:
            delete_entity: The function responsible for deleting an entity.
            identifiers: A list of entity identifiers to delete.
            kwargs: Additional arguments to pass to the delete function.
        """
        for identifier in identifiers:
            delete_entity(identifier, **kwargs)

    # Dispatch table for delete operations
    operations = {
        "study": lambda: delete_list_of_entities(
            merlin_db.delete_study, args.study, remove_associated_runs=not args.keep_associated_runs
        ),
        "run": lambda: delete_list_of_entities(merlin_db.delete_run, args.run),
        "logical-worker": lambda: delete_list_of_entities(merlin_db.delete_logical_worker, args.worker),
        "all-studies": lambda: merlin_db.delete_all_studies(remove_associated_runs=not args.keep_associated_runs),
        "all-runs": merlin_db.delete_all_runs,
        "all-logical-workers": merlin_db.delete_all_logical_workers,
        "everything": lambda: merlin_db.delete_everything(force=args.force),
    }

    # Execute the appropriate operation or log an error
    operation = operations.get(args.delete_type)
    if operation:
        operation()
    else:
        LOG.error("No valid delete option provided.")
