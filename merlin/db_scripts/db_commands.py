"""
This module acts as an interface for users to interact with Merlin's
database.
"""

import logging
import uuid
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
    db_workers = merlin_db.get_all_workers()

    print("Merlin Database Information")
    print("---------------------------")
    print("General Information:")
    print(f"- Database Type: {merlin_db.get_db_type()}")
    print(f"- Database Version: {merlin_db.get_db_version()}")

    print()
    print("Studies:")
    print(f"- Total studies: {len(db_studies)}")
    # TODO add something about recent studies that looks like so:
    # - Recent Studies:
    #     1. Study ID: 123, Name: "Experiment A"
    #     2. Study ID: 124, Name: "Experiment B"
    #     3. Study ID: 125, Name: "Experiment C"
    #     (and 9 more studies)

    print()
    print("Runs:")
    print(f"- Total runs: {len(db_runs)}")
    # TODO add something about recent runs that looks like so:
    # - Recent Runs:
    #     1. Run ID: 456, Workspace: "/path/to/workspace"
    #     2. Run ID: 457, Workspace: "/path/to/workspace"
    #     3. Run ID: 458, Workspace: "/path/to/workspace"
    #     (and 42 more runs)

    print()
    print("Workers:")
    print(f"- Total workers: {len(db_workers)}")

    print()


def database_get(args: Namespace):
    """
    Handles the delegation of get operations to Merlin's database.

    Args:
        args: Parsed CLI arguments from the user.
    """
    merlin_db = MerlinDatabase()

    def get_by_uuid_or_name(get_by_uuid: Callable, get_by_name: Callable, identifiers: List[str]) -> List[DatabaseEntity]:
        """
        Attempts to retrieve items using UUIDs first, and falls back to retrieving by names if the
        UUIDs are invalid.

        Args:
            get_by_uuid: Function to retrieve items using UUIDs.
            get_by_name: Function to retrieve items using names.
            identifiers: List of identifiers to retrieve the items (either UUIDs or names).

        Returns:
            The retrieved items from the database.
        """
        results = []
        for identifier in identifiers:
            try:
                uuid.UUID(identifier)
                results.append(get_by_uuid(identifier))
            except ValueError:
                results.append(get_by_name(identifier))
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
            get_by_uuid_or_name(merlin_db.get_study, merlin_db.get_study_by_name, args.study),
            "No studies found for the given identifiers."
        ),
        "run": lambda: print_items(
            get_by_uuid_or_name(merlin_db.get_run, merlin_db.get_run_by_workspace, args.run),
            "No runs found for the given identifiers."
        ),
        "worker": lambda: print_items(
            get_by_uuid_or_name(merlin_db.get_worker, merlin_db.get_worker_by_name, args.worker),
            "No workers found for the given identifiers."
        ),
        "all-studies": lambda: print_items(merlin_db.get_all_studies(), "No studies found in the database."),
        "all-runs": lambda: print_items(merlin_db.get_all_runs(), "No runs found in the database."),
        "all-workers": lambda: print_items(merlin_db.get_all_workers(), "No workers found in the database."),
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

    def delete_by_uuid_or_name(delete_by_uuid: Callable, delete_by_name: Callable, identifiers: List[str]):
        """
        Attempts to retrieve items using UUIDs first, and falls back to retrieving by names if the
        UUIDs are invalid.

        Args:
            delete_by_uuid: Function to delete items using UUIDs.
            delete_by_name: Function to delete items using names.
            identifiers: List of identifiers to delete (either UUIDs or names).
        """
        for identifier in identifiers:
            try:
                uuid.UUID(identifier)
                delete_by_uuid(identifier)
            except ValueError:
                delete_by_name(identifier)

    # Dispatch table for get operations
    operations = {
        "study": lambda: delete_by_uuid_or_name(merlin_db.delete_study, merlin_db.delete_study_by_name, args.study),
        "run": lambda: delete_by_uuid_or_name(merlin_db.delete_run, merlin_db.delete_run_by_workspace, args.run),
        "worker": lambda: delete_by_uuid_or_name(merlin_db.delete_worker, merlin_db.delete_worker_by_name, args.worker),
        "all-studies": lambda: merlin_db.delete_all_studies(),
        "all-runs": lambda: merlin_db.delete_all_runs(),
        "all-workers": lambda: merlin_db.delete_all_workers(),
        # TODO need an operation to flush the entire database
    }

    # Execute the appropriate operation or log an error
    operation = operations.get(args.delete_type)
    if operation:
        operation()
    else:
        LOG.error("No valid delete option provided.")
