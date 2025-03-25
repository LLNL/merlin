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

# TODO
# - clean up this function
# - do we want to enforce every ResultsBackend instantiation to have retrieve_worker and retrieve_worker_by_name?
#   - maybe we can just condense this to `retrieve_worker` and then the instantiated classes can handle the rest under the hood?
# - add functionality to delete runs so you can delete runs by workspace
# - add functionality to delete studies so you can delete studies by ID
# - add worker monitor functionality to the monitor class
# - figure out what to do with worker IDs in run entries
# - add docstrings to all of the new methods across all files (see files_changed_march_24)
def database_get(args: Namespace):
    """
    Handles the delegation of get operations to Merlin's database.

    Args:
        args: Parsed CLI arguments from the user.
    """
    merlin_db = MerlinDatabase()

    def get_by_uuid_or_name(get_by_uuid: Callable, get_by_name: Callable, identifiers: List[str]):
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

    if args.delete_type == "study":
        # try:
        #     uuid.UUID(args.study)
        #     merlin_db.delete_study(args.study, (not args.keep_associated_runs))
        # except ValueError:
        #      # If it's not a valid UUID, assume it's a worker name
        #     merlin_db.delete_study_by_name(args.study, remove_associated_runs=(not args.keep_associated_runs))

        merlin_db.delete_study_by_name(args.study, remove_associated_runs=(not args.keep_associated_runs))
    elif args.delete_type == "all-studies":
        merlin_db.delete_all_studies(remove_associated_runs=(not args.keep_associated_runs))
    elif args.delete_type == "run":
        # try:
        #     uuid.UUID(args.run)
        #     merlin_db.delete_run(args.run)
        # except ValueError:
        #      # If it's not a valid UUID, assume it's a worker name
        #     merlin_db.delete_run_by_workspace(args.run)

        merlin_db.delete_run(args.run)
    elif args.delete_type == "all-runs":
        merlin_db.delete_all_runs()
    elif args.delete_type == "worker":
        try:
            uuid.UUID(args.worker)
            merlin_db.delete_worker(args.worker)
        except ValueError:
             # If it's not a valid UUID, assume it's a worker name
            merlin_db.delete_worker_by_name(args.worker)
    elif args.delete_type == "all-workers":
        merlin_db.delete_all_workers()
    # elif args.delete_type == "everything":
    #     # TODO implement this logic
    else:
        LOG.error("No valid delete option provided.")
