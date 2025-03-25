"""
This module acts as an interface for users to interact with Merlin's
database.
"""

import logging
import uuid
from argparse import Namespace

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

    if args.get_type == "study":
        print(merlin_db.get_study(args.study))
    elif args.get_type == "all-studies":
        all_studies = merlin_db.get_all_studies()
        if all_studies:
            for study in all_studies:
                print(study)
        else:
            LOG.info("No studies found in the database.")
    elif args.get_type == "run":
        print(merlin_db.get_run(args.run))
    elif args.get_type == "all-runs":
        all_runs = merlin_db.get_all_runs()
        if all_runs:
            for run in all_runs:
                print(run)
        else:
            LOG.info("No runs found in the database.")
    elif args.get_type == "worker":
        print(merlin_db.get_worker(args.worker))
    elif args.get_type == "all-workers":
        all_workers = merlin_db.get_all_workers()
        if all_workers:
            for worker in all_workers:
                print(worker)
        else:
            LOG.info("No workers found in the database")
    else:
        LOG.error("No valid delete option provided.")


def database_delete(args: Namespace):
    """
    Handles the delegation of delete operations to Merlin's database.

    Args:
        args: Parsed CLI arguments from the user.
    """
    merlin_db = MerlinDatabase()

    if args.delete_type == "study":
        merlin_db.delete_study(args.study, remove_associated_runs=(not args.keep_associated_runs))
    elif args.delete_type == "all-studies":
        merlin_db.delete_all_studies(remove_associated_runs=(not args.keep_associated_runs))
    elif args.delete_type == "run":
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
