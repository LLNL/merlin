"""
"""
import logging
from argparse import Namespace

from merlin.db_scripts.db_interaction import MerlinDatabase

LOG = logging.getLogger("merlin")


def database_info():
    """
    Print information about the database to the console.
    """
    merlin_db = MerlinDatabase()
    db_studies = merlin_db.get_all_studies()
    db_runs = merlin_db.get_all_runs()

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
        for study in all_studies:
            print(study)
    elif args.get_type == "run":
        print(merlin_db.get_run(args.run))
    elif args.get_type == "all-runs":
        all_runs = merlin_db.get_all_runs()
        for run in all_runs:
            print(run)
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
        merlin_db.get_study(args.study, remove_associated_runs=(not args.keep_associated_runs))
    elif args.delete_type == "all-studies":
        merlin_db.delete_all_studies()
    elif args.delete_type == "run":
        merlin_db.delete_run(args.run)
    elif args.delete_type == "all-runs":
        merlin_db.delete_all_runs()
    # elif args.delete_type == "everything":
    #     # TODO implement this logic; currently it's the same as delete-all-studies
    else:
        LOG.error("No valid delete option provided.")
