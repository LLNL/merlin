##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Implements the `database gc` subcommand for the Merlin CLI.

This module defines the `DatabaseGarbageCollectionCommand` class, which enables
automated cleanup of stale and orphaned database entries. The garbage collection
process identifies and removes runs with missing filesystem workspaces, workers
that no longer have valid runs, and studies without associated runs.

The command supports selective cleanup through entity-specific flags and includes
a dry-run mode for previewing actions before execution. This ensures data integrity
by maintaining consistency between database records and filesystem state.
"""

import logging
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser, Namespace

from merlin.cli.commands.command_entry_point import CommandEntryPoint
from merlin.db_scripts.garbage_collector import DatabaseGarbageCollector


LOG = logging.getLogger("merlin")


class DatabaseGarbageCollectionCommand(CommandEntryPoint):
    """
    Handles the `database gc` subcommand, which runs garbage collection
    across the database to remove runs without workspaces, orphaned workers,
    and studies with no associated runs.

    Methods:
        add_parser: Adds the `gc` command to the CLI parser.
        process_command: Processes the CLI input and runs garbage collection.
    """

    def add_parser(self, subparsers: ArgumentParser):
        """
        Add the `gc` command parser to the CLI argument parser.

        Parameters:
            subparsers: The subparsers object to which the `gc` command parser will be added.
        """
        db_gc_parser: ArgumentParser = subparsers.add_parser(
            "gc",
            aliases=["garbage-collect", "cleanup"],
            help="Clean up stale database entries (runs with missing workspaces, orphaned workers, etc.)",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )
        db_gc_parser.set_defaults(func=self.process_command)

        db_gc_parser.add_argument(
            "--dry-run",
            action="store_true",
            default=False,
            help="Show what would be deleted without actually deleting anything.",
        )

        db_gc_parser.add_argument(
            "--skip-runs",
            action="store_true",
            default=False,
            help="Skip checking for runs with invalid workspaces.",
        )

        db_gc_parser.add_argument(
            "--skip-workers",
            action="store_true",
            default=False,
            help="Skip checking for orphaned workers (both logical and physical).",
        )

        db_gc_parser.add_argument(
            "--skip-logical-workers",
            action="store_true",
            default=False,
            help="Skip checking for orphaned logical workers.",
        )

        db_gc_parser.add_argument(
            "--skip-physical-workers",
            action="store_true",
            default=False,
            help="Skip checking for orphaned physical workers.",
        )

        db_gc_parser.add_argument(
            "--skip-studies",
            action="store_true",
            default=False,
            help="Skip checking for empty studies.",
        )

        db_gc_parser.add_argument(
            "--force",
            "-f",
            action="store_true",
            default=False,
            help="Skip confirmation prompt (use with caution).",
        )

    def process_command(self, args: Namespace):
        """
        CLI command for running database garbage collection.

        Args:
            args: Parsed CLI arguments containing:\n
                - `dry_run`: If True, only report issues without deleting.
                - `skip_runs`: If True, skip run validation.
                - `skip_workers`: If True, skip worker orphan detection.
                - `skip_studies`: If True, skip empty study detection.
                - `force`: If True, skip confirmation prompt.
        """
        # Initialize database and collector
        collector = DatabaseGarbageCollector()

        # Determine what to check
        check_runs = not args.skip_runs
        check_logical_workers = not (args.skip_workers or args.skip_logical_workers)
        check_physical_workers = not (args.skip_workers or args.skip_physical_workers)
        check_studies = not args.skip_studies

        # Run garbage collection
        if args.dry_run:
            collector.scan(
                check_runs=check_runs,
                check_logical_workers=check_logical_workers,
                check_physical_workers=check_physical_workers,
                check_studies=check_studies,
            )
        else:
            collector.scan_and_clean(
                check_runs=check_runs,
                check_logical_workers=check_logical_workers,
                check_physical_workers=check_physical_workers,
                check_studies=check_studies,
                force=args.force,
            )
