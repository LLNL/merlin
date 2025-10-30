##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
CLI module for cancelling running studies.

This module defines the `CancelCommand` class, which handles the `cancel` subcommand
of the Merlin CLI. The `cancel` command is intended to terminate running studies
gracefully. It acts as a wrapper around the `merlin purge` and `merlin stop-workers`
commands. Additionally, any running studies associated with the study being cancelled
will be marked as cancelled in the database. This ensures that the `merlin monitor`
command will no longer track these studies.

In short, this command will:

1. Purge the queues from the study
2. Stop the workers for that study
3. Mark all runs associated with that study as cancelled
"""

# pylint: disable=duplicate-code

import logging
from argparse import ArgumentParser, Namespace

from merlin.cli.commands.command_entry_point import CommandEntryPoint
from merlin.cli.utils import get_merlin_spec_with_override
from merlin.study.manager import StudyManager


LOG = logging.getLogger("merlin")


class CancelCommand(CommandEntryPoint):
    """
    Handles `cancel` CLI command for terminating running studies gracefully.

    Methods:
        add_parser: Adds the `cancel` command to the CLI parser.
        process_command: Processes the CLI input and dispatches the appropriate action.
    """

    def add_parser(self, subparsers: ArgumentParser):
        """
        Add the `cancel` command parser to the CLI argument parser.

        Parameters:
            subparsers (ArgumentParser): The subparsers object to which the `cancel` command parser will be added.
        """
        cancel: ArgumentParser = subparsers.add_parser(
            "cancel",
            help="Terminate running studies gracefully by purging queues, stopping workers, and marking runs as cancelled.",
        )
        cancel.set_defaults(func=self.process_command)

        cancel.add_argument(
            "specification",
            type=str,
            help="Path to a Merlin YAML spec file for the study you want to cancel.",
        )

        # Options to skip certain steps in the cancellation process
        cancel.add_argument(
            "--no-purge",
            action="store_true",
            help="Skip purging the queues for the study.",
        )
        cancel.add_argument(
            "--no-stop-workers",
            action="store_true",
            help="Skip stopping the workers for the study.",
        )
        cancel.add_argument(
            "--no-mark-cancelled",
            action="store_true",
            help="Skip marking runs as cancelled in the database.",
        )

        # Option to substitute variables in the specification file
        cancel.add_argument(
            "--vars",
            action="store",
            dest="variables",
            type=str,
            nargs="+",
            default=None,
            help="Specify desired Merlin variable values to override those found in the specification. Space-delimited. "
            "Example: '--vars LEARN=path/to/new_learn.py EPOCHS=3'",
        )

    def process_command(self, args: Namespace):
        """
        CLI command to cancel a running study.

        Args:
            args: Parsed CLI arguments.
        """
        spec, _ = get_merlin_spec_with_override(args)

        study_manager = StudyManager()
        result = study_manager.cancel(
            spec=spec,
            purge_queues=not args.no_purge,
            stop_workers=not args.no_stop_workers,
            mark_runs_cancelled=not args.no_mark_cancelled,
        )

        # Print summary
        result_summary = (
            "\nCancellation Summary:\n"
            f"  Study: {result['study_name']}\n"
            f"  Runs cancelled: {result['runs_cancelled']}\n"
            f"  Queues purged: {len(result['queues_purged'])}\n"
            f"  Workers stopped: {len(result['workers_stopped'])}"
        )
        LOG.info(result_summary)
