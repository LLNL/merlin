##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
CLI module for monitoring Merlin workflows and maintaining active allocations.

This module provides the `MonitorCommand` class, which implements the `monitor`
subcommand in the Merlin CLI. The purpose of the `monitor` command is to
periodically check the status of workflow tasks and worker activity to ensure
the allocation (e.g., on a computing cluster) remains alive while jobs are
in progress.
"""

# pylint: disable=duplicate-code

import logging
import time
from argparse import ArgumentParser, Namespace, RawTextHelpFormatter

from merlin.cli.commands.command_entry_point import CommandEntryPoint
from merlin.cli.utils import get_merlin_spec_with_override
from merlin.router import check_merlin_status


LOG = logging.getLogger("merlin")


class MonitorCommand(CommandEntryPoint):
    """
    Handles `monitor` CLI command for monitoring workflows to ensure the allocation remains alive.

    Methods:
        add_parser: Adds the `monitor` command to the CLI parser.
        process_command: Processes the CLI input and dispatches the appropriate action.
    """

    def add_parser(self, subparsers: ArgumentParser):
        """
        Add the `monitor` command parser to the CLI argument parser.

        Parameters:
            subparsers (ArgumentParser): The subparsers object to which the `monitor` command parser will be added.
        """
        monitor: ArgumentParser = subparsers.add_parser(
            "monitor",
            help="Check for active workers on an allocation.",
            formatter_class=RawTextHelpFormatter,
        )
        monitor.set_defaults(func=self.process_command)
        monitor.add_argument("specification", type=str, help="Path to a Merlin YAML spec file")
        monitor.add_argument(
            "--steps",
            nargs="+",
            type=str,
            dest="steps",
            default=["all"],
            help="The specific steps (tasks on the server) in the YAML file defining the queues you want to monitor",
        )
        monitor.add_argument(
            "--vars",
            action="store",
            dest="variables",
            type=str,
            nargs="+",
            default=None,
            help="Specify desired Merlin variable values to override those found in the specification. Space-delimited. "
            "Example: '--vars LEARN=path/to/new_learn.py EPOCHS=3'",
        )
        monitor.add_argument(
            "--task_server",
            type=str,
            default="celery",
            help="Task server type for which to monitor the workers.\
                                Default: %(default)s",
        )
        monitor.add_argument(
            "--sleep",
            type=int,
            default=60,
            help="Sleep duration between checking for workers.\
                                        Default: %(default)s",
        )

    def process_command(self, args: Namespace):
        """
        CLI command to monitor Merlin workers and queues to maintain
        allocation status.

        This function periodically checks the status of Merlin workers and
        the associated queues to ensure that the allocation remains active.
        It includes a sleep interval to wait before each check, including
        the initial one.

        Args:
            args: Parsed command-line arguments, which may include:\n
                - `sleep`: The duration (in seconds) to wait before
                checking the queue status again.
        """
        spec, _ = get_merlin_spec_with_override(args)

        # Give the user time to queue up jobs in case they haven't already
        time.sleep(args.sleep)

        # Check if we still need our allocation
        while check_merlin_status(args, spec):
            LOG.info("Monitor: found tasks in queues and/or tasks being processed")
            time.sleep(args.sleep)

        LOG.info("Monitor: ... stop condition met")
