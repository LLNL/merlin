##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
CLI module for shutting down Merlin workers.

This module defines the `StopWorkersCommand` class, which handles the `stop-workers`
subcommand in the Merlin CLI. It provides functionality to stop running workers that
are connected to a task server such as Celery.
"""

# pylint: disable=duplicate-code

import logging
from argparse import ArgumentParser, Namespace

from merlin.ascii_art import banner_small
from merlin.cli.commands.command_entry_point import CommandEntryPoint
from merlin.router import stop_workers
from merlin.spec.specification import MerlinSpec
from merlin.utils import verify_filepath


LOG = logging.getLogger("merlin")


class StopWorkersCommand(CommandEntryPoint):
    """
    Handles `stop-workers` CLI command for shutting down Merlin workers.

    Methods:
        add_parser: Adds the `stop-workers` command to the CLI parser.
        process_command: Processes the CLI input and dispatches the appropriate action.
    """

    def add_parser(self, subparsers: ArgumentParser):
        """
        Add the `stop-workers` command parser to the CLI argument parser.

        Parameters:
            subparsers (ArgumentParser): The subparsers object to which the `stop-workers` command parser will be added.
        """
        stop: ArgumentParser = subparsers.add_parser("stop-workers", help="Attempt to stop all task server workers.")
        stop.set_defaults(func=self.process_command)
        stop.add_argument(
            "--spec",
            type=str,
            default=None,
            help="Path to a Merlin YAML spec file from which to read worker names to stop.",
        )
        stop.add_argument(
            "--task_server",
            type=str,
            default="celery",
            help="Task server type from which to stop workers.\
                                Default: %(default)s",
        )
        stop.add_argument("--queues", type=str, default=None, nargs="+", help="specific queues to stop")
        stop.add_argument(
            "--workers",
            type=str,
            action="store",
            nargs="+",
            default=None,
            help="regex match for specific workers to stop",
        )

    def process_command(self, args: Namespace):
        """
        CLI command for stopping all workers.

        This function stops any active workers connected to a user's task server.
        If the `--spec` argument is provided, this function retrieves the names of
        workers from a the spec file and then issues a command to stop them.

        Args:
            args: Parsed command-line arguments, which may include:\n
                - `spec`: Path to the specification file to load worker names.
                - `task_server`: Address of the task server to send the stop command to.
                - `queues`: List of queue names to filter the workers.
                - `workers`: List of specific worker names to stop.
        """
        print(banner_small)
        worker_names = []

        # Load in the spec if one was provided via the CLI
        if args.spec:
            spec_path = verify_filepath(args.spec)
            spec = MerlinSpec.load_specification(spec_path)
            worker_names = spec.get_worker_names()
            for worker_name in worker_names:
                if "$" in worker_name:
                    LOG.warning(f"Worker '{worker_name}' is unexpanded. Target provenance spec instead?")

        # Send stop command to router
        stop_workers(args.task_server, worker_names, args.queues, args.workers)
