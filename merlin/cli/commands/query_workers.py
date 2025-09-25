##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
CLI module for querying active Merlin task server workers.

This module defines the `QueryWorkersCommand` class, which implements the
`query-workers` subcommand for the Merlin CLI. The command allows users to
inspect the state of connected workers on a task server (e.g., Celery),
optionally filtering by queues or worker names.
"""

# pylint: disable=duplicate-code

import logging
from argparse import ArgumentParser, Namespace

from merlin.ascii_art import banner_small
from merlin.cli.commands.command_entry_point import CommandEntryPoint
from merlin.spec.specification import MerlinSpec
from merlin.utils import verify_filepath
from merlin.workers.formatters.formatter_factory import worker_formatter_factory
from merlin.workers.handlers.handler_factory import worker_handler_factory


LOG = logging.getLogger("merlin")


class QueryWorkersCommand(CommandEntryPoint):
    """
    Handles `query-workers` CLI command for querying information about Merlin workers.

    Methods:
        add_parser: Adds the `query-workers` command to the CLI parser.
        process_command: Processes the CLI input and dispatches the appropriate action.
    """

    def add_parser(self, subparsers: ArgumentParser):
        """
        Add the `query-workers` command parser to the CLI argument parser.

        Parameters:
            subparsers (ArgumentParser): The subparsers object to which the `query-workers` command parser will be added.
        """
        query: ArgumentParser = subparsers.add_parser("query-workers", help="List connected task server workers.")
        query.set_defaults(func=self.process_command)
        query.add_argument(
            "--task_server",
            type=str,
            default="celery",
            help="Task server type from which to query workers.\
                                Default: %(default)s",
        )
        query.add_argument(
            "--spec",
            type=str,
            default=None,
            help="Path to a Merlin YAML spec file from which to read worker names to query.",
        )
        query.add_argument("--queues", type=str, default=None, nargs="+", help="Specific queues to query workers from.")
        query.add_argument(
            "--workers",
            type=str,
            action="store",
            nargs="+",
            default=None,
            help="Specific logical worker names to query.",
        )
        format_default = "rich"
        query.add_argument(
            "-f",
            "--format",
            choices=worker_formatter_factory.list_available(),
            default=format_default,
            help=f"Output format. Default: {format_default}",
        )

    def process_command(self, args: Namespace):
        """
        CLI command for finding all workers.

        This function retrieves and queries the names of any active workers.
        If the `--spec` argument is included, only query the workers defined in the spec file.

        Args:
            args: Parsed command-line arguments, which may include:\n
                - `spec`: Path to the specification file.
                - `task_server`: Address of the task server to query.
                - `queues`: List of queue names to filter workers.
                - `workers`: List of specific worker names to query.
        """
        print(banner_small)

        worker_names = []
        if args.workers:
            worker_names.extend(args.workers)

        # Get the workers from the spec file if --spec provided
        spec = None
        if args.spec:
            spec_path = verify_filepath(args.spec)
            spec = MerlinSpec.load_specification(spec_path)
            worker_names.extend(spec.get_worker_names())
            for worker_name in worker_names:
                if "$" in worker_name:
                    LOG.warning(f"Worker '{worker_name}' is unexpanded. Target provenance spec instead?")
            LOG.debug(f"Searching for the following workers to stop based on the spec {args.spec}: {worker_names}")

        task_server = spec.merlin["resources"]["task_server"] if spec else args.task_server
        worker_handler = worker_handler_factory.create(task_server)
        worker_handler.query_workers(args.format, queues=args.queues, workers=worker_names)
