##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
CLI module for inspecting task server queue statistics in Merlin workflows.

This module defines the `QueueInfoCommand` class, which implements the
`queue-info` subcommand for the Merlin CLI. The command enables users to query
detailed information about queues used in Merlin workflows, including the number
of tasks in each queue and the number of connected workers.
"""

# pylint: disable=duplicate-code

import logging
from argparse import ArgumentParser, Namespace

from tabulate import tabulate

from merlin.ascii_art import banner_small
from merlin.cli.commands.command_entry_point import CommandEntryPoint
from merlin.cli.utils import get_merlin_spec_with_override
from merlin.router import dump_queue_info, query_queues


LOG = logging.getLogger("merlin")


class QueueInfoCommand(CommandEntryPoint):
    """
    Handles `queue-info` CLI command for querying info about the queues on the servers.

    Methods:
        add_parser: Adds the `queue-info` command to the CLI parser.
        process_command: Processes the CLI input and dispatches the appropriate action.
    """

    def add_parser(self, subparsers: ArgumentParser):
        """
        Add the `queue-info` command parser to the CLI argument parser.

        Parameters:
            subparsers (ArgumentParser): The subparsers object to which the `queue-info` command parser will be added.
        """
        queue_info: ArgumentParser = subparsers.add_parser(
            "queue-info",
            help="List queue statistics (queue name, number of tasks in the queue, number of connected workers).",
        )
        queue_info.set_defaults(func=self.process_command)
        queue_info.add_argument(
            "--dump",
            type=str,
            help="Dump the queue information to a file. Provide the filename (must be .csv or .json)",
            default=None,
        )
        queue_info.add_argument(
            "--specific-queues", nargs="+", type=str, help="Display queue stats for specific queues you list here"
        )
        queue_info.add_argument(
            "--task_server",
            type=str,
            default="celery",
            help="Task server type. Default: %(default)s",
        )
        spec_group = queue_info.add_argument_group("specification options")
        spec_group.add_argument(
            "--spec",
            dest="specification",
            type=str,
            help="Path to a Merlin YAML spec file. \
                                This will only display information for queues defined in this spec file. \
                                This is the same behavior as the status command prior to Merlin version 1.11.0.",
        )
        spec_group.add_argument(
            "--steps",
            nargs="+",
            type=str,
            dest="steps",
            default=["all"],
            help="The specific steps in the YAML file you want to query the queues of. "
            "This option MUST be used with the --spec option",
        )
        spec_group.add_argument(
            "--vars",
            action="store",
            dest="variables",
            type=str,
            nargs="+",
            default=None,
            help="Specify desired Merlin variable values to override those found in the specification. Space-delimited. "
            "This option MUST be used with the --spec option. Example: '--vars LEARN=path/to/new_learn.py EPOCHS=3'",
        )

    def process_command(self, args: Namespace):
        """
        CLI command for finding all workers and their associated queues.

        This function processes the command-line arguments to retrieve and display
        information about the available workers and their queues within the task server.
        It validates the necessary parameters, handles potential file dumping, and
        formats the output for easy readability.

        Args:
            args: Parsed CLI arguments containing user inputs related to the query.

        Raises:
            ValueError:
                - If a specification is not provided when steps are specified and the
                steps do not include "all".
                - If variables are included without a corresponding specification.
                - If the specified dump filename does not end with '.json' or '.csv'.
        """
        print(banner_small)

        # Ensure a spec is provided if steps are provided
        if not args.specification:
            if "all" not in args.steps:
                raise ValueError("The --steps argument MUST be used with the --specification argument.")
            if args.variables:
                raise ValueError("The --vars argument MUST be used with the --specification argument.")

        # Ensure a supported file type is provided with the dump option
        if args.dump is not None:
            if not args.dump.endswith(".json") and not args.dump.endswith(".csv"):
                raise ValueError("Unsupported file type. Dump files must be either '.json' or '.csv'.")

        spec = None
        # Load the spec if necessary
        if args.specification:
            spec, _ = get_merlin_spec_with_override(args)

        # Obtain the queue information
        queue_information = query_queues(args.task_server, spec, args.steps, args.specific_queues)

        if queue_information:
            # Format the queue information so we can pass it to the tabulate library
            formatted_queue_info = [("Queue Name", "Task Count", "Worker Count")]
            for queue_name, queue_stats in queue_information.items():
                formatted_queue_info.append((queue_name, queue_stats["jobs"], queue_stats["consumers"]))

            # Print the queue information
            print()
            print(tabulate(formatted_queue_info, headers="firstrow"))
            print()

            # Dump queue information to an output file if necessary
            if args.dump:
                dump_queue_info(args.task_server, queue_information, args.dump)
