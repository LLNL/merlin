##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
CLI module for launching Merlin worker processes.

This module defines the `RunWorkersCommand` class, which implements the `run-workers`
subcommand in the Merlin CLI. The command starts worker processes that execute tasks
defined in a Merlin YAML workflow specification, associating workers with the
correct task queues without queuing tasks themselves.
"""

# pylint: disable=duplicate-code

import logging
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser, Namespace

from merlin.ascii_art import banner_small
from merlin.cli.commands.command_entry_point import CommandEntryPoint
from merlin.cli.utils import get_merlin_spec_with_override
from merlin.config.configfile import initialize_config
from merlin.router import launch_workers


LOG = logging.getLogger("merlin")


class RunWorkersCommand(CommandEntryPoint):
    """
    Handles `run-workers` CLI command for launching Merlin workers.

    Methods:
        add_parser: Adds the `run-workers` command to the CLI parser.
        process_command: Processes the CLI input and dispatches the appropriate action.
    """

    def add_parser(self, subparsers: ArgumentParser):
        """
        Add the `run-workers` command parser to the CLI argument parser.

        Parameters:
            subparsers (ArgumentParser): The subparsers object to which the `run-workers` command parser will be added.
        """
        run_workers: ArgumentParser = subparsers.add_parser(
            "run-workers",
            help="Run the workers associated with the Merlin YAML study "
            "specification. Does -not- queue tasks, just workers tied "
            "to the correct queues.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )
        run_workers.set_defaults(func=self.process_command)
        run_workers.add_argument("specification", type=str, help="Path to a Merlin YAML spec file")
        run_workers.add_argument(
            "--worker-args",
            type=str,
            dest="worker_args",
            default="",
            help="celery worker arguments in quotes.",
        )
        run_workers.add_argument(
            "--steps",
            nargs="+",
            type=str,
            dest="worker_steps",
            default=["all"],
            help="The specific steps in the YAML file you want workers for",
        )
        run_workers.add_argument(
            "--echo",
            action="store_true",
            default=False,
            dest="worker_echo_only",
            help="Just echo the command; do not actually run it",
        )
        run_workers.add_argument(
            "--vars",
            action="store",
            dest="variables",
            type=str,
            nargs="+",
            default=None,
            help="Specify desired Merlin variable values to override those found in the specification. Space-delimited. "
            "Example: '--vars LEARN=path/to/new_learn.py EPOCHS=3'",
        )
        run_workers.add_argument(
            "--disable-logs",
            action="store_true",
            help="Turn off the logs for the celery workers. Note: having the -l flag "
            "in your workers' args section will overwrite this flag for that worker.",
        )

    def process_command(self, args: Namespace):
        """
        CLI command for launching workers.

        This function initializes worker processes for executing tasks as defined
        in the Merlin specification.

        Args:
            args: Parsed CLI arguments containing:\n
                - `worker_echo_only`: If True, don't start the workers and just echo the launch command
                - Additional worker-related parameters such as:
                    - `worker_steps`: Only start workers for these steps.
                    - `worker_args`: Arguments to pass to the worker processes.
                    - `disable_logs`: If True, disables logging for the worker processes.
        """
        if not args.worker_echo_only:
            print(banner_small)
        else:
            initialize_config(local_mode=True)

        spec, filepath = get_merlin_spec_with_override(args)
        if not args.worker_echo_only:
            LOG.info(f"Launching workers from '{filepath}'")

        # Launch the workers
        launch_worker_status = launch_workers(
            spec, args.worker_steps, args.worker_args, args.disable_logs, args.worker_echo_only
        )

        if args.worker_echo_only:
            print(launch_worker_status)
        else:
            LOG.debug(f"celery command: {launch_worker_status}")
