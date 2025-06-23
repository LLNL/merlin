##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
CLI module for purging tasks from Merlin queues on the task server.

This module defines the `PurgeCommand` class, which implements the `purge`
subcommand in the Merlin CLI. The command is used to remove tasks from
queues either entirely or selectively, based on the specified steps in
a Merlin YAML workflow specification.
"""

# pylint: disable=duplicate-code

import logging
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser, Namespace

from merlin.ascii_art import banner_small
from merlin.cli.commands.command_entry_point import CommandEntryPoint
from merlin.cli.utils import get_merlin_spec_with_override
from merlin.router import purge_tasks


LOG = logging.getLogger("merlin")


class PurgeCommand(CommandEntryPoint):
    """
    Handles `purge` CLI command for removing tasks from queues on the server.

    Methods:
        add_parser: Adds the `purge` command to the CLI parser.
        process_command: Processes the CLI input and dispatches the appropriate action.
    """

    def add_parser(self, subparsers: ArgumentParser):
        """
        Add the `purge` command parser to the CLI argument parser.

        Parameters:
            subparsers (ArgumentParser): The subparsers object to which the `purge` command parser will be added.
        """
        purge: ArgumentParser = subparsers.add_parser(
            "purge",
            help="Remove all tasks from all merlin queues (default).              "
            "If a user would like to purge only selected queues use:    "
            "--steps to give a steplist, the queues will be defined from the step list",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )
        purge.set_defaults(func=self.process_command)
        purge.add_argument("specification", type=str, help="Path to a Merlin YAML spec file")
        purge.add_argument(
            "-f",
            "--force",
            action="store_true",
            dest="purge_force",
            default=False,
            help="Purge the tasks without confirmation",
        )
        purge.add_argument(
            "--steps",
            nargs="+",
            type=str,
            dest="purge_steps",
            default=["all"],
            help="The specific steps in the YAML file from which you want to purge the queues. \
            The input is a space separated list.",
        )
        purge.add_argument(  # pylint: disable=duplicate-code
            "--vars",
            action="store",
            dest="variables",
            type=str,
            nargs="+",
            default=None,
            help="Specify desired Merlin variable values to override those found in the specification. Space-delimited. "
            "Example: '--vars MY_QUEUE=hello'",
        )

    def process_command(self, args: Namespace):
        """
        CLI command for purging tasks from the task server.

        This function removes specified tasks from the task server based on the provided
        Merlin specification. It allows for targeted purging or forced removal of tasks.

        Args:
            args: Parsed CLI arguments containing:\n
                - `purge_force`: If True, forces the purge operation without confirmation.
                - `purge_steps`: Steps or criteria based on which tasks will be purged.
        """
        print(banner_small)
        spec, _ = get_merlin_spec_with_override(args)
        ret = purge_tasks(
            spec.merlin["resources"]["task_server"],
            spec,
            args.purge_force,
            args.purge_steps,
        )

        LOG.info(f"Purge return = {ret} .")
