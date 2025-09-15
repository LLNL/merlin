##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Merlin CLI `example` command module.

This module defines the `ExampleCommand` class, which integrates into the Merlin
command-line interface to support downloading and setting up example workflows.
"""

# pylint: disable=duplicate-code

import logging
from argparse import ArgumentParser, Namespace, RawTextHelpFormatter

from merlin.ascii_art import banner_small
from merlin.cli.commands.command_entry_point import CommandEntryPoint
from merlin.examples.generator import list_examples, setup_example


LOG = logging.getLogger("merlin")


class ExampleCommand(CommandEntryPoint):
    """
    Handles `example` CLI command for downloading built-in Merlin examples.

    Methods:
        add_parser: Adds the `example` command to the CLI parser.
        process_command: Processes the CLI input and dispatches the appropriate action.
    """

    def add_parser(self, subparsers: ArgumentParser):
        """
        Add the `example` command parser to the CLI argument parser.

        Parameters:
            subparsers (ArgumentParser): The subparsers object to which the `example` command parser will be added.
        """
        example: ArgumentParser = subparsers.add_parser(
            "example",
            help="Generate an example merlin workflow.",
            formatter_class=RawTextHelpFormatter,
        )
        example.set_defaults(func=self.process_command)
        example.add_argument(
            "workflow",
            action="store",
            type=str,
            help="The name of the example workflow to setup. Use 'merlin example list' to see available options.",
        )
        example.add_argument(
            "-p",
            "--path",
            action="store",
            type=str,
            default=None,
            help="Specify a path to write the workflow to. Defaults to current working directory",
        )

    def process_command(self, args: Namespace):
        """
        CLI command to set up or list Merlin example workflows.

        This function either lists all available example workflows or sets
        up a specified example workflow to be run in the root directory. The
        behavior is determined by the `workflow` argument.

        Args:
            args: Parsed command-line arguments, which may include:\n
                - `workflow`: The action to perform; should be "list"
                to display all examples or the name of a specific example
                workflow to set up.
                - `path`: The directory where the example workflow
                should be set up. Only applicable when `workflow` is not "list".
        """
        if args.workflow == "list":
            print(list_examples())
        else:
            print(banner_small)
            setup_example(args.workflow, args.path)
