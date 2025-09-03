##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
CLI module for executing Merlin or Maestro workflows.

This module defines the `RunCommand` class, which handles the `run` subcommand in the
Merlin CLI. The command initializes and runs a workflow based on a specified YAML
study specification file. It supports overriding variables, supplying samples files,
dry-run execution, and parallel parameter generation options.
"""

# pylint: disable=duplicate-code

from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser, Namespace
from typing import Optional

from merlin.ascii_art import banner_small
from merlin.cli.commands.command_entry_point import CommandEntryPoint
from merlin.cli.utils import parse_override_vars
from merlin.config.configfile import initialize_config
from merlin.router import run_task_server
from merlin.study.study import MerlinStudy
from merlin.utils import ARRAY_FILE_FORMATS, verify_filepath


class RunCommand(CommandEntryPoint):
    """
    Handles `run` CLI command for running a workflow (sending tasks to the queues on the broker).

    Methods:
        add_parser: Adds the `run` command to the CLI parser.
        process_command: Processes the CLI input and dispatches the appropriate action.
    """

    def add_parser(self, subparsers: ArgumentParser):
        """
        Add the `run` command parser to the CLI argument parser.

        Parameters:
            subparsers (ArgumentParser): The subparsers object to which the `run` command parser will be added.
        """
        run: ArgumentParser = subparsers.add_parser(
            "run",
            help="Run a workflow using a Merlin or Maestro YAML study " "specification.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )
        run.set_defaults(func=self.process_command)
        run.add_argument("specification", type=str, help="Path to a Merlin or Maestro YAML file")
        run.add_argument(
            "--local",
            action="store_const",
            dest="run_mode",
            const="local",
            default="distributed",
            help="Run locally instead of distributed",
        )
        run.add_argument(
            "--vars",
            action="store",
            dest="variables",
            type=str,
            nargs="+",
            default=None,
            help="Specify desired Merlin variable values to override those found in the specification. Space-delimited. "
            "Example: '--vars LEARN=path/to/new_learn.py EPOCHS=3'",
        )
        # TODO add all supported formats to doc string  # pylint: disable=fixme
        run.add_argument(
            "--samplesfile",
            action="store",
            dest="samples_file",
            type=str,
            default=None,
            help=f"Specify file containing samples. Valid choices: {ARRAY_FILE_FORMATS}",
        )
        run.add_argument(
            "--dry",
            action="store_true",
            dest="dry",
            default=False,
            help="Flag to dry-run a workflow, which sets up the workspace but does not launch tasks.",
        )
        run.add_argument(
            "--no-errors",
            action="store_true",
            dest="no_errors",
            default=False,
            help="Flag to ignore some flux errors for testing (often used with --dry --local).",
        )
        run.add_argument(
            "--pgen",
            action="store",
            dest="pgen_file",
            type=str,
            default=None,
            help="Provide a pgen file to override global.parameters.",
        )
        run.add_argument(
            "--pargs",
            type=str,
            action="append",
            default=[],
            help="A string that represents a single argument to pass "
            "a custom parameter generation function. Reuse '--parg' "
            "to pass multiple arguments. [Use with '--pgen']",
        )

    def process_command(self, args: Namespace):
        """
        CLI command for running a study.

        This function initializes and runs a study using the specified parameters.
        It handles file verification, variable parsing, and checks for required
        arguments related to the study configuration and execution.

        Args:
            args: Parsed CLI arguments containing:\n
                - `specification`: Path to the specification file for the study.
                - `variables`: Optional variable overrides for the study.
                - `samples_file`: Optional path to a samples file.
                - `dry`: If True, runs the study in dry-run mode (without actual execution).
                - `no_errors`: If True, suppresses error reporting.
                - `pgen_file`: Optional path to the pgen file, required if `pargs` is specified.
                - `pargs`: Additional arguments for parallel processing.

        Raises:
            ValueError:
                If the `pargs` parameter is used without specifying a `pgen_file`.
        """
        print(banner_small)
        filepath: str = verify_filepath(args.specification)
        variables_dict: str = parse_override_vars(args.variables)
        samples_file: Optional[str] = None
        if args.samples_file:
            samples_file = verify_filepath(args.samples_file)

        # pgen checks
        if args.pargs and not args.pgen_file:
            raise ValueError("Cannot use the 'pargs' parameter without specifying a 'pgen'!")
        if args.pgen_file:
            verify_filepath(args.pgen_file)

        study: MerlinStudy = MerlinStudy(
            filepath,
            override_vars=variables_dict,
            samples_file=samples_file,
            dry_run=args.dry,
            no_errors=args.no_errors,
            pgen_file=args.pgen_file,
            pargs=args.pargs,
        )

        if args.run_mode == "local":
            initialize_config(local_mode=True)

        run_task_server(study, args.run_mode)
