##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
CLI module for restarting existing Merlin workflows.

This module defines the `RestartCommand` class, which provides functionality
to restart an existing Merlin workflow from a previously saved workspace.

The command verifies the provided workspace directory, locates the appropriate
provenance specification file (an expanded YAML spec), and re-initializes the
workflow study from that point. It supports running the restarted workflow
either locally or in a distributed mode.
"""

# pylint: disable=duplicate-code

import glob
import logging
import os
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser, Namespace
from typing import List, Optional

from merlin.ascii_art import banner_small
from merlin.cli.commands.command_entry_point import CommandEntryPoint
from merlin.config.configfile import initialize_config
from merlin.router import run_task_server
from merlin.study.study import MerlinStudy
from merlin.utils import verify_dirpath, verify_filepath


LOG = logging.getLogger("merlin")


class RestartCommand(CommandEntryPoint):
    """
    Handles `restart` CLI command for restarting existing workflows.

    Methods:
        add_parser: Adds the `restart` command to the CLI parser.
        process_command: Processes the CLI input and dispatches the appropriate action.
    """

    def add_parser(self, subparsers: ArgumentParser):
        """
        Add the `restart` command parser to the CLI argument parser.

        Parameters:
            subparsers (ArgumentParser): The subparsers object to which the `restart` command parser will be added.
        """
        restart: ArgumentParser = subparsers.add_parser(
            "restart",
            help="Restart a workflow using an existing Merlin workspace.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )
        restart.set_defaults(func=self.process_command)
        restart.add_argument("restart_dir", type=str, help="Path to an existing Merlin workspace directory")
        restart.add_argument(  # TODO should this just be boolean instead of store_const?
            "--local",
            action="store_const",
            dest="run_mode",
            const="local",
            default="distributed",
            help="Run locally instead of distributed",
        )

    def process_command(self, args: Namespace):
        """
        CLI command for restarting a study.

        This function handles the restart process by verifying the specified restart
        directory, locating a valid provenance specification file, and initiating
        the study from that point.

        Args:
            args: Parsed CLI arguments containing:\n
                - `restart_dir`: Path to the directory where the restart specifications are located.
                - `run_mode`: The mode for running the study (e.g., normal, dry-run).

        Raises:
            ValueError: If the `restart_dir` does not contain a valid provenance spec file or
                if multiple files match the specified pattern.
        """
        print(banner_small)
        restart_dir: str = verify_dirpath(args.restart_dir)
        filepath: str = os.path.join(args.restart_dir, "merlin_info", "*.expanded.yaml")
        possible_specs: Optional[List[str]] = glob.glob(filepath)
        if not possible_specs:  # len == 0
            raise ValueError(f"'{filepath}' does not match any provenance spec file to restart from.")
        if len(possible_specs) > 1:
            raise ValueError(f"'{filepath}' matches more than one provenance spec file to restart from.")
        filepath: str = verify_filepath(possible_specs[0])
        LOG.info(f"Restarting workflow at '{restart_dir}'")
        study: MerlinStudy = MerlinStudy(filepath, restart_dir=restart_dir)

        if args.run_mode == "local":
            initialize_config(local_mode=True)

        run_task_server(study, args.run_mode)
