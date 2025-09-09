##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
CLI module for querying and displaying the status of Merlin workflows.

This module defines two primary CLI command handlers:

- `StatusCommand`: Handles the `status` command, which shows a high-level summary
  of the current state of a Merlin study or workflow. It supports querying status
  based on a Merlin YAML specification file or an existing study workspace, with
  options for output formatting and filtering.

- `DetailedStatusCommand`: Extends `StatusCommand` to provide a more granular,
  task-by-task view of the workflow status via the `detailed-status` command.
  This includes extensive filtering and display customization options.
"""

# pylint: disable=duplicate-code

import logging
from argparse import ArgumentParser, Namespace

from merlin.ascii_art import banner_small
from merlin.cli.commands.command_entry_point import CommandEntryPoint
from merlin.spec.expansion import get_spec_with_expansion
from merlin.study.status import DetailedStatus, Status
from merlin.study.status_constants import VALID_RETURN_CODES, VALID_STATUS_FILTERS
from merlin.study.status_renderers import status_renderer_factory
from merlin.utils import verify_dirpath, verify_filepath


LOG = logging.getLogger("merlin")


class StatusCommand(CommandEntryPoint):
    """
    Handles `status` CLI command for checking the high-level status of a workflow.

    Methods:
        add_parser: Adds the `status` command to the CLI parser.
        process_command: Processes the CLI input and dispatches the appropriate action.
    """

    def add_parser(self, subparsers: ArgumentParser):
        """
        Add the `status` command parser to the CLI argument parser.

        Parameters:
            subparsers (ArgumentParser): The subparsers object to which the `status` command parser will be added.
        """
        status_cmd: ArgumentParser = subparsers.add_parser(
            "status",
            help="Display a summary of the status of a study.",
        )
        status_cmd.set_defaults(func=self.process_command, detailed=False)
        status_cmd.add_argument(
            "spec_or_workspace", type=str, help="Path to a Merlin YAML spec file or a launched Merlin study"
        )
        status_cmd.add_argument(
            "--cb-help", action="store_true", help="Colorblind help; uses different symbols to represent different statuses"
        )
        status_cmd.add_argument(
            "--dump", type=str, help="Dump the status to a file. Provide the filename (must be .csv or .json).", default=None
        )
        status_cmd.add_argument(
            "--no-prompts",
            action="store_true",
            help="Ignore any prompts provided. This will default to the latest study \
                if you provide a spec file rather than a study workspace.",
        )
        status_cmd.add_argument(
            "--task_server",
            type=str,
            default="celery",
            help="Task server type.\
                                Default: %(default)s",
        )
        status_cmd.add_argument(
            "-o",
            "--output-path",
            action="store",
            type=str,
            default=None,
            help="Specify a location to look for output workspaces. Only used when a spec file is passed as the argument "
            "to 'status'; this will NOT be used if an output workspace is passed as the argument.",
        )

    def process_command(self, args: Namespace):
        """
        CLI command for querying the status of studies.

        This function processes the given command-line arguments to determine the
        status of a study. It constructs either a [`Status`][study.status.Status] object
        or a [`DetailedStatus`][study.status.DetailedStatus] object based on the specified
        command and the arguments provided. The function handles validations for the task
        server input and the output format specified for status dumping.

        Object mapping:
            - `merlin status` -> [`Status`][study.status.Status] object
            - `merlin detailed-status` -> [`DetailedStatus`][study.status.DetailedStatus]
            object

        Args:
            args: Parsed CLI arguments containing user inputs for the status query.

        Raises:
            ValueError:
                - If the task server specified is not supported (only "celery" is valid).
                - If the --dump filename provided does not end with ".csv" or ".json".
        """
        print(banner_small)

        # Ensure task server is valid
        if args.task_server != "celery":
            raise ValueError("Currently the only supported task server is celery.")

        # Make sure dump is valid if provided
        if args.dump and (not args.dump.endswith(".csv") and not args.dump.endswith(".json")):
            raise ValueError("The --dump option takes a filename that must end with .csv or .json")

        # Establish whether the argument provided by the user was a spec file or a study directory
        spec_display = False
        try:
            file_or_ws = verify_filepath(args.spec_or_workspace)
            spec_display = True
        except ValueError:
            try:
                file_or_ws = verify_dirpath(args.spec_or_workspace)
            except ValueError:
                LOG.error(f"The file or directory path {args.spec_or_workspace} does not exist.")
                return

        # If we're loading status based on a spec, load in the spec provided
        if spec_display:
            args.specification = file_or_ws
            args.spec_provided = get_spec_with_expansion(args.specification)

        # Get either a Status object or DetailedStatus object
        if args.detailed:
            status_obj = DetailedStatus(args, spec_display, file_or_ws)
        else:
            status_obj = Status(args, spec_display, file_or_ws)

        # Handle output appropriately
        if args.dump:
            status_obj.dump()
        else:
            status_obj.display()


class DetailedStatusCommand(StatusCommand):
    """
    Handles `detailed-status` CLI command for checking the in-depth status of a workflow.

    Methods:
        add_parser: Adds the `detailed-status` command to the CLI parser.
        process_command: Processes the CLI input and dispatches the appropriate action.
    """

    def add_parser(self, subparsers: ArgumentParser):
        """
        Add the `detailed-status` command parser to the CLI argument parser.

        Parameters:
            subparsers (ArgumentParser): The subparsers object to which the `detailed-status` command parser will be added.
        """
        detailed_status: ArgumentParser = subparsers.add_parser(
            "detailed-status",
            help="Display a task-by-task status of a study.",
        )
        detailed_status.set_defaults(func=self.process_command, detailed=True)
        detailed_status.add_argument(
            "spec_or_workspace", type=str, help="Path to a Merlin YAML spec file or a launched Merlin study"
        )
        detailed_status.add_argument(
            "--dump", type=str, help="Dump the status to a file. Provide the filename (must be .csv or .json).", default=None
        )
        detailed_status.add_argument(
            "--task_server",
            type=str,
            default="celery",
            help="Task server type.\
                                Default: %(default)s",
        )
        detailed_status.add_argument(
            "-o",
            "--output-path",
            action="store",
            type=str,
            default=None,
            help="Specify a location to look for output workspaces. Only used when a spec file is passed as the argument "
            "to 'status'; this will NOT be used if an output workspace is passed as the argument.",
        )
        status_filter_group = detailed_status.add_argument_group("filter options")
        status_filter_group.add_argument(
            "--max-tasks", action="store", type=int, help="Sets a limit on how many tasks can be displayed"
        )
        status_filter_group.add_argument(
            "--return-code",
            action="store",
            nargs="+",
            type=str.upper,
            choices=VALID_RETURN_CODES,
            help="Filter which tasks to display based on their return code",
        )
        status_filter_group.add_argument(
            "--steps",
            nargs="+",
            type=str,
            dest="steps",
            default=["all"],
            help="Filter which tasks to display based on the steps they're associated with",
        )
        status_filter_group.add_argument(
            "--task-queues",
            nargs="+",
            type=str,
            help="Filter which tasks to display based on the task queue they're in",
        )
        status_filter_group.add_argument(
            "--task-status",
            action="store",
            nargs="+",
            type=str.upper,
            choices=VALID_STATUS_FILTERS,
            help="Filter which tasks to display based on their status",
        )
        status_filter_group.add_argument(
            "--workers",
            nargs="+",
            type=str,
            help="Filter which tasks to display based on which workers are processing them",
        )
        status_display_group = detailed_status.add_argument_group("display options")
        status_display_group.add_argument(
            "--disable-pager", action="store_true", help="Turn off the pager functionality when viewing the status"
        )
        status_display_group.add_argument(
            "--disable-theme",
            action="store_true",
            help="Turn off styling for the status layout (If you want styling but it's not working, try modifying "
            "the MANPAGER or PAGER environment variables to be 'less -r'; i.e. export MANPAGER='less -r')",
        )
        status_display_group.add_argument(
            "--layout",
            type=str,
            choices=status_renderer_factory.get_layouts(),
            default="default",
            help="Alternate status layouts [Default: %(default)s]",
        )
        status_display_group.add_argument(
            "--no-prompts",
            action="store_true",
            help="Ignore any prompts provided. This will default to the latest study \
                if you provide a spec file rather than a study workspace.",
        )
