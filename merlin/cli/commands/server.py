##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Merlin CLI server command module.

This module defines the `ServerCommand` class, which provides subcommands
for managing the Merlin server components such as initialization, starting,
stopping, restarting, and configuring the server. These subcommands are integrated
into the Merlin CLI via `argparse`.
"""

# pylint: disable=duplicate-code

import logging
import os
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser, Namespace

from merlin.cli.commands.command_entry_point import CommandEntryPoint
from merlin.server.server_commands import config_server, init_server, restart_server, start_server, status_server, stop_server


LOG = logging.getLogger("merlin")


class ServerCommand(CommandEntryPoint):
    """
    Handles `server` CLI commands for interacting with Merlin's containerized server.

    Methods:
        add_parser: Adds the `server` command and its subcommands to the CLI parser.
        process_command: Processes the CLI input and dispatches the appropriate action.
    """

    def _add_config_subcommand(self, server_commands: ArgumentParser):
        """
         Add the `config` subcommand to the server command parser.

        Parameters:
            server_commands (ArgumentParser): The server subparser to which the config command will be added.
        """
        server_config: ArgumentParser = server_commands.add_parser(
            "config",
            help="Making configurations for to the merlin server instance.",
            description="Config server.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )
        server_config.add_argument(
            "-ip",
            "--ipaddress",
            action="store",
            type=str,
            help="Set the binded IP address for the merlin server container.",
        )
        server_config.add_argument(
            "-p",
            "--port",
            action="store",
            type=int,
            help="Set the binded port for the merlin server container.",
        )
        server_config.add_argument(
            "-pwd",
            "--password",
            action="store",
            type=str,
            help="Set the password file to be used for merlin server container.",
        )
        server_config.add_argument(
            "--add-user",
            action="store",
            nargs=2,
            type=str,
            help="Create a new user for merlin server instance. (Provide both username and password)",
        )
        server_config.add_argument("--remove-user", action="store", type=str, help="Remove an exisiting user.")
        server_config.add_argument(
            "-d",
            "--directory",
            action="store",
            type=str,
            help="Set the working directory of the merlin server container.",
        )
        server_config.add_argument(
            "-ss",
            "--snapshot-seconds",
            action="store",
            type=int,
            help="Set the number of seconds merlin server waits before checking if a snapshot is needed.",
        )
        server_config.add_argument(
            "-sc",
            "--snapshot-changes",
            action="store",
            type=int,
            help="Set the number of changes that are required to be made to the merlin server before a snapshot is made.",
        )
        server_config.add_argument(
            "-sf",
            "--snapshot-file",
            action="store",
            type=str,
            help="Set the snapshot filename for database dumps.",
        )
        server_config.add_argument(
            "-am",
            "--append-mode",
            action="store",
            type=str,
            help="The appendonly mode to be set. The avaiable options are always, everysec, no.",
        )
        server_config.add_argument(
            "-af",
            "--append-file",
            action="store",
            type=str,
            help="Set append only filename for merlin server container.",
        )

    def add_parser(self, subparsers: ArgumentParser):
        """
        Add the `server` command parser to the CLI argument parser.

        Parameters:
            subparsers (ArgumentParser): The subparsers object to which the `server` command parser will be added.
        """
        server: ArgumentParser = subparsers.add_parser(
            "server",
            help="Manage broker and results server for merlin workflow.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )
        server.set_defaults(func=self.process_command)

        server_commands: ArgumentParser = server.add_subparsers(dest="commands")

        # `merlin server init` subcommand
        server_commands.add_parser(
            "init",
            help="Initialize merlin server resources.",
            description="Initialize merlin server",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )

        # `merlin server status` subcommand
        server_commands.add_parser(
            "status",
            help="View status of the current server containers.",
            description="View status",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )

        # `merlin server start` subcommand
        server_commands.add_parser(
            "start",
            help="Start a containerized server to be used as an broker and results server.",
            description="Start server",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )

        # `merlin server stop` subcommand
        server_commands.add_parser(
            "stop",
            help="Stop an instance of redis containers currently running.",
            description="Stop server.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )

        # `merlin server restart` subcommand
        server_commands.add_parser(
            "restart",
            help="Restart merlin server instance",
            description="Restart server.",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )

        # `merlin server config` subcommand
        self._add_config_subcommand(server_commands)

    def process_command(self, args: Namespace):
        """
        Route to the appropriate server function based on the command
        specified via the CLI.

        This function processes commands related to server management,
        directing the flow to the corresponding function for actions such
        as initializing, starting, stopping, checking status, restarting,
        or configuring the server.

        Args:
            args: Parsed command-line arguments, which includes:\n
                - `commands`: The server management command to execute.
                Possible values are:
                    - `init`: Initialize the server.
                    - `start`: Start the server.
                    - `stop`: Stop the server.
                    - `status`: Check the server status.
                    - `restart`: Restart the server.
                    - `config`: Configure the server.
        """
        try:
            lc_all_val = os.environ["LC_ALL"]
            if lc_all_val != "C":
                raise ValueError(
                    f"The 'LC_ALL' environment variable is currently set to {lc_all_val} but it must be set to 'C'."
                )
        except KeyError:
            LOG.debug("The 'LC_ALL' environment variable was not set. Setting this to 'C'.")
            os.environ["LC_ALL"] = "C"  # Necessary for Redis to configure LOCALE

        if args.commands == "init":
            init_server()
        elif args.commands == "start":
            start_server()
        elif args.commands == "stop":
            stop_server()
        elif args.commands == "status":
            status_server()
        elif args.commands == "restart":
            restart_server()
        elif args.commands == "config":
            config_server(args)
