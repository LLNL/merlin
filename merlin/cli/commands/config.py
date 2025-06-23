##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
CLI command for managing Merlin configuration files.

This module defines the `ConfigCommand` class, which provides the CLI interface
to create, update, and switch between different Merlin configuration files.
These configurations control task server, broker, and backend settings used
by the Merlin workflow system.
"""

# pylint: disable=duplicate-code

import logging
import os
from argparse import SUPPRESS, ArgumentDefaultsHelpFormatter, ArgumentParser, ArgumentTypeError, Namespace

import yaml

from merlin.cli.commands.command_entry_point import CommandEntryPoint
from merlin.config.merlin_config_manager import MerlinConfigManager


LOG = logging.getLogger("merlin")


class ConfigCommand(CommandEntryPoint):
    """
    CLI command group for managing Merlin configuration files.

    This class adds the `config` command and its subcommands to the Merlin CLI,
    and dispatches the CLI input to Merlin's codebase.

    Attributes:
        default_config_file (str): The default path to the Merlin config file (`~/.merlin/app.yaml`).

    Methods:
        add_parser: Adds the `config` command and its subcommands to the CLI parser.
        process_command: Processes the CLI input and dispatches the appropriate action.
    """

    default_config_file = os.path.join(os.path.expanduser("~"), ".merlin", "app.yaml")

    def _add_create_subcommand(self, mconfig_subparsers: ArgumentParser):
        """
        Add the `create` subcommand to generate a new configuration file.

        Parameters:
            mconfig_subparsers (ArgumentParser): The subparsers object to add the subcommand to.
        """
        config_create_parser = mconfig_subparsers.add_parser("create", help="Create a new configuration file.")
        config_create_parser.add_argument(
            "--task-server",
            type=str,
            default="celery",
            help="Task server type for which to create the config. Default: %(default)s",
        )
        config_create_parser.add_argument(
            "-o",
            "--output-file",
            dest="config_file",
            type=str,
            default=self.default_config_file,
            help=f"Optional file name for your configuration. Default: {self.default_config_file}",
        )
        config_create_parser.add_argument(
            "--broker",
            type=str,
            default=None,
            help="Optional broker type, backend will be redis. Default: rabbitmq",
        )

    def _add_update_broker_subcommand(self, mconfig_subparsers: ArgumentParser):
        """
        Add the `update-broker` subcommand to modify broker-related settings.

        Parameters:
            mconfig_subparsers (ArgumentParser): The subparsers object to add the subcommand to.
        """
        config_broker_parser = mconfig_subparsers.add_parser("update-broker", help="Update broker settings in app.yaml")
        config_broker_parser.add_argument(
            "-t",
            "--type",
            required=True,
            choices=["redis", "rabbitmq"],
            help="Type of broker to configure (redis or rabbitmq).",
        )
        config_broker_parser.add_argument(
            "--cf",
            "--config-file",
            dest="config_file",
            default=self.default_config_file,
            help=f"The path to the config file that will be updated. Default: {self.default_config_file}",
        )
        config_broker_parser.add_argument("-u", "--username", help="Broker username (only for rabbitmq)")
        config_broker_parser.add_argument("--pf", "--password-file", dest="password_file", help="Path to password file")
        config_broker_parser.add_argument("-s", "--server", help="The URL of the server")
        config_broker_parser.add_argument("-p", "--port", type=int, help="Broker port")
        config_broker_parser.add_argument("-v", "--vhost", help="Broker vhost (only for rabbitmq)")
        config_broker_parser.add_argument("-c", "--cert-reqs", help="Broker cert requirements")
        config_broker_parser.add_argument("-d", "--db-num", type=int, help="Redis database number (only for redis).")

    def _add_update_backend_subcommand(self, mconfig_subparsers: ArgumentParser):
        """
        Add the `update-backend` subcommand to modify results backend settings.

        Parameters:
            mconfig_subparsers (ArgumentParser): The subparsers object to add the subcommand to.
        """
        config_backend_parser = mconfig_subparsers.add_parser(
            "update-backend", help="Update results backend settings in app.yaml"
        )
        config_backend_parser.add_argument(
            "-t",
            "--type",
            required=True,
            choices=["redis"],
            help="Type of results backend to configure.",
        )
        config_backend_parser.add_argument(
            "--cf",
            "--config-file",
            dest="config_file",
            default=self.default_config_file,
            help=f"The path to the config file that will be updated. Default: {self.default_config_file}",
        )
        config_backend_parser.add_argument("-u", "--username", help="Backend username")
        config_backend_parser.add_argument("--pf", "--password-file", dest="password_file", help="Path to password file")
        config_backend_parser.add_argument("-s", "--server", help="The URL of the server")
        config_backend_parser.add_argument("-p", "--port", help="Backend port")
        config_backend_parser.add_argument("-d", "--db-num", help="Backend database number")
        config_backend_parser.add_argument("-c", "--cert-reqs", help="Backend cert requirements")
        config_backend_parser.add_argument("-e", "--encryption-key", help="Path to encryption key file")

    def _add_use_subcommand(self, mconfig_subparsers: ArgumentParser):
        """
        Add the `use` subcommand to switch to a different Merlin config file.

        Parameters:
            mconfig_subparsers (ArgumentParser): The subparsers object to add the subcommand to.
        """
        config_use_parser = mconfig_subparsers.add_parser("use", help="Use a different configuration file.")
        config_use_parser.add_argument("config_file", type=str, help="The path to the new configuration file to use.")

    def add_parser(self, subparsers: ArgumentParser):
        """
        Add the `config` command parser to the CLI argument parser.

        Parameters:
            subparsers (ArgumentParser): The subparsers object to which the `config` command parser will be added.
        """
        mconfig: ArgumentParser = subparsers.add_parser(
            "config",
            help="Create a default merlin server config file in ~/.merlin",
            formatter_class=ArgumentDefaultsHelpFormatter,
        )
        mconfig.set_defaults(func=self.process_command)
        # The below option makes it so the `config_path.txt` file is written to the test directory
        mconfig.add_argument(
            "-t",
            "--test",
            action="store_true",
            help=SUPPRESS,  # Hides from `--help`
        )
        mconfig_subparsers = mconfig.add_subparsers(dest="commands", help="Subcommands for 'config'")

        self._add_create_subcommand(mconfig_subparsers)
        self._add_update_broker_subcommand(mconfig_subparsers)
        self._add_update_backend_subcommand(mconfig_subparsers)
        self._add_use_subcommand(mconfig_subparsers)

    def process_command(self, args: Namespace):
        """
        CLI command to manage Merlin configuration files.

        This function handles various configuration-related operations based on
        the provided subcommand. It ensures that the specified configuration
        file has a valid YAML extension (i.e., `.yaml` or `.yml`).

        If no output file is explicitly provided, a default path is used.

        Args:
            args (Namespace): Parsed command-line arguments.
        """
        if args.commands != "create":  # Check that this is a valid yaml file
            try:
                with open(args.config_file, "r") as conf_file:
                    yaml.safe_load(conf_file)
            except FileNotFoundError as fnf_exc:
                raise ArgumentTypeError(f"The file '{args.config_file}' does not exist.") from fnf_exc
            except yaml.YAMLError as yaml_exc:
                raise ArgumentTypeError(f"The file '{args.config_file}' is not a valid YAML file.") from yaml_exc

        config_manager = MerlinConfigManager(args)

        if args.commands == "create":
            config_manager.create_template_config()
            config_manager.save_config_path()
        elif args.commands == "update-broker":
            config_manager.update_broker()
        elif args.commands == "update-backend":
            config_manager.update_backend()
        elif args.commands == "use":  # Config file path is updated in constructor of MerlinConfigManager
            config_manager.config_file = args.config_file
            config_manager.save_config_path()
