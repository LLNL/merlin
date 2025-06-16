"""

"""

import logging
from argparse import ArgumentParser, Namespace

from merlin.cli.commands.command_entry_point import CommandEntryPoint

LOG = logging.getLogger("merlin")


class InfoCommand(CommandEntryPoint):
    """
    
    """

    def add_parser(self, subparsers: ArgumentParser):
        """
        Add the `info` command parser to the CLI argument parser.

        Parameters:
            subparsers (ArgumentParser): The subparsers object to which the `info` command parser will be added.
        """
        info: ArgumentParser = subparsers.add_parser(
            "info",
            help="display info about the merlin configuration and the python configuration. Useful for debugging.",
        )
        info.set_defaults(func=self.process_command)

    def process_command(self, args: Namespace):
        """
        CLI command to print merlin configuration info.

        Args:
            args: Parsed CLI arguments.
        """
        # if this is moved to the toplevel per standard style, merlin is unable to generate the (needed) default config file
        from merlin import display  # pylint: disable=import-outside-toplevel

        display.print_info(args)