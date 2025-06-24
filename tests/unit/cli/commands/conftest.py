from argparse import ArgumentParser

import pytest

from merlin.cli.commands.command_entry_point import CommandEntryPoint
from tests.fixture_types import FixtureCallable


@pytest.fixture
def create_parser() -> FixtureCallable:
    """
    A fixture to help create a parser for any command.

    Returns:
        A function that creates a parser.
    """

    def _create_parser(cmd: CommandEntryPoint) -> ArgumentParser:
        """
        Returns an `ArgumentParser` configured with the `cmd` command and its subcommands.

        Returns:
            Parser with the `cmd` command and its subcommands registered.
        """
        parser = ArgumentParser()
        subparsers = parser.add_subparsers(dest="main_command")
        cmd.add_parser(subparsers)
        return parser
    
    return _create_parser