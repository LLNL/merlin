##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Fixtures for files in this `cli/` test directory.
"""

from argparse import ArgumentParser

import pytest

from merlin.cli.commands.command_entry_point import CommandEntryPoint
from tests.fixture_types import FixtureCallable, FixtureStr


@pytest.fixture(scope="session")
def cli_testing_dir(create_testing_dir: FixtureCallable, temp_output_dir: FixtureStr) -> FixtureStr:
    """
    Fixture to create a temporary output directory for tests related to testing the
    `cli` directory.

    Args:
        create_testing_dir: A fixture which returns a function that creates the testing directory.
        temp_output_dir: The path to the temporary ouptut directory we'll be using for this test run.

    Returns:
        The path to the temporary testing directory for tests of files in the `cli` directory.
    """
    return create_testing_dir(temp_output_dir, "cli_testing")


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
