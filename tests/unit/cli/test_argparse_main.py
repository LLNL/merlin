##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `argparse_main.py` file.
"""

from argparse import ArgumentParser

import pytest
from _pytest.capture import CaptureFixture
from pytest_mock import MockerFixture

from merlin import VERSION
from merlin.cli.argparse_main import DEFAULT_LOG_LEVEL, HelpParser, build_main_parser
from tests.fixture_types import FixtureList


@pytest.fixture
def mock_all_commands(mocker: MockerFixture) -> FixtureList:
    """
    Patch `ALL_COMMANDS` with dummy `CommandEntryPoint`-like objects
    that define a working `add_parser` method.

    Args:
        mocker: PyTest mocker fixture.

    Returns:
        A list of `DummyCommand` objects to be used for tests.
    """

    class DummyCommand:
        def __init__(self, name):
            self.name = name

        def add_parser(self, subparsers):
            subparsers.add_parser(self.name)

    dummy_commands = [DummyCommand("run"), DummyCommand("purge")]
    mocker.patch("merlin.cli.argparse_main.ALL_COMMANDS", dummy_commands)
    return dummy_commands


def test_help_parser_error(mocker: MockerFixture, capsys: CaptureFixture):
    """
    Test that HelpParser.error prints help and exits with code 2.

    Args:
        mocker: PyTest mocker fixture.
        capsys: PyTest capsys fixture.
    """
    mock_exit = mocker.patch("sys.exit", side_effect=SystemExit(2))

    parser = HelpParser(prog="test")
    with pytest.raises(SystemExit) as e:
        parser.error("test error")

    captured = capsys.readouterr()
    assert "error: test error" in captured.err
    assert e.value.code == 2
    mock_exit.assert_called_once_with(2)


def test_build_main_parser_adds_all_commands(mock_all_commands: FixtureList):
    """
    Test that build_main_parser adds subcommands from ALL_COMMANDS.

    Args:
        mock_all_commands: A list of `DummyCommand` objects to be used for tests.
    """
    parser = build_main_parser()
    assert isinstance(parser, ArgumentParser)

    # Commands from mock are "run" and "purge", so we should be able to parse those
    args = parser.parse_args(["--level", "DEBUG", "run"])
    assert args.level == "DEBUG"
    assert args.subparsers == "run"

    args = parser.parse_args(["purge"])
    assert args.level == DEFAULT_LOG_LEVEL
    assert args.subparsers == "purge"


def test_version_flag_prints_version_and_exits(mocker: MockerFixture, capsys: CaptureFixture, mock_all_commands: FixtureList):
    """
    Test that --version prints the correct version and exits.

    Args:
        mocker: PyTest mocker fixture.
        capsys: PyTest capsys fixture.
        mock_all_commands: A list of `DummyCommand` objects to be used for tests.
    """
    mocker.patch("sys.exit", side_effect=SystemExit(0))
    parser = build_main_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["--version"])

    output = capsys.readouterr()
    assert VERSION in output.out
