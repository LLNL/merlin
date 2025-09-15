##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `server.py` file of the `cli/` folder.
"""

import os
from argparse import Namespace

import pytest
from pytest_mock import MockerFixture

from merlin.cli.commands.server import ServerCommand
from tests.fixture_types import FixtureCallable


def test_add_parser_sets_up_server_command(create_parser: FixtureCallable):
    """
    Test that the `server` command parser sets up the expected defaults and subcommands.

    Args:
        create_parser: A fixture to help create a parser.
    """
    command = ServerCommand()
    parser = create_parser(command)
    args = parser.parse_args(["server", "init"])
    assert hasattr(args, "func")
    assert args.func.__name__ == command.process_command.__name__
    assert args.commands == "init"


def test_process_command_calls_init(mocker: MockerFixture):
    """
    Ensure `init` subcommand calls `init_server`.

    Args:
        mocker: PyTest mocker fixture.
    """
    mock = mocker.patch("merlin.cli.commands.server.init_server")
    ServerCommand().process_command(Namespace(commands="init"))
    mock.assert_called_once()


def test_process_command_calls_start(mocker: MockerFixture):
    """
    Ensure `start` subcommand calls `start_server`.

    Args:
        mocker: PyTest mocker fixture.
    """
    mock = mocker.patch("merlin.cli.commands.server.start_server")
    ServerCommand().process_command(Namespace(commands="start"))
    mock.assert_called_once()


def test_process_command_calls_stop(mocker: MockerFixture):
    """
    Ensure `stop` subcommand calls `stop_server`.

    Args:
        mocker: PyTest mocker fixture.
    """
    mock = mocker.patch("merlin.cli.commands.server.stop_server")
    ServerCommand().process_command(Namespace(commands="stop"))
    mock.assert_called_once()


def test_process_command_calls_status(mocker: MockerFixture):
    """
    Ensure `status` subcommand calls `status_server`.

    Args:
        mocker: PyTest mocker fixture.
    """
    mock = mocker.patch("merlin.cli.commands.server.status_server")
    ServerCommand().process_command(Namespace(commands="status"))
    mock.assert_called_once()


def test_process_command_calls_restart(mocker: MockerFixture):
    """
    Ensure `restart` subcommand calls `restart_server`.

    Args:
        mocker: PyTest mocker fixture.
    """
    mock = mocker.patch("merlin.cli.commands.server.restart_server")
    ServerCommand().process_command(Namespace(commands="restart"))
    mock.assert_called_once()


def test_process_command_calls_config(mocker: MockerFixture):
    """
    Ensure `config` subcommand calls `config_server` with the provided args.

    Args:
        mocker: PyTest mocker fixture.
    """
    mock = mocker.patch("merlin.cli.commands.server.config_server")
    args = Namespace(commands="config", ip="127.0.0.1", port=8888)
    ServerCommand().process_command(args)
    mock.assert_called_once_with(args)


def test_process_command_sets_lc_all_if_missing(mocker: MockerFixture):
    """
    Ensure LC_ALL is set to 'C' if it's missing in the environment.

    Args:
        mocker: PyTest mocker fixture.
    """
    mocker.patch("merlin.cli.commands.server.start_server")
    mocker.patch.dict("os.environ", {}, clear=True)
    args = Namespace(commands="start")
    ServerCommand().process_command(args)
    assert os.environ["LC_ALL"] == "C"


def test_process_command_raises_if_lc_all_invalid(mocker: MockerFixture):
    """
    Ensure a ValueError is raised if LC_ALL is set to a value other than 'C'.

    Args:
        mocker: PyTest mocker fixture.
    """
    mocker.patch.dict("os.environ", {"LC_ALL": "en_US.UTF-8"})
    with pytest.raises(ValueError, match="LC_ALL.*must be set to 'C'"):
        ServerCommand().process_command(Namespace(commands="start"))
