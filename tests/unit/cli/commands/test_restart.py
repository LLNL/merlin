##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `restart.py` file of the `cli/` folder.
"""

from argparse import Namespace
from pathlib import Path

import pytest
from pytest_mock import MockerFixture

from merlin.cli.commands.restart import RestartCommand
from tests.fixture_types import FixtureCallable, FixtureStr


def test_add_parser_sets_up_restart_command(create_parser: FixtureCallable):
    """
    Ensure the `restart` command registers the correct arguments and sets the default function.

    Args:
        create_parser: A fixture to help create a parser.
    """
    command = RestartCommand()
    parser = create_parser(command)
    args = parser.parse_args(["restart", "some/dir"])
    assert hasattr(args, "func")
    assert args.func.__name__ == command.process_command.__name__
    assert args.restart_dir == "some/dir"
    assert args.run_mode == "distributed"


def test_process_command_with_valid_restart_spec(mocker: MockerFixture, cli_testing_dir: FixtureStr):
    """
    Test `process_command` runs successfully when one expanded spec file is found.

    Args:
        mocker: PyTest mocker fixture.
        cli_testing_dir: The path to the temporary ouptut directory for cli tests.
    """
    # Set up directory and mock spec file
    cli_testing_dir = Path(cli_testing_dir)
    restart_dir = cli_testing_dir / "workspace"
    merlin_info_dir = restart_dir / "merlin_info"
    merlin_info_dir.mkdir(parents=True)
    spec_file = merlin_info_dir / "restart.expanded.yaml"
    spec_file.write_text("fake spec content")

    # Mocks
    mock_verify_dirpath = mocker.patch("merlin.cli.commands.restart.verify_dirpath", return_value=str(restart_dir))
    mock_verify_filepath = mocker.patch("merlin.cli.commands.restart.verify_filepath", return_value=str(spec_file))
    mock_study = mocker.patch("merlin.cli.commands.restart.MerlinStudy")
    mock_run = mocker.patch("merlin.cli.commands.restart.run_task_server")
    mock_log = mocker.patch("merlin.cli.commands.restart.LOG")

    args = Namespace(restart_dir=str(restart_dir), run_mode="distributed")
    cmd = RestartCommand()
    cmd.process_command(args)

    mock_verify_dirpath.assert_called_once_with(str(restart_dir))
    mock_verify_filepath.assert_called_once_with(str(spec_file))
    mock_study.assert_called_once_with(str(spec_file), restart_dir=str(restart_dir))
    mock_run.assert_called_once_with(mock_study.return_value, "distributed")
    mock_log.info.assert_called_once()


def test_process_command_initializes_config_in_local_mode(mocker: MockerFixture, cli_testing_dir: FixtureStr):
    """
    Test that `initialize_config` is called with `local_mode=True` when `--local` is passed.

    Args:
        mocker: PyTest mocker fixture.
        cli_testing_dir: The path to the temporary ouptut directory for cli tests.
    """
    cli_testing_dir = Path(cli_testing_dir)
    restart_dir = cli_testing_dir / "restart"
    merlin_info = restart_dir / "merlin_info"
    merlin_info.mkdir(parents=True)
    spec_file = merlin_info / "spec.expanded.yaml"
    spec_file.write_text("fake content")

    mocker.patch("merlin.cli.commands.restart.verify_dirpath", return_value=str(restart_dir))
    mocker.patch("merlin.cli.commands.restart.verify_filepath", return_value=str(spec_file))
    mocker.patch("glob.glob", return_value=[str(spec_file)])
    mocker.patch("merlin.cli.commands.restart.MerlinStudy")
    mock_run = mocker.patch("merlin.cli.commands.restart.run_task_server")
    mock_init = mocker.patch("merlin.cli.commands.restart.initialize_config")

    args = Namespace(restart_dir=str(restart_dir), run_mode="local")
    RestartCommand().process_command(args)

    mock_init.assert_called_once_with(local_mode=True)
    mock_run.assert_called_once()


def test_process_command_raises_if_no_spec_found(mocker: MockerFixture):
    """
    Test `process_command` raises ValueError when no matching spec file is found.

    Args:
        mocker: PyTest mocker fixture.
    """
    mocker.patch("merlin.cli.commands.restart.verify_dirpath", return_value="fake_dir")
    mocker.patch("glob.glob", return_value=[])

    args = Namespace(restart_dir="fake_dir", run_mode="distributed")
    with pytest.raises(ValueError, match="does not match any provenance spec file"):
        RestartCommand().process_command(args)


def test_process_command_raises_if_multiple_specs_found(mocker: MockerFixture):
    """
    Test `process_command` raises ValueError when multiple spec files are found.

    Args:
        mocker: PyTest mocker fixture.
    """
    mocker.patch("merlin.cli.commands.restart.verify_dirpath", return_value="fake_dir")
    mocker.patch("glob.glob", return_value=["file1.yaml", "file2.yaml"])

    args = Namespace(restart_dir="fake_dir", run_mode="distributed")
    with pytest.raises(ValueError, match="matches more than one provenance spec file"):
        RestartCommand().process_command(args)
