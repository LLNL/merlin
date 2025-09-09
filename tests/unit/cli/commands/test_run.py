##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `run.py` file of the `cli/` folder.
"""

from argparse import Namespace

import pytest
from pytest_mock import MockerFixture

from merlin.cli.commands.run import RunCommand
from tests.fixture_types import FixtureCallable


def test_add_parser_sets_up_run_command(create_parser: FixtureCallable):
    """
    Ensure the `run` command sets the correct defaults and required arguments.

    Args:
        create_parser: A fixture to help create a parser.
    """
    command = RunCommand()
    parser = create_parser(command)
    args = parser.parse_args(["run", "study.yaml"])
    assert hasattr(args, "func")
    assert args.func.__name__ == command.process_command.__name__
    assert args.specification == "study.yaml"
    assert args.variables is None
    assert args.samples_file is None
    assert args.dry is False
    assert args.no_errors is False
    assert args.pgen_file is None
    assert args.pargs == []
    assert args.run_mode == "distributed"


def test_process_command_raises_on_pargs_without_pgen(mocker: MockerFixture):
    """
    Ensure a ValueError is raised when --pargs is given without --pgen.

    Args:
        mocker: PyTest mocker fixture.
    """
    mocker.patch("merlin.cli.commands.run.verify_filepath", return_value="study.yaml")
    mocker.patch("merlin.cli.commands.run.parse_override_vars", return_value={})

    args = Namespace(
        specification="study.yaml",
        variables=None,
        samples_file=None,
        dry=False,
        no_errors=False,
        pgen_file=None,
        pargs=["foo"],
        run_mode="distributed",
    )

    with pytest.raises(ValueError, match="Cannot use the 'pargs' parameter without specifying a 'pgen'!"):
        RunCommand().process_command(args)
