##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `example.py` file of the `cli/` folder.
"""

from argparse import Namespace

from _pytest.capture import CaptureFixture
from pytest_mock import MockerFixture

from merlin.cli.commands.example import ExampleCommand
from tests.fixture_types import FixtureCallable


def test_example_parser_sets_func(create_parser: FixtureCallable):
    """
    Ensure the `example` command sets the correct default function.

    Args:
        create_parser: A fixture to help create a parser.
    """
    command = ExampleCommand()
    parser = create_parser(command)
    args = parser.parse_args(["example", "list"])
    assert hasattr(args, "func")
    assert args.func.__name__ == command.process_command.__name__
    assert args.workflow == "list"
    assert args.path is None


def test_process_command_list(capsys: CaptureFixture, mocker: MockerFixture):
    """
    Verify that `list_examples()` is called when workflow is `list`.

    Args:
        capsys: PyTest capsys fixture.
        mocker: PyTest mocker fixture.
    """
    mock_list = mocker.patch("merlin.cli.commands.example.list_examples", return_value="example_a\nexample_b")
    args = Namespace(workflow="list", path=None)

    ExampleCommand().process_command(args)

    captured = capsys.readouterr()
    assert "example_a" in captured.out
    mock_list.assert_called_once()


def test_process_command_setup(capsys: CaptureFixture, mocker: MockerFixture):
    """
    Verify that banner_small is printed and setup_example is called with correct args.

    Args:
        capsys: PyTest capsys fixture.
        mocker: PyTest mocker fixture.
    """
    mock_setup = mocker.patch("merlin.cli.commands.example.setup_example")
    mocker.patch("merlin.cli.commands.example.banner_small", "FAKE_BANNER")

    args = Namespace(workflow="test-workflow", path="/some/path")
    ExampleCommand().process_command(args)

    captured = capsys.readouterr()
    assert "FAKE_BANNER" in captured.out
    mock_setup.assert_called_once_with("test-workflow", "/some/path")
