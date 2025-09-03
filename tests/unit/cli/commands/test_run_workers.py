##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `run_workers.py` file of the `cli/` folder.
"""

from merlin.cli.commands.run_workers import RunWorkersCommand
from tests.fixture_types import FixtureCallable


def test_add_parser_sets_up_run_workers_command(create_parser: FixtureCallable):
    """
    Test that the `run-workers` command registers all expected arguments and sets the correct defaults.

    Args:
        create_parser: A fixture to help create a parser.
    """
    command = RunWorkersCommand()
    parser = create_parser(command)
    args = parser.parse_args(["run-workers", "workflow.yaml"])
    assert hasattr(args, "func")
    assert args.func.__name__ == command.process_command.__name__
    assert args.specification == "workflow.yaml"
    assert args.worker_args == ""
    assert args.worker_steps == ["all"]
    assert args.worker_echo_only is False
    assert args.variables is None
    assert args.disable_logs is False
