##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `queue_info.py` file of the `cli/` folder.
"""

from argparse import Namespace

import pytest
from _pytest.capture import CaptureFixture
from pytest_mock import MockerFixture

from merlin.cli.commands.queue_info import QueueInfoCommand
from tests.fixture_types import FixtureCallable


def test_add_parser_sets_up_queue_info_command(create_parser: FixtureCallable):
    """
    Ensure the `queue-info` command sets the correct default function.

    Args:
        create_parser: A fixture to help create a parser.
    """
    command = QueueInfoCommand()
    parser = create_parser(command)
    args = parser.parse_args(["queue-info"])
    assert hasattr(args, "func")
    assert args.func.__name__ == command.process_command.__name__
    assert args.task_server == "celery"
    assert args.dump is None
    assert args.specific_queues is None
    assert args.specification is None
    assert args.steps == ["all"]
    assert args.variables is None


def test_process_command_no_spec_valid_args(mocker: MockerFixture, capsys: CaptureFixture):
    """
    Test `process_command` executes successfully without a spec file.

    Args:
        mocker: PyTest mocker fixture.
        capsys: PyTest capsys fixture.
    """
    query_mock = mocker.patch(
        "merlin.cli.commands.queue_info.query_queues",
        return_value={
            "queue1": {"jobs": 5, "consumers": 2},
            "queue2": {"jobs": 3, "consumers": 1},
        },
    )

    args = Namespace(specification=None, steps=["all"], variables=None, dump=None, specific_queues=None, task_server="celery")

    cmd = QueueInfoCommand()
    cmd.process_command(args)

    query_mock.assert_called_once_with("celery", None, ["all"], None)

    output = capsys.readouterr().out
    assert "queue1" in output and "queue2" in output


def test_process_command_with_spec(mocker: MockerFixture):
    """
    Test `process_command` behavior when a spec file is provided.

    Args:
        mocker: PyTest mocker fixture.
    """
    mock_spec = mocker.Mock()
    mock_get_spec = mocker.patch(
        "merlin.cli.commands.queue_info.get_merlin_spec_with_override", return_value=(mock_spec, None)
    )
    query_mock = mocker.patch("merlin.cli.commands.queue_info.query_queues", return_value={})

    args = Namespace(
        specification="workflow.yaml", steps=["all"], variables=None, dump=None, specific_queues=None, task_server="celery"
    )

    cmd = QueueInfoCommand()
    cmd.process_command(args)

    mock_get_spec.assert_called_once_with(args)
    query_mock.assert_called_once_with("celery", mock_spec, ["all"], None)


def test_process_command_dumps_queue_info(mocker: MockerFixture):
    """
    Test that queue information is correctly dumped to a file when `--dump` is provided.

    Args:
        mocker: PyTest mocker fixture.
    """
    queue_data = {
        "queue1": {"jobs": 10, "consumers": 4},
    }
    mocker.patch("merlin.cli.commands.queue_info.query_queues", return_value=queue_data)
    dump_mock = mocker.patch("merlin.cli.commands.queue_info.dump_queue_info")

    args = Namespace(
        specification=None, steps=["all"], variables=None, dump="output.json", specific_queues=None, task_server="celery"
    )

    cmd = QueueInfoCommand()
    cmd.process_command(args)

    dump_mock.assert_called_once_with("celery", queue_data, "output.json")


def test_process_command_raises_on_bad_dump_extension():
    """
    Test that an unsupported file extension for `--dump` raises a ValueError.
    """
    args = Namespace(
        specification=None, steps=["all"], variables=None, dump="badfile.txt", specific_queues=None, task_server="celery"
    )

    with pytest.raises(ValueError, match="Unsupported file type"):
        QueueInfoCommand().process_command(args)


def test_process_command_raises_on_steps_without_spec():
    """
    Test that using `--steps` without `--spec` raises a ValueError.
    """
    args = Namespace(
        specification=None, steps=["step1", "step2"], variables=None, dump=None, specific_queues=None, task_server="celery"
    )

    with pytest.raises(ValueError, match="--steps argument MUST be used with the --specification"):
        QueueInfoCommand().process_command(args)


def test_process_command_raises_on_vars_without_spec():
    """
    Test that using `--vars` without `--spec` raises a ValueError.
    """
    args = Namespace(
        specification=None, steps=["all"], variables=["VAR=1"], dump=None, specific_queues=None, task_server="celery"
    )

    with pytest.raises(ValueError, match="--vars argument MUST be used with the --specification"):
        QueueInfoCommand().process_command(args)
