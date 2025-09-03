##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `query_workers.py` file of the `cli/` folder.
"""

import logging
from argparse import Namespace

from _pytest.capture import CaptureFixture
from pytest_mock import MockerFixture

from merlin.cli.commands.query_workers import QueryWorkersCommand
from tests.fixture_types import FixtureCallable


def test_add_parser_sets_up_query_workers_command(create_parser: FixtureCallable):
    """
    Ensure the `query-workers` command sets the correct default function.

    Args:
        create_parser: A fixture to help create a parser.
    """
    command = QueryWorkersCommand()
    parser = create_parser(command)
    args = parser.parse_args(["query-workers"])
    assert hasattr(args, "func")
    assert args.func.__name__ == command.process_command.__name__
    assert args.task_server == "celery"
    assert args.spec is None
    assert args.queues is None
    assert args.workers is None


def test_process_command_without_spec(mocker: MockerFixture):
    """
    Ensure `process_command` calls `query_workers` directly if no spec is provided.

    Args:
        mocker: PyTest mocker fixture.
    """
    query_workers_mock = mocker.patch("merlin.cli.commands.query_workers.query_workers")

    args = Namespace(
        task_server="celery",
        spec=None,
        queues=["q1", "q2"],
        workers=["worker1", "worker2"],
    )

    cmd = QueryWorkersCommand()
    cmd.process_command(args)

    query_workers_mock.assert_called_once_with("celery", [], ["q1", "q2"], ["worker1", "worker2"])


def test_process_command_with_spec(mocker: MockerFixture, caplog: CaptureFixture):
    """
    Ensure `process_command` loads worker names from spec and passes them to `query_workers`.

    Args:
        mocker: PyTest mocker fixture.
        caplog: PyTest caplog fixture.
    """
    caplog.set_level(logging.DEBUG)

    mock_spec = mocker.Mock()
    mock_spec.get_worker_names.return_value = ["foo", "bar"]

    mocker.patch("merlin.cli.commands.query_workers.verify_filepath", return_value="some/path/spec.yaml")
    mocker.patch("merlin.cli.commands.query_workers.MerlinSpec.load_specification", return_value=mock_spec)
    query_workers_mock = mocker.patch("merlin.cli.commands.query_workers.query_workers")

    args = Namespace(
        task_server="celery",
        spec="workflow.yaml",
        queues=None,
        workers=None,
    )

    cmd = QueryWorkersCommand()
    cmd.process_command(args)

    query_workers_mock.assert_called_once_with("celery", ["foo", "bar"], None, None)
    assert "Searching for the following workers to stop" in caplog.text


def test_process_command_logs_warning_for_unexpanded_worker(mocker: MockerFixture, caplog: CaptureFixture):
    """
    Ensure a warning is logged if a worker name from the spec contains `$`.

    Args:
        mocker: PyTest mocker fixture.
        caplog: PyTest caplog fixture.
    """
    caplog.set_level(logging.WARNING)

    mock_spec = mocker.Mock()
    mock_spec.get_worker_names.return_value = ["$ENV_VAR", "actual_worker"]

    mocker.patch("merlin.cli.commands.query_workers.verify_filepath", return_value="workflow.yaml")
    mocker.patch("merlin.cli.commands.query_workers.MerlinSpec.load_specification", return_value=mock_spec)
    query_workers_mock = mocker.patch("merlin.cli.commands.query_workers.query_workers")

    args = Namespace(
        task_server="celery",
        spec="workflow.yaml",
        queues=None,
        workers=None,
    )

    cmd = QueryWorkersCommand()
    cmd.process_command(args)

    assert "Worker '$ENV_VAR' is unexpanded. Target provenance spec instead?" in caplog.text
    query_workers_mock.assert_called_once_with("celery", ["$ENV_VAR", "actual_worker"], None, None)
