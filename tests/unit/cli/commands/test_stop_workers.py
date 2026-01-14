##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `stop_workers.py` file of the `cli/` folder.
"""

from argparse import Namespace

from _pytest.capture import CaptureFixture
from pytest_mock import MockerFixture

from merlin.cli.commands.stop_workers import StopWorkersCommand
from tests.fixture_types import FixtureCallable


def test_add_parser_sets_up_stop_workers_command(create_parser: FixtureCallable):
    """
    Ensure the stop-workers command parser sets correct defaults and accepts args.

    Args:
        create_parser: A fixture to help create a parser.
    """
    command = StopWorkersCommand()
    parser = create_parser(command)
    args = parser.parse_args(["stop-workers", "--task_server", "celery", "--queues", "queue1", "queue2"])
    assert hasattr(args, "func")
    assert args.func.__name__ == command.process_command.__name__
    assert args.task_server == "celery"
    assert args.queues == ["queue1", "queue2"]
    assert args.workers is None
    assert args.spec is None
    assert args.dry_run is False


def test_process_command_calls_stop_workers_no_spec(mocker: MockerFixture):
    """
    Ensure stop_workers is called when no spec is provided.

    Args:
        mocker: PyTest mocker fixture.
    """
    mocker.patch("merlin.cli.commands.stop_workers.banner_small", "BANNER")
    mock_handler_factory = mocker.patch("merlin.cli.commands.stop_workers.worker_handler_factory.create")
    mock_handler = mock_handler_factory.return_value
    mock_handler.stop_workers = mocker.MagicMock()

    args = Namespace(spec=None, task_server="celery", queues=["q1"], workers=["worker1"], dry_run=False)
    StopWorkersCommand().process_command(args)

    mock_handler.stop_workers.assert_called_once_with(queues=["q1"], workers=["worker1"], dry_run=False)


def test_process_command_with_spec_and_worker_names(mocker: MockerFixture):
    """
    Test loading a spec file, getting worker names, and calling stop_workers with them.

    Args:
        mocker: PyTest mocker fixture.
    """
    mocker.patch("merlin.cli.commands.stop_workers.banner_small", "BANNER")
    mock_handler_factory = mocker.patch("merlin.cli.commands.stop_workers.worker_handler_factory.create")
    mock_handler = mock_handler_factory.return_value
    mock_handler.stop_workers = mocker.MagicMock()
    mock_verify = mocker.patch("merlin.cli.commands.stop_workers.verify_filepath", return_value="study.yaml")

    mock_spec = mocker.patch("merlin.cli.commands.stop_workers.MerlinSpec")
    mock_spec.load_specification.return_value.get_worker_names.return_value = ["worker.alpha", "worker.beta"]

    args = Namespace(
        spec="study.yaml",
        task_server="celery",
        queues=None,
        workers=None,
        dry_run=False,
    )
    StopWorkersCommand().process_command(args)

    mock_verify.assert_called_once_with("study.yaml")
    mock_spec.load_specification.assert_called_once_with("study.yaml")
    mock_handler.stop_workers.assert_called_once_with(queues=None, workers=["worker.alpha", "worker.beta"], dry_run=False)


def test_process_command_logs_warning_on_unexpanded_worker(mocker: MockerFixture, caplog: CaptureFixture):
    """
    Ensure warning is logged if unexpanded worker names are detected.

    Args:
        mocker: PyTest mocker fixture.
        caplog: PyTest caplog fixture.
    """
    caplog.set_level("WARNING", logger="merlin")

    mocker.patch("merlin.cli.commands.stop_workers.banner_small", "BANNER")
    mock_handler_factory = mocker.patch("merlin.cli.commands.stop_workers.worker_handler_factory.create")
    mock_handler = mock_handler_factory.return_value
    mock_handler.stop_workers = mocker.MagicMock()
    mocker.patch("merlin.cli.commands.stop_workers.verify_filepath", return_value="spec.yaml")

    mock_spec = mocker.patch("merlin.cli.commands.stop_workers.MerlinSpec")
    mock_spec.load_specification.return_value.get_worker_names.return_value = ["worker.1", "worker.$step"]

    args = Namespace(spec="spec.yaml", task_server="celery", queues=None, workers=None, dry_run=False)
    StopWorkersCommand().process_command(args)

    assert any("is unexpanded" in record.message for record in caplog.records)
    mock_handler.stop_workers.assert_called_once_with(queues=None, workers=["worker.1", "worker.$step"], dry_run=False)
