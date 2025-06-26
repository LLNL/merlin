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


def test_process_command_runs_study_successfully(mocker: MockerFixture):
    """
    Test full run command flow with mock MerlinStudy and MerlinDatabase.

    Args:
        mocker: PyTest mocker fixture.
    """
    mock_filepath = "/abs/path/study.yaml"
    mocker.patch("merlin.cli.commands.run.verify_filepath", return_value=mock_filepath)
    mocker.patch("merlin.cli.commands.run.parse_override_vars", return_value={"FOO": "bar"})

    mock_study = mocker.Mock()
    mock_study.expanded_spec.name = "study"
    mock_study.workspace = "/some/workspace"
    mock_study.expanded_spec.get_queue_list.return_value = ["queue1"]
    mock_study.expanded_spec.get_task_queues.return_value = {"step1": "queue1"}
    mock_study.expanded_spec.get_worker_step_map.return_value = {"workerA": ["step1"]}
    mock_merlin_study = mocker.patch("merlin.cli.commands.run.MerlinStudy", return_value=mock_study)

    mock_db_instance = mocker.Mock()
    mock_run_entity = mocker.Mock()
    mock_logical_worker = mocker.Mock()
    mock_run_entity.get_id.return_value = 1
    mock_logical_worker.get_id.return_value = 2
    mock_db_instance.create.side_effect = [mock_run_entity, mock_logical_worker]
    mock_db = mocker.patch("merlin.cli.commands.run.MerlinDatabase", return_value=mock_db_instance)

    run_task_mock = mocker.patch("merlin.cli.commands.run.run_task_server")

    args = Namespace(
        specification="study.yaml",
        variables=["FOO=bar"],
        samples_file=None,
        dry=False,
        no_errors=False,
        pgen_file=None,
        pargs=[],
        run_mode="distributed",
    )

    RunCommand().process_command(args)

    mock_merlin_study.assert_called_once()
    mock_db.assert_called_once()
    run_task_mock.assert_called_once_with(mock_study, "distributed")
    mock_logical_worker.add_run.assert_called_once_with(1)
    mock_run_entity.add_worker.assert_called_once_with(2)


def test_process_command_with_local_mode_initializes_config(mocker: MockerFixture):
    """
    Ensure local mode initializes local config and still runs task server.

    Args:
        mocker: PyTest mocker fixture.
    """
    # Mocks
    mocker.patch("merlin.cli.commands.run.verify_filepath", return_value="study.yaml")
    mocker.patch("merlin.cli.commands.run.parse_override_vars", return_value={})
    
    mock_study = mocker.Mock()
    mock_expanded_spec = mocker.Mock()
    mock_expanded_spec.name = "study"
    mock_expanded_spec.get_queue_list.return_value = ["q"]
    mock_expanded_spec.get_task_queues.return_value = {"step1": "q"}
    mock_expanded_spec.get_worker_step_map.return_value = {"workerA": ["step1"]}
    mock_study.expanded_spec = mock_expanded_spec
    mock_study.workspace = "/workspace"
    mocker.patch("merlin.cli.commands.run.MerlinStudy", return_value=mock_study)

    mock_db = mocker.Mock()
    mock_run_entity = mocker.Mock(get_id=mocker.Mock(return_value=1))
    mock_logical_worker = mocker.Mock(get_id=mocker.Mock(return_value=2))
    mock_db.create.side_effect = [mock_run_entity, mock_logical_worker]
    mocker.patch("merlin.cli.commands.run.MerlinDatabase", return_value=mock_db)

    mock_initialize = mocker.patch("merlin.cli.commands.run.initialize_config")
    mocker.patch("merlin.cli.commands.run.run_task_server")

    args = Namespace(
        specification="study.yaml",
        variables=None,
        samples_file=None,
        dry=False,
        no_errors=False,
        pgen_file=None,
        pargs=[],
        run_mode="local"
    )

    RunCommand().process_command(args)

    mock_initialize.assert_called_once_with(local_mode=True)


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
        run_mode="distributed"
    )

    with pytest.raises(ValueError, match="Cannot use the 'pargs' parameter without specifying a 'pgen'!"):
        RunCommand().process_command(args)

