##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the `merlin/workers/celery_worker.py` module.
"""

import os
import pytest
from typing import Any

from pytest_mock import MockerFixture

from merlin.db_scripts.merlin_db import MerlinDatabase
from merlin.exceptions import MerlinWorkerLaunchError
from merlin.workers import CeleryWorker
from tests.fixture_types import FixtureCallable, FixtureDict, FixtureStr


@pytest.fixture
def workers_testing_dir(create_testing_dir: FixtureCallable, temp_output_dir: FixtureStr) -> FixtureStr:
    """
    Fixture to create a temporary output directory for tests related to the workers functionality.

    Args:
        create_testing_dir: A fixture which returns a function that creates the testing directory.
        temp_output_dir: The path to the temporary output directory we'll be using for this test run.

    Returns:
        The path to the temporary testing directory for workers tests.
    """
    return create_testing_dir(temp_output_dir, "workers_testing")


@pytest.fixture
def basic_config() -> FixtureDict[str, Any]:
    """
    Fixture that provides a basic CeleryWorker configuration dictionary.

    Returns:
        A dictionary representing a minimal valid CeleryWorker config.
    """
    return {
        "args": "",
        "queues": ["queue1", "queue2"],
        "batch": {"nodes": 1},
        "machines": [],
    }


@pytest.fixture
def dummy_env(workers_testing_dir: FixtureStr) -> FixtureDict[str, str]:
    """
    Fixture that provides a mock environment dictionary with OUTPUT_PATH set.

    Args:
        workers_testing_dir: The path to the temporary testing directory for workers tests.

    Returns:
        A dictionary simulating environment variables, including OUTPUT_PATH.
    """
    return {"OUTPUT_PATH": workers_testing_dir}


@pytest.fixture
def mock_db(mocker: MockerFixture) -> MerlinDatabase:
    """
    Fixture that patches the MerlinDatabase constructor.

    This prevents CeleryWorker from writing to the real Merlin database during
    unit tests. Returns a mock instance of MerlinDatabase.

    Args:
        mocker: Pytest mocker fixture.

    Returns:
        A mocked MerlinDatabase instance.
    """
    return mocker.patch("merlin.workers.celery_worker.MerlinDatabase")


def test_constructor_sets_fields_and_calls_db_create(
    basic_config: FixtureDict[str, Any],
    dummy_env: FixtureDict[str, str],
    mock_db: MerlinDatabase,
):
    """
    Test that CeleryWorker constructor sets all fields correctly and triggers database creation.

    This test verifies that:
        - The worker fields (name, args, queues, batch, machines, overlap) are set from config.
        - The MerlinDatabase.create method is called with the correct arguments.

    Args:
        basic_config: A minimal configuration dictionary for the worker.
        dummy_env: A dictionary simulating the environment variables.
        mock_db: A mocked MerlinDatabase to prevent real database interaction.
    """
    worker = CeleryWorker("worker1", basic_config, dummy_env, overlap=True)

    assert worker.name == "worker1"
    assert worker.args == ""
    assert worker.queues == ["queue1", "queue2"]
    assert worker.batch == {"nodes": 1}
    assert worker.machines == []
    assert worker.overlap is True

    mock_db.return_value.create.assert_called_once_with("logical_worker", "worker1", ["queue1", "queue2"])


def test_verify_args_adds_name_and_logging_flags(
    mocker: MockerFixture,
    basic_config: FixtureDict[str, Any],
    dummy_env: FixtureDict[str, str],
    mock_db: MerlinDatabase,
):
    """
    Test that `_verify_args()` appends required flags to the Celery args string.

    This test ensures that the `-n <name>` and `-l <loglevel>` flags are added
    to the worker's CLI args if they are not already present. It also verifies
    that warnings are logged if the worker is configured for parallel batch execution
    but missing concurrency-related flags.

    NOTE: Although the mock_db fixture is not directly used in this test, it is required
    to prevent the constructor from making real database writes during CeleryWorker
    instantiation.

    Args:
        mocker: Pytest mocker fixture.
        basic_config: Fixture providing a basic CeleryWorker configuration.
        dummy_env: Fixture providing a mock environment dictionary.
        mock_db: Mocked MerlinDatabase to avoid real database writes.
    """
    mocker.patch("merlin.workers.celery_worker.batch_check_parallel", return_value=True)
    mock_logger = mocker.patch("merlin.workers.celery_worker.LOG")
    worker = CeleryWorker("w1", basic_config, dummy_env)

    worker._verify_args()

    assert "-n w1" in worker.args
    assert "-l" in worker.args
    assert mock_logger.warning.called


def test_get_launch_command_returns_expanded_command(
    mocker: MockerFixture,
    basic_config: FixtureDict[str, Any],
    dummy_env: FixtureDict[str, str],
    mock_db: MerlinDatabase,
):
    """
    Test that `get_launch_command()` constructs a valid Celery command.

    This test verifies that the command string returned by `get_launch_command()`
    includes a Celery invocation and is properly constructed using the
    `batch_worker_launch` utility. It mocks the batch launcher to ensure
    consistent output.

    NOTE: Although the mock_db fixture is not directly used in this test, it is required
    to prevent the constructor from making real database writes during CeleryWorker
    instantiation.

    Args:
        mocker: Pytest mocker fixture.
        basic_config: Fixture providing a basic CeleryWorker configuration.
        dummy_env: Fixture providing a mock environment dictionary.
        mock_db: Mocked MerlinDatabase to avoid real database writes.
    """
    mocker.patch("merlin.workers.celery_worker.batch_worker_launch", return_value="celery -A ...")
    worker = CeleryWorker("w2", basic_config, dummy_env)

    cmd = worker.get_launch_command("--override", disable_logs=True)

    assert isinstance(cmd, str)
    assert "celery" in cmd


def test_should_launch_rejects_if_machine_check_fails(
    mocker: MockerFixture,
    basic_config: FixtureDict[str, Any],
    dummy_env: FixtureDict[str, str],
    mock_db: MerlinDatabase,
):
    """
    Test that `should_launch` returns False if the machine check fails.

    This test simulates a scenario where `check_machines` returns False,
    indicating that the current machine is not authorized to launch the worker.
    It verifies that `should_launch` correctly rejects launching in this case.

    NOTE: Although the mock_db fixture is not directly used in this test, it is required
    to prevent the constructor from making real database writes during CeleryWorker
    instantiation.

    Args:
        mocker: Pytest mocker fixture.
        basic_config: Configuration dictionary containing the list of valid machines.
        dummy_env: Environment variable dictionary (unused in this test).
        mock_db: Mocked MerlinDatabase to avoid real database writes.
    """
    basic_config["machines"] = ["host1"]
    mocker.patch("merlin.workers.celery_worker.check_machines", return_value=False)

    worker = CeleryWorker("w3", basic_config, dummy_env)
    result = worker.should_launch()

    assert result is False


def test_should_launch_rejects_if_output_path_missing(
    mocker: MockerFixture,
    basic_config: FixtureDict[str, Any],
    dummy_env: FixtureDict[str, str],
    mock_db: MerlinDatabase,
):
    """
    Test that `should_launch` returns False if the output path does not exist.

    This test verifies that `should_launch` refuses to launch if the `OUTPUT_PATH`
    specified in the environment does not exist, even when the machine check passes.

    NOTE: Although the mock_db fixture is not directly used in this test, it is required
    to prevent the constructor from making real database writes during CeleryWorker
    instantiation.

    Args:
        mocker: Pytest mocker fixture.
        basic_config: Configuration dictionary including machine constraints.
        dummy_env: Environment variable dictionary containing an invalid output path.
        mock_db: Mocked MerlinDatabase to avoid real database writes.
    """
    basic_config["machines"] = ["host1"]
    dummy_env["OUTPUT_PATH"] = "/nonexistent"
    mocker.patch("merlin.workers.celery_worker.check_machines", return_value=True)
    mocker.patch("os.path.exists", return_value=False)

    worker = CeleryWorker("w4", basic_config, dummy_env)
    result = worker.should_launch()

    assert result is False


def test_should_launch_rejects_due_to_running_queues(
    mocker: MockerFixture,
    basic_config: FixtureDict[str, Any],
    dummy_env: FixtureDict[str, str],
    mock_db: MerlinDatabase,
):
    """
    Test that `should_launch` returns False when a conflicting queue is already running.

    This test simulates the scenario where one of the worker's queues is already active 
    in the system. The `get_running_queues` function is patched to return a list of 
    active queues containing "queue1", which matches the worker's queue configuration.

    NOTE: Although the mock_db fixture is not directly used in this test, it is required
    to prevent the constructor from making real database writes during CeleryWorker
    instantiation.

    Args:
        mocker: Pytest mocker fixture.
        basic_config: Fixture providing base worker config.
        dummy_env: Fixture providing environment variables.
        mock_db: Fixture for the Merlin database mock.
    """
    mocker.patch("merlin.study.celeryadapter.get_running_queues", return_value=["queue1"])

    worker = CeleryWorker("w5", basic_config, dummy_env)
    result = worker.should_launch()

    assert result is False


def test_launch_worker_runs_if_should_launch(
    mocker: MockerFixture,
    basic_config: FixtureDict[str, Any],
    dummy_env: FixtureDict[str, str],
    mock_db: MerlinDatabase,
):
    """
    Test that `launch_worker` executes the launch command if `should_launch` returns True.

    This test verifies that when a worker passes the `should_launch` check, it constructs 
    a launch command and executes it via `subprocess.Popen`. Both the launch condition 
    and the command are mocked to avoid side effects. It also confirms that a debug 
    log message is emitted during execution.

    NOTE: Although the mock_db fixture is not directly used in this test, it is required
    to prevent the constructor from making real database writes during CeleryWorker
    instantiation.

    Args:
        mocker: Pytest mocker fixture.
        basic_config: Fixture providing base worker config.
        dummy_env: Fixture providing environment variables.
        mock_db: Fixture for the Merlin database mock.
    """
    mocker.patch.object(CeleryWorker, "should_launch", return_value=True)
    mocker.patch.object(CeleryWorker, "get_launch_command", return_value="echo hello")
    mock_popen = mocker.patch("merlin.workers.celery_worker.subprocess.Popen")
    mock_logger = mocker.patch("merlin.workers.celery_worker.LOG")

    worker = CeleryWorker("w6", basic_config, dummy_env)
    worker.launch_worker()

    mock_popen.assert_called_once()
    assert mock_logger.debug.called


def test_launch_worker_raises_if_popen_fails(
    mocker: MockerFixture,
    basic_config: FixtureDict[str, Any],
    dummy_env: FixtureDict[str, str],
    mock_db: MerlinDatabase,
):
    """
    Test that `launch_worker` raises `MerlinWorkerLaunchError` when `subprocess.Popen` fails.

    This test simulates a failure in launching a worker by patching `Popen` to raise an `OSError`.
    It verifies that the appropriate exception is raised and that the failure is not silently ignored.

    NOTE: Although the mock_db fixture is not directly used in this test, it is required
    to prevent the constructor from making real database writes during CeleryWorker
    instantiation.

    Args:
        mocker: Pytest mocker fixture.
        basic_config: Basic configuration dictionary fixture.
        dummy_env: Dummy environment dictionary fixture.
        mock_db: Mocked MerlinDatabase object.
    """
    mocker.patch.object(CeleryWorker, "should_launch", return_value=True)
    mocker.patch.object(CeleryWorker, "get_launch_command", return_value="fail")
    mocker.patch("merlin.workers.celery_worker.subprocess.Popen", side_effect=OSError("boom"))
    mocker.patch("merlin.workers.celery_worker.LOG")

    worker = CeleryWorker("w7", basic_config, dummy_env)

    with pytest.raises(MerlinWorkerLaunchError):
        worker.launch_worker()


def test_get_metadata_returns_expected_dict(
    basic_config: FixtureDict[str, Any],
    dummy_env: FixtureDict[str, str],
    mock_db: MerlinDatabase,
):
    """
    Test that `get_metadata` returns the expected dictionary with worker configuration.

    This test constructs a `CeleryWorker` and calls `get_metadata`, verifying that
    the returned dictionary matches the fields set during initialization.

    NOTE: Although the mock_db fixture is not directly used in this test, it is required
    to prevent the constructor from making real database writes during CeleryWorker
    instantiation.

    Args:
        basic_config: Basic configuration dictionary fixture.
        dummy_env: Dummy environment dictionary fixture.
        mock_db: Mocked MerlinDatabase object.
    """
    worker = CeleryWorker("meta_worker", basic_config, dummy_env)

    metadata = worker.get_metadata()

    assert metadata["name"] == "meta_worker"
    assert metadata["queues"] == ["queue1", "queue2"]
    assert metadata["args"] == ""
    assert metadata["machines"] == []
    assert metadata["batch"] == {"nodes": 1}
