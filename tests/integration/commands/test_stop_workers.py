##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module will contain the testing logic for the `stop-workers` command.
"""

import os
import subprocess
from contextlib import contextmanager
from enum import Enum
from typing import List

from tests.context_managers.celery_workers_manager import CeleryWorkersManager
from tests.fixture_data_classes import RedisBrokerAndBackend
from tests.fixture_types import FixtureStr
from tests.integration.conditions import Condition, HasRegex
from tests.integration.helper_funcs import check_test_conditions, copy_app_yaml_to_cwd, load_workers_from_spec


# pylint: disable=unused-argument,import-outside-toplevel


class WorkerMessages(Enum):
    """
    Enumerated strings to help keep track of the messages
    that we're expecting (or not expecting) to see from the
    tests in this module.
    """

    NO_WORKERS_MSG_STOP = "No workers found to stop"
    STEP_1_WORKER = "step_1_merlin_test_worker"
    STEP_2_WORKER = "step_2_merlin_test_worker"
    OTHER_WORKER = "other_merlin_test_worker"


class TestStopWorkersCommands:
    """
    Tests for the `merlin stop-workers` command.
    Most of these tests will:
    1. Start workers from a spec file used for testing
        - Use CeleryWorkerManager for this to ensure safe stoppage of workers
          if something goes wrong
    2. Run the test command from a subprocess
    """

    @contextmanager
    def run_test_with_workers(  # pylint: disable=too-many-arguments
        self,
        path_to_test_specs: FixtureStr,
        merlin_server_dir: FixtureStr,
        conditions: List[Condition],
        flag: str = None,
    ):
        """
        Helper method to run common testing logic for tests with workers started.
        This method must also be a context manager so we can check the status of the
        workers prior to the CeleryWorkersManager running it's exit code that shuts down
        all active workers.

        This method will:
        0. Read in the necessary fixtures as parameters. These fixtures grab paths to
           our test specs and the merlin server directory created from starting the
           containerized redis server.
        1. Load in the worker specifications from the `multiple_workers.yaml` file.
        2. Use a context manager to start up the workers on the celery app connected to
           the containerized redis server
        3. Copy the app.yaml file for the containerized redis server to the current working
           directory so that merlin will connect to it when we run our test
        4. Run the test command that's provided and check that the conditions given are
           passing.
        5. Yield control back to the calling method.
        6. Safely terminate workers that may have not been stopped once the calling method
           completes.

        Parameters:
            path_to_test_specs:
                A fixture to provide the path to the directory containing test specifications.
            merlin_server_dir:
                A fixture to provide the path to the merlin_server directory that will be
                created by the `redis_server` fixture.
            conditions:
                A list of `Condition` instances that need to pass in order for this test to
                be successful.
            flag:
                An optional flag to add to the command that we're testing so we can test
                different functionality for the command.
        """
        from merlin.celery import app as celery_app

        # Grab worker configurations from the spec file
        multiple_worker_spec = os.path.join(path_to_test_specs, "multiple_workers.yaml")
        workers_from_spec = load_workers_from_spec(multiple_worker_spec)

        # We use a context manager to start workers so that they'll safely stop even if this test fails
        with CeleryWorkersManager(celery_app) as workers_manager:
            workers_manager.launch_workers(workers_from_spec)

            # Copy the app.yaml to the cwd so merlin will connect to the testing server
            copy_app_yaml_to_cwd(merlin_server_dir)

            # Run the test
            command = "merlin stop-workers"
            if flag:
                command += f" {flag}"
            result = subprocess.run(command, capture_output=True, text=True, shell=True)

            info = {
                "stdout": result.stdout,
                "stderr": result.stderr,
                "return_code": result.returncode,
            }

            # Ensure all test conditions are satisfied
            check_test_conditions(conditions, info)

            yield

    def test_no_workers(
        self,
        redis_broker_and_backend_function: RedisBrokerAndBackend,
        merlin_server_dir: FixtureStr,
    ):
        """
        Test the `merlin stop-workers` command with no workers started in the first place.

        This test will:
        0. Setup the pytest fixtures which include:
            - starting a containerized Redis server
            - updating the CONFIG object to point to the containerized Redis server
            - obtaining the path to the merlin server directory created from starting
              the containerized Redis server
        1. Copy the app.yaml file for the containerized redis server to the current working
           directory so that merlin will connect to it when we run our test
        2. Run the test command that's provided and check that the conditions given are
           passing.

        Parameters:
            redis_broker_and_backend_function: Fixture for setting up Redis broker and
                backend for function-scoped tests.
            merlin_server_dir:
                A fixture to provide the path to the merlin_server directory that will be
                created by the `redis_server` fixture.
        """
        conditions = [
            HasRegex(WorkerMessages.NO_WORKERS_MSG_STOP.value),
            HasRegex(WorkerMessages.STEP_1_WORKER.value, negate=True),
            HasRegex(WorkerMessages.STEP_2_WORKER.value, negate=True),
            HasRegex(WorkerMessages.OTHER_WORKER.value, negate=True),
        ]

        # Copy the app.yaml to the cwd so merlin will connect to the testing server
        copy_app_yaml_to_cwd(merlin_server_dir)

        # Run the test
        result = subprocess.run("merlin stop-workers", capture_output=True, text=True, shell=True)
        info = {
            "stdout": result.stdout,
            "stderr": result.stderr,
            "return_code": result.returncode,
        }

        # Ensure all test conditions are satisfied
        check_test_conditions(conditions, info)

    def test_no_flags(
        self,
        redis_broker_and_backend_function: RedisBrokerAndBackend,
        path_to_test_specs: FixtureStr,
        merlin_server_dir: FixtureStr,
    ):
        """
        Test the `merlin stop-workers` command with no flags.

        Run the command and ensure the text output from Merlin is correct.
        To see more information on exactly what this test is doing, see the
        `run_test_with_workers()` method.

        Parameters:
            redis_broker_and_backend_function: Fixture for setting up Redis broker and
                backend for function-scoped tests.
            path_to_test_specs:
                A fixture to provide the path to the directory containing test specifications.
            merlin_server_dir:
                A fixture to provide the path to the merlin_server directory that will be
                created by the `redis_server` fixture.
        """
        conditions = [
            HasRegex(WorkerMessages.NO_WORKERS_MSG_STOP.value, negate=True),
            HasRegex(WorkerMessages.STEP_1_WORKER.value),
            HasRegex(WorkerMessages.STEP_2_WORKER.value),
            HasRegex(WorkerMessages.OTHER_WORKER.value),
        ]
        with self.run_test_with_workers(path_to_test_specs, merlin_server_dir, conditions):
            # After the test runs and before the CeleryWorkersManager exits, ensure there are no workers on the app
            from merlin.celery import app as celery_app

            active_queues = celery_app.control.inspect().active_queues()
            assert active_queues is None

    def test_spec_flag(
        self,
        redis_broker_and_backend_function: RedisBrokerAndBackend,
        path_to_test_specs: FixtureStr,
        merlin_server_dir: FixtureStr,
    ):
        """
        Test the `merlin stop-workers` command with the `--spec` flag.

        Run the command with the `--spec` flag and ensure the text output
        from Merlin is correct. To see more information on exactly what this
        test is doing, see the `run_test_with_workers()` method.

        Parameters:
            redis_broker_and_backend_function: Fixture for setting up Redis broker and
                backend for function-scoped tests.
            path_to_test_specs:
                A fixture to provide the path to the directory containing test specifications.
            merlin_server_dir:
                A fixture to provide the path to the merlin_server directory that will be
                created by the `redis_server` fixture.
        """
        conditions = [
            HasRegex(WorkerMessages.NO_WORKERS_MSG_STOP.value, negate=True),
            HasRegex(WorkerMessages.STEP_1_WORKER.value),
            HasRegex(WorkerMessages.STEP_2_WORKER.value),
            HasRegex(WorkerMessages.OTHER_WORKER.value),
        ]
        with self.run_test_with_workers(
            path_to_test_specs,
            merlin_server_dir,
            conditions,
            flag=f"--spec {os.path.join(path_to_test_specs, 'multiple_workers.yaml')}",
        ):
            from merlin.celery import app as celery_app

            active_queues = celery_app.control.inspect().active_queues()
            assert active_queues is None

    def test_workers_flag(
        self,
        redis_broker_and_backend_function: RedisBrokerAndBackend,
        path_to_test_specs: FixtureStr,
        merlin_server_dir: FixtureStr,
    ):
        """
        Test the `merlin stop-workers` command with the `--workers` flag.

        Run the command with the `--workers` flag and ensure the text output
        from Merlin is correct. To see more information on exactly what this
        test is doing, see the `run_test_with_workers()` method.

        Parameters:
            redis_broker_and_backend_function: Fixture for setting up Redis broker and
                backend for function-scoped tests.
            path_to_test_specs:
                A fixture to provide the path to the directory containing test specifications.
            merlin_server_dir:
                A fixture to provide the path to the merlin_server directory that will be
                created by the `redis_server` fixture.
        """
        conditions = [
            HasRegex(WorkerMessages.NO_WORKERS_MSG_STOP.value, negate=True),
            HasRegex(WorkerMessages.STEP_1_WORKER.value),
            HasRegex(WorkerMessages.STEP_2_WORKER.value),
            HasRegex(WorkerMessages.OTHER_WORKER.value, negate=True),
        ]
        with self.run_test_with_workers(
            path_to_test_specs,
            merlin_server_dir,
            conditions,
            flag=f"--workers {WorkerMessages.STEP_1_WORKER.value} {WorkerMessages.STEP_2_WORKER.value}",
        ):
            from merlin.celery import app as celery_app

            active_queues = celery_app.control.inspect().active_queues()
            worker_name = f"celery@{WorkerMessages.OTHER_WORKER.value}"
            assert worker_name in active_queues

    def test_queues_flag(
        self,
        redis_broker_and_backend_function: RedisBrokerAndBackend,
        path_to_test_specs: FixtureStr,
        merlin_server_dir: FixtureStr,
    ):
        """
        Test the `merlin stop-workers` command with the `--queues` flag.

        Run the command with the `--queues` flag and ensure the text output
        from Merlin is correct. To see more information on exactly what this
        test is doing, see the `run_test_with_workers()` method.

        Parameters:
            redis_broker_and_backend_function: Fixture for setting up Redis broker and
                backend for function-scoped tests.
            path_to_test_specs:
                A fixture to provide the path to the directory containing test specifications.
            merlin_server_dir:
                A fixture to provide the path to the merlin_server directory that will be
                created by the `redis_server` fixture.
        """
        conditions = [
            HasRegex(WorkerMessages.NO_WORKERS_MSG_STOP.value, negate=True),
            HasRegex(WorkerMessages.STEP_1_WORKER.value),
            HasRegex(WorkerMessages.STEP_2_WORKER.value, negate=True),
            HasRegex(WorkerMessages.OTHER_WORKER.value, negate=True),
        ]
        with self.run_test_with_workers(
            path_to_test_specs,
            merlin_server_dir,
            conditions,
            flag="--queues hello_queue",
        ):
            from merlin.celery import app as celery_app

            active_queues = celery_app.control.inspect().active_queues()
            workers_that_should_be_alive = [
                f"celery@{WorkerMessages.OTHER_WORKER.value}",
                f"celery@{WorkerMessages.STEP_2_WORKER.value}",
            ]
            for worker_name in workers_that_should_be_alive:
                assert worker_name in active_queues


# pylint: enable=unused-argument,import-outside-toplevel
