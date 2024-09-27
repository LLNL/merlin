"""
Tests for the `merlin stop-workers` command.
"""

import os
from enum import Enum

from tests.integration.commands.base_classes import BaseStopWorkersAndQueryWorkersTest
from tests.integration.conditions import HasRegex


class WorkerMessages(Enum):
    """
    Enumerated strings to help keep track of the messages
    that we're expecting (or not expecting) to see from the
    tests in this module.
    """

    NO_WORKERS_MSG = "No workers found to stop"
    STEP_1_WORKER = "step_1_merlin_test_worker"
    STEP_2_WORKER = "step_2_merlin_test_worker"
    OTHER_WORKER = "other_merlin_test_worker"


class TestStopWorkers(BaseStopWorkersAndQueryWorkersTest):
    """
    Tests for the `merlin stop-workers` command. Most of these tests will:
    1. Start workers from a spec file used for testing
        - Use CeleryWorkerManager for this to ensure safe stoppage of workers
          if something goes wrong
    2. Run the `merlin stop-workers` command from a subprocess
    """

    command_to_test = "merlin stop-workers"

    def test_no_workers(
        self,
        redis_server: str,
        redis_results_backend_config: "Fixture",  # noqa: F821
        redis_broker_config: "Fixture",  # noqa: F821
        merlin_server_dir: str,
    ):
        """
        Test the `merlin stop-workers` command with no workers started in the first place.

        Run the `merlin stop-workers` command and ensure that a "no workers found" message
        is written to the output. To see more information on exactly what this test is doing,
        see the `run_test_without_workers()` method of the base class.

        Parameters:
            redis_server:
                A fixture that starts a containerized redis server instance that runs on
                localhost:6379.
            redis_results_backend_config:
                A fixture that modifies the CONFIG object so that it points the results
                backend configuration to the containerized redis server we start up with
                the `redis_server` fixture. The CONFIG object is what merlin uses to connect
                to a server.
            redis_broker_config:
                A fixture that modifies the CONFIG object so that it points the broker
                configuration to the containerized redis server we start up with the
                `redis_server` fixture. The CONFIG object is what merlin uses to connect
                to a server.
            merlin_server_dir:
                A fixture to provide the path to the merlin_server directory that will be
                created by the `redis_server` fixture.
        """
        conditions = [
            HasRegex(WorkerMessages.NO_WORKERS_MSG.value),  # No workers should be launched so we should see this
            HasRegex(WorkerMessages.STEP_1_WORKER.value, negate=True),  # None of these workers should be started
            HasRegex(WorkerMessages.STEP_2_WORKER.value, negate=True),  # None of these workers should be started
            HasRegex(WorkerMessages.OTHER_WORKER.value, negate=True),  # None of these workers should be started
        ]
        self.run_test_without_workers(merlin_server_dir, conditions, self.command_to_test)

    def test_no_flags(
        self,
        redis_server: str,
        redis_results_backend_config: "Fixture",  # noqa: F821
        redis_broker_config: "Fixture",  # noqa: F821
        path_to_test_specs: str,
        merlin_server_dir: str,
    ):
        """
        Test the `merlin stop-workers` command with no flags.

        Run the `merlin stop-workers` command and ensure that all workers are stopped.
        To see more information on exactly what this test is doing, see the
        `run_test_with_workers()` method of the base class.

        Parameters:
            redis_server:
                A fixture that starts a containerized redis server instance that runs on
                localhost:6379.
            redis_results_backend_config:
                A fixture that modifies the CONFIG object so that it points the results
                backend configuration to the containerized redis server we start up with
                the `redis_server` fixture. The CONFIG object is what merlin uses to connect
                to a server.
            redis_broker_config:
                A fixture that modifies the CONFIG object so that it points the broker
                configuration to the containerized redis server we start up with the
                `redis_server` fixture. The CONFIG object is what merlin uses to connect
                to a server.
            path_to_test_specs:
                A fixture to provide the path to the directory containing test specifications.
            merlin_server_dir:
                A fixture to provide the path to the merlin_server directory that will be
                created by the `redis_server` fixture.
        """
        from merlin.celery import app as celery_app

        # Define test conditions
        conditions = [
            HasRegex(WorkerMessages.NO_WORKERS_MSG.value, negate=True),  # Some workers should be stopped
            HasRegex(WorkerMessages.STEP_1_WORKER.value),  # This worker should be stopped
            HasRegex(WorkerMessages.STEP_2_WORKER.value),  # This worker should be stopped
            HasRegex(WorkerMessages.OTHER_WORKER.value),  # This worker should be stopped
        ]

        # Run the test
        with self.run_test_with_workers(path_to_test_specs, merlin_server_dir, conditions, self.command_to_test):
            # After the test runs and before the CeleryWorkersManager exits, ensure there are no workers on the app
            active_queues = celery_app.control.inspect().active_queues()
            assert active_queues is None

    def test_spec_flag(
        self,
        redis_server: str,
        redis_results_backend_config: "Fixture",  # noqa: F821
        redis_broker_config: "Fixture",  # noqa: F821
        path_to_test_specs: str,
        merlin_server_dir: str,
    ):
        """
        Test the `merlin stop-workers` command with the `--spec` flag.

        Run the `merlin stop-workers` command with the `--spec` flag and ensure that all
        workers are stopped. To see more information on exactly what this test is doing,
        see the `run_test_with_workers()` method of the base class.

        Parameters:
            redis_server:
                A fixture that starts a containerized redis server instance that runs on
                localhost:6379.
            redis_results_backend_config:
                A fixture that modifies the CONFIG object so that it points the results
                backend configuration to the containerized redis server we start up with
                the `redis_server` fixture. The CONFIG object is what merlin uses to connect
                to a server.
            redis_broker_config:
                A fixture that modifies the CONFIG object so that it points the broker
                configuration to the containerized redis server we start up with the
                `redis_server` fixture. The CONFIG object is what merlin uses to connect
                to a server.
            path_to_test_specs:
                A fixture to provide the path to the directory containing test specifications.
            merlin_server_dir:
                A fixture to provide the path to the merlin_server directory that will be
                created by the `redis_server` fixture.
        """
        from merlin.celery import app as celery_app
        conditions = [
            HasRegex(WorkerMessages.NO_WORKERS_MSG.value, negate=True),  # Some workers should be stopped
            HasRegex(WorkerMessages.STEP_1_WORKER.value),  # This worker should be stopped
            HasRegex(WorkerMessages.STEP_2_WORKER.value),  # This worker should be stopped
            HasRegex(WorkerMessages.OTHER_WORKER.value),  # This worker should be stopped
        ]
        with self.run_test_with_workers(
            path_to_test_specs,
            merlin_server_dir,
            conditions,
            self.command_to_test,
            flag=f"--spec {os.path.join(path_to_test_specs, 'multiple_workers.yaml')}",
        ):
            active_queues = celery_app.control.inspect().active_queues()
            assert active_queues is None

    def test_workers_flag(
        self,
        redis_server: str,
        redis_results_backend_config: "Fixture",  # noqa: F821
        redis_broker_config: "Fixture",  # noqa: F821
        path_to_test_specs: str,
        merlin_server_dir: str,
    ):
        """
        Test the `merlin stop-workers` command with the `--workers` flag.

        Run the `merlin stop-workers` command with the `--workers` flag and ensure that
        only the workers given with this flag are stopped. To see more information on
        exactly what this test is doing, see the `run_test_with_workers()` method of the
        base class.

        Parameters:
            redis_server:
                A fixture that starts a containerized redis server instance that runs on
                localhost:6379.
            redis_results_backend_config:
                A fixture that modifies the CONFIG object so that it points the results
                backend configuration to the containerized redis server we start up with
                the `redis_server` fixture. The CONFIG object is what merlin uses to connect
                to a server.
            redis_broker_config:
                A fixture that modifies the CONFIG object so that it points the broker
                configuration to the containerized redis server we start up with the
                `redis_server` fixture. The CONFIG object is what merlin uses to connect
                to a server.
            path_to_test_specs:
                A fixture to provide the path to the directory containing test specifications.
            merlin_server_dir:
                A fixture to provide the path to the merlin_server directory that will be
                created by the `redis_server` fixture.
        """
        from merlin.celery import app as celery_app
        conditions = [
            HasRegex(WorkerMessages.NO_WORKERS_MSG.value, negate=True),  # Some workers should be stopped
            HasRegex(WorkerMessages.STEP_1_WORKER.value),  # This worker should be stopped
            HasRegex(WorkerMessages.STEP_2_WORKER.value),  # This worker should be stopped
            HasRegex(WorkerMessages.OTHER_WORKER.value, negate=True),  # This worker should NOT be stopped
        ]
        with self.run_test_with_workers(
            path_to_test_specs,
            merlin_server_dir,
            conditions,
            self.command_to_test,
            flag=f"--workers {WorkerMessages.STEP_1_WORKER.value} {WorkerMessages.STEP_2_WORKER.value}",
        ):
            active_queues = celery_app.control.inspect().active_queues()
            worker_name = f"celery@{WorkerMessages.OTHER_WORKER.value}"
            assert worker_name in active_queues


    def test_queues_flag(
        self,
        redis_server: str,
        redis_results_backend_config: "Fixture",  # noqa: F821
        redis_broker_config: "Fixture",  # noqa: F821
        path_to_test_specs: str,
        merlin_server_dir: str,
    ):
        """
        Test the `merlin stop-workers` command with the `--queues` flag.

        Run the `merlin stop-workers` command with the `--queues` flag and ensure that
        only the workers attached to the given queues are stopped. To see more information
        on exactly what this test is doing, see the `run_test_with_workers()` method of the
        base class.

        Parameters:
            redis_server:
                A fixture that starts a containerized redis server instance that runs on
                localhost:6379.
            redis_results_backend_config:
                A fixture that modifies the CONFIG object so that it points the results
                backend configuration to the containerized redis server we start up with
                the `redis_server` fixture. The CONFIG object is what merlin uses to connect
                to a server.
            redis_broker_config:
                A fixture that modifies the CONFIG object so that it points the broker
                configuration to the containerized redis server we start up with the
                `redis_server` fixture. The CONFIG object is what merlin uses to connect
                to a server.
            path_to_test_specs:
                A fixture to provide the path to the directory containing test specifications.
            merlin_server_dir:
                A fixture to provide the path to the merlin_server directory that will be
                created by the `redis_server` fixture.
        """
        from merlin.celery import app as celery_app
        conditions = [
            HasRegex(WorkerMessages.NO_WORKERS_MSG.value, negate=True),  # One workers should be stopped
            HasRegex(WorkerMessages.STEP_1_WORKER.value),  # This worker should be stopped
            HasRegex(WorkerMessages.STEP_2_WORKER.value, negate=True),  # This worker should NOT be stopped
            HasRegex(WorkerMessages.OTHER_WORKER.value, negate=True),  # This worker should NOT be stopped
        ]
        with self.run_test_with_workers(
            path_to_test_specs,
            merlin_server_dir,
            conditions,
            self.command_to_test,
            flag="--queues hello_queue",
        ):
            active_queues = celery_app.control.inspect().active_queues()
            workers_that_should_be_alive = [
                f"celery@{WorkerMessages.OTHER_WORKER.value}", f"celery@{WorkerMessages.STEP_2_WORKER.value}"
            ]
            for worker_name in workers_that_should_be_alive:
                assert worker_name in active_queues
