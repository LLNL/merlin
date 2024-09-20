"""
Tests for the `merlin stop-workers` command.
"""

import os
import re
import shutil
import subprocess
import yaml
from enum import Enum
from typing import List

from celery import Celery

from tests.integration.conditions import HasRegex
from tests.context_managers.celery_workers_manager import CeleryWorkersManager


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


class TestStopWorkers:
    """
    Tests for the `merlin stop-workers` command. Most of these tests will:
    1. Start workers from a spec file used for testing
        - Use CeleryWorkerManager for this to ensure safe stoppage of workers
          if something goes wrong
    2. Run the `merlin stop-workers` command from a subprocess
    """

    def load_workers_from_spec(self, spec_filepath: str) -> dict:
        """
        Load worker specifications from a YAML file.

        This function reads a YAML file containing study specifications and
        extracts the worker information under the "merlin" section. It
        constructs a dictionary in the form that CeleryWorkersManager.launch_workers
        requires.

        Parameters:
            spec_filepath: The file path to the YAML specification file.

        Returns:
            A dictionary containing the worker specifications from the
                "merlin" section of the YAML file.
        """
        # Read in the contents of the spec file
        with open(spec_filepath, "r") as spec_file:
            spec_contents = yaml.load(spec_file, yaml.Loader)

        # Initialize an empty dictionary to hold worker_info
        worker_info = {}
        
        # Access workers and steps from spec_contents
        workers = spec_contents["merlin"]["resources"]["workers"]
        study_steps = {step['name']: step['run']['task_queue'] for step in spec_contents['study']}

        # Grab the concurrency and queues from each worker and add it to the worker_info dict
        for worker_name, worker_settings in workers.items():
            match = re.search(r'--concurrency\s+(\d+)', worker_settings["args"])
            concurrency = int(match.group(1)) if match else 1
            queues = [study_steps[step] for step in worker_settings["steps"]]
            worker_info[worker_name] = {"concurrency": concurrency, "queues": queues}

        return worker_info

    def copy_app_yaml_to_cwd(self, merlin_server_dir: str):
        """
        Copy the app.yaml file from the directory provided to the current working
        directory.

        Grab the app.yaml file from `merlin_server_dir` and copy it to the current
        working directory so that Merlin will read this in as the server configuration
        for whatever test is calling this.

        Parameters:
            merlin_server_dir:
                A fixture to provide the path to the merlin_server directory that will be
                created by the `redis_server` fixture.
        """
        copied_app_yaml = os.path.join(os.getcwd(), "app.yaml")
        if not os.path.exists(copied_app_yaml):
            server_app_yaml = os.path.join(merlin_server_dir, "app.yaml")
            shutil.copy(server_app_yaml, copied_app_yaml)

    def run_test_with_workers(
        self,
        redis_server: str,
        redis_results_backend_config: "Fixture",  # noqa: F821
        redis_broker_config: "Fixture",  # noqa: F821
        path_to_test_specs: str,
        merlin_server_dir: str,
        conditions: List,
        flag: str = None,
    ):
        """
        Helper function to run common testing logic for tests with workers started.

        This function will:
        0. Read in the necessary fixtures as parameters. The purpose of these fixtures
           include:
            - Starting a containerized redis server
            - Updating the CONFIG object to point to the containerized redis server
            - Grabbing paths to our test specs and the merlin server directory created
              from starting the containerized redis server
        1. Load in the worker specifications from the `multiple_workers.yaml` file.
        2. Use a context manager to start up the workers on the celery app connected to
           the containerized redis server
        3. Copy the app.yaml file for the containerized redis server to the current working
           directory so that merlin will connect to it when we run our test
        4. Run the test command that's provided and check that the conditions given are
           passing.

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
            conditions:
                A list of `Condition` instances that need to pass in order for this test to
                be successful.
            flag:
                An optional flag to add to the `merlin stop-workers` command so we can test
                different functionality for the command.
        """
        from merlin.celery import app as celery_app

        # Grab worker configurations from the spec file
        multiple_worker_spec = os.path.join(path_to_test_specs, "multiple_workers.yaml")
        workers_from_spec = self.load_workers_from_spec(multiple_worker_spec)

        # We use a context manager to start workers so that they'll safely stop even if this test fails
        with CeleryWorkersManager(celery_app) as workers_manager:
            workers_manager.launch_workers(workers_from_spec)

            # Copy the app.yaml to the cwd so merlin will connect to the testing server
            self.copy_app_yaml_to_cwd(merlin_server_dir)

            # Run the test
            stop_workers_cmd = "merlin stop-workers"
            cmd_to_test = f"{stop_workers_cmd} {flag}" if flag is not None else stop_workers_cmd
            result = subprocess.run(cmd_to_test, capture_output=True, text=True, shell=True)

            info = {
                "stdout": result.stdout,
                "stderr": result.stderr,
                "return_code": result.returncode,
            }

            # Ensure all test conditions are satisfied
            for condition in conditions:
                condition.ingest_info(info)
                assert condition.passes

    def test_no_workers(
        self,
        redis_server: str,
        redis_results_backend_config: "Fixture",  # noqa: F821
        redis_broker_config: "Fixture",  # noqa: F821
        merlin_server_dir: str,
    ):
        """
        Test the `merlin stop-workers` command with no workers started in the first place.

        This test will:
        0. Set up the appropriate fixtures. This includes:
            - Starting a containerized redis server
            - Updating the CONFIG object to point to the containerized redis server
            - Grabbing the path to the merlin server directory created from starting the
              containerized redis server
        1. Copy the app.yaml file for the containerized redis server to the current working
           directory so that merlin will connect to it when we run our test
        2. Run the `merlin stop-workers` command and ensure that no workers are stopped (this
           should be the case since no workers were started in the first place)

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
        # Copy the app.yaml to the cwd so merlin will connect to the testing server
        self.copy_app_yaml_to_cwd(merlin_server_dir)

        # Define our test conditions
        conditions = [
            HasRegex(WorkerMessages.NO_WORKERS_MSG.value),  # No workers should be launched so we should see this
            HasRegex(WorkerMessages.STEP_1_WORKER.value, negate=True),  # None of these workers should be started
            HasRegex(WorkerMessages.STEP_2_WORKER.value, negate=True),  # None of these workers should be started
            HasRegex(WorkerMessages.OTHER_WORKER.value, negate=True),  # None of these workers should be started
        ]

        # Run the test
        result = subprocess.run("merlin stop-workers", capture_output=True, text=True, shell=True)
        info = {
            "stdout": result.stdout,
            "stderr": result.stderr,
            "return_code": result.returncode,
        }

        # Ensure all test conditions are satisfied
        for condition in conditions:
            condition.ingest_info(info)
            assert condition.passes
        
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
        `run_test_with_workers()` method.

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
        conditions = [
            HasRegex(WorkerMessages.NO_WORKERS_MSG.value, negate=True),  # Some workers should be stopped
            HasRegex(WorkerMessages.STEP_1_WORKER.value),  # This worker should be stopped
            HasRegex(WorkerMessages.STEP_2_WORKER.value),  # This worker should be stopped
            HasRegex(WorkerMessages.OTHER_WORKER.value),  # This worker should be stopped
        ]
        self.run_test_with_workers(
            redis_server,
            redis_results_backend_config,
            redis_broker_config,
            path_to_test_specs,
            merlin_server_dir,
            conditions,
        )

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
        see the `run_test_with_workers()` method.

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
        conditions = [
            HasRegex(WorkerMessages.NO_WORKERS_MSG.value, negate=True),  # Some workers should be stopped
            HasRegex(WorkerMessages.STEP_1_WORKER.value),  # This worker should be stopped
            HasRegex(WorkerMessages.STEP_2_WORKER.value),  # This worker should be stopped
            HasRegex(WorkerMessages.OTHER_WORKER.value),  # This worker should be stopped
        ]
        self.run_test_with_workers(
            redis_server,
            redis_results_backend_config,
            redis_broker_config,
            path_to_test_specs,
            merlin_server_dir,
            conditions,
            flag=f"--spec {os.path.join(path_to_test_specs, "multiple_workers.yaml")}"
        )

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
        exactly what this test is doing, see the `run_test_with_workers()` method.

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
        conditions = [
            HasRegex(WorkerMessages.NO_WORKERS_MSG.value, negate=True),  # Some workers should be stopped
            HasRegex(WorkerMessages.STEP_1_WORKER.value),  # This worker should be stopped
            HasRegex(WorkerMessages.STEP_2_WORKER.value),  # This worker should be stopped
            HasRegex(WorkerMessages.OTHER_WORKER.value, negate=True),  # This worker should NOT be stopped
        ]
        self.run_test_with_workers(
            redis_server,
            redis_results_backend_config,
            redis_broker_config,
            path_to_test_specs,
            merlin_server_dir,
            conditions,
            flag=f"--workers {WorkerMessages.STEP_1_WORKER.value} {WorkerMessages.STEP_2_WORKER.value}"
        )

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
        on exactly what this test is doing, see the `run_test_with_workers()` method.

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
        conditions = [
            HasRegex(WorkerMessages.NO_WORKERS_MSG.value, negate=True),  # One workers should be stopped
            HasRegex(WorkerMessages.STEP_1_WORKER.value),  # This worker should be stopped
            HasRegex(WorkerMessages.STEP_2_WORKER.value, negate=True),  # This worker should NOT be stopped
            HasRegex(WorkerMessages.OTHER_WORKER.value, negate=True),  # This worker should NOT be stopped
        ]
        self.run_test_with_workers(
            redis_server,
            redis_results_backend_config,
            redis_broker_config,
            path_to_test_specs,
            merlin_server_dir,
            conditions,
            flag=f"--queues hello_queue"
        )
