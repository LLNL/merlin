"""
This module will contain the base classes used for
the integration tests in this command directory.
"""

import os
import subprocess
from typing import List

from tests.context_managers.celery_workers_manager import CeleryWorkersManager
from tests.integration.conditions import Condition
from tests.integration.helper_funcs import check_test_conditions, copy_app_yaml_to_cwd, load_workers_from_spec


class BaseWorkerInteractionTests:
    """
    Base class for tests that interact with the worker in some way.
    Contains necessary methods for executing the tests.
    """

    def run_test_with_workers(
        self,
        path_to_test_specs: str,
        merlin_server_dir: str,
        conditions: List[Condition],
        command: str,
        flag: str = None,
    ):
        """
        Helper function to run common testing logic for tests with workers started.

        This function will:
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

        Parameters:
            path_to_test_specs:
                A fixture to provide the path to the directory containing test specifications.
            merlin_server_dir:
                A fixture to provide the path to the merlin_server directory that will be
                created by the `redis_server` fixture.
            conditions:
                A list of `Condition` instances that need to pass in order for this test to
                be successful.
            command:
                The command that we're testing. E.g. "merlin stop-workers"
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
            cmd_to_test = f"{command} {flag}" if flag else command
            result = subprocess.run(cmd_to_test, capture_output=True, text=True, shell=True)

            info = {
                "stdout": result.stdout,
                "stderr": result.stderr,
                "return_code": result.returncode,
            }

            # Ensure all test conditions are satisfied
            check_test_conditions(conditions, info)
    
    def run_test_without_workers(self, merlin_server_dir: str, conditions: List[Condition], command: str):
        """
        Helper function to run common testing logic for tests with no workers started.

        This test will:
        0. Read in the `merlin_server_dir` fixture as a parameter. This fixture is a
           path to the merlin server directory created from starting the containerized
           redis server.
        1. Copy the app.yaml file for the containerized redis server to the current working
           directory so that merlin will connect to it when we run our test
        2. Run the test command that's provided and check that the conditions given are
           passing.

        Parameters:
            merlin_server_dir:
                A fixture to provide the path to the merlin_server directory that will be
                created by the `redis_server` fixture.
            conditions:
                A list of `Condition` instances that need to pass in order for this test to
                be successful.
            command:
                The command that we're testing. E.g. "merlin stop-workers"
        """
        # Copy the app.yaml to the cwd so merlin will connect to the testing server
        copy_app_yaml_to_cwd(merlin_server_dir)

        # Run the test
        result = subprocess.run(self.command_to_test, capture_output=True, text=True, shell=True)
        info = {
            "stdout": result.stdout,
            "stderr": result.stderr,
            "return_code": result.returncode,
        }

        # Ensure all test conditions are satisfied
        check_test_conditions(conditions, info)