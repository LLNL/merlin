##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module will contain the testing logic for the `merlin run-workers` command.
"""

import os
import subprocess
import time

import pytest

from tests.fixture_data_classes import RedisBrokerAndBackend
from tests.fixture_types import FixtureCallable, FixtureDict, FixtureStr
from tests.integration.conditions import HasRegex, HasReturnCode
from tests.integration.helper_funcs import check_test_conditions, copy_app_yaml_to_cwd


@pytest.fixture(scope="session")
def run_workers_command_testing_dir(create_testing_dir: FixtureCallable, temp_output_dir: FixtureStr) -> FixtureStr:
    """
    Fixture to create a temporary output directory for tests related to testing the
    `merlin run-workers` functionality.

    Args:
        create_testing_dir: A fixture which returns a function that creates the testing directory.
        temp_output_dir: The path to the temporary ouptut directory we'll be using for this test run.

    Returns:
        The path to the temporary testing directory for `merlin run-workers` tests.
    """
    return create_testing_dir(temp_output_dir, "run_command_testing")


class TestRunWorkersCommand:
    """
    Base class for testing the `merlin run-workers` command.
    """

    @pytest.mark.parametrize(
        "shell",
        ["/bin/bash", "/bin/sh", "/bin/tcsh", "/bin/csh", "/bin/zsh"],
    )
    def test_workers_start_for_different_shells(  # pylint: disable=too-many-arguments,too-many-positional-arguments,unused-argument
        self,
        shell: str,
        base_study_config: FixtureDict,
        create_spec_file: FixtureCallable,
        run_workers_command_testing_dir: FixtureStr,
        redis_broker_and_backend_function: RedisBrokerAndBackend,
        merlin_server_dir: FixtureStr,
    ):
        """
        Test that workers spin up for different shells.

        Args:
            shell: The shell to test (parametrized)
            base_study_config: Base study configuration fixture
            create_study_file: Fixture to create study files
            run_workers_command_testing_dir: Temp output directory for run-workers tests
            redis_broker_and_backend_function: Fixture for setting up Redis broker and
                backend for function-scoped tests.
            merlin_server_dir:
                A fixture to provide the path to the merlin_server directory that will be
                created by the `redis_server` fixture.
        """
        # Skip test if shell is not available on the system
        if not os.path.exists(shell):
            pytest.skip(f"Shell {shell} not available on this system")

        # Create study config for this shell
        study_config = base_study_config.copy()
        study_config["batch"]["shell"] = shell

        # Create the spec file
        spec_file = create_spec_file(study_config, run_workers_command_testing_dir, "test_workers_actually_start.yaml")

        # Copy the app.yaml to the cwd so merlin will connect to the testing server
        copy_app_yaml_to_cwd(merlin_server_dir)

        with subprocess.Popen(["merlin", "run-workers", spec_file], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True):
            # Give the workers time to start
            time.sleep(10)

            # Ping the workers to see if they've started
            ping_process = subprocess.run(["celery", "-A", "merlin", "inspect", "ping"], capture_output=True, text=True)

            # Store information about the ping in a dict
            info = {
                "stdout": ping_process.stdout,
                "stderr": ping_process.stderr,
                "return_code": ping_process.returncode,
            }

            # Establish test conditions
            conditions = [
                HasReturnCode(),
                HasRegex(r"celery@test_worker.*: OK"),
            ]

            # Check that all of the test conditions pass
            check_test_conditions(conditions, info)

            # Run merlin's stop-workers first
            subprocess.run(["merlin", "stop-workers"])
