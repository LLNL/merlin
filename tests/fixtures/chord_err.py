"""
Fixtures specifically for help testing the chord_err workflow.
"""
import os
import subprocess

import pytest

from tests.fixture_types import FixtureCallable, FixtureModification, FixtureRedis, FixtureStr
from tests.integration.helper_funcs import copy_app_yaml_to_cwd, run_workflow


@pytest.fixture(scope="session")
def chord_err_testing_dir(create_testing_dir: FixtureCallable, temp_output_dir: FixtureStr) -> FixtureStr:
    """
    Fixture to create a temporary output directory for tests related to testing the
    chord_err workflow.

    Args:
        create_testing_dir: A fixture which returns a function that creates the testing directory.
        temp_output_dir: The path to the temporary ouptut directory we'll be using for this test run.
    
    Returns:
        The path to the temporary testing directory for chord_err workflow tests.
    """
    return create_testing_dir(temp_output_dir, "chord_err_testing")


@pytest.fixture(scope="session")
def chord_err_name() -> FixtureStr:
    """
    Defines a specific name to use for the chord_err workflow. This helps ensure
    that even if changes were made to the chord_err workflow, tests using this fixture
    should still run the same thing.

    Returns:
        A string representing the name to use for the chord_err workflow.
    """
    return "chord_err_test"


@pytest.fixture(scope="class")
def chord_err_run_workflow(
    redis_client: FixtureRedis,
    redis_results_backend_config_class: FixtureModification,
    redis_broker_config_class: FixtureModification,
    path_to_test_specs: FixtureStr,
    merlin_server_dir: FixtureStr,
    chord_err_testing_dir: FixtureStr,
    chord_err_name: FixtureStr,
) -> subprocess.CompletedProcess:
    """
    Run the chord error workflow.

    This fixture sets up and executes the chord error workflow using the specified configurations
    and parameters. It prepares the environment by modifying the CONFIG object to connect to a
    Redis server and runs the workflow with the provided name and output path.

    Args:
        redis_client: A fixture that connects us to a redis client that we can interact with.
        redis_results_backend_config_class: A fixture that modifies the CONFIG object so that it
            points the results backend configuration to the containerized redis server we start up
            with the [`redis_server`][conftest.redis_server] fixture. The CONFIG object is what merlin
            uses to connect to a server.
        redis_broker_config_class: A fixture that modifies the CONFIG object so that it points
            the broker configuration to the containerized redis server we start up with the
            [`redis_server`][conftest.redis_server] fixture. The CONFIG object is what merlin uses
            to connect to a server.
        path_to_test_specs: A fixture to provide the path to the directory containing test specifications.
        merlin_server_dir: A fixture to provide the path to the merlin_server directory that will be
            created by the [`redis_server`][conftest.redis_server] fixture.
        chord_err_testing_dir: The path to the temporary testing directory for chord_err workflow tests.
        chord_err_name: A string representing the name to use for the chord_err workflow.

    Returns:
        The completed process object containing information about the execution of the workflow, including
            return code, stdout, and stderr.
    """    
    # Setup the test
    copy_app_yaml_to_cwd(merlin_server_dir)
    chord_err_path = os.path.join(path_to_test_specs, "chord_err.yaml")

    # Create the variables to pass in to the workflow
    vars_to_substitute = [
        f"NAME={chord_err_name}",
        f"OUTPUT_PATH={chord_err_testing_dir}"
    ]

    # Run the workflow
    return run_workflow(redis_client, chord_err_path, vars_to_substitute)
