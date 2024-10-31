"""
Fixtures specifically for help testing the feature_demo workflow.
"""
import os
import subprocess
from time import sleep

import pytest

from tests.context_managers.celery_task_manager import CeleryTaskManager
from tests.context_managers.celery_workers_manager import CeleryWorkersManager
from tests.fixture_types import FixtureInt, FixtureModification, FixtureRedis, FixtureStr
from tests.integration.helper_funcs import check_test_conditions, copy_app_yaml_to_cwd, load_workers_from_spec


@pytest.fixture(scope="session")
def feature_demo_testing_dir(temp_output_dir: FixtureStr) -> FixtureStr:
    """
    Fixture to create a temporary output directory for tests related to testing the
    feature_demo workflow.

    Args:
        temp_output_dir: The path to the temporary ouptut directory we'll be using for this test run.
    
    Returns:
        The path to the temporary testing directory for feature_demo workflow tests.
    """
    testing_dir = f"{temp_output_dir}/feature_demo_testing"
    if not os.path.exists(testing_dir):
        os.mkdir(testing_dir)

    return testing_dir


@pytest.fixture(scope="session")
def feature_demo_num_samples() -> FixtureInt:
    """
    Defines a specific number of samples to use for the feature_demo workflow.
    This helps ensure that even if changes were made to the feature_demo workflow,
    tests using this fixture should still run the same thing.

    Returns:
        An integer representing the number of samples to use in the feature_demo workflow.
    """
    return 8


@pytest.fixture(scope="session")
def feature_demo_name() -> FixtureStr:
    """
    Defines a specific name to use for the feature_demo workflow. This helps ensure
    that even if changes were made to the feature_demo workflow, tests using this fixture
    should still run the same thing.

    Returns:
        A string representing the name to use for the feature_demo workflow.
    """
    return "feature_demo_test"


@pytest.fixture(scope="class")
def feature_demo_run_workflow(
    redis_client: FixtureRedis,
    redis_results_backend_config_class: FixtureModification,
    redis_broker_config_class: FixtureModification,
    path_to_merlin_codebase: FixtureStr,
    merlin_server_dir: FixtureStr,
    feature_demo_testing_dir: FixtureStr,
    feature_demo_num_samples: FixtureInt,
    feature_demo_name: FixtureStr,
) -> subprocess.CompletedProcess:
    """
    Run the feature demo workflow.

    This fixture sets up and executes the feature demo workflow using the specified configurations
    and parameters. It prepares the environment by modifying the CONFIG object to connect to a
    Redis server and runs the demo workflow with the provided sample size and name. It utilizes
    context managers to safely send tasks to the server and start up workers. The workflow is given
    30 seconds to complete which should be plenty of time.

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
        path_to_merlin_codebase: A fixture to provide the path to the directory containing Merlin's
            core functionality.
        merlin_server_dir: A fixture to provide the path to the merlin_server directory that will be
            created by the [`redis_server`][conftest.redis_server] fixture.
        feature_demo_testing_dir: The path to the temp output directory for feature_demo workflow tests.
        feature_demo_num_samples: An integer representing the number of samples to use in the feature_demo
            workflow.
        feature_demo_name: A string representing the name to use for the feature_demo workflow.

    Returns:
        The completed process object containing information about the execution of the workflow, including
            return code, stdout, and stderr.
    """
    # TODO might want to generalize the logic in this function into a new function that runs workflows
    from merlin.celery import app as celery_app
    
    # Setup the test
    copy_app_yaml_to_cwd(merlin_server_dir)
    demo_workflow = os.path.join("examples", "workflows", "feature_demo", "feature_demo.yaml")
    feature_demo_path = os.path.join(path_to_merlin_codebase, demo_workflow)

    # Create the variables to pass in to the workflow
    vars_to_substitute = [
        f"N_SAMPLES={feature_demo_num_samples}",
        f"NAME={feature_demo_name}",
        f"OUTPUT_PATH={feature_demo_testing_dir}"
    ]

    run_workers_proc = None

    with CeleryTaskManager(celery_app, redis_client) as CTM:
        # Send the tasks to the server
        run_proc = subprocess.run(
            f"merlin run {feature_demo_path} --vars {' '.join(vars_to_substitute)}",
            shell=True,
            capture_output=True,
            text=True,
        )

        # We use a context manager to start workers so that they'll safely stop even if this test fails
        with CeleryWorkersManager(celery_app) as CWM:
            # Start the workers then add them to the context manager so they can be stopped safely later
            run_workers_proc = subprocess.Popen(
                f"merlin run-workers {feature_demo_path}".split(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True
            )
            CWM.add_run_workers_process(run_workers_proc.pid)

            # Let the workflow try to run for 30 seconds
            sleep(30)

    return run_workers_proc
