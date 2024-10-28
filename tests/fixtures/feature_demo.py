"""
Fixtures specifically for help testing the feature_demo workflow.
"""
import os

import pytest

from tests.context_managers.celery_task_manager import CeleryTaskManager
from tests.context_managers.celery_workers_manager import CeleryWorkersManager
from tests.fixture_types import FixtureInt, FixtureModification, FixtureRedis, FixtureStr, FixtureTuple
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
        An string representing the name to use for the feature_demo workflow.
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
) -> FixtureTuple[str, str]:
    """
    """
    from merlin.celery import app as celery_app
        
    # os.chdir(feature_demo_testing_dir)
    copy_app_yaml_to_cwd(merlin_server_dir)
    feature_demo_path = os.path.join(path_to_merlin_codebase, self.demo_workflow)
    # test_output_path = os.path.join(feature_demo_testing_dir, self.get_test_name())
    test_name = self.get_test_name()

    vars_to_substitute = [
        f"N_SAMPLES={feature_demo_num_samples}",
        f"NAME={feature_demo_name}",
        f"OUTPUT_PATH={feature_demo_testing_dir}"
    ]

    run_workers_proc = None

    with CeleryTaskManager(celery_app, redis_client) as CTM:
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

    stdout, stderr = run_workers_proc.communicate()
    return stdout, stderr
