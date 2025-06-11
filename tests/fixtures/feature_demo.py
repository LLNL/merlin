##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Fixtures specifically for help testing the feature_demo workflow.
"""

import os
import subprocess

import pytest

from tests.fixture_data_classes import FeatureDemoSetup, RedisBrokerAndBackend
from tests.fixture_types import FixtureCallable, FixtureInt, FixtureStr
from tests.integration.helper_funcs import copy_app_yaml_to_cwd, run_workflow


# pylint: disable=redefined-outer-name


@pytest.fixture(scope="session")
def feature_demo_testing_dir(create_testing_dir: FixtureCallable, temp_output_dir: FixtureStr) -> FixtureStr:
    """
    Fixture to create a temporary output directory for tests related to testing the
    feature_demo workflow.

    Args:
        create_testing_dir: A fixture which returns a function that creates the testing directory.
        temp_output_dir: The path to the temporary ouptut directory we'll be using for this test run.

    Returns:
        The path to the temporary testing directory for feature_demo workflow tests.
    """
    return create_testing_dir(temp_output_dir, "feature_demo_testing")


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


@pytest.fixture(scope="session")
def feature_demo_setup(
    feature_demo_testing_dir: FixtureStr,
    feature_demo_num_samples: FixtureInt,
    feature_demo_name: FixtureStr,
    path_to_merlin_codebase: FixtureStr,
) -> FeatureDemoSetup:
    """
    Fixture for setting up the environment required for testing the feature demo workflow.

    This fixture prepares the necessary configuration and paths for executing tests related
    to the feature demo workflow. It aggregates the required parameters into a single
    [`FeatureDemoSetup`][fixture_data_classes.FeatureDemoSetup] data class instance, which
    simplifies the management of these parameters in tests.

    Args:
        feature_demo_testing_dir: The path to the temporary output directory where
            feature demo workflow tests will store their results.
        feature_demo_num_samples: An integer representing the number of samples
            to use in the feature demo workflow.
        feature_demo_name: A string representing the name to use for the feature
            demo workflow.
        path_to_merlin_codebase: The base path to the Merlin codebase, which is
            used to locate the feature demo YAML file.

    Returns:
        A [`FeatureDemoSetup`][fixture_data_classes.FeatureDemoSetup] instance containing
            the testing directory, number of samples, name, and path to the feature demo
            YAML file, which can be used in tests that require this setup.
    """
    demo_workflow = os.path.join("examples", "workflows", "feature_demo", "feature_demo.yaml")
    feature_demo_path = os.path.join(path_to_merlin_codebase, demo_workflow)
    return FeatureDemoSetup(
        testing_dir=feature_demo_testing_dir,
        num_samples=feature_demo_num_samples,
        name=feature_demo_name,
        path=feature_demo_path,
    )


@pytest.fixture(scope="class")
def feature_demo_run_workflow(
    redis_broker_and_backend_class: RedisBrokerAndBackend,
    feature_demo_setup: FeatureDemoSetup,
    merlin_server_dir: FixtureStr,
) -> subprocess.CompletedProcess:
    """
    Run the feature demo workflow.

    This fixture sets up and executes the feature demo workflow using the specified configurations
    and parameters. It prepares the environment by modifying the CONFIG object to connect to a
    Redis server and runs the demo workflow with the provided sample size and name.

    Args:
        redis_broker_and_backend_class: Fixture for setting up Redis broker and
            backend for class-scoped tests.
        feature_demo_setup: A fixture that returns a [`FeatureDemoSetup`][fixture_data_classes.FeatureDemoSetup]
            instance.
        merlin_server_dir: A fixture to provide the path to the merlin_server directory that will be
            created by the [`redis_server`][conftest.redis_server] fixture.

    Returns:
        The completed process object containing information about the execution of the workflow, including
            return code, stdout, and stderr.
    """
    # Setup the test
    copy_app_yaml_to_cwd(merlin_server_dir)

    # Create the variables to pass in to the workflow
    vars_to_substitute = [
        f"N_SAMPLES={feature_demo_setup.num_samples}",
        f"NAME={feature_demo_setup.name}",
        f"OUTPUT_PATH={feature_demo_setup.testing_dir}",
    ]

    # Run the workflow
    return run_workflow(redis_broker_and_backend_class.client, feature_demo_setup.path, vars_to_substitute)
