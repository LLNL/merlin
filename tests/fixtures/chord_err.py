##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Fixtures specifically for help testing the chord_err workflow.
"""

import os
import subprocess

import pytest

from tests.fixture_data_classes import ChordErrorSetup, RedisBrokerAndBackend
from tests.fixture_types import FixtureCallable, FixtureStr
from tests.integration.helper_funcs import copy_app_yaml_to_cwd, run_workflow


# pylint: disable=redefined-outer-name


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


@pytest.fixture(scope="session")
def chord_err_setup(
    chord_err_testing_dir: FixtureStr,
    chord_err_name: FixtureStr,
    path_to_test_specs: FixtureStr,
) -> ChordErrorSetup:
    """
    Fixture for setting up the environment required for testing the chord error workflow.

    This fixture prepares the necessary configuration and paths for executing tests related
    to the chord error workflow. It aggregates the required parameters into a single
    [`ChordErrorSetup`][fixture_data_classes.ChordErrorSetup] data class instance, which
    simplifies the management of these parameters in tests.

    Args:
        chord_err_testing_dir: The path to the temporary output directory where chord error
            workflow tests will store their results.
        chord_err_name: A string representing the name to use for the chord error workflow.
        path_to_test_specs: The base path to the Merlin test specs directory, which is
            used to locate the chord error YAML file.

    Returns:
        A [`ChordErrorSetup`][fixture_data_classes.ChordErrorSetup] instance containing
            the testing directory, name, and path to the chord error YAML file, which can
            be used in tests that require this setup.
    """
    chord_err_path = os.path.join(path_to_test_specs, "chord_err.yaml")
    return ChordErrorSetup(
        testing_dir=chord_err_testing_dir,
        name=chord_err_name,
        path=chord_err_path,
    )


@pytest.fixture(scope="class")
def chord_err_run_workflow(
    redis_broker_and_backend_class: RedisBrokerAndBackend,
    chord_err_setup: ChordErrorSetup,
    merlin_server_dir: FixtureStr,
) -> subprocess.CompletedProcess:
    """
    Run the chord error workflow.

    This fixture sets up and executes the chord error workflow using the specified configurations
    and parameters. It prepares the environment by modifying the CONFIG object to connect to a
    Redis server and runs the workflow with the provided name and output path.

    Args:
        redis_broker_and_backend_class: Fixture for setting up Redis broker and
            backend for class-scoped tests.
        chord_err_setup: A fixture that returns a [`ChordErrorSetup`][fixture_data_classes.ChordErrorSetup]
            instance.
        merlin_server_dir: A fixture to provide the path to the merlin_server directory that will be
            created by the [`redis_server`][conftest.redis_server] fixture.

    Returns:
        The completed process object containing information about the execution of the workflow, including
            return code, stdout, and stderr.
    """
    # Setup the test
    copy_app_yaml_to_cwd(merlin_server_dir)
    # chord_err_path = os.path.join(path_to_test_specs, "chord_err.yaml")

    # Create the variables to pass in to the workflow
    vars_to_substitute = [f"NAME={chord_err_setup.name}", f"OUTPUT_PATH={chord_err_setup.testing_dir}"]

    # Run the workflow
    return run_workflow(redis_broker_and_backend_class.client, chord_err_setup.path, vars_to_substitute)
