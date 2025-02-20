"""
Fixtures specifically for help testing the monitor command.
"""

import os
import subprocess

import pytest

from tests.fixture_data_classes import MonitorSetup, RedisBrokerAndBackend
from tests.fixture_types import FixtureCallable, FixtureStr


# pylint: disable=redefined-outer-name


@pytest.fixture(scope="session")
def monitor_testing_dir(create_testing_dir: FixtureCallable, temp_output_dir: FixtureStr) -> FixtureStr:
    """
    Fixture to create a temporary output directory for tests related to testing the
    monitor workflow.

    Args:
        create_testing_dir: A fixture which returns a function that creates the testing directory.
        temp_output_dir: The path to the temporary ouptut directory we'll be using for this test run.

    Returns:
        The path to the temporary testing directory for monitor tests.
    """
    return create_testing_dir(temp_output_dir, "monitor_testing")


@pytest.fixture(scope="session")
def monitor_setup(
    monitor_testing_dir: FixtureStr,
    path_to_test_specs: FixtureStr,
) -> MonitorSetup:
    """
    Fixture for setting up the environment required for testing the monitor command.

    This fixture prepares the necessary configuration and paths for executing tests related
    to the monitor command. It aggregates the required parameters into a single
    [`MonitorSetup`][fixture_data_classes.MonitorSetup] data class instance, which
    simplifies the management of these parameters in tests.

    Args:
        monitor_testing_dir: The path to the temporary output directory where monitor
            command tests will store their results.
        path_to_test_specs: The base path to the Merlin test specs directory.

    Returns:
        A [`MonitorSetup`][fixture_data_classes.MonitorSetup] instance containing
            information needed for the monitor command tests.
    """
    auto_restart_yaml_path = os.path.join(path_to_test_specs, "monitor_auto_restart_test.yaml")
    return MonitorSetup(
        testing_dir=monitor_testing_dir,
        auto_restart_yaml=auto_restart_yaml_path,
    )