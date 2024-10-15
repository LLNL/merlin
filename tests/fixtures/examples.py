"""
Fixtures specifically for help testing the modules in the examples/ directory.
"""

import os

import pytest

from tests.fixture_types import FixtureStr


@pytest.fixture(scope="session")
def examples_testing_dir(temp_output_dir: FixtureStr) -> FixtureStr:
    """
    Fixture to create a temporary output directory for tests related to the examples functionality.

    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
    :returns: The path to the temporary testing directory for examples tests
    """
    testing_dir = f"{temp_output_dir}/examples_testing"
    if not os.path.exists(testing_dir):
        os.mkdir(testing_dir)

    return testing_dir
