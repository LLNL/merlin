"""
Fixtures specifically for help testing the `merlin run` command.
"""

import pytest

from tests.fixture_types import FixtureCallable, FixtureStr


@pytest.fixture(scope="session")
def run_command_testing_dir(create_testing_dir: FixtureCallable, temp_output_dir: FixtureStr) -> FixtureStr:
    """
    Fixture to create a temporary output directory for tests related to testing the
    `merlin run` functionality.

    Args:
        create_testing_dir: A fixture which returns a function that creates the testing directory.
        temp_output_dir: The path to the temporary ouptut directory we'll be using for this test run.

    Returns:
        The path to the temporary testing directory for `merlin run` tests.
    """
    return create_testing_dir(temp_output_dir, "run_command_testing")
