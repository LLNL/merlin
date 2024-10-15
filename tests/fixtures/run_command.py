"""
Fixtures specifically for help testing the `merlin run` command.
"""
import os

import pytest


@pytest.fixture(scope="session")
def run_command_testing_dir(temp_output_dir: str) -> str:
    """
    Fixture to create a temporary output directory for tests related to testing the
    `merlin run` functionality.

    Args:
        temp_output_dir: The path to the temporary ouptut directory we'll be using for this test run
    
    Returns:
        The path to the temporary testing directory for `merlin run` tests
    """
    testing_dir = f"{temp_output_dir}/run_command_testing"
    if not os.path.exists(testing_dir):
        os.mkdir(testing_dir)

    return testing_dir
