"""
Fixtures specifically for help testing the functionality related to
status/detailed-status.
"""

import os
from pathlib import Path

import pytest


@pytest.fixture(scope="class")
def status_testing_dir(temp_output_dir: str) -> str:
    """
    A pytest fixture to set up a temporary directory to write files to for testing status.

    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
    """
    testing_dir = f"{temp_output_dir}/status_testing/"
    if not os.path.exists(testing_dir):
        os.mkdir(testing_dir)

    return testing_dir


@pytest.fixture(scope="class")
def status_empty_file(status_testing_dir: str) -> str:  # pylint: disable=W0621
    """
    A pytest fixture to create an empty status file.

    :param status_testing_dir: A pytest fixture that defines a path to the the output directory we'll write to
    """
    empty_file = Path(f"{status_testing_dir}/empty_status.json")
    if not empty_file.exists():
        empty_file.touch()

    return empty_file
