"""
Fixtures specifically for help testing the functionality related to
status/detailed-status.
"""

import os
from argparse import Namespace
from copy import deepcopy
from pathlib import Path

import pytest
import yaml

from tests.unit.study.status_test_files import status_test_variables


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


@pytest.fixture(scope="class")
def status_set_sample_path():
    """
    A pytest fixture to set the path to the samples file in the test spec.
    """

    # Read in the contents of the expanded spec
    with open(status_test_variables.EXPANDED_SPEC_PATH, "r") as expanded_file:
        initial_expanded_contents = yaml.load(expanded_file, yaml.Loader)

    # Create a copy of the contents so we can reset the file when these tests are done
    modified_contents = deepcopy(initial_expanded_contents)

    # Modify the samples file path
    modified_contents["merlin"]["samples"]["file"] = status_test_variables.SAMPLES_PATH

    # Write the new contents to the expanded spec
    with open(status_test_variables.EXPANDED_SPEC_PATH, "w") as expanded_file:
        yaml.dump(modified_contents, expanded_file)

    yield

    # Reset the contents of the samples file path
    with open(status_test_variables.EXPANDED_SPEC_PATH, "w") as expanded_file:
        yaml.dump(initial_expanded_contents, expanded_file)


@pytest.fixture(scope="function")
def status_args():
    """
    A pytest fixture to set up a namespace with all the arguments necessary for
    the Status object.
    """
    return Namespace(
        subparsers="status",
        level="INFO",
        detailed=False,
        output_path=None,
        task_server="celery",
        cb_help=False,
        dump=None,
        no_prompts=True,  # We'll set this to True here since it's easier to test this way
    )
