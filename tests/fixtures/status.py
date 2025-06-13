##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Fixtures specifically for help testing the functionality related to
status/detailed-status.
"""

import os
import shutil
from argparse import Namespace
from pathlib import Path

import pytest
import yaml

from tests.fixture_types import FixtureCallable, FixtureNamespace, FixtureStr
from tests.unit.study.status_test_files import status_test_variables


# pylint: disable=redefined-outer-name


@pytest.fixture(scope="session")
def status_testing_dir(create_testing_dir: FixtureCallable, temp_output_dir: FixtureStr) -> FixtureStr:
    """
    A pytest fixture to set up a temporary directory to write files to for testing status.

    Args:
        create_testing_dir: A fixture which returns a function that creates the testing directory.
        temp_output_dir: The path to the temporary ouptut directory we'll be using for this test run.

    Returns:
        The path to the temporary testing directory for status tests.
    """
    return create_testing_dir(temp_output_dir, "status_testing")


@pytest.fixture(scope="class")
def status_empty_file(status_testing_dir: FixtureStr) -> FixtureStr:
    """
    A pytest fixture to create an empty status file.

    :param status_testing_dir: A pytest fixture that defines a path to the the output
                               directory we'll write to
    :returns: The path to the empty status file
    """
    empty_file = Path(f"{status_testing_dir}/empty_status.json")
    if not empty_file.exists():
        empty_file.touch()

    return empty_file


@pytest.fixture(scope="session")
def status_spec_path(status_testing_dir: FixtureStr) -> FixtureStr:  # pylint: disable=W0621
    """
    Copy the test spec to the temp directory and modify the OUTPUT_PATH in the spec
    to point to the temp location.

    :param status_testing_dir: A pytest fixture that defines a path to the the output
                               directory we'll write to
    :returns: The path to the spec file
    """
    test_spec = f"{os.path.dirname(__file__)}/../unit/study/status_test_files/status_test_spec.yaml"
    spec_in_temp_dir = f"{status_testing_dir}/status_test_spec.yaml"
    shutil.copy(test_spec, spec_in_temp_dir)  # copy test spec to temp directory

    # Modify the OUTPUT_PATH variable to point to the temp directory
    with open(spec_in_temp_dir, "r") as spec_file:
        spec_contents = yaml.load(spec_file, yaml.Loader)
    spec_contents["env"]["variables"]["OUTPUT_PATH"] = status_testing_dir
    with open(spec_in_temp_dir, "w") as spec_file:
        yaml.dump(spec_contents, spec_file)

    return spec_in_temp_dir


def set_sample_path(output_workspace: str):
    """
    A pytest fixture to set the path to the samples file in the test spec.

    :param output_workspace: The workspace that we'll pull the spec file to update from
    """
    temp_merlin_info_path = f"{output_workspace}/merlin_info"
    expanded_spec_path = f"{temp_merlin_info_path}/status_test_spec.expanded.yaml"

    # Read in the contents of the expanded spec
    with open(expanded_spec_path, "r") as expanded_file:
        expanded_contents = yaml.load(expanded_file, yaml.Loader)

    # Modify the samples file path
    expanded_contents["merlin"]["samples"]["file"] = f"{temp_merlin_info_path}/samples.csv"

    # Write the new contents to the expanded spec
    with open(expanded_spec_path, "w") as expanded_file:
        yaml.dump(expanded_contents, expanded_file)


@pytest.fixture(scope="session")
def status_output_workspace(status_testing_dir: FixtureStr) -> FixtureStr:  # pylint: disable=W0621
    """
    A pytest fixture to copy the test output workspace for status to the temporary
    status testing directory.

    :param status_testing_dir: A pytest fixture that defines a path to the the output
                               directory we'll write to
    :returns: The path to the output workspace in the temp status testing directory
    """
    output_workspace = f"{status_testing_dir}/{status_test_variables.VALID_WORKSPACE}"
    shutil.copytree(status_test_variables.VALID_WORKSPACE_PATH, output_workspace)  # copy over the files
    set_sample_path(output_workspace)  # set the path to the samples file in the expanded yaml
    return output_workspace


@pytest.fixture(scope="function")
def status_args() -> FixtureNamespace:
    """
    A pytest fixture to set up a namespace with all the arguments necessary for
    the Status object.

    :returns: The namespace with necessary arguments for the Status object
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


@pytest.fixture(scope="session")
def status_nested_workspace(status_testing_dir: FixtureStr) -> FixtureStr:  # pylint: disable=W0621
    """
    Create an output workspace that contains another output workspace within one of its
    steps. In this case it will copy the status test workspace then within the 'just_samples'
    step we'll copy the status test workspace again but with a different name.

    :param status_testing_dir: A pytest fixture that defines a path to the the output
                               directory we'll write to
    :returns: The path to the top level workspace
    """
    top_level_workspace = f"{status_testing_dir}/status_test_study_nested_20240520-163524"
    nested_workspace = f"{top_level_workspace}/just_samples/nested_workspace_20240520-163524"
    shutil.copytree(status_test_variables.VALID_WORKSPACE_PATH, top_level_workspace)  # copy over the top level workspace
    shutil.copytree(status_test_variables.VALID_WORKSPACE_PATH, nested_workspace)  # copy over the nested workspace
    set_sample_path(top_level_workspace)  # set the path to the samples file in the expanded yaml of the top level workspace
    set_sample_path(nested_workspace)  # set the path to the samples file in the expanded yaml of the nested workspace
    return top_level_workspace
