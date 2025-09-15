##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Fixtures specifically for help testing the modules in the examples/ directory.
"""

import pytest

from tests.fixture_types import FixtureCallable, FixtureStr


@pytest.fixture(scope="session")
def examples_testing_dir(create_testing_dir: FixtureCallable, temp_output_dir: FixtureStr) -> FixtureStr:
    """
    Fixture to create a temporary output directory for tests related to the examples functionality.

    Args:
        create_testing_dir: A fixture which returns a function that creates the testing directory.
        temp_output_dir: The path to the temporary output directory we'll be using for this test run.

    Returns:
        The path to the temporary testing directory for examples tests.
    """
    return create_testing_dir(temp_output_dir, "examples_testing")
