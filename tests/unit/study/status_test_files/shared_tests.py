###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.10.1.
#
# For details, see https://github.com/LLNL/merlin.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################
"""
This module contains all shared tests needed for testing both
the Status object and the DetailedStatus object.
"""
from io import StringIO
from typing import Union
from unittest.mock import patch

from deepdiff import DeepDiff
from tabulate import tabulate

from merlin.display import ANSI_COLORS
from merlin.study.status import DetailedStatus, Status
from tests.unit.study.status_test_files import status_test_variables


def assert_correct_attribute_creation(status_obj: Union[Status, DetailedStatus]):
    """
    Ensure that attributes of the Status/DetailedStatus class are initiated correctly.
    This covers the get_steps_to_display, _create_step_tracker, spec.get_tasks_per_step,
    get_requested_statuses, get_step_statuses, and num_requested_statuses methods.

    :param `status_obj`: The Status or DetailedStatus object that we're testing
    """
    # Ensuring step_tracker was created properly
    step_tracker_diff = DeepDiff(status_obj.step_tracker, status_test_variables.FULL_STEP_TRACKER, ignore_order=True)
    assert step_tracker_diff == {}

    # Ensuring tasks_per_step was created properly
    tasks_per_step_diff = DeepDiff(status_obj.tasks_per_step, status_test_variables.TASKS_PER_STEP, ignore_order=True)
    assert tasks_per_step_diff == {}

    # Ensuring real_step_name_map was created properly
    real_step_name_map_diff = DeepDiff(
        status_obj.real_step_name_map, status_test_variables.REAL_STEP_NAME_MAP, ignore_order=True
    )
    assert real_step_name_map_diff == {}

    # Ensuring requested_statuses was created properly
    requested_statuses_diff = DeepDiff(
        status_obj.requested_statuses, status_test_variables.ALL_REQUESTED_STATUSES, ignore_order=True
    )
    assert requested_statuses_diff == {}

    # Ensuring num_requested_statuses is getting the correct amount of statuses
    assert status_obj.num_requested_statuses == status_test_variables.NUM_ALL_REQUESTED_STATUSES


def run_study_selector_prompt_valid_input(status_obj: Union[Status, DetailedStatus]):
    """
    This is testing the prompt that's displayed when multiple study output
    directories are found. We use a patch context manager to send the value "2" to the input
    when prompted. We'll also capture the output that's sent to stdout using StringIO
    and a patch context manager. The stdout will have a table like so be displayed:
        Index  Study Name
    -------  ---------------------------------
            1  status_test_study_20230713-000000
            2  status_test_study_20230717-162921
    and since we're sending the value "2" to input, the result from this should be the
    absolute path to the status_test_study_20230717-162921 directory.

    :prompt `status_obj`: The Status or DetailedStatus object that we're running this test for
    """
    # If we don't switch this to False then the prompt will never be displayed
    status_obj.args.no_prompts = False

    # When we call the _obtain_study_method, which will prompt the user to select a study,
    # the input message will be captured in mock_input
    with patch("builtins.input", side_effect=["2"]) as mock_input:
        # Setup variables for our captured_output and the messages we expect to see in the output
        captured_output = StringIO()
        potential_studies = [(1, status_test_variables.DUMMY_WORKSPACE), (2, status_test_variables.VALID_WORKSPACE)]
        tabulated_info = tabulate(potential_studies, headers=["Index", "Study Name"])
        studies_found_msg = f"Found 2 potential studies:\n{tabulated_info}"
        prompt_msg = "Which study would you like to view the status of? Use the index on the left: "

        # When we call the _obtain_study method, which will prompt the user to select a study,
        # the stdout will be captured in captured_output
        with patch("sys.stdout", new=captured_output):
            result = status_obj._obtain_study(status_test_variables.PATH_TO_TEST_FILES, 2, potential_studies)

    # We first check that the studies found message was in the captured output of the _obtain_study call
    assert studies_found_msg in captured_output.getvalue()
    # We then check that the input prompt was given one time with the provided prompt
    mock_input.assert_called_once_with(prompt_msg)
    # Finally, we check that the _obtain_study method returned the correct workspace
    assert result == status_test_variables.VALID_WORKSPACE_PATH


def run_study_selector_prompt_invalid_input(status_obj: Union[Status, DetailedStatus]):
    """
    This test is very similar to the run_study_selector_prompt_valid_input test above but here
    we're testing for invalid inputs before a valid input is given. Invalid inputs for this test
    are any inputs that are not the integers 1 or 2. We'll use a patch context manager to send all
    of our invalid inputs in until the last one which will be valid (in order to get the function
    to return).

    :prompt `status_obj`: The Status or DetailedStatus object that we're running this test for
    """
    # If we don't switch this to False then the prompt will never be displayed
    status_obj.args.no_prompts = False

    with patch("builtins.input", side_effect=["0", "-1", "3", "a", "1.5", "2"]) as mock_input:
        # Setup variables for our captured_output and the messages we expect to see in the output
        captured_output = StringIO()
        potential_studies = [(1, status_test_variables.DUMMY_WORKSPACE), (2, status_test_variables.VALID_WORKSPACE)]
        tabulated_info = tabulate(potential_studies, headers=["Index", "Study Name"])
        studies_found_msg = f"Found 2 potential studies:\n{tabulated_info}"
        invalid_msg = f"{ANSI_COLORS['RED']}Input must be an integer between 1 and 2.{ANSI_COLORS['RESET']}"
        invalid_prompt_msg = "Enter a different index: "

        # When we call the _obtain_study method, which will prompt the user to select a study,
        # the stdout will be captured in captured_output
        with patch("sys.stdout", new=captured_output):
            result = status_obj._obtain_study(status_test_variables.PATH_TO_TEST_FILES, 2, potential_studies)

    # We first check that the studies found message was in the captured output of the _obtain_study call
    captured_output_value = captured_output.getvalue()
    assert studies_found_msg in captured_output_value

    # We then check that the input was called with the invalid prompt
    mock_input.assert_called_with(invalid_prompt_msg)

    # There should be 5 instances of the invalid input message here (from '0', '-1', '3', 'a', '1.5')
    count = 0
    while invalid_msg in captured_output_value:
        count += 1
        captured_output_value = captured_output_value.replace(invalid_msg, "", 1)
    assert count == 5

    # Finally, we check that the _obtain_study method returned the correct workspace
    assert result == status_test_variables.VALID_WORKSPACE_PATH
