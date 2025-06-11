##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module contains all shared tests needed for testing both
the Status object and the DetailedStatus object.
"""
import csv
import json
import os
from io import StringIO
from time import sleep
from typing import Dict, List, Tuple, Union
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

    # Ensuring requested_statuses was created properly
    requested_statuses_diff = DeepDiff(
        status_obj.requested_statuses, status_test_variables.ALL_REQUESTED_STATUSES, ignore_order=True
    )
    assert requested_statuses_diff == {}

    # Ensuring run time info was calculated correctly
    run_time_info_diff = DeepDiff(status_obj.run_time_info, status_test_variables.RUN_TIME_INFO, ignore_order=True)
    assert run_time_info_diff == {}

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


def run_json_dump_test(status_obj: Union[Status, DetailedStatus], expected_output: Dict):
    """
    Test the json dump functionality. This tests both the write and append
    dump functionalities. The file needs to exist already for an append so it's
    better to keep these tests together. This covers the dump method.

    :param `status_obj`: A Status or DetailedStatus object that we're testing the dump functionality for
    :param `expected_output`: The expected output from the dump that we'll compare against
    """
    try:
        # Test write dump functionality for json
        status_obj.dump()
        with open(status_obj.args.dump, "r") as json_df:
            json_df_contents = json.load(json_df)
        # There should only be one entry in the json dump file so this will only 'loop' once
        for dump_entry in json_df_contents.values():
            json_dump_diff = DeepDiff(dump_entry, expected_output)
            assert json_dump_diff == {}

        # Test append dump functionality for json
        # If we don't sleep for 1 second here the program will run too fast and the timestamp for the append dump will be the same
        # as the timestamp for the write dump, which causes the write dump entry to be overridden
        sleep(1)
        # Here, the file already exists from the previous test so it will automatically append to the file
        status_obj.dump()
        with open(status_obj.args.dump, "r") as json_df:
            json_df_append_contents = json.load(json_df)
        # There should be two entries here now, both with the same statuses just different timestamps
        assert len(json_df_append_contents) == 2
        for dump_entry in json_df_append_contents.values():
            json_append_dump_diff = DeepDiff(dump_entry, expected_output)
            assert json_append_dump_diff == {}
    # Make sure we always remove the test file that's created from this dump
    finally:
        try:
            os.remove(status_obj.args.dump)
        except FileNotFoundError:
            pass


def _format_csv_data(csv_dump_data: csv.DictReader) -> Dict[str, List[str]]:
    """
    Helper function for testing the csv dump functionality to format csv data read in
    from the dump file.

    :param `csv_dump_data`: The DictReader object that has the csv data from the dump file
    :returns: A formatted dict where keys are fieldnames of the csv file and values are the columns for each field
    """
    # Create a formatted dict to store the csv data in csv_dump_data
    csv_dump_output = {field_name: [] for field_name in csv_dump_data.fieldnames}
    for row in csv_dump_data:
        for key, val in row.items():
            # TODO when we add entries for restarts we'll need to change this
            if key == "restarts":
                csv_dump_output[key].append(int(val))
            else:
                csv_dump_output[key].append(val)
    return csv_dump_output


def build_row_list(csv_formatted_dict: Dict[str, List[Union[str, int]]]) -> List[Tuple]:
    """
    Given a dict where each key/val pair represents column label/column values, create a
    list of rows. For example:
    input: {"a": [1, 2, 3], "b": [4, 5, 6]}
    output: [("a", "b"), (1, 4), (2, 5), (3, 6)]

    :param `csv_formatted_dict`: The dict of csv columns that we're converting to a list of rows
    :returns: A list of rows created from the `csv_formatted_dict`
    """
    column_labels = tuple(csv_formatted_dict.keys())
    row_list = list(zip(*csv_formatted_dict.values()))
    row_list.insert(0, column_labels)
    return row_list


def run_csv_dump_test(status_obj: Union[Status, DetailedStatus], expected_output: List[Tuple]):
    """
    Test the csv dump functionality. This tests both the write and append
    dump functionalities. The file needs to exist already for an append so it's
    better to keep these tests together. This covers the format_status_for_csv
    and dump methods.

    :param `status_obj`: A Status or DetailedStatus object that we're testing the dump functionality for
    :param `expected_output`: The expected output from the dump that we'll compare against
    """
    try:
        # Test write dump functionality for csv
        status_obj.dump()
        with open(status_obj.args.dump, "r") as csv_df:
            csv_dump_data = csv.DictReader(csv_df)
            # Make sure a timestamp field was created
            assert "time_of_status" in csv_dump_data.fieldnames

            # Format the csv data that we just read in and create a set of timestamps
            csv_dump_output = _format_csv_data(csv_dump_data)
            timestamps = set(csv_dump_output["time_of_status"])

            # We don't care if the timestamp matches, we only care that there should be exactly one timestamp here
            del csv_dump_output["time_of_status"]
            assert len(timestamps) == 1

            # Check for differences (should be none)
            csv_dump_output = build_row_list(csv_dump_output)
            csv_dump_diff = DeepDiff(csv_dump_output, expected_output, ignore_order=True)
            assert csv_dump_diff == {}

        # Test append dump functionality for csv
        # If we don't sleep for 1 second here the program will run too fast and the timestamp for the append dump will be the same
        # as the timestamp for the write dump, which makes it impossible to differentiate between different dump calls
        sleep(1)
        # Here, the file already exists from the previous test so it will automatically append to the file
        status_obj.dump()
        with open(status_obj.args.dump, "r") as csv_df:
            csv_append_dump_data = csv.DictReader(csv_df)
            # Make sure a timestamp field still exists
            assert "time_of_status" in csv_append_dump_data.fieldnames

            # Format the csv data that we just read in and create a set of timestamps
            csv_append_dump_output = _format_csv_data(csv_append_dump_data)
            timestamps = set(csv_append_dump_output["time_of_status"])

            # We don't care if the timestamp matches, we only care that there should be exactly two timestamps here now
            del csv_append_dump_output["time_of_status"]
            assert len(timestamps) == 2

            # Since there are now two dumps, we need to double up the expected output too (except for the keys)
            expected_keys = expected_output[0]
            expected_output.remove(expected_keys)
            expected_output.extend(expected_output)
            expected_output.insert(0, expected_keys)

            # Check for differences (should be none)
            csv_append_dump_output = build_row_list(csv_append_dump_output)
            csv_append_dump_diff = DeepDiff(csv_append_dump_output, expected_output, ignore_order=True, report_repetition=True)
            assert csv_append_dump_diff == {}
    # Make sure we always remove the test file that's created from this dump
    finally:
        try:
            os.remove(status_obj.args.dump)
        except FileNotFoundError:
            pass
