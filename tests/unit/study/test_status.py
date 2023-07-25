"""
Tests for the Status class in the status.py module
"""
import csv
import json
import os
import unittest
from argparse import Namespace
from copy import deepcopy
from datetime import datetime
from time import sleep
from typing import Dict, List

from deepdiff import DeepDiff

from merlin.study.status import Status
from tests.unit.study.status_test_files import shared_tests, status_test_variables


class TestMerlinStatus(unittest.TestCase):
    """Test the logic for methods in the Status class."""

    def setUp(self):
        """
        We'll create an argparse namespace here that can be modified on a
        test-by-test basis.
        """
        # We'll set all of the args needed to create the DetailedStatus object here and then
        # just modify them on a test-by-test basis
        self.args = Namespace(
            subparsers="status",
            level="INFO",
            detailed=False,
            variables=None,
            task_server="celery",
            cb_help=False,
            dump=None,
            no_prompts=True,  # We'll set this to True here since it's easier to test this way
        )

    def test_spec_setup_nonexistent_file(self):
        """
        Test the creation of a Status object using a nonexistent spec file.
        This should not let us create the object and instead throw an error.
        """
        with self.assertRaises(ValueError):
            invalid_spec_path = f"{status_test_variables.PATH_TO_TEST_FILES}/nonexistent.yaml"
            _ = Status(args=self.args, spec_display=True, file_or_ws=invalid_spec_path)

    def test_spec_setup_no_prompts(self):
        """
        Test the creation of a Status object using a valid spec file with no
        prompts allowed. By default for this test class, no_prompts is True.
        This also tests that the attributes created upon initialization are
        correct. The methods covered here are _load_from_spec and _obtain_study,
        as well as any methods covered in assert_correct_attribute_creation
        """
        status_obj = Status(args=self.args, spec_display=True, file_or_ws=status_test_variables.SPEC_PATH)
        assert isinstance(status_obj, Status)

        shared_tests.assert_correct_attribute_creation(status_obj)

    def test_prompt_for_study_with_valid_input(self):
        """
        This is testing the prompt that's displayed when multiple study output
        directories are found. This tests the _obtain_study method using valid inputs.
        """
        # We're going to load in a status object without prompts first and then use that to call the method
        # that prompts the user for input
        status_obj = Status(args=self.args, spec_display=True, file_or_ws=status_test_variables.SPEC_PATH)
        shared_tests.run_study_selector_prompt_valid_input(status_obj)

    def test_prompt_for_study_with_invalid_input(self):
        """
        This is testing the prompt that's displayed when multiple study output
        directories are found. This tests the _obtain_study method using invalid inputs.
        """
        # We're going to load in a status object without prompts first and then use that to call the method
        # that prompts the user for input
        status_obj = Status(args=self.args, spec_display=True, file_or_ws=status_test_variables.SPEC_PATH)
        shared_tests.run_study_selector_prompt_invalid_input(status_obj)

    def test_workspace_setup_nonexistent_workspace(self):
        """
        Test the creation of a Status object using a nonexistent workspace directory.
        This should not let us create the object and instead throw an error.
        """
        # Testing non existent workspace (in reality main.py should deal with this for us but we'll check it just in case)
        with self.assertRaises(ValueError):
            invalid_workspace = f"{status_test_variables.PATH_TO_TEST_FILES}/nonexistent_20230101-000000/"
            _ = Status(args=self.args, spec_display=False, file_or_ws=invalid_workspace)

    def test_workspace_setup_not_a_merlin_directory(self):
        """
        Test the creation of a Status object using an existing directory that is NOT
        an output directory from a merlin study (i.e. the directory does not have a
        merlin_info/ subdirectory). This should not let us create the object and instead
        throw an error.
        """
        with self.assertRaises(ValueError):
            _ = Status(args=self.args, spec_display=False, file_or_ws=status_test_variables.DUMMY_WORKSPACE_PATH)

    def test_workspace_setup_valid_workspace(self):
        """
        Test the creation of a Status object using a valid workspace directory.
        This also tests that the attributes created upon initialization are
        correct. The _load_from_workspace method is covered here, as well as any
        methods covered in assert_correct_attribute_creation.
        """
        status_obj = Status(args=self.args, spec_display=False, file_or_ws=status_test_variables.VALID_WORKSPACE_PATH)
        assert isinstance(status_obj, Status)

        shared_tests.assert_correct_attribute_creation(status_obj)

    def test_json_formatter(self):
        """
        Test the json formatter for the dump method. This covers the format_json_dump method.
        """
        # Create a timestamp and the status object that we'll run tests on
        date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status_obj = Status(args=self.args, spec_display=False, file_or_ws=status_test_variables.VALID_WORKSPACE_PATH)

        # Test json formatter
        json_format_diff = DeepDiff(status_obj.format_json_dump(date), {date: status_test_variables.ALL_REQUESTED_STATUSES})
        self.assertEqual(json_format_diff, {})

    def test_csv_formatter(self):
        """
        Test the csv formatter for the dump method. This covers the format_csv_dump method.
        """
        # Create a timestamp and the status object that we'll run tests on
        date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status_obj = Status(args=self.args, spec_display=False, file_or_ws=status_test_variables.VALID_WORKSPACE_PATH)

        # Test csv formatter
        correct_csv_format = {"Time of Status": [date] * len(status_test_variables.ALL_FORMATTED_STATUSES["Step Name"])}
        correct_csv_format.update(status_test_variables.ALL_FORMATTED_STATUSES)
        csv_format_diff = DeepDiff(status_obj.format_csv_dump(date), correct_csv_format)
        self.assertEqual(csv_format_diff, {})

    def test_json_dump(self):
        """
        Test the json dump functionality. This tests both the write and append
        dump functionalities. The file needs to exist already for an append so it's
        better to keep these tests together. This covers the dump method.
        """
        # Create the status object that we'll run tests on
        status_obj = Status(args=self.args, spec_display=False, file_or_ws=status_test_variables.VALID_WORKSPACE_PATH)
        # Set the dump file
        json_dump_file = f"{status_test_variables.PATH_TO_TEST_FILES}/dump_test.json"
        status_obj.args.dump = json_dump_file

        # Test write dump functionality for json
        status_obj.dump()
        with open(json_dump_file, "r") as json_df:
            json_df_contents = json.load(json_df)
        # There should only be one entry in the json dump file so this will only 'loop' once
        for dump_entry in json_df_contents.values():
            json_dump_diff = DeepDiff(dump_entry, status_test_variables.ALL_REQUESTED_STATUSES)
            self.assertEqual(json_dump_diff, {})

        # Test append dump functionality for json
        # If we don't sleep for 1 second here the program will run too fast and the timestamp for the append dump will be the same
        # as the timestamp for the write dump, which causes the write dump entry to be overridden
        sleep(1)
        # Here, the file already exists from the previous test so it will automatically append to the file
        status_obj.dump()
        with open(json_dump_file, "r") as json_df:
            json_df_append_contents = json.load(json_df)
        # There should be two entries here now, both with the same statuses just different timestamps
        assert len(json_df_append_contents) == 2
        for dump_entry in json_df_append_contents.values():
            json_append_dump_diff = DeepDiff(dump_entry, status_test_variables.ALL_REQUESTED_STATUSES)
            self.assertEqual(json_append_dump_diff, {})

        # The tests are done now so we can remove the dump file
        os.remove(json_dump_file)

    def _format_csv_data(self, csv_dump_data: csv.DictReader) -> Dict[str, List[str]]:
        """
        Helper method for testing the csv dump functionality to format csv data read in
        from the dump file.

        :param `csv_dump_data`: The DictReader object that has the csv data from the dump file
        :returns: A formatted dict where keys are fieldnames of the csv file and values are the columns for each field
        """
        # Create a formatted dict to store the csv data in csv_dump_data
        csv_dump_output = {field_name: [] for field_name in csv_dump_data.fieldnames}
        for row in csv_dump_data:
            for key, val in row.items():
                # TODO when we add entries for restart we'll need to change this
                if key == "Restarts":
                    csv_dump_output[key].append(int(val))
                else:
                    csv_dump_output[key].append(val)
        return csv_dump_output

    def test_csv_dump(self):
        """
        Test the csv dump functionality. This tests both the write and append
        dump functionalities. The file needs to exist already for an append so it's
        better to keep these tests together. This covers the format_status_for_display
        and dump methods.
        """
        # Create the status object that we'll run tests on
        status_obj = Status(args=self.args, spec_display=False, file_or_ws=status_test_variables.VALID_WORKSPACE_PATH)

        # Set the dump file
        csv_dump_file = f"{status_test_variables.PATH_TO_TEST_FILES}/dump_test.csv"
        status_obj.args.dump = csv_dump_file

        # Test write dump functionality for csv
        status_obj.dump()
        with open(csv_dump_file, "r") as csv_df:
            csv_dump_data = csv.DictReader(csv_df)
            # Make sure a timestamp field was created
            assert "Time of Status" in csv_dump_data.fieldnames

            # Format the csv data that we just read in and create a set of timestamps
            csv_dump_output = self._format_csv_data(csv_dump_data)
            timestamps = set(csv_dump_output["Time of Status"])

            # We don't care if the timestamp matches, we only care that there should be exactly one timestamp here
            del csv_dump_output["Time of Status"]
            assert len(timestamps) == 1

            # Check for differences (should be none)
            csv_dump_diff = DeepDiff(csv_dump_output, status_test_variables.ALL_FORMATTED_STATUSES)
            self.assertEqual(csv_dump_diff, {})

        # Test append dump functionality for csv
        # If we don't sleep for 1 second here the program will run too fast and the timestamp for the append dump will be the same
        # as the timestamp for the write dump, which makes it impossible to differentiate between different dump calls
        sleep(1)
        # Here, the file already exists from the previous test so it will automatically append to the file
        status_obj.dump()
        with open(csv_dump_file, "r") as csv_df:
            csv_append_dump_data = csv.DictReader(csv_df)
            # Make sure a timestamp field still exists
            assert "Time of Status" in csv_append_dump_data.fieldnames

            # Format the csv data that we just read in and create a set of timestamps
            csv_append_dump_output = self._format_csv_data(csv_append_dump_data)
            timestamps = set(csv_append_dump_output["Time of Status"])

            # We don't care if the timestamp matches, we only care that there should be exactly two timestamps here now
            del csv_append_dump_output["Time of Status"]
            assert len(timestamps) == 2

            # Since there are two dumps, we need to double up the formatted statuses too
            appended_formatted_statuses = deepcopy(status_test_variables.ALL_FORMATTED_STATUSES)
            for key, val in status_test_variables.ALL_FORMATTED_STATUSES.items():
                appended_formatted_statuses[key].extend(val)

            csv_append_dump_diff = DeepDiff(csv_append_dump_output, appended_formatted_statuses)
            self.assertEqual(csv_append_dump_diff, {})

        # The tests are done now so we can remove the dump file
        os.remove(csv_dump_file)

    def test_display(self):
        """
        Test the status display functionality without actually displaying anything.
        Running the display in test_mode will just provide us with the state_info
        dict created for each step that is typically used for display. We'll ensure
        this state_info dict is created properly here. This covers the display method.
        """
        # Create the status object that we'll run tests on
        status_obj = Status(args=self.args, spec_display=False, file_or_ws=status_test_variables.VALID_WORKSPACE_PATH)

        # Get the status info that display would use if it were printing output
        all_status_info = status_obj.display(test_mode=True)

        # Check the information for each step
        for step_name, state_info in all_status_info.items():
            # If state_info is a dict then the step should be started; if it's a string then it's unstarted
            if isinstance(state_info, dict):
                assert step_name in status_test_variables.FULL_STEP_TRACKER["started_steps"]
            elif isinstance(state_info, str):
                assert step_name in status_test_variables.FULL_STEP_TRACKER["unstarted_steps"]

            # Make sure all the state info dicts for each step match what they should be
            state_info_diff = DeepDiff(state_info, status_test_variables.DISPLAY_INFO[step_name])
            self.assertEqual(state_info_diff, {})


if __name__ == "__main__":
    unittest.main()
