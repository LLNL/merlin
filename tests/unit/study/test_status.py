###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.10.2.
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
Tests for the Status class in the status.py module
"""
import unittest
from argparse import Namespace
from copy import deepcopy
from datetime import datetime

import yaml
from deepdiff import DeepDiff

from merlin.main import get_merlin_spec_with_override
from merlin.study.status import Status
from tests.unit.study.status_test_files import shared_tests, status_test_variables


class TestMerlinStatus(unittest.TestCase):
    """Test the logic for methods in the Status class."""

    @classmethod
    def setUpClass(cls):
        """
        We need to modify the path to the samples file in the expanded spec for these tests.
        This will only happen once when these tests are initialized.
        """
        # Read in the contents of the expanded spec
        with open(status_test_variables.EXPANDED_SPEC_PATH, "r") as expanded_file:
            cls.initial_expanded_contents = yaml.load(expanded_file, yaml.Loader)

        # Create a copy of the contents so we can reset the file when these tests are done
        modified_contents = deepcopy(cls.initial_expanded_contents)

        # Modify the samples file path
        modified_contents["merlin"]["samples"]["file"] = status_test_variables.SAMPLES_PATH

        # Write the new contents to the expanded spec
        with open(status_test_variables.EXPANDED_SPEC_PATH, "w") as expanded_file:
            yaml.dump(modified_contents, expanded_file)

    @classmethod
    def tearDownClass(cls):
        """
        When these tests are done we'll reset the contents of the expanded spec path
        to their initial states.
        """
        with open(status_test_variables.EXPANDED_SPEC_PATH, "w") as expanded_file:
            yaml.dump(cls.initial_expanded_contents, expanded_file)

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
            self.args.specification = invalid_spec_path
            self.args.spec_provided, _ = get_merlin_spec_with_override(self.args)
            _ = Status(args=self.args, spec_display=True, file_or_ws=invalid_spec_path)

    def test_spec_setup_no_prompts(self):
        """
        Test the creation of a Status object using a valid spec file with no
        prompts allowed. By default for this test class, no_prompts is True.
        This also tests that the attributes created upon initialization are
        correct. The methods covered here are _load_from_spec and _obtain_study,
        as well as any methods covered in assert_correct_attribute_creation
        """
        self.args.specification = status_test_variables.SPEC_PATH
        self.args.spec_provided, _ = get_merlin_spec_with_override(self.args)
        status_obj = Status(args=self.args, spec_display=True, file_or_ws=status_test_variables.SPEC_PATH)
        assert isinstance(status_obj, Status)

        shared_tests.assert_correct_attribute_creation(status_obj)

    def test_prompt_for_study_with_valid_input(self):
        """
        This is testing the prompt that's displayed when multiple study output
        directories are found. This tests the _obtain_study method using valid inputs.
        """
        # We need to load in the MerlinSpec object and save it to the args we'll give to Status
        self.args.specification = status_test_variables.SPEC_PATH
        self.args.spec_provided, _ = get_merlin_spec_with_override(self.args)

        # We're going to load in a status object without prompts first and then use that to call the method
        # that prompts the user for input
        status_obj = Status(args=self.args, spec_display=True, file_or_ws=status_test_variables.SPEC_PATH)
        shared_tests.run_study_selector_prompt_valid_input(status_obj)

    def test_prompt_for_study_with_invalid_input(self):
        """
        This is testing the prompt that's displayed when multiple study output
        directories are found. This tests the _obtain_study method using invalid inputs.
        """
        # We need to load in the MerlinSpec object and save it to the args we'll give to Status
        self.args.specification = status_test_variables.SPEC_PATH
        self.args.spec_provided, _ = get_merlin_spec_with_override(self.args)

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

        # Build the correct format and store each row in a list (so we can ignore the order)
        correct_csv_format = {"time_of_status": [date] * len(status_test_variables.ALL_FORMATTED_STATUSES["step_name"])}
        correct_csv_format.update(status_test_variables.ALL_FORMATTED_STATUSES)
        correct_csv_format = shared_tests.build_row_list(correct_csv_format)

        # Run the csv_formatter and store each row it creates in a list
        actual_csv_format = shared_tests.build_row_list(status_obj.format_csv_dump(date))

        # Compare differences (should be none)
        csv_format_diff = DeepDiff(actual_csv_format, correct_csv_format, ignore_order=True)
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

        # Run the json dump test
        shared_tests.run_json_dump_test(status_obj, status_test_variables.ALL_REQUESTED_STATUSES)

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

        # Run the csv dump test
        expected_output = shared_tests.build_row_list(status_test_variables.ALL_FORMATTED_STATUSES)
        shared_tests.run_csv_dump_test(status_obj, expected_output)

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
