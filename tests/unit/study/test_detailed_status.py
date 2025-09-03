##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the DetailedStatus class in the status.py module
"""
import re
import unittest
from argparse import Namespace
from copy import deepcopy
from io import StringIO
from typing import Dict, List
from unittest.mock import MagicMock, call, patch

import yaml
from deepdiff import DeepDiff

from merlin.spec.expansion import get_spec_with_expansion
from merlin.study.status import DetailedStatus
from tests.unit.study.status_test_files import shared_tests, status_test_variables


class TestBaseDetailedStatus(unittest.TestCase):
    """
    Base class for all detailed status tests to provide the setup configuration.
    """

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
            subparsers="detailed-status",
            level="INFO",
            detailed=True,
            output_path=None,
            task_server="celery",
            dump=None,
            no_prompts=True,  # We'll set this to True here since it's easier to test this way (in most cases)
            max_tasks=None,
            return_code=None,
            steps=["all"],
            task_queues=None,
            task_status=None,
            workers=None,
            disable_pager=True,  # We'll set this to True here since it's easier to test this way
            disable_theme=False,
            layout="default",
        )

        # Create the DetailedStatus object without adding any arguments
        # We'll modify the arguments on a test-by-test basis
        self.detailed_status_obj = DetailedStatus(
            args=self.args, spec_display=False, file_or_ws=status_test_variables.VALID_WORKSPACE_PATH
        )


class TestSetup(TestBaseDetailedStatus):
    """
    This tests the setup of the DetailedStatus class.
    """

    def test_workspace_setup(self):
        """
        Test the setup of the DetailedStatus class using a workspace as input. This should have the same
        behavior as setting up the Status class but will hold additional args. Here the DetailedStatus
        instance is created in setUp but since it doesn't use any filters, we can just make sure all
        of the attributes were initiated correctly.
        """
        # Ensure the attributes shared with the Status class that are created upon initialization are correct
        shared_tests.assert_correct_attribute_creation(self.detailed_status_obj)

        # The steps arg is expanded from ["all"] to a list of every step name upon class creation
        self.assertEqual(
            self.detailed_status_obj.args.steps,
            ["just_samples", "just_parameters", "params_and_samples", "fail_step", "cancel_step", "unstarted_step"],
        )

        # We didn't give the steps filter arg so this should be False
        self.assertEqual(self.detailed_status_obj.steps_filter_provided, False)

    def test_spec_setup(self):
        """
        Test the setup of the DetailedStatus class using a spec file as input. This should have the same
        behavior as setting up the Status class but will hold additional args.
        """
        # We have to reset this to be all since it will have already been expanded due to the setUp method from
        # the base class
        self.args.steps = ["all"]

        # We need to load in the MerlinSpec object and save it to the args we'll give to DetailedStatus
        self.args.specification = status_test_variables.SPEC_PATH
        self.args.spec_provided = get_spec_with_expansion(self.args.specification)

        # Create the new object using a specification rather than a workspace
        detailed_status_obj = DetailedStatus(args=self.args, spec_display=True, file_or_ws=status_test_variables.SPEC_PATH)

        # Ensure the attributes shared with the Status class that are created upon initialization are correct
        shared_tests.assert_correct_attribute_creation(detailed_status_obj)

        # The steps arg is expanded from ["all"] to a list of every step name upon class creation
        self.assertEqual(
            detailed_status_obj.args.steps,
            ["just_samples", "just_parameters", "params_and_samples", "fail_step", "cancel_step", "unstarted_step"],
        )

        # We didn't give the steps filter arg so this should be False
        self.assertEqual(detailed_status_obj.steps_filter_provided, False)


class TestDumpFunctionality(TestBaseDetailedStatus):
    """
    This class will test the dump functionality for dumping detailed-status
    to csv and json files. It will run the same test as we run for the Status
    command and it will also run dump tests with some filters applied.
    """

    def test_json_dump(self):
        """
        Test the json dump functionality. This tests both the write and append
        dump functionalities. The file needs to exist already for an append so it's
        better to keep these tests together.

        This will test a json dump using the detailed-status command without applying
        any filters.
        """
        # Set the dump file
        json_dump_file = f"{status_test_variables.PATH_TO_TEST_FILES}/detailed_dump_test.json"
        self.detailed_status_obj.args.dump = json_dump_file

        # Run the json dump test
        shared_tests.run_json_dump_test(self.detailed_status_obj, status_test_variables.ALL_REQUESTED_STATUSES)

    def test_csv_dump(self):
        """
        Test the csv dump functionality. This tests both the write and append
        dump functionalities. The file needs to exist already for an append so it's
        better to keep these tests together.

        This will test a csv dump using the detailed-status command without applying
        any filters.
        """
        # Set the dump file
        csv_dump_file = f"{status_test_variables.PATH_TO_TEST_FILES}/detailed_dump_test.csv"
        self.detailed_status_obj.args.dump = csv_dump_file

        # Run the csv dump test
        expected_output = shared_tests.build_row_list(status_test_variables.ALL_FORMATTED_STATUSES)
        shared_tests.run_csv_dump_test(self.detailed_status_obj, expected_output)

    def test_json_dump_with_filters(self):
        """
        Test the json dump functionality while using filters. This tests both the write and append
        dump functionalities. The file needs to exist already for an append so it's
        better to keep these tests together.
        """
        # Need to create a new DetailedStatus object so that filters are loaded from the beginning
        args = Namespace(
            subparsers="detailed-status",
            level="INFO",
            detailed=True,
            output_path=None,
            task_server="celery",
            dump=f"{status_test_variables.PATH_TO_TEST_FILES}/detailed_dump_test.json",  # Set the dump file
            no_prompts=True,
            max_tasks=None,
            return_code=None,
            steps=["all"],
            task_queues=None,
            task_status=["FAILED", "CANCELLED"],  # Set filters for failed and cancelled tasks
            workers=None,
            disable_pager=True,
            disable_theme=False,
            layout="default",
        )
        detailed_status_obj = DetailedStatus(
            args=args, spec_display=False, file_or_ws=status_test_variables.VALID_WORKSPACE_PATH
        )

        # Run the json dump test (we should only get failed and cancelled statuses)
        shared_tests.run_json_dump_test(detailed_status_obj, status_test_variables.REQUESTED_STATUSES_FAIL_AND_CANCEL)

    def test_csv_dump_with_filters(self):
        """
        Test the csv dump functionality while using filters. This tests both the write and append
        dump functionalities. The file needs to exist already for an append so it's
        better to keep these tests together.
        """
        # Need to create a new DetailedStatus object so that filters are loaded from the beginning
        args = Namespace(
            subparsers="detailed-status",
            level="INFO",
            detailed=True,
            output_path=None,
            task_server="celery",
            dump=f"{status_test_variables.PATH_TO_TEST_FILES}/detailed_dump_test.csv",  # Set the dump file
            no_prompts=True,
            max_tasks=None,
            return_code=None,
            steps=["all"],
            task_queues=None,
            task_status=["FAILED", "CANCELLED"],  # Set filters for failed and cancelled tasks
            workers=None,
            disable_pager=True,
            disable_theme=False,
            layout="default",
        )
        detailed_status_obj = DetailedStatus(
            args=args, spec_display=False, file_or_ws=status_test_variables.VALID_WORKSPACE_PATH
        )

        # Run the csv dump test (we should only get failed and cancelled statuses)
        expected_output = shared_tests.build_row_list(status_test_variables.FORMATTED_STATUSES_FAIL_AND_CANCEL)
        shared_tests.run_csv_dump_test(detailed_status_obj, expected_output)


class TestPromptFunctionality(TestBaseDetailedStatus):
    """
    This class is strictly for testing that all prompt functionality that's
    possible through the DetailedStatus class is running correctly.

    The types of prompts are:
    - prompts for selecting a study to view the status of (similar to Status class)
    - prompts for filtering statuses further when using the disable-pager option

    This class will test 5 methods:
    - _obtain_study (similar to Status class)
    - display and, by association, filter_via_prompts
    - get_user_filters
    - get_user_max_tasks
    """

    ###############################################
    # Testing _obtain_study()
    ###############################################

    def test_prompt_for_study_with_valid_input(self):
        """
        This is testing the prompt that's displayed when multiple study output
        directories are found. This tests the _obtain_study method with valid input.
        """
        # We need to load in the MerlinSpec object and save it to the args we'll give to DetailedStatus
        self.args.specification = status_test_variables.SPEC_PATH
        self.args.spec_provided = get_spec_with_expansion(self.args.specification)

        # We're going to load in a status object without prompts first and then use that to call the method
        # that prompts the user for input
        status_obj = DetailedStatus(args=self.args, spec_display=True, file_or_ws=status_test_variables.SPEC_PATH)
        shared_tests.run_study_selector_prompt_valid_input(status_obj)

    def test_prompt_for_study_with_invalid_input(self):
        """
        This is testing the prompt that's displayed when multiple study output
        directories are found. This tests the _obtain_study method with invalid inputs.
        """
        # We need to load in the MerlinSpec object and save it to the args we'll give to DetailedStatus
        self.args.specification = status_test_variables.SPEC_PATH
        self.args.spec_provided = get_spec_with_expansion(self.args.specification)

        # We're going to load in a status object without prompts first and then use that to call the method
        # that prompts the user for input
        status_obj = DetailedStatus(args=self.args, spec_display=True, file_or_ws=status_test_variables.SPEC_PATH)
        shared_tests.run_study_selector_prompt_invalid_input(status_obj)

    ###############################################
    # Testing get_user_filters()
    ###############################################

    def run_mock_input_with_filters(self, input_to_test: str, expected_return: bool, max_tasks: str = None):
        """
        This will pass in `input_to_test` (and `max_tasks` if set) as input to the prompt
        that's displayed by the `get_user_filters` method. This function will then compare
        the expected return vs the actual return value.

        Not explicitly shown in this function is the side effect that `get_user_filters`
        will modify one or more of `self.args.task_status`, `self.args.return_code`,
        `self.args.workers`, and/or `self.args.max_tasks`. The values set for these will
        be compared in the test method that calls this method.

        :param input_to_test: A string to pass into the input prompt raised by `get_user_filters`
        :param expected_return: The expected return value of the `get_user_filters` call
        :param max_tasks: A string (really an int) to pass into the second input prompt raised
                          by `get_user_filters` when `MAX_TASKS` is requested
        """
        # Add max_tasks entry to our side effect if necessary (this is what's passed as input)
        side_effect = [input_to_test]
        if max_tasks is not None:
            side_effect.append(max_tasks)

        # Redirect the input prompt to be stored in mock_input and not displayed in stdout
        with patch("builtins.input", side_effect=side_effect) as mock_input:
            # We use patch here to keep stdout from get_user_filters from being displayed
            with patch("sys.stdout"):
                # Run the method we're testing and capture the result
                result = self.detailed_status_obj.get_user_filters()

            calls = [call("How would you like to filter the tasks? ")]
            if max_tasks is not None:
                calls.append(call("What limit would you like to set? (must be an integer greater than 0) "))

            # Make sure the prompt is called with the initial prompt message
            # mock_input.assert_called_with("How would you like to filter the tasks? ")
            mock_input.assert_has_calls(calls)

            self.assertEqual(result, expected_return)

    def run_invalid_get_user_filters_test(self, inputs_to_test: List[str]):
        """
        This will pass every input in `inputs_to_test` to the get_user_filters
        method. All of the inputs in `inputs_to_test` should be invalid except
        for the final one. We'll capture the output from stdout and look to make
        sure the correct number of "invalid input" messages were displayed.

        :param `inputs_to_test`: A list of invalid inputs (except for the last input)
                                 to give to the prompt displayed in get_user_filters
        """
        # Create a variable to store the captured stdout
        captured_output = StringIO()

        # Redirect the input prompt to be stored in mock_input and not displayed in stdout
        with patch("builtins.input", side_effect=inputs_to_test) as mock_input:
            # We use patch here to keep stdout from get_user_filters from being displayed
            with patch("sys.stdout", new=captured_output):
                # Run the method we're testing (it won't return anything until we hit the valid
                # exit filter so we don't save the result)
                _ = self.detailed_status_obj.get_user_filters()

            # Make sure the prompt is called with the initial prompt message
            mock_input.assert_called_with("How would you like to filter the tasks? ")

        # Find all occurrences of the invalid messages
        all_invalid_msgs = re.findall(r"Invalid input: .*\. Input must be one of the following", captured_output.getvalue())

        # The last input to test will be valid (so this test can exit properly) so we have
        # to account for that when we check how many invalid msgs we got in our output
        self.assertEqual(len(all_invalid_msgs), len(inputs_to_test) - 1)

    def reset_filters(self):
        """
        Reset the filters so they can be set again from a starting stage.
        """
        self.detailed_status_obj.args.task_status = None
        self.detailed_status_obj.args.return_code = None
        self.detailed_status_obj.args.workers = None
        self.detailed_status_obj.args.max_tasks = None

    def test_get_user_filters_exit(self):
        """
        This will test the exit input to the get_user_filters method.
        """
        inputs_to_test = ["E", "EXIT", "E, EXIT"]
        for input_to_test in inputs_to_test:
            self.run_mock_input_with_filters(input_to_test, True)  # The return should be true so we know to exit
            self.assertEqual(self.detailed_status_obj.args.task_status, None)
            self.assertEqual(self.detailed_status_obj.args.return_code, None)
            self.assertEqual(self.detailed_status_obj.args.workers, None)
            self.assertEqual(self.detailed_status_obj.args.max_tasks, None)

            self.reset_filters()

    def test_get_user_filters_task_status(self):
        """
        This will test the task status input to the get_user_filters method.
        """
        inputs_to_test = ["FAILED", "CANCELLED", "FAILED, CANCELLED"]
        expected_outputs = [["FAILED"], ["CANCELLED"], ["FAILED", "CANCELLED"]]
        for input_to_test, expected_output in zip(inputs_to_test, expected_outputs):
            self.run_mock_input_with_filters(input_to_test, False)
            self.assertEqual(self.detailed_status_obj.args.task_status, expected_output)
            self.assertEqual(self.detailed_status_obj.args.return_code, None)
            self.assertEqual(self.detailed_status_obj.args.workers, None)
            self.assertEqual(self.detailed_status_obj.args.max_tasks, None)

            self.reset_filters()

    def test_get_user_filters_return_codes(self):
        """
        This will test the return codes input to the get_user_filters method.
        """
        inputs_to_test = ["SOFT_FAIL", "STOP_WORKERS", "SOFT_FAIL, STOP_WORKERS"]
        expected_outputs = [["SOFT_FAIL"], ["STOP_WORKERS"], ["SOFT_FAIL", "STOP_WORKERS"]]
        for input_to_test, expected_output in zip(inputs_to_test, expected_outputs):
            self.run_mock_input_with_filters(input_to_test, False)
            self.assertEqual(self.detailed_status_obj.args.task_status, None)
            self.assertEqual(self.detailed_status_obj.args.return_code, expected_output)
            self.assertEqual(self.detailed_status_obj.args.workers, None)
            self.assertEqual(self.detailed_status_obj.args.max_tasks, None)

            self.reset_filters()

    def test_get_user_filters_max_tasks(self):
        """
        This will test the max tasks input to the get_user_filters method.
        """
        inputs_to_test = ["MAX_TASKS"]
        max_tasks = 23
        for input_to_test in inputs_to_test:
            self.run_mock_input_with_filters(input_to_test, False, max_tasks=str(max_tasks))
            self.assertEqual(self.detailed_status_obj.args.task_status, None)
            self.assertEqual(self.detailed_status_obj.args.return_code, None)
            self.assertEqual(self.detailed_status_obj.args.workers, None)
            self.assertEqual(self.detailed_status_obj.args.max_tasks, max_tasks)

            self.reset_filters()

    def test_get_user_filters_status_and_return_code(self):
        """
        This will test a combination of the task status and return code filters as inputs
        to the get_user_filters method. The only args that should be set here are the task_status
        and return_code args.
        """
        filter1 = "CANCELLED"
        filter2 = "SOFT_FAIL"
        self.run_mock_input_with_filters(", ".join([filter1, filter2]), False)
        self.assertEqual(self.detailed_status_obj.args.task_status, [filter1])
        self.assertEqual(self.detailed_status_obj.args.return_code, [filter2])
        self.assertEqual(self.detailed_status_obj.args.workers, None)
        self.assertEqual(self.detailed_status_obj.args.max_tasks, None)

    def test_get_user_filters_status_and_workers(self):
        """
        This will test a combination of the task status and workers filters as inputs
        to the get_user_filters method. The only args that should be set here are the task_status
        and workers args.
        """
        filter1 = "CANCELLED"
        filter2 = "sample_worker"
        self.run_mock_input_with_filters(", ".join([filter1, filter2]), False)
        self.assertEqual(self.detailed_status_obj.args.task_status, [filter1])
        self.assertEqual(self.detailed_status_obj.args.return_code, None)
        self.assertEqual(self.detailed_status_obj.args.workers, [filter2])
        self.assertEqual(self.detailed_status_obj.args.max_tasks, None)

    def test_get_user_filters_status_and_max_tasks(self):
        """
        This will test a combination of the task status and max tasks filters as inputs
        to the get_user_filters method. The only args that should be set here are the task_status
        and max_tasks args.
        """
        filter1 = "FINISHED"
        filter2 = "MAX_TASKS"
        max_tasks = 4
        self.run_mock_input_with_filters(", ".join([filter1, filter2]), False, max_tasks=str(max_tasks))
        self.assertEqual(self.detailed_status_obj.args.task_status, [filter1])
        self.assertEqual(self.detailed_status_obj.args.return_code, None)
        self.assertEqual(self.detailed_status_obj.args.workers, None)
        self.assertEqual(self.detailed_status_obj.args.max_tasks, max_tasks)

    def test_get_user_filters_return_code_and_workers(self):
        """
        This will test a combination of the return code and workers filters as inputs
        to the get_user_filters method. The only args that should be set here are the return_code
        and workers args.
        """
        filter1 = "STOP_WORKERS"
        filter2 = "sample_worker"
        self.run_mock_input_with_filters(", ".join([filter1, filter2]), False)
        self.assertEqual(self.detailed_status_obj.args.task_status, None)
        self.assertEqual(self.detailed_status_obj.args.return_code, [filter1])
        self.assertEqual(self.detailed_status_obj.args.workers, [filter2])
        self.assertEqual(self.detailed_status_obj.args.max_tasks, None)

    def test_get_user_filters_return_code_and_max_tasks(self):
        """
        This will test a combination of the return code and max tasks filters as inputs
        to the get_user_filters method. The only args that should be set here are the return_code
        and max_tasks args.
        """
        filter1 = "RETRY"
        filter2 = "MAX_TASKS"
        max_tasks = 4
        self.run_mock_input_with_filters(", ".join([filter1, filter2]), False, max_tasks=str(max_tasks))
        self.assertEqual(self.detailed_status_obj.args.task_status, None)
        self.assertEqual(self.detailed_status_obj.args.return_code, [filter1])
        self.assertEqual(self.detailed_status_obj.args.workers, None)
        self.assertEqual(self.detailed_status_obj.args.max_tasks, max_tasks)

    def test_get_user_filters_workers_and_max_tasks(self):
        """
        This will test a combination of the workers and max tasks filters as inputs
        to the get_user_filters method. The only args that should be set here are the workers
        and max_tasks args.
        """
        filter1 = "sample_worker"
        filter2 = "MAX_TASKS"
        max_tasks = 4
        self.run_mock_input_with_filters(", ".join([filter1, filter2]), False, max_tasks=str(max_tasks))
        self.assertEqual(self.detailed_status_obj.args.task_status, None)
        self.assertEqual(self.detailed_status_obj.args.return_code, None)
        self.assertEqual(self.detailed_status_obj.args.workers, [filter1])
        self.assertEqual(self.detailed_status_obj.args.max_tasks, max_tasks)

    def test_get_user_filters_only_invalid_inputs(self):
        """
        This will test sending invalid inputs to the prompt that the get_user_filters
        method displays. The last input we send will be a valid exit input in order
        to get the test to exit in a clean manner.
        """
        inputs_to_test = [
            "MAX_TASK",  # test single invalid input
            "fail, cancel",  # test two invalid inputs together (should only raise one invalid input message)
            "",  # test empty input
            "FAILED CANCELLED",  # test two valid inputs not separated by comma
            "E",  # this one is valid and we'll use it to exit
        ]
        self.run_invalid_get_user_filters_test(inputs_to_test)

    def test_get_user_filters_invalid_with_valid_inputs(self):
        """
        This will test sending invalid inputs to the prompt that the get_user_filters
        method displays alongside valid inputs. The last input we send will be a valid
        exit input in order to get the test to exit in a clean manner.
        """
        inputs_to_test = [
            "MAX_TASKS, INVALID",  # test invalid input with max tasks
            "failed, invalid",  # test invalid input with task status
            "stop_workers, invalid",  # test invalid input with return code
            "SUCCESS, FAILED, INVALID, MAX_TASKS",  # test a combination of all filters with an invalid one
            "E",  # this one is valid and we'll use it to exit
        ]
        self.run_invalid_get_user_filters_test(inputs_to_test)

    ###############################################
    # Testing get_user_max_tasks()
    ###############################################

    # There are 19 tasks in total for the status tests. Here, 1 is an edge
    # case. Any positive number is valid (even a number greater than 19)
    @patch("builtins.input", side_effect=["1", "10", "20"])
    def test_get_user_max_tasks_valid_inputs(self, mock_input: MagicMock):
        """
        This will test sending valid inputs to the get_user_max_tasks method.

        :param `mock_input`: A MagicMock object to send inputs to the prompt
        """
        expected_outputs = [1, 10, 20]
        for expected_output in expected_outputs:
            # We use patch here to keep stdout from get_user_tasks from being displayed
            with patch("sys.stdout"):
                # Run the method we're testing and save the result
                self.detailed_status_obj.get_user_max_tasks()

            # Make sure the prompt is called with the correct prompt message
            mock_input.assert_called_with("What limit would you like to set? (must be an integer greater than 0) ")
            # Ensure we get correct output
            self.assertEqual(self.detailed_status_obj.args.max_tasks, expected_output)

    # '1' is a valid input and we'll use that to exit safely from this test
    @patch("builtins.input", side_effect=["0", "-1", "1.5", "a", "1"])
    def test_get_user_max_tasks_invalid_inputs(self, mock_input: MagicMock):
        """
        This will test sending valid inputs to the get_user_max_tasks method.

        :param `mock_input`: A MagicMock object to send inputs to the prompt
        """
        captured_output = StringIO()
        # We use patch here to capture the stdout from get_user_max_tasks
        with patch("sys.stdout", new=captured_output):
            # Run the method we're testing (it won't return anything until we hit the valid
            # filter so we don't save the result)
            self.detailed_status_obj.get_user_max_tasks()

        # Make sure the prompt is called with the correct prompt message
        mock_input.assert_called_with("What limit would you like to set? (must be an integer greater than 0) ")

        # There should be 4 "invalid input" messages so make sure there are
        all_invalid_msgs = re.findall(
            r"Invalid input. The limit must be an integer greater than 0.", captured_output.getvalue()
        )
        self.assertEqual(len(all_invalid_msgs), 4)

    ###############################################
    # Testing display()
    ###############################################

    @patch("builtins.input", side_effect=["c"])
    def test_display_ync_prompt_c(self, mock_input: MagicMock):
        """
        Test the first prompt that's given when you ask for detailed
        status with the disable pager and there's a bunch of tasks. In
        this test we're just cancelling the display (i.e. inputting 'c').

        :param `mock_input`: A MagicMock object to send inputs to the prompt
        """
        # We have to set the no_prompts argument to False or else this won't work
        self.detailed_status_obj.args.no_prompts = False

        captured_output = StringIO()
        with patch("sys.stdout", new=captured_output):
            # Setting display to test mode will change the limit before a
            # prompt is shown from 250 to 15
            self.detailed_status_obj.display(test_mode=True)

        # Ensure the display y/n/c prompt was given
        mock_input.assert_called_once_with(
            "About to display 19 tasks without a pager. Would you like to apply additional filters? (y/n/c) "
        )

        # Ensure the display was cancelled
        assert "Cancelling status display." in captured_output.getvalue()

    @patch("builtins.input", side_effect=["n"])
    def test_display_ync_prompt_n(self, mock_input: MagicMock):
        """
        Test the first prompt that's given when you ask for detailed
        status with the disable pager and there's a bunch of tasks. In
        this test we're telling the prompt that we don't want to apply
        additional filters (i.e. inputting 'n').

        :param `mock_input`: A MagicMock object to send inputs to the prompt
        """
        # We have to set the no_prompts argument to False or else this won't work
        self.detailed_status_obj.args.no_prompts = False

        captured_output = StringIO()
        with patch("sys.stdout", new=captured_output):
            # Setting display to test mode will change the limit before a
            # prompt is shown from 250 to 15
            self.detailed_status_obj.display(test_mode=True)

        # Ensure the display y/n/c prompt was given
        mock_input.assert_called_once_with(
            "About to display 19 tasks without a pager. Would you like to apply additional filters? (y/n/c) "
        )

        # Ensure the display was told not to apply anymore filters
        assert "Not filtering further. Displaying 19 tasks..." in captured_output.getvalue()

        # Ensure the requested_statuses dict holds all statuses still
        self.assertEqual(self.detailed_status_obj.requested_statuses, status_test_variables.ALL_REQUESTED_STATUSES)

    @patch("builtins.input", side_effect=["y", "e", "c"])
    def test_display_ync_prompt_y(self, mock_input: MagicMock):
        """
        Test the first prompt that's given when you ask for detailed
        status with the disable pager and there's a bunch of tasks. In
        this test we're telling the prompt that we do want to apply
        additional filters (i.e. inputting 'y').

        The input chain is as follows:
        The prompt will first ask if we want to filter further and we'll input
        'y' -> this takes us to the second input asking how we want to filter
        and we'll input 'e' to exit -> this will take us back to the first prompt
        and we'll enter 'c' to cancel the display operation

        :param `mock_input`: A MagicMock object to send inputs to the prompt
        """
        # We have to set the no_prompts argument to False or else this won't work
        self.detailed_status_obj.args.no_prompts = False

        with patch("sys.stdout"):
            # Setting display to test mode will change the limit before a
            # prompt is shown from 250 to 15
            self.detailed_status_obj.display(test_mode=True)

        # There should be 3 input calls: the initial prompt, the next prompt after entering
        # 'y', and then going back to the initial prompt after entering 'e' to exit
        self.assertEqual(len(mock_input.mock_calls), 3)

        # Create the list of calls that should be made (this is in sequential order; the order DOES matter here)
        initial_prompt = "About to display 19 tasks without a pager. Would you like to apply additional filters? (y/n/c) "
        secondary_prompt = "How would you like to filter the tasks? "
        calls = [call(initial_prompt), call(secondary_prompt), call(initial_prompt)]

        # Ensure the correct calls have been made
        mock_input.assert_has_calls(calls)

    @patch("builtins.input", side_effect=["a", "0", "", "c"])
    def test_display_ync_prompt_invalid_inputs(self, mock_input: MagicMock):
        """
        Test the first prompt that's given when you ask for detailed
        status with the disable pager and there's a bunch of tasks. In
        this test we're testing against invalid inputs and finishing the
        test off by inputting 'c' to cancel the display.

        :param `mock_input`: A MagicMock object to send inputs to the prompt
        """
        # We have to set the no_prompts argument to False or else this won't work
        self.detailed_status_obj.args.no_prompts = False

        with patch("sys.stdout"):
            # Setting display to test mode will change the limit before a
            # prompt is shown from 250 to 15
            self.detailed_status_obj.display(test_mode=True)

        # The call order should have the initial prompt followed by an invalid prompt for each
        # of our 3 invalid inputs ('a', '0', and '')
        initial_prompt = "About to display 19 tasks without a pager. Would you like to apply additional filters? (y/n/c) "
        invalid_prompt = "Invalid input. You must enter either 'y' for yes, 'n' for no, or 'c' for cancel: "
        calls = [call(initial_prompt)] + [call(invalid_prompt)] * 3

        # Ensure the mock_input has the correct calls in the correct order
        mock_input.assert_has_calls(calls)

    ###############################################
    # Testing display(), filter_via_prompts(),
    # get_user_filters(), and get_user_max_tasks()
    #
    # Sort of an integration test but all of these
    # methods revolve around display
    ###############################################

    @patch("builtins.input", side_effect=["y", "FAILED, STOP_WORKERS"])
    def test_display_full_filter_process(self, mock_input: MagicMock):
        """
        This test will run through the prompts given to users when they disable
        the pager and there are a bunch of tasks to display. This will test a
        run with no invalid inputs (each method has been individually tested above
        for invalid inputs).

        This should pull up two prompts: the first asking if we want to apply
        additional filters which we'll input 'y' to -> the second asking us
        how we'd like to filter, which we'll input 'FAILED, STOP_WORKERS' to.
        This uses both the task_status and return_code filters to ask for
        any failed or cancelled tasks we have.

        :param `mock_input`: A MagicMock object to send inputs to the prompt
        """
        # We have to set the no_prompts argument to False or else this won't work
        self.detailed_status_obj.args.no_prompts = False

        with patch("sys.stdout"):
            # Setting display to test mode will change the limit before a
            # prompt is shown from 250 to 15
            self.detailed_status_obj.display(test_mode=True)

        # The call order should have the initial prompt followed by a prompt asking how
        # we want to filter our tasks (this is in a specific order)
        initial_prompt = "About to display 19 tasks without a pager. Would you like to apply additional filters? (y/n/c) "
        secondary_prompt = "How would you like to filter the tasks? "
        calls = [call(initial_prompt), call(secondary_prompt)]

        # Ensure the mock_input has the correct calls in the correct order
        mock_input.assert_has_calls(calls)

        # Ensure the requested_statuses dict holds all failed and cancelled tasks
        self.assertEqual(self.detailed_status_obj.requested_statuses, status_test_variables.REQUESTED_STATUSES_FAIL_AND_CANCEL)

    @patch("builtins.input", side_effect=["y", "SUCCESS, MAX_TASKS", "3"])
    def test_display_full_filter_process_max_tasks(self, mock_input: MagicMock):
        """
        This test will run through the prompts given to users when they disable
        the pager and there are a bunch of tasks to display. This will test a
        run with no invalid inputs (each method has been individually tested above
        for invalid inputs).

        This should pull up three prompts: the first asking if we want to apply
        additional filters which we'll input 'y' to -> the second asking us
        how we'd like to filter, which we'll input 'SUCCESS, MAX_TASKS' to ->
        the third and final asking us what limit we'd like to set for the max_tasks
        value

        :param `mock_input`: A MagicMock object to send inputs to the prompt
        """
        # We have to set the no_prompts argument to False or else this won't work
        self.detailed_status_obj.args.no_prompts = False

        with patch("sys.stdout"):
            # Setting display to test mode will change the limit before a
            # prompt is shown from 250 to 15
            self.detailed_status_obj.display(test_mode=True)

        # The call order should have the initial prompt followed by a prompt asking how
        # we want to filter our tasks followed by a prompt asking us what limit we'd
        # like to set (this is in a specific order)
        initial_prompt = "About to display 19 tasks without a pager. Would you like to apply additional filters? (y/n/c) "
        secondary_prompt = "How would you like to filter the tasks? "
        tertiary_prompt = "What limit would you like to set? (must be an integer greater than 0) "
        calls = [call(initial_prompt), call(secondary_prompt), call(tertiary_prompt)]

        # Ensure the mock_input has the correct calls in the correct order
        mock_input.assert_has_calls(calls)

        # Ensure the requested_statuses dict holds only 3 successful tasks
        self.assertEqual(self.detailed_status_obj.num_requested_statuses, 3)


class TestFilterApplication(TestBaseDetailedStatus):
    """
    This class is strictly for testing that filters are applied correctly.

    The types of filters are:
    steps, max_tasks, return_code, task_status, task_queues, and workers.

    By the time filters are applied in the execution process, the filters
    will have already been verified so we don't need to check against invalid
    inputs (that's what the TestFilterVerification class is for).

    This class will test 3 methods: get_steps_to_display (this applies the
    steps and task_queues filters), apply_filters (this applies the return_code,
    task_status, workers, and max_tasks filters), and apply_max_tasks_limit (this
    applies just the max_tasks filter).
    """

    def test_apply_default_steps(self):
        """
        This will test the default application of the steps filter. When the
        detailed_status_obj variable is created in setUp, the default value
        for steps will already be being used, and the get_steps_to_display method
        will be called upon initialization. Therefore, we can just ensure it was
        processed correctly without needing to directly call it.
        """
        # The steps arg is expanded from ["all"] to a list of every step name upon class creation
        self.assertEqual(
            self.detailed_status_obj.args.steps,
            ["just_samples", "just_parameters", "params_and_samples", "fail_step", "cancel_step", "unstarted_step"],
        )

        # The step_tracker dict should have every step here
        step_tracker_diff = DeepDiff(
            status_test_variables.FULL_STEP_TRACKER, self.detailed_status_obj.step_tracker, ignore_order=True
        )
        self.assertEqual(step_tracker_diff, {})

    def run_get_steps_to_display_test(self, expected_step_tracker: Dict):
        """
        A helper method to combine similar code for the get_steps_to_display tests.
        This is where the get_steps_to_display method is actually called and tested against.
        """
        # Call get_steps_to_display to get the step_tracker object and make sure it matches the expected output
        step_tracker_diff = DeepDiff(expected_step_tracker, self.detailed_status_obj.get_steps_to_display(), ignore_order=True)
        self.assertEqual(step_tracker_diff, {})

    def test_apply_single_step(self):
        """
        This tests the application of the steps filter with only one step.
        """
        # Modify the steps argument and create the expected output
        self.detailed_status_obj.args.steps = ["just_parameters"]
        expected_step_tracker = {"started_steps": ["just_parameters"], "unstarted_steps": []}

        # Run the test
        self.run_get_steps_to_display_test(expected_step_tracker)

    def test_apply_multiple_steps(self):
        """
        This tests the application of the steps filter with multiple steps.
        """
        # Modify the steps argument and create the expected output
        self.detailed_status_obj.args.steps = ["just_parameters", "just_samples", "fail_step"]
        expected_step_tracker = {"started_steps": ["just_parameters", "just_samples", "fail_step"], "unstarted_steps": []}

        # Run the test
        self.run_get_steps_to_display_test(expected_step_tracker)

    def test_apply_single_task_queue(self):
        """
        This tests the application of the task_queues filter with only one task queue.
        """
        # Modify the task_queues argument and create the expected output
        self.detailed_status_obj.args.task_queues = ["just_parameters_queue"]
        expected_step_tracker = {"started_steps": ["just_parameters"], "unstarted_steps": []}

        # We need to reset steps to "all" otherwise this test won't work
        self.detailed_status_obj.args.steps = ["all"]

        # Run the test
        self.run_get_steps_to_display_test(expected_step_tracker)

    def test_apply_multiple_task_queues(self):
        """
        This tests the application of the task_queues filter with multiple task queues.
        """
        # Modify the task_queues argument and create the expected output
        self.detailed_status_obj.args.task_queues = ["just_parameters_queue", "just_samples_queue", "fail_queue"]
        expected_step_tracker = {"started_steps": ["just_parameters", "just_samples", "fail_step"], "unstarted_steps": []}

        # We need to reset steps to "all" otherwise this test won't work
        self.detailed_status_obj.args.steps = ["all"]

        # Run the test
        self.run_get_steps_to_display_test(expected_step_tracker)

    def test_apply_max_tasks(self):
        """
        The max_tasks filter has no default to test against as the default value is None
        and will not trigger the apply_max_task_limit method. We'll test the application of this
        method here by modifying max tasks and calling it. This method will modify the
        requested_statuses dict so we'll check against that.
        """
        # Set the max_tasks limit and apply it
        self.detailed_status_obj.args.max_tasks = 3
        self.detailed_status_obj.apply_max_tasks_limit()

        # Ensure the max_tasks limit was applied to the requested_statuses
        self.assertEqual(self.detailed_status_obj.num_requested_statuses, 3)

    def run_apply_filters_test(self, expected_requested_statuses: Dict):
        """
        A helper method to combine similar code for the apply_filters tests.
        The apply_filters method is tested here as a side effect of calling
        load_requested_statuses.
        """
        # Apply any filter given before this method was called
        self.detailed_status_obj.load_requested_statuses()

        # Ensure the requested statuses are as expected
        requested_statuses_diff = DeepDiff(
            expected_requested_statuses, self.detailed_status_obj.requested_statuses, ignore_order=True
        )
        self.assertEqual(requested_statuses_diff, {})

    def test_apply_single_worker(self):
        """
        This tests the application of the workers filter with only one worker.
        """
        # Set the workers filter and run the test
        self.detailed_status_obj.args.workers = ["other_worker"]
        self.run_apply_filters_test(status_test_variables.REQUESTED_STATUSES_JUST_OTHER_WORKER)

    def test_apply_multiple_workers(self):
        """
        This tests the application of the workers filter with multiple worker.
        """
        # Set the workers filter and run the test
        self.detailed_status_obj.args.workers = ["other_worker", "sample_worker"]
        self.run_apply_filters_test(status_test_variables.ALL_REQUESTED_STATUSES)

    def test_apply_single_return_code(self):
        """
        This tests the application of the return_code filter with only one return codes.
        """
        # Set the return code filter and run the test
        self.detailed_status_obj.args.return_code = ["SOFT_FAIL"]
        self.run_apply_filters_test(status_test_variables.REQUESTED_STATUSES_JUST_FAILED_STEP)

    def test_apply_multiple_return_codes(self):
        """
        This tests the application of the return_code filter with multiple return codes.
        """
        # Set the return code filter and run the test
        self.detailed_status_obj.args.return_code = ["SOFT_FAIL", "STOP_WORKERS"]
        self.run_apply_filters_test(status_test_variables.REQUESTED_STATUSES_FAIL_AND_CANCEL)

    def test_apply_single_task_status(self):
        """
        This tests the application of the task_status filter with only one task status.
        """
        # Set the return code filter and run the test
        self.detailed_status_obj.args.task_status = ["FAILED"]
        self.run_apply_filters_test(status_test_variables.REQUESTED_STATUSES_JUST_FAILED_STEP)

    def test_apply_multiple_task_statuses(self):
        """
        This tests the application of the task_status filter with multiple task statuses.
        """
        # Set the return code filter and run the test
        self.detailed_status_obj.args.task_status = ["FAILED", "CANCELLED"]
        self.run_apply_filters_test(status_test_variables.REQUESTED_STATUSES_FAIL_AND_CANCEL)


class TestFilterVerification(TestBaseDetailedStatus):
    """
    This class is strictly for testing the verification process when filters
    are given to the DetailedStatus object. This does NOT test that filters
    are applied properly, just that they're verified correctly.

    The types of filters are:
    steps, max_tasks, return_code, task_status, task_queues, and workers.

    For every filter we'll test the verification process against valid and
    invalid inputs. Additionally, for every filter besides max_tasks, even
    though I don't think it's possible for this scenario to get passed through
    to the DetailedStatus class, we'll test against an empty list as input.
    """

    def test_verify_filter_args_valid_steps(self):
        """
        Test the verification process of the steps filter using valid steps.
        This covers part of the _verify_filter_args method and one use case of the
        _verify_filters method that is called by _verify_filter_args.
        """
        # Test single valid step
        valid_step = ["just_samples"]
        self.detailed_status_obj.args.steps = valid_step
        # Calling verify_filter_args should not change anything here
        self.detailed_status_obj._verify_filter_args()
        self.assertEqual(self.detailed_status_obj.args.steps, valid_step)

        # Test multiple valid steps
        valid_steps = ["just_samples", "just_parameters"]
        self.detailed_status_obj.args.steps = valid_steps
        # Calling verify_filter_args should not change anything here
        self.detailed_status_obj._verify_filter_args()
        self.assertEqual(self.detailed_status_obj.args.steps, valid_steps)

    def test_verify_filter_args_invalid_steps(self):
        """
        Test the verification process of the steps filter using invalid steps.
        This covers part of the _verify_filter_args method and one use case of the
        _verify_filters method that is called by _verify_filter_args.
        """
        # Testing "invalid_step" as only step
        self.detailed_status_obj.args.steps = ["invalid_step"]
        # Calling verify_filter_args should remove "invalid_step"
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.steps, [])

        # Testing "invalid_step" as first step
        self.detailed_status_obj.args.steps = ["invalid_step", "just_samples"]
        # Calling verify_filter_args should allow "just_samples" to stay but not "invalid_step"
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.steps, ["just_samples"])

        # Testing "invalid_step" as last step
        self.detailed_status_obj.args.steps = ["just_samples", "invalid_step"]
        # Calling verify_filter_args should allow "just_samples" to stay but not "invalid_step"
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.steps, ["just_samples"])

        # Testing "invalid_step" as middle step
        self.detailed_status_obj.args.steps = ["just_samples", "invalid_step", "just_parameters"]
        # Calling verify_filter_args should allow "just_samples" and "just_parameters" to stay but not "invalid_step"
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.steps, ["just_samples", "just_parameters"])

        # Testing multiple invalid steps
        self.detailed_status_obj.args.steps = ["just_samples", "invalid_step_1", "just_parameters", "invalid_step_2"]
        # Calling verify_filter_args should allow only "just_samples" and "just_parameters" to stay
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.steps, ["just_samples", "just_parameters"])

    def test_verify_filter_args_no_steps(self):
        """
        Test the verification process of the steps filter using no steps. I don't think this
        is even possible to get passed to the DetailedStatus object but we'll test it just in
        case. This covers part of the _verify_filter_args method and one use case of the
        _verify_filters method that is called by _verify_filter_args.
        """
        # Modify the steps filter so we can re-run the verify_filters_args with this filter
        self.detailed_status_obj.args.steps = []

        # Calling verify_filter_args should just keep the empty list
        self.detailed_status_obj._verify_filter_args()
        self.assertEqual(self.detailed_status_obj.args.steps, [])

    def test_verify_filter_args_valid_max_tasks(self):
        """
        Test the verification process of the max_tasks filter using a valid max_tasks value.
        This covers part of the _verify_filter_args method and one use case of the
        _verify_filters method that is called by _verify_filter_args.
        """
        # Test valid max tasks
        valid_max_tasks = 12
        self.detailed_status_obj.args.max_tasks = valid_max_tasks
        # Calling verify_filter_args should not change anything here
        self.detailed_status_obj._verify_filter_args()
        self.assertEqual(self.detailed_status_obj.args.max_tasks, valid_max_tasks)

    def test_verify_filter_args_invalid_max_tasks(self):
        """
        Test the verification process of the max_tasks filter using invalid max_tasks
        values. We don't need to test for too high of a value since the code will
        automatically reset the value to however large requested_statuses is.
        This covers part of the _verify_filter_args method and one use case of the
        _verify_filters method that is called by _verify_filter_args.
        """
        # Testing negative max_tasks value
        self.detailed_status_obj.args.max_tasks = -1
        # Calling verify_filter_args should reset max_tasks to None
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.max_tasks, None)

        # Testing max_tasks value of zero (edge case)
        self.detailed_status_obj.args.max_tasks = 0
        # Calling verify_filter_args should reset max_tasks to None
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.max_tasks, None)

        # Testing max_tasks value that's not an integer
        self.detailed_status_obj.args.max_tasks = 1.5
        # Calling verify_filter_args should reset max_tasks to None
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.max_tasks, None)

    def test_verify_filter_args_valid_task_status(self):
        """
        Test the verification process of the task_status filter using valid task_status values.
        This covers part of the _verify_filter_args method and one use case of the
        _verify_filters method that is called by _verify_filter_args.
        """
        # Test single valid task status
        valid_task_status = ["FINISHED"]
        self.detailed_status_obj.args.task_status = valid_task_status
        # Calling verify_filter_args should not change anything here
        self.detailed_status_obj._verify_filter_args()
        self.assertEqual(self.detailed_status_obj.args.task_status, valid_task_status)

        # Test multiple valid task statuses
        valid_task_statuses = ["FINISHED", "FAILED", "CANCELLED"]
        self.detailed_status_obj.args.task_status = valid_task_statuses
        # Calling verify_filter_args should not change anything here
        self.detailed_status_obj._verify_filter_args()
        self.assertEqual(self.detailed_status_obj.args.task_status, valid_task_statuses)

    def test_verify_filter_args_invalid_task_status(self):
        """
        Test the verification process of the task_status filter using invalid task_status values.
        This covers part of the _verify_filter_args method and one use case of the
        _verify_filters method that is called by _verify_filter_args.
        """
        # Testing a single invalid filter
        self.detailed_status_obj.args.task_status = ["INVALID"]
        # Calling verify_filter_args should remove the invalid filter
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.task_status, [])

        # Testing invalid filter as first filter
        self.detailed_status_obj.args.task_status = ["INVALID", "DRY_RUN"]
        # Calling verify_filter_args should only allow "DRY_RUN" to remain
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.task_status, ["DRY_RUN"])

        # Testing invalid filter as last filter
        self.detailed_status_obj.args.task_status = ["UNKNOWN", "INVALID"]
        # Calling verify_filter_args should only allow "UNKNOWN" to remain
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.task_status, ["UNKNOWN"])

        # Testing invalid filter as middle filter
        self.detailed_status_obj.args.task_status = ["INITIALIZED", "INVALID", "RUNNING"]
        # Calling verify_filter_args should only allow "INITIALIZED" and "RUNNING" to remain
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.task_status, ["INITIALIZED", "RUNNING"])

        # Testing multiple invalid filters
        self.detailed_status_obj.args.task_status = ["INVALID_1", "CANCELLED", "INVALID_2"]
        # Calling verify_filter_args should only allow "CANCELLED" to remain
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.task_status, ["CANCELLED"])

    def test_verify_filter_args_no_task_status(self):
        """
        Test the verification process of the task_status filter using no task_status. I don't think
        this is even possible to get passed to the DetailedStatus object but we'll test it just in
        case. This covers part of the _verify_filter_args method and one use case of the
        _verify_filters method that is called by _verify_filter_args.
        """
        # Testing empty task status filter
        self.detailed_status_obj.args.task_status = []

        # Calling verify_filter_args should just keep the empty list
        self.detailed_status_obj._verify_filter_args()
        self.assertEqual(self.detailed_status_obj.args.task_status, [])

    def test_verify_filter_args_valid_return_code(self):
        """
        Test the verification process of the task_status filter using valid task_status values.
        This covers part of the _verify_filter_args method and one use case of the
        _verify_filters method that is called by _verify_filter_args.
        """
        # Test single valid task status
        valid_return_code = ["SUCCESS"]
        self.detailed_status_obj.args.return_code = valid_return_code
        # Calling verify_filter_args should not change anything here
        self.detailed_status_obj._verify_filter_args()
        self.assertEqual(self.detailed_status_obj.args.return_code, valid_return_code)

        # Test multiple valid task statuses
        valid_return_codes = ["SOFT_FAIL", "DRY_SUCCESS", "SUCCESS"]
        self.detailed_status_obj.args.return_code = valid_return_codes
        # Calling verify_filter_args should not change anything here
        self.detailed_status_obj._verify_filter_args()
        self.assertEqual(self.detailed_status_obj.args.return_code, valid_return_codes)

    def test_verify_filter_args_invalid_return_code(self):
        """
        Test the verification process of the return_code filter using invalid return_code values.
        This covers part of the _verify_filter_args method and one use case of the
        _verify_filters method that is called by _verify_filter_args.
        """
        # Testing a single invalid filter
        self.detailed_status_obj.args.return_code = ["INVALID"]
        # Calling verify_filter_args should remove the invalid filter
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.return_code, [])

        # Testing invalid filter as first filter
        self.detailed_status_obj.args.return_code = ["INVALID", "SOFT_FAIL"]
        # Calling verify_filter_args should only allow "SOFT_FAIL" to remain
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.return_code, ["SOFT_FAIL"])

        # Testing invalid filter as last filter
        self.detailed_status_obj.args.return_code = ["HARD_FAIL", "INVALID"]
        # Calling verify_filter_args should only allow "HARD_FAIL" to remain
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.return_code, ["HARD_FAIL"])

        # Testing invalid filter as middle filter
        self.detailed_status_obj.args.return_code = ["STOP_WORKERS", "INVALID", "UNRECOGNIZED"]
        # Calling verify_filter_args should only allow "STOP_WORKERS" and "UNRECOGNIZED" to remain
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.return_code, ["STOP_WORKERS", "UNRECOGNIZED"])

        # Testing multiple invalid filters
        self.detailed_status_obj.args.return_code = ["INVALID_1", "SUCCESS", "INVALID_2"]
        # Calling verify_filter_args should only allow "SUCCESS" to remain
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.return_code, ["SUCCESS"])

    def test_verify_filter_args_no_return_code(self):
        """
        Test the verification process of the return_code filter using no return_code. I don't think
        this is even possible to get passed to the DetailedStatus object but we'll test it just in
        case. This covers part of the _verify_filter_args method and one use case of the
        _verify_filters method that is called by _verify_filter_args.
        """
        # Testing empty return code filter
        self.detailed_status_obj.args.return_code = []

        # Calling verify_filter_args should just keep the empty list
        self.detailed_status_obj._verify_filter_args()
        self.assertEqual(self.detailed_status_obj.args.return_code, [])

    def test_verify_filter_args_valid_task_queue(self):
        """
        Test the verification process of the task_queues filter using valid task_queues values.
        This covers part of the _verify_filter_args method and one use case of the
        _verify_filters method that is called by _verify_filter_args.
        """
        # Test single valid task status
        valid_task_queue = ["just_samples_queue"]
        self.detailed_status_obj.args.task_queues = valid_task_queue
        # Calling verify_filter_args should not change anything here
        self.detailed_status_obj._verify_filter_args()
        self.assertEqual(self.detailed_status_obj.args.task_queues, valid_task_queue)

        # Test multiple valid task statuses
        valid_task_queues = ["just_samples_queue", "just_parameters_queue", "both_queue"]
        self.detailed_status_obj.args.task_queues = valid_task_queues
        # Calling verify_filter_args should not change anything here
        self.detailed_status_obj._verify_filter_args()
        self.assertEqual(self.detailed_status_obj.args.task_queues, valid_task_queues)

    def test_verify_filter_args_invalid_task_queue(self):
        """
        Test the verification process of the task_queues filter using invalid task_queues values.
        This covers part of the _verify_filter_args method and one use case of the
        _verify_filters method that is called by _verify_filter_args.
        """
        # Testing a single invalid filter
        self.detailed_status_obj.args.task_queues = ["invalid_queue"]
        # Calling verify_filter_args should remove the invalid filter
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.task_queues, [])

        # Testing invalid filter as first filter
        self.detailed_status_obj.args.task_queues = ["invalid_queue", "unstarted_queue"]
        # Calling verify_filter_args should only allow "unstarted_queue" to remain
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.task_queues, ["unstarted_queue"])

        # Testing invalid filter as last filter
        self.detailed_status_obj.args.task_queues = ["fail_queue", "invalid_queue"]
        # Calling verify_filter_args should only allow "fail_queue" to remain
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.task_queues, ["fail_queue"])

        # Testing invalid filter as middle filter
        self.detailed_status_obj.args.task_queues = ["cancel_queue", "invalid_queue", "both_queue"]
        # Calling verify_filter_args should only allow "cancel_queue" and "both_queue" to remain
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.task_queues, ["cancel_queue", "both_queue"])

        # Testing multiple invalid filters
        self.detailed_status_obj.args.task_queues = ["invalid_queue_1", "just_samples_queue", "invalid_queue_2"]
        # Calling verify_filter_args should only allow "just_samples_queue" to remain
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.task_queues, ["just_samples_queue"])

    def test_verify_filter_args_no_task_queue(self):
        """
        Test the verification process of the task_queues filter using no task_queues. I don't think
        this is even possible to get passed to the DetailedStatus object but we'll test it just in
        case. This covers part of the _verify_filter_args method and one use case of the
        _verify_filters method that is called by _verify_filter_args.
        """
        # Testing empty task queues filter
        self.detailed_status_obj.args.task_queues = []

        # Calling verify_filter_args should just keep the empty list
        self.detailed_status_obj._verify_filter_args()
        self.assertEqual(self.detailed_status_obj.args.task_queues, [])

    def test_verify_filter_args_valid_workers(self):
        """
        Test the verification process of the task_queue filter using valid task_queue values.
        This covers part of the _verify_filter_args method and one use case of the
        _verify_filters method that is called by _verify_filter_args.
        """
        # Test single valid task status
        valid_worker = ["sample_worker"]
        self.detailed_status_obj.args.workers = valid_worker
        # Calling verify_filter_args should not change anything here
        self.detailed_status_obj._verify_filter_args()
        self.assertEqual(self.detailed_status_obj.args.workers, valid_worker)

        # Test multiple valid task statuses
        valid_workers = ["sample_worker", "other_worker"]
        self.detailed_status_obj.args.workers = valid_workers
        # Calling verify_filter_args should not change anything here
        self.detailed_status_obj._verify_filter_args()
        self.assertEqual(self.detailed_status_obj.args.workers, valid_workers)

    def test_verify_filter_args_invalid_workers(self):
        """
        Test the verification process of the workers filter using invalid workers values.
        This covers part of the _verify_filter_args method and one use case of the
        _verify_filters method that is called by _verify_filter_args.
        """
        # Testing a single invalid filter
        self.detailed_status_obj.args.workers = ["invalid_worker"]
        # Calling verify_filter_args should remove the invalid filter
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.workers, [])

        # Testing invalid filter as first filter
        self.detailed_status_obj.args.workers = ["invalid_worker", "sample_worker"]
        # Calling verify_filter_args should only allow "sample_worker" to remain
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.workers, ["sample_worker"])

        # Testing invalid filter as last filter
        self.detailed_status_obj.args.workers = ["sample_worker", "invalid_worker"]
        # Calling verify_filter_args should only allow "sample_worker" to remain
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.workers, ["sample_worker"])

        # Testing invalid filter as middle filter
        self.detailed_status_obj.args.workers = ["sample_worker", "invalid_worker", "other_worker"]
        # Calling verify_filter_args should only allow "sample_worker" and "other_worker" to remain
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.workers, ["sample_worker", "other_worker"])

        # Testing multiple invalid filters
        self.detailed_status_obj.args.workers = ["invalid_worker_1", "sample_worker", "invalid_worker_2"]
        # Calling verify_filter_args should only allow "sample_worker" to remain
        self.detailed_status_obj._verify_filter_args(suppress_warnings=True)
        self.assertEqual(self.detailed_status_obj.args.workers, ["sample_worker"])

    def test_verify_filter_args_no_workers(self):
        """
        Test the verification process of the workers filter using no workers. I don't think
        this is even possible to get passed to the DetailedStatus object but we'll test it just in
        case. This covers part of the _verify_filter_args method and one use case of the
        _verify_filters method that is called by _verify_filter_args.
        """
        # Testing empty workers filter
        self.detailed_status_obj.args.workers = []

        # Calling verify_filter_args should just keep the empty list
        self.detailed_status_obj._verify_filter_args()
        self.assertEqual(self.detailed_status_obj.args.workers, [])


if __name__ == "__main__":
    unittest.main()
