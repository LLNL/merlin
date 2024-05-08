###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.12.1.
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
import json
import os
import unittest
from argparse import Namespace
from copy import deepcopy
from datetime import datetime
from json.decoder import JSONDecodeError

import pytest
import yaml
from deepdiff import DeepDiff
from filelock import Timeout

from merlin.spec.expansion import get_spec_with_expansion
from merlin.study.status import Status, read_status, status_conflict_handler, write_status
from tests.unit.study.status_test_files import shared_tests, status_test_variables


class TestStatusReading:
    """Test the logic for reading in status files"""

    cancel_step_dir = f"{status_test_variables.VALID_WORKSPACE_PATH}/cancel_step"
    status_file = f"{cancel_step_dir}/MERLIN_STATUS.json"
    lock_file = f"{cancel_step_dir}/status.lock"

    def test_basic_read(self):
        """
        Test the basic reading functionality of `read_status`. There should
        be no errors thrown and the correct status dict should be returned.
        """
        actual_statuses = read_status(self.status_file, self.lock_file)
        read_status_diff = DeepDiff(
            actual_statuses, status_test_variables.REQUESTED_STATUSES_JUST_CANCELLED_STEP, ignore_order=True
        )
        assert read_status_diff == {}

    def test_timeout_raise_errors_disabled(self, mocker: "Fixture", caplog: "Fixture"):  # noqa: F821
        """
        Test the timeout functionality of the `read_status` function with
        `raise_errors` set to False. This should log a warning message and
        return an empty dict.
        This test will create a mock of the FileLock object in order to
        force a timeout to be raised.

        :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
        :param caplog: A built-in fixture from the pytest library to capture logs
        """

        # Set the mock to raise a timeout
        mock_filelock = mocker.patch("merlin.study.status.FileLock")
        mock_lock = mocker.MagicMock()
        mock_lock.acquire.side_effect = Timeout(self.lock_file)
        mock_filelock.return_value = mock_lock

        # Check that the return is as we expect
        actual_status = read_status(self.status_file, self.lock_file)
        assert actual_status == {}

        # Check that a warning is logged
        expected_log = f"Timed out when trying to read status from '{self.status_file}'"
        assert expected_log in caplog.text, "Missing expected log message"

    def test_timeout_raise_errors_enabled(self, mocker: "Fixture", caplog: "Fixture"):  # noqa: F821
        """
        Test the timeout functionality of the `read_status` function with
        `raise_errors` set to True. This should log a warning message and
        raise a Timeout exception.
        This test will create a mock of the FileLock object in order to
        force a timeout to be raised.

        :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
        :param caplog: A built-in fixture from the pytest library to capture logs
        """

        # Set the mock to raise a timeout
        mock_filelock = mocker.patch("merlin.study.status.FileLock")
        mock_lock = mocker.MagicMock()
        mock_lock.acquire.side_effect = Timeout(self.lock_file)
        mock_filelock.return_value = mock_lock

        # Check that a Timeout exception is raised
        with pytest.raises(Timeout):
            read_status(self.status_file, self.lock_file, raise_errors=True)

        # Check that a warning is logged
        expected_log = f"Timed out when trying to read status from '{self.status_file}'"
        assert expected_log in caplog.text, "Missing expected log message"

    def test_file_not_found_no_fnf_no_errors(self, caplog: "Fixture"):  # noqa: F821
        """
        Test the file not found functionality with the `display_fnf_message`
        and `raise_errors` options both set to False. This should just return
        an empty dict and not log anything.

        :param caplog: A built-in fixture from the pytest library to capture logs
        """
        dummy_file = "i_dont_exist.json"
        actual_status = read_status(dummy_file, self.lock_file, display_fnf_message=False, raise_errors=False)
        assert actual_status == {}
        assert caplog.text == ""

    def test_file_not_found_with_fnf_no_errors(self, caplog: "Fixture"):  # noqa: F821
        """
        Test the file not found functionality with the `display_fnf_message`
        set to True and the `raise_errors` option set to False. This should
        return an empty dict and log a warning.

        :param caplog: A built-in fixture from the pytest library to capture logs
        """
        dummy_file = "i_dont_exist.json"
        actual_status = read_status(dummy_file, self.lock_file, display_fnf_message=True, raise_errors=False)
        assert actual_status == {}
        assert f"Could not find '{dummy_file}'" in caplog.text

    def test_file_not_found_no_fnf_with_errors(self, caplog: "Fixture"):  # noqa: F821
        """
        Test the file not found functionality with the `display_fnf_message`
        set to False and the `raise_errors` option set to True. This should
        raise a FileNotFound error and not log anything.

        :param caplog: A built-in fixture from the pytest library to capture logs
        """
        dummy_file = "i_dont_exist.json"
        with pytest.raises(FileNotFoundError):
            read_status(dummy_file, self.lock_file, display_fnf_message=False, raise_errors=True)
        assert caplog.text == ""

    def test_file_not_found_with_fnf_and_errors(self, caplog: "Fixture"):  # noqa: F821
        """
        Test the file not found functionality with the `display_fnf_message`
        and `raise_errors` options both set to True. This should raise a FileNotFound
        error and log a warning.

        :param caplog: A built-in fixture from the pytest library to capture logs
        """
        dummy_file = "i_dont_exist.json"
        with pytest.raises(FileNotFoundError):
            read_status(dummy_file, self.lock_file, display_fnf_message=True, raise_errors=True)
        assert f"Could not find '{dummy_file}'" in caplog.text

    def test_json_decode_raise_errors_disabled(self, caplog: "Fixture", status_empty_file: str):  # noqa: F821
        """
        Test the json decode error functionality with `raise_errors` disabled.
        This should log a warning and return an empty dict.

        :param caplog: A built-in fixture from the pytest library to capture logs
        :param status_empty_file: A pytest fixture to give us an empty status file
        """
        actual_status = read_status(status_empty_file, self.lock_file, raise_errors=False)
        assert actual_status == {}
        assert f"JSONDecodeError raised when trying to read status from '{status_empty_file}'" in caplog.text

    def test_json_decode_raise_errors_enabled(self, caplog: "Fixture", status_empty_file: str):  # noqa: F821
        """
        Test the json decode error functionality with `raise_errors` enabled.
        This should log a warning and raise a JSONDecodeError.

        :param caplog: A built-in fixture from the pytest library to capture logs
        :param status_empty_file: A pytest fixture to give us an empty status file
        """
        with pytest.raises(JSONDecodeError):
            read_status(status_empty_file, self.lock_file, raise_errors=True)
        assert f"JSONDecodeError raised when trying to read status from '{status_empty_file}'" in caplog.text

    @pytest.mark.parametrize("exception", [TypeError, ValueError, NotImplementedError, IOError, UnicodeError, OSError])
    def test_broad_exception_handler_raise_errors_disabled(
        self, mocker: "Fixture", caplog: "Fixture", exception: Exception  # noqa: F821
    ):
        """
        Test the broad exception handler with `raise_errors` disabled. This should
        log a warning and return an empty dict.

        :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
        :param caplog: A built-in fixture from the pytest library to capture logs
        :param exception: An exception to force `read_status` to raise.
                          Values for this are obtained from parametrized list above.
        """

        # Set the mock to raise an exception
        mock_filelock = mocker.patch("merlin.study.status.FileLock")
        mock_lock = mocker.MagicMock()
        mock_lock.acquire.side_effect = exception()
        mock_filelock.return_value = mock_lock

        actual_status = read_status(self.status_file, self.lock_file, raise_errors=False)
        assert actual_status == {}
        assert f"An exception was raised while trying to read status from '{self.status_file}'!" in caplog.text

    @pytest.mark.parametrize("exception", [TypeError, ValueError, NotImplementedError, IOError, UnicodeError, OSError])
    def test_broad_exception_handler_raise_errors_enabled(
        self, mocker: "Fixture", caplog: "Fixture", exception: Exception  # noqa: F821
    ):
        """
        Test the broad exception handler with `raise_errors` enabled. This should
        log a warning and raise whichever exception is passed in (see list of
        parametrized exceptions in the decorator above).

        :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
        :param caplog: A built-in fixture from the pytest library to capture logs
        :param exception: An exception to force `read_status` to raise.
                          Values for this are obtained from parametrized list above.
        """

        # Set the mock to raise an exception
        mock_filelock = mocker.patch("merlin.study.status.FileLock")
        mock_lock = mocker.MagicMock()
        mock_lock.acquire.side_effect = exception()
        mock_filelock.return_value = mock_lock

        with pytest.raises(exception):
            read_status(self.status_file, self.lock_file, raise_errors=True)
        assert f"An exception was raised while trying to read status from '{self.status_file}'!" in caplog.text


class TestStatusWriting:
    """Test the logic for writing to status files"""

    status_to_write = {"status": "TESTING"}

    def test_basic_write(self, status_testing_dir: str):
        """
        Test the basic functionality of the `write_status` function. This
        should write status to a file.

        :param status_testing_dir: A pytest fixture defined in `tests/fixtures/status.py`
                                   that defines a path to the the output directory we'll write to
        """

        # Test variables
        status_filepath = f"{status_testing_dir}/basic_write.json"
        lock_file = f"{status_testing_dir}/basic_write.lock"

        # Run the test
        write_status(self.status_to_write, status_filepath, lock_file)

        # Check that the path exists and that it contains the dummy status content
        assert os.path.exists(status_filepath)
        with open(status_filepath, "r") as sfp:
            dummy_status = json.load(sfp)
        assert dummy_status == self.status_to_write

    @pytest.mark.parametrize("exception", [TypeError, ValueError, NotImplementedError, IOError, UnicodeError, OSError])
    def test_exception_raised(
        self, mocker: "Fixture", caplog: "Fixture", status_testing_dir: str, exception: Exception  # noqa: F821
    ):
        """
        Test the exception handler using several different exceptions defined in the
        parametrized list in the decorator above. This should log a warning and not
        create the status file that we provide.

        :param mocker: A built-in fixture from the pytest-mock library to create a Mock object
        :param caplog: A built-in fixture from the pytest library to capture logs
        :param status_testing_dir: A pytest fixture defined in `tests/fixtures/status.py`
                                   that defines a path to the the output directory we'll write to
        :param exception: An exception to force `read_status` to raise.
                          Values for this are obtained from parametrized list above.
        """

        # Set the mock to raise an exception
        mock_filelock = mocker.patch("merlin.study.status.FileLock")
        mock_lock = mocker.MagicMock()
        mock_lock.acquire.side_effect = exception()
        mock_filelock.return_value = mock_lock

        # Test variables
        status_filepath = f"{status_testing_dir}/exception_{exception.__name__}.json"
        lock_file = f"{status_testing_dir}/exception_{exception.__name__}.lock"

        write_status(self.status_to_write, status_filepath, lock_file)
        assert f"An exception was raised while trying to write status to '{status_filepath}'!" in caplog.text
        assert not os.path.exists(status_filepath)


class TestStatusConflictHandler:
    """Test the functionality of the `status_conflict_handler` function."""

    def test_parameter_conflict(self, caplog: "Fixture"):  # noqa: F821
        """
        Test that conflicting parameters are handled properly. This is a special
        case of the use-initial-and-log-warning rule since parameter tokens vary
        and have to be added to the `merge_rules` dict on the fly.

        :param caplog: A built-in fixture from the pytest library to capture logs
        """

        # Create two dicts with conflicting parameter values
        key = "TOKEN"
        dict_a = {"parameters": {"cmd": {key: "value"}, "restart": None}}
        dict_b = {"parameters": {"cmd": {key: "new_value"}, "restart": None}}
        path = ["parameters", "cmd"]

        # Run the test
        merged_val = status_conflict_handler(
            dict_a_val=dict_a["parameters"]["cmd"][key], dict_b_val=dict_b["parameters"]["cmd"][key], key=key, path=path
        )

        # Check that everything ran properly
        expected_log = (
            f"Conflict at key '{key}' while merging status files. Defaulting to initial value. "
            "This could lead to incorrect status information, you may want to re-run in debug mode and "
            "check the files in the output directory for this task."
        )
        assert merged_val == "value"
        assert expected_log in caplog.text

    def test_non_existent_key(self, caplog: "Fixture"):  # noqa: F821
        """
        Test providing `status_conflict_handler` a key that doesn't exist in
        the `merge_rule` dict. This should log a warning and return None.

        :param caplog: A built-in fixture from the pytest library to capture logs
        """
        key = "i_dont_exist"
        merged_val = status_conflict_handler(key=key)
        assert merged_val is None
        assert f"The key '{key}' does not have a merge rule defined. Setting this merge to None." in caplog.text

    def test_rule_string_concatenate(self):
        """
        Test the string-concatenate merge rule. This should combine
        the strings provided in `dict_a_val` and `dict_b_val` into one
        comma-delimited string.
        """

        # Create two dicts with conflicting task-queue values
        key = "task_queue"
        val1 = "existing_task_queue"
        val2 = "new_task_queue"
        dict_a = {key: val1}
        dict_b = {key: val2}

        # Run the test and make sure the values are being concatenated
        merged_val = status_conflict_handler(
            dict_a_val=dict_a[key],
            dict_b_val=dict_b[key],
            key=key,
        )
        assert merged_val == f"{val1}, {val2}"

    def test_rule_use_initial_and_log_warning(self, caplog: "Fixture"):  # noqa: F821
        """
        Test the use-initial-and-log-warning merge rule. This should
        return the value passed in to `dict_a_val` and log a warning
        message.

        :param caplog: A built-in fixture from the pytest library to capture logs
        """

        # Create two dicts with conflicting status values
        key = "status"
        dict_a = {key: "SUCCESS"}
        dict_b = {key: "FAILED"}

        # Run the test
        merged_val = status_conflict_handler(
            dict_a_val=dict_a[key],
            dict_b_val=dict_b[key],
            key=key,
        )

        # Check that everything ran properly
        expected_log = (
            f"Conflict at key '{key}' while merging status files. Defaulting to initial value. "
            "This could lead to incorrect status information, you may want to re-run in debug mode and "
            "check the files in the output directory for this task."
        )
        assert merged_val == "SUCCESS"
        assert expected_log in caplog.text

    def test_rule_use_longest_time_no_dict_a_time(self):
        """
        Test the use-longest-time merge rule with no time set for `dict_a_val`.
        This should default to using the time in `dict_b_val`.
        """
        key = "elapsed_time"
        expected_time = "12h:34m:56s"
        dict_a = {key: "--:--:--"}
        dict_b = {key: expected_time}

        merged_val = status_conflict_handler(
            dict_a_val=dict_a[key],
            dict_b_val=dict_b[key],
            key=key,
        )
        assert merged_val == expected_time

    def test_rule_use_longest_time_no_dict_b_time(self):
        """
        Test the use-longest-time merge rule with no time set for `dict_b_val`.
        This should default to using the time in `dict_a_val`.
        """
        key = "run_time"
        expected_time = "12h:34m:56s"
        dict_a = {key: expected_time}
        dict_b = {key: "--:--:--"}

        merged_val = status_conflict_handler(
            dict_a_val=dict_a[key],
            dict_b_val=dict_b[key],
            key=key,
        )
        assert merged_val == expected_time

    def test_rule_use_longest_time(self):
        """
        Test the use-longest-time merge rule with times set for both `dict_a_val`
        and `dict_b_val`. This should use whichever time is longer.
        """

        # Set up test variables
        key = "run_time"
        short_time = "01h:04m:33s"
        long_time = "12h:34m:56s"

        # Run test with dict b having the longer time
        dict_a = {key: short_time}
        dict_b = {key: long_time}
        merged_val = status_conflict_handler(
            dict_a_val=dict_a[key],
            dict_b_val=dict_b[key],
            key=key,
        )
        assert merged_val == "0d:" + long_time  # Time manipulation in status_conflict_handler will prepend '0d:'

        # Run test with dict a having the longer time
        dict_a_2 = {key: long_time}
        dict_b_2 = {key: short_time}
        merged_val_2 = status_conflict_handler(
            dict_a_val=dict_a_2[key],
            dict_b_val=dict_b_2[key],
            key=key,
        )
        assert merged_val_2 == "0d:" + long_time

    @pytest.mark.parametrize(
        "dict_a_val, dict_b_val, expected",
        [
            (0, 0, 0),
            (0, 1, 1),
            (1, 0, 1),
            (-1, 0, 0),
            (0, -1, 0),
            (23, 20, 23),
            (17, 21, 21),
        ],
    )
    def test_rule_use_max(self, dict_a_val: int, dict_b_val: int, expected: int):
        """
        Test the use-max merge rule. This should take the maximum of 2 values.

        :param dict_a_val: The value to pass in for dict_a_val
        :param dict_b_val: The value to pass in for dict_b_val
        :param expected: The expected value from this test
        """
        key = "restarts"
        merged_val = status_conflict_handler(
            dict_a_val=dict_a_val,
            dict_b_val=dict_b_val,
            key=key,
        )
        assert merged_val == expected


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
            output_path=None,
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
            self.args.spec_provided = get_spec_with_expansion(self.args.specification)
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
        self.args.spec_provided = get_spec_with_expansion(self.args.specification)
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
        self.args.spec_provided = get_spec_with_expansion(self.args.specification)

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
        self.args.spec_provided = get_spec_with_expansion(self.args.specification)

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
        better to keep these tests together. This covers the format_status_for_csv
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
            state_info_diff = DeepDiff(state_info, status_test_variables.DISPLAY_INFO[step_name], ignore_order=True)
            self.assertEqual(state_info_diff, {})

    def test_get_runtime_avg_std_dev(self):
        """
        Test the functionality that calculates the run time average and standard
        deviation for each step. This test covers the get_runtime_avg_std_dev method.
        """
        dummy_step_status = {
            "dummy_step_PARAM.1": {
                "task_queue": "dummy_queue",
                "workers": "dummy_worker",
                "dummy_step/PARAM.1/00": {
                    "status": "FINISHED",
                    "return_code": "MERLIN_SUCCESS",
                    "elapsed_time": "0d:02h:00m:00s",
                    "run_time": "0d:01h:38m:27s",  # 3600 + 2280 + 27 = 5907 seconds
                    "restarts": 0,
                },
                "dummy_step/PARAM.1/01": {
                    "status": "FINISHED",
                    "return_code": "MERLIN_SUCCESS",
                    "elapsed_time": "0d:02h:00m:00s",
                    "run_time": "0d:01h:45m:08s",  # 3600 + 2700 + 8 = 6308 seconds
                    "restarts": 0,
                },
            },
            "dummy_step_PARAM.2": {
                "task_queue": "dummy_queue",
                "workers": "dummy_worker",
                "dummy_step/PARAM.2/00": {
                    "status": "FINISHED",
                    "return_code": "MERLIN_SUCCESS",
                    "elapsed_time": "0d:02h:00m:00s",
                    "run_time": "0d:01h:52m:33s",  # 3600 + 3120 + 33 = 6753 seconds
                    "restarts": 0,
                },
                "dummy_step/PARAM.2/01": {
                    "status": "FINISHED",
                    "return_code": "MERLIN_SUCCESS",
                    "elapsed_time": "0d:02h:00m:00s",
                    "run_time": "0d:01h:08m:40s",  # 3600 + 480 + 40 = 4120 seconds
                    "restarts": 0,
                },
            },
        }

        status_obj = Status(args=self.args, spec_display=False, file_or_ws=status_test_variables.VALID_WORKSPACE_PATH)
        status_obj.get_runtime_avg_std_dev(dummy_step_status, "dummy_step")

        # Set expected values
        expected_avg = "01h:36m:12s"  # Mean is 5772 seconds = 01h:36m:12s
        expected_std_dev = "±16m:40s"  # Std dev is 1000 seconds = 16m:40s

        # Make sure the values were calculated as expected
        self.assertEqual(status_obj.run_time_info["dummy_step"]["avg_run_time"], expected_avg)
        self.assertEqual(status_obj.run_time_info["dummy_step"]["run_time_std_dev"], expected_std_dev)


if __name__ == "__main__":
    unittest.main()
