##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Tests for the celeryadapter module.
"""
import csv
import json
import os
from datetime import datetime
from typing import Dict

from celery import Celery
from deepdiff import DeepDiff

from merlin.config import Config
from merlin.spec.specification import MerlinSpec
from merlin.study import celeryadapter
from tests.unit.study.status_test_files.shared_tests import _format_csv_data
from tests.unit.study.status_test_files.status_test_variables import SPEC_PATH


# from time import sleep
# import pytest
# from celery.canvas import Signature
# @pytest.mark.order(before="TestInactive")
# class TestActive:
#     """
#     This class will test functions in the celeryadapter.py module.
#     It will run tests where we need active queues/workers to interact with.

#     NOTE: The tests in this class must be ran before the TestInactive class or else the
#     Celery workers needed for this class don't start

#     TODO: fix the bug noted above and then check if we still need pytest-order
#     """

#     def test_query_celery_queues(
#         self, celery_app: Celery, launch_workers: "Fixture", worker_queue_map: Dict[str, str]  # noqa: F821
#     ):
#         """
#         Test the query_celery_queues function by providing it with a list of active queues.
#         This should return a dict where keys are queue names and values are more dicts containing
#         the number of jobs and consumers in that queue.

#         :param `celery_app`: A pytest fixture for the test Celery app
#         :param launch_workers: A pytest fixture that launches celery workers for us to interact with
#         :param worker_queue_map: A pytest fixture that returns a dict of workers and queues
#         """
#         # Set up a dummy configuration to use in the test
#         dummy_config = Config({"broker": {"name": "redis"}})

#         # Get the actual output
#         queues_to_query = list(worker_queue_map.values())
#         actual_queue_info = celeryadapter.query_celery_queues(queues_to_query, app=celery_app, config=dummy_config)

#         # Ensure all 3 queues in worker_queue_map were queried before looping
#         assert len(actual_queue_info) == 3

#         # Ensure each queue has a worker attached
#         for queue_name, queue_info in actual_queue_info.items():
#             assert queue_name in worker_queue_map.values()
#             assert queue_info == {"consumers": 1, "jobs": 0}

#     def test_get_running_queues(self, launch_workers: "Fixture", worker_queue_map: Dict[str, str]):  # noqa: F821
#         """
#         Test the get_running_queues function with queues active.
#         This should return a list of active queues.

#         :param `launch_workers`: A pytest fixture that launches celery workers for us to interact with
#         :param `worker_queue_map`: A pytest fixture that returns a dict of workers and queues
#         """
#         result = celeryadapter.get_running_queues("merlin_test_app", test_mode=True)
#         assert sorted(result) == sorted(list(worker_queue_map.values()))

#     def test_get_active_celery_queues(
#         self, celery_app: Celery, launch_workers: "Fixture", worker_queue_map: Dict[str, str]  # noqa: F821
#     ):
#         """
#         Test the get_active_celery_queues function with queues active.
#         This should return a tuple where the first entry is a dict of queue info
#         and the second entry is a list of worker names.

#         :param `celery_app`: A pytest fixture for the test Celery app
#         :param `launch_workers`: A pytest fixture that launches celery workers for us to interact with
#         :param `worker_queue_map`: A pytest fixture that returns a dict of workers and queues
#         """
#         # Start the queues and run the test
#         queue_result, worker_result = celeryadapter.get_active_celery_queues(celery_app)

#         # Ensure we got output before looping
#         assert len(queue_result) == len(worker_result) == 3

#         for worker, queue in worker_queue_map.items():
#             # Check that the entry in the queue_result dict for this queue is correct
#             assert queue in queue_result
#             assert len(queue_result[queue]) == 1
#             assert worker in queue_result[queue][0]

#             # Remove this entry from the queue_result dict
#             del queue_result[queue]

#             # Check that this worker was added to the worker_result list
#             worker_found = False
#             for worker_name in worker_result[:]:
#                 if worker in worker_name:
#                     worker_found = True
#                     worker_result.remove(worker_name)
#                     break
#             assert worker_found

#         # Ensure there was no extra output that we weren't expecting
#         assert queue_result == {}
#         assert worker_result == []

#     def test_build_set_of_queues(
#         self, celery_app: Celery, launch_workers: "Fixture", worker_queue_map: Dict[str, str]  # noqa: F821
#     ):
#         """
#         Test the build_set_of_queues function with queues active.
#         This should return a set of queues (the queues defined in setUp).
#         """
#         # Run the test
#         result = celeryadapter.build_set_of_queues(
#             steps=["all"], spec=None, specific_queues=None, verbose=False, app=celery_app
#         )
#         assert result == set(worker_queue_map.values())

#     @pytest.mark.order(index=1)
#     def test_check_celery_workers_processing_tasks(
#         self,
#         celery_app: Celery,
#         sleep_sig: Signature,
#         launch_workers: "Fixture",  # noqa: F821
#     ):
#         """
#         Test the check_celery_workers_processing function with workers active and a task in a queue.
#         This function will query workers for any tasks they're still processing. We'll send a
#         a task that sleeps for 3 seconds to our workers before we run this test so that there should be
#         a task for this function to find.

#         NOTE: the celery app fixture shows strange behavior when using app.control.inspect() calls (which
#         check_celery_workers_processing uses) so we have to run this test first in this class in order to
#         have it run properly.

#         :param celery_app: A pytest fixture for the test Celery app
#         :param sleep_sig: A pytest fixture for a celery signature of a task that sleeps for 3 sec
#         :param launch_workers: A pytest fixture that launches celery workers for us to interact with
#         """
#         # Our active workers/queues are test_worker_[0-2]/test_queue_[0-2] so we're
#         # sending this to test_queue_0 for test_worker_0 to process
#         queue_for_signature = "test_queue_0"
#         sleep_sig.set(queue=queue_for_signature)
#         result = sleep_sig.delay()

#         # We need to give the task we just sent to the server a second to get picked up by the worker
#         sleep(1)

#         # Run the test now that the task should be getting processed
#         active_queue_test = celeryadapter.check_celery_workers_processing([queue_for_signature], celery_app)
#         assert active_queue_test is True

#         # Now test that a queue without any tasks returns false
#         # We sent the signature to task_queue_0 so task_queue_1 shouldn't have any tasks to find
#         non_active_queue_test = celeryadapter.check_celery_workers_processing(["test_queue_1"], celery_app)
#         assert non_active_queue_test is False

#         # Wait for the worker to finish running the task
#         result.get()


class TestInactive:
    """
    This class will test functions in the celeryadapter.py module.
    It will run tests where we don't need any active queues/workers to interact with.
    """

    def test_query_celery_queues(self, celery_app: Celery, worker_queue_map: Dict[str, str]):  # noqa: F821
        """
        Test the query_celery_queues function by providing it with a list of inactive queues.
        This should return a dict where keys are queue names and values are more dicts containing
        the number of jobs and consumers in that queue (which should be 0 for both here).

        :param `celery_app`: A pytest fixture for the test Celery app
        :param worker_queue_map: A pytest fixture that returns a dict of workers and queues
        """
        # Set up a dummy configuration to use in the test
        dummy_config = Config({"broker": {"name": "redis"}})

        # Get the actual output
        queues_to_query = list(worker_queue_map.values())
        actual_queue_info = celeryadapter.query_celery_queues(queues_to_query, app=celery_app, config=dummy_config)

        # Ensure all 3 queues in worker_queue_map were queried before looping
        assert len(actual_queue_info) == 3

        # Ensure each queue has no worker attached (since the queues should be inactive here)
        for queue_name, queue_info in actual_queue_info.items():
            assert queue_name in worker_queue_map.values()
            assert queue_info == {"consumers": 0, "jobs": 0}

    def test_celerize_queues(self, worker_queue_map: Dict[str, str]):
        """
        Test the celerize_queues function. This should add the celery queue_tag
        to the front of the queues we provide it.

        :param `worker_queue_map`: A pytest fixture that returns a dict of workers and queues
        """
        # Create variables to be used in the test
        queue_tag = "[merlin]_"
        queues_to_check = list(worker_queue_map.values())
        dummy_config = Config({"celery": {"queue_tag": queue_tag}})

        # Run the test
        celeryadapter.celerize_queues(queues_to_check, dummy_config)

        # Ensure the queue tag was added to every queue
        for queue in queues_to_check:
            assert queue_tag in queue

    def test_get_running_queues(self):
        """
        Test the get_running_queues function with no queues active.
        This should return an empty list.
        """
        result = celeryadapter.get_running_queues("merlin_test_app", test_mode=True)
        assert not result

    def test_get_active_celery_queues(self, celery_app: Celery):
        """
        Test the get_active_celery_queues function with no queues active.
        This should return a tuple where the first entry is an empty dict
        and the second entry is an empty list.

        :param `celery_app`: A pytest fixture for the test Celery app
        """
        queue_result, worker_result = celeryadapter.get_active_celery_queues(celery_app)
        assert not queue_result
        assert not worker_result

    def test_check_celery_workers_processing_tasks(self, celery_app: Celery, worker_queue_map: Dict[str, str]):
        """
        Test the check_celery_workers_processing function with no workers active.
        This function will query workers for any tasks they're still processing. Since no workers are active
        this should return False.

        :param celery_app: A pytest fixture for the test Celery app
        :param worker_queue_map: A pytest fixture that returns a dict of workers and queues
        """
        # Run the test now that the task should be getting processed
        result = celeryadapter.check_celery_workers_processing(list(worker_queue_map.values()), celery_app)
        assert result is False

    def test_build_set_of_queues(self, celery_app: Celery):
        """
        Test the build_set_of_queues function with no queues active.
        This should return an empty set.

        :param celery_app: A pytest fixture for the test Celery app
        """
        result = celeryadapter.build_set_of_queues(
            steps=["all"], spec=None, specific_queues=None, verbose=False, app=celery_app
        )
        assert result == set()

    def test_build_set_of_queues_with_spec(self, celery_app: Celery):
        """
        Test the build_set_of_queues function with a spec provided as input.
        This should return a set of the queues defined in the spec file.

        :param celery_app: A pytest fixture for the test Celery app
        """
        # Create the spec object that we'll pass to build_set_of_queues
        spec = MerlinSpec.load_specification(SPEC_PATH)

        # Run the test
        result = celeryadapter.build_set_of_queues(
            steps=["all"], spec=spec, specific_queues=None, verbose=False, app=celery_app
        )

        # Build the expected output list
        merlin_tag = "[merlin]_"
        expected_output = [
            "just_samples_queue",
            "just_parameters_queue",
            "both_queue",
            "fail_queue",
            "cancel_queue",
            "unstarted_queue",
        ]
        for i, output in enumerate(expected_output):
            expected_output[i] = f"{merlin_tag}{output}"

        assert result == set(expected_output)

    def test_build_set_of_queues_with_specific_queues(self, celery_app: Celery):
        """
        Test the build_set_of_queues function with specific queues provided as input.
        This should return a set of all the queues listed in the specific_queues argument.

        :param celery_app: A pytest fixture for the test Celery app
        """
        # Build the list of specific queues to search for
        specific_queues = ["test_queue_1", "test_queue_2"]

        # Run the test
        result = celeryadapter.build_set_of_queues(
            steps=["all"], spec=None, specific_queues=specific_queues, verbose=False, app=celery_app
        )

        assert result == set(specific_queues)

    def test_build_set_of_queues_with_specific_queues_and_spec(self, celery_app: Celery):
        """
        Test the build_set_of_queues function with specific queues and a yaml spec provided as input.
        The specific queues provided here will have a mix of queues that exist in the spec and
        queues that do not exist in the spec. This should only return the queues that exist in the
        spec.

        :param celery_app: A pytest fixture for the test Celery app
        """
        # Create the spec object that we'll pass to build_set_of_queues
        spec = MerlinSpec.load_specification(SPEC_PATH)

        # Build the list of queues to search for
        valid_queues = ["cancel_queue", "fail_queue", "both_queue"]
        invalid_queues = ["not_a_real_queue", "this_one_is_also_invalid"]

        # Run the test
        result = celeryadapter.build_set_of_queues(
            steps=["all"], spec=spec, specific_queues=valid_queues + invalid_queues, verbose=False, app=celery_app
        )

        # Build the expected output list
        expected_output = [f"[merlin]_{queue}" for queue in valid_queues]

        assert result == set(expected_output)

    def test_build_set_of_queues_with_steps_and_spec(self, celery_app: Celery):
        """
        Test the build_set_of_queues function with steps and a yaml spec provided as input.
        This should return the queues associated with the steps that we provide.

        :param celery_app: A pytest fixture for the test Celery app
        """
        # Create the spec object that we'll pass to build_set_of_queues
        spec = MerlinSpec.load_specification(SPEC_PATH)

        # Build the list of steps to get queues from
        steps = ["cancel_step", "fail_step"]

        # Run the test
        result = celeryadapter.build_set_of_queues(steps=steps, spec=spec, specific_queues=None, verbose=False, app=celery_app)

        # Build the expected output list
        merlin_tag = "[merlin]_"
        expected_output = ["cancel_queue", "fail_queue"]
        for i, output in enumerate(expected_output):
            expected_output[i] = f"{merlin_tag}{output}"

        assert result == set(expected_output)

    def test_build_csv_queue_info(self, worker_queue_map: Dict[str, str]):
        """
        Test the build_csv_queue_info function by providing it with a fake query return
        and timestamp. This should return a formatted dict that will look like so:
        {'time': [<timestamp>], 'queue1_name:tasks': [0], 'queue1_name:consumers': [1]}

        :param worker_queue_map: A pytest fixture that returns a dict of workers and queues
        """
        # Get a timestamp to be used in the test
        date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Initialize the expected output (we'll build the rest in the for loop below)
        expected_output = {"time": [date]}

        # Build the fake query return and the expected output
        query_return = {}
        for queue in worker_queue_map.values():
            query_return[queue] = {"consumers": 1, "jobs": 0}
            expected_output[f"{queue}:tasks"] = ["0"]
            expected_output[f"{queue}:consumers"] = ["1"]

        # Run the test
        result = celeryadapter.build_csv_queue_info(query_return, date)
        assert result == expected_output

    def test_build_json_queue_info(self, worker_queue_map: Dict[str, str]):
        """
        Test the build_json_queue_info function by providing it with a fake query return
        and timestamp. This should return a dictionary of the form:
        {<timestamp>: {"queue1_name": {"tasks": 0, "consumers": 1}}}

        :param worker_queue_map: A pytest fixture that returns a dict of workers and queues
        """
        # Get a timestamp to be used in the test
        date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Initialize the expected output
        expected_output = {date: {}}

        # Build the fake query return and the expected output
        query_return = {}
        for queue in worker_queue_map.values():
            query_return[queue] = {"consumers": 1, "jobs": 0}
            expected_output[date][queue] = {"tasks": 0, "consumers": 1}

        # Run the test
        result = celeryadapter.build_json_queue_info(query_return, date)
        assert result == expected_output

    def test_dump_celery_queue_info_csv(self, worker_queue_map: Dict[str, str]):
        """
        Test the dump_celery_queue_info function which is essentially a wrapper for the
        build_json_queue_info and build_csv_queue_info functions. This test will
        create a csv file with queue info data.

        :param worker_queue_map: A pytest fixture that returns a dict of workers and queues
        """
        # Initialize an output filepath and expected output dict
        outfile = f"{os.path.dirname(__file__)}/queue-info.csv"
        expected_output = {}

        # Build the fake query return
        query_return = {}
        for queue in worker_queue_map.values():
            query_return[queue] = {"consumers": 1, "jobs": 0}
            expected_output[f"{queue}:tasks"] = ["0"]
            expected_output[f"{queue}:consumers"] = ["1"]

        # Run the test
        celeryadapter.dump_celery_queue_info(query_return, outfile)

        # Ensure the file was created
        assert os.path.exists(outfile)

        try:
            with open(outfile, "r") as csv_df:
                csv_dump_data = csv.DictReader(csv_df)

                # Make sure a timestamp field was created
                assert "time" in csv_dump_data.fieldnames

                # Format the csv data that we just read in
                csv_dump_output = _format_csv_data(csv_dump_data)

                # We did one dump so we should only have 1 timestamp; we don't care about the value
                assert len(csv_dump_output["time"]) == 1
                del csv_dump_output["time"]

                # Make sure the rest of the csv file was created as expected
                dump_diff = DeepDiff(csv_dump_output, expected_output)
                assert not dump_diff
        finally:
            try:
                os.remove(outfile)
            except FileNotFoundError:
                pass

    def test_dump_celery_queue_info_json(self, worker_queue_map: Dict[str, str]):
        """
        Test the dump_celery_queue_info function which is essentially a wrapper for the
        build_json_queue_info and build_csv_queue_info functions. This test will
        create a json file with queue info data.

        :param worker_queue_map: A pytest fixture that returns a dict of workers and queues
        """
        # Initialize an output filepath and expected output dict
        outfile = f"{os.path.dirname(__file__)}/queue-info.json"
        expected_output = {}

        # Build the fake query return
        query_return = {}
        for queue in worker_queue_map.values():
            query_return[queue] = {"consumers": 1, "jobs": 0}
            expected_output[queue] = {"tasks": 0, "consumers": 1}

        # Run the test
        celeryadapter.dump_celery_queue_info(query_return, outfile)

        # Ensure the file was created
        assert os.path.exists(outfile)

        try:
            with open(outfile, "r") as json_df:
                json_df_contents = json.load(json_df)
            # There should only be one entry in the json dump file so this will only 'loop' once
            for dump_entry in json_df_contents.values():
                json_dump_diff = DeepDiff(dump_entry, expected_output)
                assert not json_dump_diff
        finally:
            try:
                os.remove(outfile)
            except FileNotFoundError:
                pass
