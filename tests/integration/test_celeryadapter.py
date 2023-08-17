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
Tests for the celeryadapter module.

These are integration tests rather than unit tests since we need to interact with
Celery and Redis for a lot of these functions.
"""
import csv
import json
import os
from datetime import datetime

from deepdiff import DeepDiff

from merlin.config import Config
from merlin.spec.specification import MerlinSpec
from merlin.study import celeryadapter
from tests.unit.study.status_test_files.status_test_variables import SPEC_PATH
from tests.integration.conftest import celery_app, launch_workers, worker_queue_map


class TestActiveQueues:
    """
    This class will test queue related functions in the celeryadapter.py module.
    It will run tests where we need active queues to interact with.
    """

    def test_build_set_of_queues(self, launch_workers, worker_queue_map):
        """
        Test the build_set_of_queues function with queues active.
        This should return a set of queues (the queues defined in setUp).
        """
        # Run the test
        result = celeryadapter.build_set_of_queues(
            steps=["all"], spec=None, specific_queues=None, verbose=False, app=celery_app
        )
        assert result == set(worker_queue_map.values())

    def test_query_celery_queues(self, launch_workers):
        """
        Test the query_celery_queues function by providing it with a list of active queues.
        This should return a list of tuples. Each tuple will contain information
        (name, num jobs, num consumers) for each queue that we provided.
        """
        # TODO Modify query_celery_queues so the output for a redis broker is the same
        # as the output for rabbit broker (this might be a celery issue)

    def test_get_running_queues(self, launch_workers, worker_queue_map):
        """
        Test the get_running_queues function with queues active.
        This should return a list of active queues.
        """
        result = celeryadapter.get_running_queues("celery_app")
        assert sorted(result) sorted(list(worker_queue_map.values()))

    def test_get_queues_active(self, launch_workers, worker_queue_map):
        """
        Test the get_queues function with queues active.
        This should return a tuple where the first entry is a dict of queue info
        and the second entry is a list of worker names.
        """
        # Start the queues and run the test
        queue_result, worker_result = celeryadapter.get_queues(celery_app)

        # Ensure we got output before looping
        assert len(queue_result) == len(worker_result) == 3

        for worker, queue in worker_queue_map.items():
            # Check that the entry in the queue_result dict for this queue is correct
            assert queue in queue_result
            assert len(queue_result[queue]) == 1
            assert worker in queue_result[queue][0]

            # Remove this entry from the queue_result dict
            del queue_result[queue]

            # Check that this worker was added to the worker_result list
            worker_found = False
            for worker_name in worker_result[:]:
                if worker in worker_name:
                    worker_found = True
                    worker_result.remove(worker_name)
                    break
            assert worker_found

        # Ensure there was no extra output that we weren't expecting
        assert queue_result == {}
        assert worker_result == []


class TestInactiveQueues:
    """
    This class will test queue related functions in the celeryadapter.py module.
    It will run tests where we don't need any active queues to interact with.
    """

    def test_build_set_of_queues(self, celery_app):
        """
        Test the build_set_of_queues function with no queues active.
        This should return an empty set.
        """
        result = celeryadapter.build_set_of_queues(
            steps=["all"], spec=None, specific_queues=None, verbose=False, app=celery_app
        )
        assert result == set()

    def test_build_set_of_queues_with_spec(self, celery_app):
        """
        Test the build_set_of_queues function with a spec provided as input.
        This should return a set of the queues defined in the spec file.
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

    def test_build_set_of_queues_with_specific_queues(self, celery_app):
        """
        Test the build_set_of_queues function with specific queues provided as input.
        This should return a set of all the queues listed in the specific_queues argument.
        """
        # Build the list of specific queues to search for
        specific_queues = ["test_queue_1", "test_queue_2"]

        # Run the test
        result = celeryadapter.build_set_of_queues(
            steps=["all"], spec=None, specific_queues=specific_queues, verbose=False, app=celery_app
        )

        assert result == set(specific_queues)

    def test_build_set_of_queues_with_specific_queues_and_spec(self, celery_app):
        """
        Test the build_set_of_queues function with specific queues and a yaml spec provided as input.
        The specific queues provided here will have a mix of queues that exist in the spec and
        queues that do not exist in the spec. This should only return the queues that exist in the
        spec.
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

    def test_build_set_of_queues_with_steps_and_spec(self, celery_app):
        """
        Test the build_set_of_queues function with steps and a yaml spec provided as input.
        This should return the queues associated with the steps that we provide.
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

    def test_query_celery_queues(self):
        """
        Test the query_celery_queues function by providing it with a list of inactive queues.
        This should return a list of strings. Each string will give a message saying that a
        particular queue was inactive
        """
        # TODO Modify query_celery_queues so the output for a redis broker is the same
        # as the output for rabbit broker (this might be a celery issue)

    def test_build_csv_queue_info(self, worker_queue_map):
        """
        Test the build_csv_queue_info function by providing it with a fake query return
        and timestamp. This should return a formatted dict that will look like so:
        {'time': [<timestamp>], 'queue1_name:tasks': [0], 'queue1_name:consumers': [1]}
        """
        # Get a timestamp to be used in the test
        date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Initialize the expected output (we'll build the rest in the for loop below)
        expected_output = {"time": [date]}

        # Build the fake query return and the expected output
        query_return = []
        for queue in worker_queue_map.values():
            query_return.append((queue, 0, 1))
            expected_output[f"{queue}:tasks"] = ["0"]
            expected_output[f"{queue}:consumers"] = ["1"]

        # Run the test
        result = celeryadapter.build_csv_queue_info(query_return, date)
        assert result == expected_output

    def test_build_json_queue_info(self, worker_queue_map):
        """
        Test the build_json_queue_info function by providing it with a fake query return
        and timestamp. This should return a dictionary of the form:
        {<timestamp>: {"queue1_name": {"tasks": 0, "consumers": 1}}}
        """
        # Get a timestamp to be used in the test
        date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Initialize the expected output
        expected_output = {date: {}}

        # Build the fake query return and the expected output
        query_return = []
        for queue in worker_queue_map.values():
            query_return.append((queue, 0, 1))
            expected_output[date][queue] = {"tasks": 0, "consumers": 1}

        # Run the test
        result = celeryadapter.build_json_queue_info(query_return, date)
        assert result == expected_output

    def test_dump_celery_queue_info_csv(self, worker_queue_map):
        """
        Test the dump_celery_queue_info function which is essentially a wrapper for the
        build_json_queue_info and build_csv_queue_info functions. This test will
        create a csv file with queue info data.
        """
        # Initialize an output filepath and expected output dict
        outfile = f"{os.path.dirname(__file__)}/queue-info.csv"
        expected_output = {}

        # Build the fake query return
        query_return = []
        for queue in worker_queue_map.values():
            query_return.append((queue, 0, 1))
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
                assert dump_diff == {}
        finally:
            try:
                os.remove(outfile)
            except FileNotFoundError:
                pass

    def test_dump_celery_queue_info_json(self, worker_queue_map):
        """
        Test the dump_celery_queue_info function which is essentially a wrapper for the
        build_json_queue_info and build_csv_queue_info functions. This test will
        create a json file with queue info data.
        """
        # Initialize an output filepath and expected output dict

        outfile = f"{os.path.dirname(__file__)}/queue-info.json"
        expected_output = {}

        # Build the fake query return
        query_return = []
        for queue in worker_queue_map.values():
            query_return.append((queue, 0, 1))
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
                assert json_dump_diff == {}
        finally:
            try:
                os.remove(outfile)
            except FileNotFoundError:
                pass

    def test_celerize_queues(self, worker_queue_map):
        """
        Test the celerize_queues function. This should add the celery queue_tag
        to the front of the queues we provide it.
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
        result = celeryadapter.get_running_queues("celery_app")
        assert result == []

    def test_get_queues(self, celery_app):
        """
        Test the get_queues function with no queues active.
        This should return a tuple where the first entry is an empty dict
        and the second entry is an empty list.
        """
        queue_result, worker_result = celeryadapter.get_queues(celery_app)
        assert queue_result == {}
        assert worker_result == []
