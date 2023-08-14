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

If tests are failing/erroring make sure that you don't have a local redis server
already up and running on 127.0.0.1.
"""
import csv
import json
import os
import subprocess
import unittest
from datetime import datetime
from time import sleep

from deepdiff import DeepDiff
from redis import Redis

from merlin.config import Config
from merlin.spec.specification import MerlinSpec
from merlin.study import celeryadapter
from tests.unit.study.dummy_app import dummy_app
from tests.unit.study.status_test_files.shared_tests import _format_csv_data
from tests.unit.study.status_test_files.status_test_variables import PATH_TO_TEST_FILES, SPEC_PATH


# If tests are failing, turn this to True to enable print statements
DEBUG_MODE = False


class DummyCelerySetup(unittest.TestCase):
    """
    This class will serve as a base class for unit tests that depend on having
    running celery related features (app, workers, queues, etc.)
    """

    @classmethod
    def setUpClass(cls):
        """
        Set up a redis server to connect to our dummy app one time that can be used for any test we need.
        """
        # Launch a local redis server that we'll connect our dummy celery app to
        # NOTE if the redis server is having trouble launching, make sure there isn't already one running on 127.0.0.1:6379
        cls.redis_server = subprocess.Popen(["redis-server"], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        # Wait for the redis server to spin up and then connect to it (no arguments will set host to localhost and port to 6379 by default)
        sleep(1)
        cls.redis_connection = Redis()

    @classmethod
    def tearDownClass(cls):
        """
        Destroy the redis server (and the dummy celery app by association) when all of the tests are done.
        """
        # Shut down the redis server that we started
        cls.redis_server.kill()
        cls.redis_server.wait()

        # We need to run this communicate command or else we get ResourceWarnings about unclosed Buffers
        stdout, stderr = cls.redis_server.communicate()
        if DEBUG_MODE:
            print(f"redis server stdout: {stdout}")
            print(f"redis server stderr: {stderr}")

        # Remove the dump file that's created by redis
        redis_dump_file = f"{os.path.dirname(__file__)}/dump.rdb"
        if os.path.exists(redis_dump_file):
            os.remove(redis_dump_file)

    def setUp(self):
        """
        Create names for test workers/queues that we'll launch if we need to.
        """
        # To store processes created during tests so that we can safely terminate them on tearDown
        self.processes = []

        # Create test worker and queue names
        self.worker_queue_map = {f"test_worker_{i}": f"test_queue_{i}" for i in range(3)}

    def tearDown(self):
        """
        This will be used to shutdown any workers/processes that we started during a test.
        This will be run even if no workers/processes were started. It's the safest way to make
        sure we don't accidentally leave workers/processes up.
        """
        # Send the shutdown signal to the workers and give them time to shut down gracefully
        dummy_app.control.broadcast("shutdown", destination=list(self.worker_queue_map.keys()))
        sleep(1)

        # Terminate the processes and wait for them to be done
        for process in self.processes:
            process.kill()
            process.wait()

            # We need to run this communicate command or else we get ResourceWarnings about unclosed Buffers
            stdout, stderr = process.communicate()
            if DEBUG_MODE:
                print(f"worker launch stdout: {stdout}")
                print(f"worker launch stderr: {stderr}")

    def launch_workers(self):
        """
        Helper method to launch workers.
        """
        # Create launch commands for the workers in the worker_queue_map
        for worker_name, queue_name in self.worker_queue_map.items():
            if DEBUG_MODE:
                print(f"Running celery launch command for worker {worker_name}...")

            worker_launch_cmd = f"celery -A dummy_app worker --concurrency 1 -n {worker_name} -Q {queue_name}".split()
            process = subprocess.Popen(
                worker_launch_cmd,
                cwd=os.path.join(os.path.dirname(os.path.abspath(__file__))),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self.processes.append(process)

            if DEBUG_MODE:
                print(f"{worker_name} launch command sent to Popen.")

        # Before releasing control back to a test, wait for the workers to launch
        if DEBUG_MODE:
            print("waiting for workers to launch...")
        self.wait_for_worker_launch()
        if DEBUG_MODE:
            print("workers launched")

    def are_workers_ready(self) -> bool:
        """
        Check to see if the workers are up and running yet.

        :returns: True if both workers are running. False otherwise.
        """
        app_stats = dummy_app.control.inspect().stats()
        if DEBUG_MODE:
            print(f"app_stats: {app_stats}")
        return app_stats is not None and len(app_stats) == len(self.worker_queue_map)

    def wait_for_worker_launch(self):
        """
        Wait for the workers to be launched before doing anything.
        """
        # Wait until all workers are ready
        max_wait_time = 2  # Maximum wait time in seconds
        wait_interval = 0.5  # Interval between checks in seconds
        waited_time = 0

        while not self.are_workers_ready() and waited_time < max_wait_time:
            sleep(wait_interval)
            waited_time += wait_interval

        # If all workers are not ready after the maximum wait time
        if not self.are_workers_ready():
            raise TimeoutError("Celery workers did not start within the expected time.")


class TestQueueInformation(DummyCelerySetup):
    """
    This class will test all queue related functions of the celeryadapter module.

    It inherits from the DummyCelerySetup class since we will need to connect to celery and
    start up workers for some of these tests.
    """

    def test_build_set_of_queues_inactive(self):
        """
        Test the build_set_of_queues function with no queues active.
        This should return an empty set.
        """
        result = celeryadapter.build_set_of_queues(
            steps=["all"], spec=None, specific_queues=None, verbose=False, app=dummy_app
        )
        self.assertEqual(result, set())

    def test_build_set_of_queues_active(self):
        """
        Test the build_set_of_queues function with queues active.
        This should return a set of queues (the queues defined in setUp).
        """
        # Start the workers so we have something active
        self.launch_workers()

        # Run the test
        result = celeryadapter.build_set_of_queues(
            steps=["all"], spec=None, specific_queues=None, verbose=False, app=dummy_app
        )
        self.assertEqual(result, set(self.worker_queue_map.values()))

    def test_build_set_of_queues_with_spec(self):
        """
        Test the build_set_of_queues function with a spec provided as input.
        This should return a set of the queues defined in the spec file.
        """
        # Create the spec object that we'll pass to build_set_of_queues
        spec = MerlinSpec.load_specification(SPEC_PATH)

        # Run the test
        result = celeryadapter.build_set_of_queues(
            steps=["all"], spec=spec, specific_queues=None, verbose=False, app=dummy_app
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

        self.assertEqual(result, set(expected_output))

    def test_build_set_of_queues_with_specific_queues(self):
        """
        Test the build_set_of_queues function with specific queues provided as input.
        This should return a set of all the queues listed in the specific_queues argument.
        """
        # Build the list of specific queues to search for
        specific_queues = ["test_queue_1", "test_queue_2"]

        # Run the test
        result = celeryadapter.build_set_of_queues(
            steps=["all"], spec=None, specific_queues=specific_queues, verbose=False, app=dummy_app
        )

        self.assertEqual(result, set(specific_queues))

    def test_build_set_of_queues_with_specific_queues_and_spec(self):
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
            steps=["all"], spec=spec, specific_queues=valid_queues + invalid_queues, verbose=False, app=dummy_app
        )

        # Build the expected output list
        expected_output = [f"[merlin]_{queue}" for queue in valid_queues]

        self.assertEqual(result, set(expected_output))

    def test_build_set_of_queues_with_steps_and_spec(self):
        """
        Test the build_set_of_queues function with steps and a yaml spec provided as input.
        This should return the queues associated with the steps that we provide.
        """
        # Create the spec object that we'll pass to build_set_of_queues
        spec = MerlinSpec.load_specification(SPEC_PATH)

        # Build the list of steps to get queues from
        steps = ["cancel_step", "fail_step"]

        # Run the test
        result = celeryadapter.build_set_of_queues(steps=steps, spec=spec, specific_queues=None, verbose=False, app=dummy_app)

        # Build the expected output list
        merlin_tag = "[merlin]_"
        expected_output = ["cancel_queue", "fail_queue"]
        for i, output in enumerate(expected_output):
            expected_output[i] = f"{merlin_tag}{output}"

        self.assertEqual(result, set(expected_output))

    def test_query_celery_queues_all_active(self):
        """
        Test the query_celery_queues function by providing it with a list of active queues.
        This should return a list of tuples. Each tuple will contain information
        (name, num jobs, num consumers) for each queue that we provided.
        """
        # TODO Modify query_celery_queues so the output for a redis broker is the same
        # as the output for rabbit broker (this might be a celery issue)

    def test_query_celery_queues_all_inactive(self):
        """
        Test the query_celery_queues function by providing it with a list of inactive queues.
        This should return a list of strings. Each string will give a message saying that a
        particular queue was inactive
        """
        # TODO Modify query_celery_queues so the output for a redis broker is the same
        # as the output for rabbit broker (this might be a celery issue)

    def test_query_celery_queues_some_active(self):
        """
        Test the query_celery_queues function by providing it with a list of both active
        and inactive queues. This should return a list of strings and tuples.
        Each string will give a message saying that a particular queue was inactive.
        Each tuple will contain information (name, num jobs, num consumers) for each queue that we provided.
        """
        # TODO Modify query_celery_queues so the output for a redis broker is the same
        # as the output for rabbit broker (this might be a celery issue)

    def test_build_csv_queue_info(self):
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
        for queue in self.worker_queue_map.values():
            query_return.append((queue, 0, 1))
            expected_output[f"{queue}:tasks"] = ["0"]
            expected_output[f"{queue}:consumers"] = ["1"]

        # Run the test
        result = celeryadapter.build_csv_queue_info(query_return, date)
        self.assertEqual(result, expected_output)

    def test_build_json_queue_info(self):
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
        for queue in self.worker_queue_map.values():
            query_return.append((queue, 0, 1))
            expected_output[date][queue] = {"tasks": 0, "consumers": 1}

        # Run the test
        result = celeryadapter.build_json_queue_info(query_return, date)
        self.assertEqual(result, expected_output)

    def test_dump_celery_queue_info_csv(self):
        """
        Test the dump_celery_queue_info function which is essentially a wrapper for the
        build_json_queue_info and build_csv_queue_info functions. This test will
        create a csv file with queue info data.
        """
        # Initialize an output filepath and expected output dict
        outfile = f"{PATH_TO_TEST_FILES}/queue-info.csv"
        expected_output = {}

        # Build the fake query return
        query_return = []
        for queue in self.worker_queue_map.values():
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
                self.assertIn("time", csv_dump_data.fieldnames)

                # Format the csv data that we just read in
                csv_dump_output = _format_csv_data(csv_dump_data)

                # We did one dump so we should only have 1 timestamp; we don't care about the value
                self.assertEqual(len(csv_dump_output["time"]), 1)
                del csv_dump_output["time"]

                # Make sure the rest of the csv file was created as expected
                dump_diff = DeepDiff(csv_dump_output, expected_output)
                self.assertEqual(dump_diff, {})
        finally:
            try:
                os.remove(outfile)
            except FileNotFoundError:
                pass

    def test_dump_celery_queue_info_json(self):
        """
        Test the dump_celery_queue_info function which is essentially a wrapper for the
        build_json_queue_info and build_csv_queue_info functions. This test will
        create a json file with queue info data.
        """
        # Initialize an output filepath and expected output dict
        outfile = f"{PATH_TO_TEST_FILES}/queue-info.json"
        expected_output = {}

        # Build the fake query return
        query_return = []
        for queue in self.worker_queue_map.values():
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
                self.assertEqual(json_dump_diff, {})
        finally:
            try:
                os.remove(outfile)
            except FileNotFoundError:
                pass

    def test_celerize_queues(self):
        """
        Test the celerize_queues function. This should add the celery queue_tag
        to the front of the queues we provide it.
        """
        # Create variables to be used in the test
        queue_tag = "[merlin]_"
        queues_to_check = list(self.worker_queue_map.values())
        dummy_config = Config({"celery": {"queue_tag": queue_tag}})

        # Run the test
        celeryadapter.celerize_queues(queues_to_check, dummy_config)

        # Ensure the queue tag was added to every queue
        for queue in queues_to_check:
            self.assertIn(queue_tag, queue)

    def test_get_running_queues_inactive(self):
        """
        Test the get_running_queues function with no queues active.
        This should return an empty list.
        """
        result = celeryadapter.get_running_queues("dummy_app")
        self.assertEqual(result, [])

    def test_get_running_queues_active(self):
        """
        Test the get_running_queues function with queues active.
        This should return a list of active queues.
        """
        self.launch_workers()
        result = celeryadapter.get_running_queues("dummy_app")
        self.assertEqual(sorted(result), sorted(list(self.worker_queue_map.values())))

    def test_get_queues_inactive(self):
        """
        Test the get_queues function with no queues active.
        This should return a tuple where the first entry is an empty dict
        and the second entry is an empty list.
        """
        queue_result, worker_result = celeryadapter.get_queues(dummy_app)
        self.assertEqual(queue_result, {})
        self.assertEqual(worker_result, [])

    def test_get_queues_active(self):
        """
        Test the get_queues function with queues active.
        This should return a tuple where the first entry is a dict of queue info
        and the second entry is a list of worker names.
        """
        # Start the queues and run the test
        self.launch_workers()
        queue_result, worker_result = celeryadapter.get_queues(dummy_app)

        # Ensure we got output before looping
        self.assertEqual(len(queue_result), 3)
        self.assertEqual(len(worker_result), 3)

        for worker, queue in self.worker_queue_map.items():
            # Check that the entry in the queue_result dict for this queue is correct
            self.assertIn(queue, queue_result)
            self.assertEqual(len(queue_result[queue]), 1)
            self.assertIn(worker, queue_result[queue][0])

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
        self.assertEqual(queue_result, {})
        self.assertEqual(worker_result, [])


# TODO add tests for the rest of this module

if __name__ == "__main__":
    unittest.main()
