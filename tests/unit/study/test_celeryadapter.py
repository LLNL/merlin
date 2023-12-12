###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.11.1.
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
"""
from time import sleep
from typing import Dict

import pytest
from celery import Celery
from celery.canvas import Signature

from merlin.config import Config
from merlin.study import celeryadapter


@pytest.mark.order(before="TestInactive")
class TestActive:
    """
    This class will test functions in the celeryadapter.py module.
    It will run tests where we need active queues/workers to interact with.

    NOTE: The tests in this class must be ran before the TestInactive class or else the
    Celery workers needed for this class don't start

    TODO: fix the bug noted above and then check if we still need pytest-order
    """

    def test_query_celery_queues(
        self, celery_app: Celery, launch_workers: "Fixture", worker_queue_map: Dict[str, str]  # noqa: F821
    ):
        """
        Test the query_celery_queues function by providing it with a list of active queues.
        This should return a dict where keys are queue names and values are more dicts containing
        the number of jobs and consumers in that queue.

        :param `celery_app`: A pytest fixture for the test Celery app
        :param launch_workers: A pytest fixture that launches celery workers for us to interact with
        :param worker_queue_map: A pytest fixture that returns a dict of workers and queues
        """
        # Set up a dummy configuration to use in the test
        dummy_config = Config({"broker": {"name": "redis"}})

        # Get the actual output
        queues_to_query = list(worker_queue_map.values())
        actual_queue_info = celeryadapter.query_celery_queues(queues_to_query, app=celery_app, config=dummy_config)

        # Ensure all 3 queues in worker_queue_map were queried before looping
        assert len(actual_queue_info) == 3

        # Ensure each queue has a worker attached
        for queue_name, queue_info in actual_queue_info.items():
            assert queue_name in worker_queue_map.values()
            assert queue_info == {"consumers": 1, "jobs": 0}

    def test_get_running_queues(self, launch_workers: "Fixture", worker_queue_map: Dict[str, str]):  # noqa: F821
        """
        Test the get_running_queues function with queues active.
        This should return a list of active queues.

        :param `launch_workers`: A pytest fixture that launches celery workers for us to interact with
        :param `worker_queue_map`: A pytest fixture that returns a dict of workers and queues
        """
        result = celeryadapter.get_running_queues("merlin_test_app", test_mode=True)
        assert sorted(result) == sorted(list(worker_queue_map.values()))

    def test_get_active_celery_queues(
        self, celery_app: Celery, launch_workers: "Fixture", worker_queue_map: Dict[str, str]  # noqa: F821
    ):
        """
        Test the get_active_celery_queues function with queues active.
        This should return a tuple where the first entry is a dict of queue info
        and the second entry is a list of worker names.

        :param `celery_app`: A pytest fixture for the test Celery app
        :param `launch_workers`: A pytest fixture that launches celery workers for us to interact with
        :param `worker_queue_map`: A pytest fixture that returns a dict of workers and queues
        """
        # Start the queues and run the test
        queue_result, worker_result = celeryadapter.get_active_celery_queues(celery_app)

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

    @pytest.mark.order(index=1)
    def test_check_celery_workers_processing_tasks(
        self,
        celery_app: Celery,
        sleep_sig: Signature,
        launch_workers: "Fixture",  # noqa: F821
    ):
        """
        Test the check_celery_workers_processing function with workers active and a task in a queue.
        This function will query workers for any tasks they're still processing. We'll send a
        a task that sleeps for 3 seconds to our workers before we run this test so that there should be
        a task for this function to find.

        NOTE: the celery app fixture shows strange behavior when using app.control.inspect() calls (which
        check_celery_workers_processing uses) so we have to run this test first in this class in order to
        have it run properly.

        :param celery_app: A pytest fixture for the test Celery app
        :param sleep_sig: A pytest fixture for a celery signature of a task that sleeps for 3 sec
        :param launch_workers: A pytest fixture that launches celery workers for us to interact with
        """
        # Our active workers/queues are test_worker_[0-2]/test_queue_[0-2] so we're
        # sending this to test_queue_0 for test_worker_0 to process
        queue_for_signature = "test_queue_0"
        sleep_sig.set(queue=queue_for_signature)
        result = sleep_sig.delay()

        # We need to give the task we just sent to the server a second to get picked up by the worker
        sleep(1)

        # Run the test now that the task should be getting processed
        active_queue_test = celeryadapter.check_celery_workers_processing([queue_for_signature], celery_app)
        assert active_queue_test is True

        # Now test that a queue without any tasks returns false
        # We sent the signature to task_queue_0 so task_queue_1 shouldn't have any tasks to find
        non_active_queue_test = celeryadapter.check_celery_workers_processing(["test_queue_1"], celery_app)
        assert non_active_queue_test is False

        # Wait for the worker to finish running the task
        result.get()


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
        assert result == []

    def test_get_active_celery_queues(self, celery_app: Celery):
        """
        Test the get_active_celery_queues function with no queues active.
        This should return a tuple where the first entry is an empty dict
        and the second entry is an empty list.

        :param `celery_app`: A pytest fixture for the test Celery app
        """
        queue_result, worker_result = celeryadapter.get_active_celery_queues(celery_app)
        assert queue_result == {}
        assert worker_result == []

    def test_check_celery_workers_processing_tasks(self, celery_app: Celery, worker_queue_map: Dict[str, str]):
        """
        Test the check_celery_workers_processing function with no workers active.
        This function will query workers for any tasks they're still processing. Since no workers are active
        this should return False.

        :param celery_app: A pytest fixture for the test Celery app
        """
        # Run the test now that the task should be getting processed
        result = celeryadapter.check_celery_workers_processing(list(worker_queue_map.values()), celery_app)
        assert result is False
