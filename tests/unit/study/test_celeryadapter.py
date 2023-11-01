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
import csv
import json
import os
from datetime import datetime
from typing import Dict

import celery
from deepdiff import DeepDiff

from merlin.config import Config
from merlin.spec.specification import MerlinSpec
from merlin.study import celeryadapter
from tests.conftest import celery_app, launch_workers, worker_queue_map


class TestActiveQueues:
    """
    This class will test queue related functions in the celeryadapter.py module.
    It will run tests where we need active queues to interact with.
    """

    def test_query_celery_queues(self, launch_workers: "Fixture"):
        """
        Test the query_celery_queues function by providing it with a list of active queues.
        This should return a list of tuples. Each tuple will contain information
        (name, num jobs, num consumers) for each queue that we provided.
        """
        # TODO Modify query_celery_queues so the output for a redis broker is the same
        # as the output for rabbit broker

    def test_get_running_queues(self, launch_workers: "Fixture", worker_queue_map: Dict[str, str]):
        """
        Test the get_running_queues function with queues active.
        This should return a list of active queues.

        :param `launch_workers`: A pytest fixture that launches celery workers for us to interact with
        :param `worker_queue_map`: A pytest fixture that returns a dict of workers and queues
        """
        result = celeryadapter.get_running_queues("test_app", test_mode=True)
        assert sorted(result) == sorted(list(worker_queue_map.values()))

    def test_get_queues_active(self, celery_app: celery.Celery, launch_workers: "Fixture", worker_queue_map: Dict[str, str]):
        """
        Test the get_queues function with queues active.
        This should return a tuple where the first entry is a dict of queue info
        and the second entry is a list of worker names.

        :param `celery_app`: A pytest fixture for the test Celery app
        :param `launch_workers`: A pytest fixture that launches celery workers for us to interact with
        :param `worker_queue_map`: A pytest fixture that returns a dict of workers and queues
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

    def test_query_celery_queues(self):
        """
        Test the query_celery_queues function by providing it with a list of inactive queues.
        This should return a list of strings. Each string will give a message saying that a
        particular queue was inactive
        """
        # TODO Modify query_celery_queues so the output for a redis broker is the same
        # as the output for rabbit broker

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
        result = celeryadapter.get_running_queues("test_app", test_mode=True)
        assert result == []

    def test_get_queues(self, celery_app: celery.Celery):
        """
        Test the get_queues function with no queues active.
        This should return a tuple where the first entry is an empty dict
        and the second entry is an empty list.

        :param `celery_app`: A pytest fixture for the test Celery app
        """
        queue_result, worker_result = celeryadapter.get_queues(celery_app)
        assert queue_result == {}
        assert worker_result == []
