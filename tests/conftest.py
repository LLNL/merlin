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
This module contains pytest fixtures to be used throughout the entire test suite.
"""
import os
import yaml
from copy import copy
from time import sleep
from typing import Any, Dict

import pytest
from _pytest.tmpdir import TempPathFactory
from celery import Celery
from celery.canvas import Signature

from merlin.config.configfile import CONFIG
from tests.context_managers.celery_workers_manager import CeleryWorkersManager
from tests.context_managers.server_manager import RedisServerManager

REDIS_PASS = "merlin-test-server"


@pytest.fixture(scope="session")
def temp_output_dir(tmp_path_factory: TempPathFactory) -> str:
    """
    This fixture will create a temporary directory to store output files of integration tests.
    The temporary directory will be stored at /tmp/`whoami`/pytest-of-`whoami`/. There can be at most
    3 temp directories in this location so upon the 4th test run, the 1st temp directory will be removed.

    :param tmp_path_factory: A built in factory with pytest to help create temp paths for testing
    :yields: The path to the temp output directory we'll use for this test run
    """
    # Log the cwd, then create and move into the temporary one
    cwd = os.getcwd()
    temp_integration_outfile_dir = tmp_path_factory.mktemp("integration_outfiles_")
    os.chdir(temp_integration_outfile_dir)

    yield temp_integration_outfile_dir

    # Move back to the directory we started at
    os.chdir(cwd)


@pytest.fixture(scope="session")
def merlin_server_dir(temp_output_dir: str) -> str:
    """
    The path to the merlin_server directory that will be created by the `redis_server` fixture.

    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
    :returns: The path to the merlin_server directory that will be created by the `redis_server` fixture
    """
    return f"{temp_output_dir}/merlin_server"


@pytest.fixture(scope="session")
def redis_server(merlin_server_dir: str, test_encryption_key: bytes) -> str:  # pylint: disable=redefined-outer-name
    """
    Start a redis server instance that runs on localhost:6379. This will yield the
    redis server uri that can be used to create a connection with celery.

    :param merlin_server_dir: The directory to the merlin test server configuration
    :param test_encryption_key: An encryption key to be used for testing
    :yields: The local redis server uri
    """
    with RedisServerManager(merlin_server_dir, REDIS_PASS, test_encryption_key) as redis_server_manager:
        redis_server_manager.initialize_server()
        redis_server_manager.start_server()
        # Yield the redis_server uri to any fixtures/tests that may need it
        yield redis_server_manager.redis_server_uri
        # The server will be stopped once this context reaches the end of it's execution here


@pytest.fixture(scope="session")
def celery_app(redis_server: str) -> Celery:  # pylint: disable=redefined-outer-name
    """
    Create the celery app to be used throughout our integration tests.

    :param redis_server: The redis server uri we'll use to connect to redis
    :returns: The celery app object we'll use for testing
    """
    return Celery("merlin_test_app", broker=redis_server, backend=redis_server)


@pytest.fixture(scope="session")
def sleep_sig(celery_app: Celery) -> Signature:  # pylint: disable=redefined-outer-name
    """
    Create a task registered to our celery app and return a signature for it.
    Once requested by a test, you can set the queue you'd like to send this to
    with `sleep_sig.set(queue=<queue name>)`. Here, <queue name> will likely be
    one of the queues defined in the `worker_queue_map` fixture.

    :param celery_app: The celery app object we'll use for testing
    :returns: A celery signature for a task that will sleep for 3 seconds
    """

    # Create a celery task that sleeps for 3 sec
    @celery_app.task
    def sleep_task():
        print("running sleep task")
        sleep(3)

    # Create a signature for this task
    return sleep_task.s()


@pytest.fixture(scope="session")
def worker_queue_map() -> Dict[str, str]:
    """
    Worker and queue names to be used throughout tests

    :returns: A dict of dummy worker/queue associations
    """
    return {f"test_worker_{i}": f"test_queue_{i}" for i in range(3)}


@pytest.fixture(scope="class")
def launch_workers(celery_app: Celery, worker_queue_map: Dict[str, str]):  # pylint: disable=redefined-outer-name
    """
    Launch the workers on the celery app fixture using the worker and queue names
    defined in the worker_queue_map fixture.

    :param celery_app: The celery app fixture that's connected to our redis server
    :param worker_queue_map: A dict where the keys are worker names and the values are queue names
    """
    # Format worker info in a format the our workers manager will be able to read
    # (basically just add in concurrency value to worker_queue_map)
    worker_info = {worker_name: {"concurrency": 1, "queues": [queue]} for worker_name, queue in worker_queue_map.items()}

    with CeleryWorkersManager(celery_app) as workers_manager:
        workers_manager.launch_workers(worker_info)
        yield


@pytest.fixture(scope="session")
def test_encryption_key() -> bytes:
    """
    An encryption key to be used for tests that need it.
    
    :returns: The test encryption key
    """
    return b"Q3vLp07Ljm60ahfU9HwOOnfgGY91lSrUmqcTiP0v9i0="


@pytest.fixture(scope="session")
def app_yaml(merlin_server_dir: str, redis_server: str) -> Dict[str, Any]:  # pylint: disable=redefined-outer-name
    """
    Load in the app.yaml file generated by starting the redis server.

    :param merlin_server_dir: The directory to the merlin test server configuration
    :param redis_server: The fixture that starts up the redis server
    :returns: The contents of the app.yaml file created by starting the redis server
    """
    with open(f"{merlin_server_dir}/app.yaml", "r") as app_yaml_file:
        app_yaml = yaml.load(app_yaml_file, yaml.Loader)
        return app_yaml


@pytest.fixture(scope="function")
def config(app_yaml: str):  # pylint: disable=redefined-outer-name
    """
    This fixture is intended to be used for testing any functionality in the codebase
    that uses the CONFIG object. This will modify the CONFIG object to use static test values
    that shouldn't change.

    :param app_yaml: The contents of the app.yaml created by starting the containerized redis server
    """
    global CONFIG
    orig_config = copy(CONFIG)

    CONFIG.broker.password = app_yaml["broker"]["password"]
    CONFIG.broker.port = app_yaml["broker"]["port"]
    CONFIG.broker.server = app_yaml["broker"]["server"]
    CONFIG.broker.username = app_yaml["broker"]["username"]
    CONFIG.broker.vhost = app_yaml["broker"]["vhost"]

    CONFIG.results_backend.password = app_yaml["results_backend"]["password"]
    CONFIG.results_backend.port = app_yaml["results_backend"]["port"]
    CONFIG.results_backend.server = app_yaml["results_backend"]["server"]
    CONFIG.results_backend.username = app_yaml["results_backend"]["username"]
    CONFIG.results_backend.encryption_key = app_yaml["results_backend"]["encryption_key"]

    yield

    CONFIG = orig_config
