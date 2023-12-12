###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.12.2b1.
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
This module contains pytest fixtures to be used throughout the entire
integration test suite.
"""
import os
import subprocess
from glob import glob
from time import sleep
from typing import Dict

import pytest
import redis
from _pytest.tmpdir import TempPathFactory
from celery import Celery
from celery.canvas import Signature

from tests.celery_test_workers import CeleryTestWorkersManager


#######################################
# Loading in Module Specific Fixtures #
#######################################
pytest_plugins = [
    fixture_file.replace("/", ".").replace(".py", "") for fixture_file in glob("tests/fixtures/[!__]*.py", recursive=True)
]

from tests.encryption_manager import EncryptionManager


class RedisServerError(Exception):
    """
    Exception to signal that the server wasn't pinged properly.
    """


class ServerInitError(Exception):
    """
    Exception to signal that there was an error initializing the server.
    """


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
def redis_pass() -> str:
    """
    This fixture represents the password to the merlin test server.

    :returns: The redis password for our test server
    """
    return "merlin-test-server"


@pytest.fixture(scope="session")
def merlin_server_dir(temp_output_dir: str, redis_pass: str) -> str:  # pylint: disable=redefined-outer-name
    """
    This fixture will initialize the merlin server (i.e. create all the files we'll
    need to start up a local redis server). It will return the path to the directory
    containing the files needed for the server to start up.

    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
    :param redis_pass: The password to the test redis server that we'll create here
    :returns: The path to the merlin_server directory with the server configurations
    """
    # Initialize the setup for the local redis server
    # We'll also set the password to 'merlin-test-server' so it'll be easy to shutdown if there's an issue
    subprocess.run(f"merlin server init; merlin server config -pwd {redis_pass}", shell=True, capture_output=True, text=True)

    # Check that the merlin server was initialized properly
    server_dir = f"{temp_output_dir}/merlin_server"
    if not os.path.exists(server_dir):
        raise ServerInitError("The merlin server was not initialized properly.")

    return server_dir


@pytest.fixture(scope="session")
def redis_server(merlin_server_dir: str, redis_pass: str) -> str:  # pylint: disable=redefined-outer-name,unused-argument
    """
    Start a redis server instance that runs on localhost:6379. This will yield the
    redis server uri that can be used to create a connection with celery.

    :param merlin_server_dir: The directory to the merlin test server configuration.
        This will not be used here but we need the server configurations before we can
        start the server.
    :param redis_pass: The raw redis password stored in the redis.pass file
    :yields: The local redis server uri
    """
    # Start the local redis server
    try:
        # Need to set LC_ALL='C' before starting the server or else redis causes a failure
        subprocess.run("export LC_ALL='C'; merlin server start", shell=True, timeout=5)
    except subprocess.TimeoutExpired:
        pass

    # Ensure the server started properly
    host = "localhost"
    port = 6379
    database = 0
    username = "default"
    redis_client = redis.Redis(host=host, port=port, db=database, password=redis_pass, username=username)
    if not redis_client.ping():
        raise RedisServerError("The redis server could not be pinged. Check that the server is running with 'ps ux'.")

    # Hand over the redis server url to any other fixtures/tests that need it
    redis_server_uri = f"redis://{username}:{redis_pass}@{host}:{port}/{database}"
    yield redis_server_uri

    # Kill the server; don't run this until all tests are done (accomplished with 'yield' above)
    kill_process = subprocess.run("merlin server stop", shell=True, capture_output=True, text=True)
    assert "Merlin server terminated." in kill_process.stderr


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

    with CeleryTestWorkersManager(celery_app) as workers_manager:
        workers_manager.launch_workers(worker_info)
        yield


@pytest.fixture(scope="session")
def encryption_output_dir(temp_output_dir: str) -> str:  # pylint: disable=redefined-outer-name
    """
    Get a temporary output directory for our encryption tests.

    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
    """
    encryption_dir = f"{temp_output_dir}/encryption_tests"
    os.mkdir(encryption_dir)
    return encryption_dir


@pytest.fixture(scope="session")
def test_encryption_key() -> bytes:
    """An encryption key to be used for tests that need it"""
    return b"Q3vLp07Ljm60ahfU9HwOOnfgGY91lSrUmqcTiP0v9i0="


@pytest.fixture(scope="class")
def use_fake_encrypt_data_key(encryption_output_dir: str, test_encryption_key: bytes):  # pylint: disable=redefined-outer-name
    """
    Create a fake encrypt data key to use for these tests. This will save the
    current data key so we can set it back to what it was prior to running
    the tests.

    :param encryption_output_dir: The path to the temporary output directory we'll be using for this test run
    """
    # Use a context manager to ensure cleanup runs even if an error occurs
    with EncryptionManager(encryption_output_dir, test_encryption_key) as encrypt_manager:
        # Set the fake encryption key
        encrypt_manager.set_fake_key()
        # Yield control to the tests
        yield
