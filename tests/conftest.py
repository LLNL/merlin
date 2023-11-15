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
This module contains pytest fixtures to be used throughout the entire
integration test suite.
"""
import multiprocessing
import os
import subprocess
from time import sleep
from typing import Dict, List

import pytest
import redis
from _pytest.tmpdir import TempPathFactory
from celery import Celery


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
        subprocess.run("export LC_ALL='C'; merlin server start", shell=True, capture_output=True, text=True, timeout=5)
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
    return Celery("test_app", broker=redis_server, backend=redis_server)


@pytest.fixture(scope="session")
def worker_queue_map() -> Dict[str, str]:
    """
    Worker and queue names to be used throughout tests

    :returns: A dict of dummy worker/queue associations
    """
    return {f"test_worker_{i}": f"test_queue_{i}" for i in range(3)}


def are_workers_ready(app: Celery, num_workers: int, verbose: bool = False) -> bool:
    """
    Check to see if the workers are up and running yet.

    :param app: The celery app fixture that's connected to our redis server
    :param num_workers: An int representing the number of workers we're looking to have started
    :param verbose: If true, enable print statements to show where we're at in execution
    :returns: True if all workers are running. False otherwise.
    """
    app_stats = app.control.inspect().stats()
    if verbose:
        print(f"app_stats: {app_stats}")
    return app_stats is not None and len(app_stats) == num_workers


def wait_for_worker_launch(app: Celery, num_workers: int, verbose: bool = False):
    """
    Poll the workers over a fixed interval of time. If the workers don't show up
    within the time limit then we'll raise a timeout error. Otherwise, the workers
    are up and running and we can continue with our tests.

    :param app: The celery app fixture that's connected to our redis server
    :param num_workers: An int representing the number of workers we're looking to have started
    :param verbose: If true, enable print statements to show where we're at in execution
    """
    max_wait_time = 2  # Maximum wait time in seconds
    wait_interval = 0.5  # Interval between checks in seconds
    waited_time = 0

    if verbose:
        print("waiting for workers to launch...")

    # Wait until all workers are ready
    while not are_workers_ready(app, num_workers, verbose=verbose) and waited_time < max_wait_time:
        sleep(wait_interval)
        waited_time += wait_interval

    # If all workers are not ready after the maximum wait time, raise an error
    if not are_workers_ready(app, num_workers, verbose=verbose):
        raise TimeoutError("Celery workers did not start within the expected time.")

    if verbose:
        print("workers launched")


def shutdown_processes(worker_processes: List[multiprocessing.Process], echo_processes: List[subprocess.Popen]):
    """
    Given lists of processes, shut them all down. Worker processes were created with the
    multiprocessing library and echo processes were created with the subprocess library,
    so we have to shut them down slightly differently.

    :param worker_processes: A list of worker processes to terminate
    :param echo_processes: A list of echo processes to terminate
    """
    # Worker processes were created with the multiprocessing library
    for worker_process in worker_processes:
        # Try to terminate the process gracefully
        worker_process.terminate()
        process_exit_code = worker_process.join(timeout=3)

        # If it won't terminate then force kill it
        if process_exit_code is None:
            worker_process.kill()

    # Gracefully terminate the echo processes
    for echo_process in echo_processes:
        echo_process.terminate()
        echo_process.wait()

    # The echo processes will spawn 3 sleep inf processes that we also need to kill
    subprocess.run("ps ux | grep 'sleep inf' | grep -v grep | awk '{print $2}' | xargs kill", shell=True)


def start_worker(app: Celery, worker_launch_cmd: List[str]):
    """
    This is where a worker is actually started. Each worker maintains control of a process until
    we tell it to stop, that's why we have to use the multiprocessing library for this. We have to use
    app.worker_main instead of the normal "celery -A <app name> worker" command to launch the workers
    since our celery app is created in a pytest fixture and is unrecognizable by the celery command.
    For each worker, the output of it's logs are sent to
    /tmp/`whoami`/pytest-of-`whoami`/pytest-current/integration_outfiles_current/ under a file with a name
    similar to: test_worker_*.log.
    NOTE: pytest-current/ will have the results of the most recent test run. If you want to see a previous run
    check under pytest-<integer value>/. HOWEVER, only the 3 most recent test runs will be saved.

    :param app: The celery app fixture that's connected to our redis server
    :param worker_launch_cmd: The command to launch a worker
    """
    app.worker_main(worker_launch_cmd)


@pytest.fixture(scope="class")
def launch_workers(celery_app: Celery, worker_queue_map: Dict[str, str]):  # pylint: disable=redefined-outer-name
    """
    Launch the workers on the celery app fixture using the worker and queue names
    defined in the worker_queue_map fixture.

    :param celery_app: The celery app fixture that's connected to our redis server
    :param worker_queue_map: A dict where the keys are worker names and the values are queue names
    """
    # Create the processes that will start the workers and store them in a list
    worker_processes = []
    echo_processes = []
    for worker, queue in worker_queue_map.items():
        worker_launch_cmd = ["worker", "-n", worker, "-Q", queue, "--concurrency", "1", f"--logfile={worker}.log"]

        # We have to use this dummy echo command to simulate a celery worker command that will show up with 'ps ux'
        # We'll sleep for infinity here and then kill this process during shutdown
        echo_process = subprocess.Popen(  # pylint: disable=consider-using-with
            f"echo 'celery test_app {' '.join(worker_launch_cmd)}'; sleep inf", shell=True
        )
        echo_processes.append(echo_process)

        # We launch workers in their own process since they maintain control of a process until we stop them
        worker_process = multiprocessing.Process(target=start_worker, args=(celery_app, worker_launch_cmd))
        worker_process.start()
        worker_processes.append(worker_process)

    # Ensure that the workers start properly before letting tests use them
    try:
        num_workers = len(worker_queue_map)
        wait_for_worker_launch(celery_app, num_workers, verbose=False)
    except TimeoutError as exc:
        # If workers don't launch in time, we need to make sure these processes stop
        shutdown_processes(worker_processes, echo_processes)
        raise exc

    # Give control to the tests that need to use workers
    yield

    # Shut down the workers and terminate the processes
    celery_app.control.broadcast("shutdown", destination=list(worker_queue_map.keys()))
    shutdown_processes(worker_processes, echo_processes)
