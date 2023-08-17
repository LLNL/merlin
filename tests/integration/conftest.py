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
This module contains pytest fixtures to be used throughout the entire
integration test suite.
"""
import os
import pytest
import redis
import multiprocessing
import subprocess
from time import sleep

from celery import Celery

try:
    from urllib import quote
except ImportError:
    from urllib.parse import quote


class RedisServerError(Exception):
    """
    Exception to signal that the server wasn't pinged properly.
    """
    def __init__(self, message=None):
        super().__init__(message)


class ServerInitError(Exception):
    """
    Exception to signal that there was an error initializing the server.
    """
    def __init__(self, message=None):
        super().__init__(message)


@pytest.fixture(scope="session")
def temp_output_dir(tmp_path_factory):
    """
    This fixture will create a temporary directory to store output files of integration tests.
    The temporary directory will be stored at /tmp/`whoami`/pytest-of-`whoami`/. There can be at most
    3 temp directories in this location so upon the 4th test run, the 1st temp directory will be removed.

    :param `tmp_path_factory`: A built in factory with pytest to help create temp paths for testing
    :yields: The path to the temp output directory we'll use for this test run
    """
    print("inside temp output dir")
    # Log the cwd, then create and move into the temporary one
    cwd = os.getcwd()
    temp_integration_outfile_dir = tmp_path_factory.mktemp("integration_outfiles_")
    os.chdir(temp_integration_outfile_dir)

    yield temp_integration_outfile_dir

    # Move back to the directory we started at
    os.chdir(cwd)


@pytest.fixture(scope="session")
def merlin_server_dir(temp_output_dir):
    """
    This fixture will initialize the merlin server (i.e. create all the files we'll
    need to start up a local redis server). It will return the path to the directory
    containing the files needed for the server to start up.

    :param `temp_output_dir`: The path to the temporary output directory we'll be using for this test run
    :returns: The path to the merlin_server directory with the server configurations
    """
    print("inside merlin server dir")
    # Initialize the setup for the local redis server
    subprocess.run("merlin server init", shell=True, capture_output=True, text=True)

    # Check that the merlin server was initialized properly
    merlin_server_dir = f"{temp_output_dir}/merlin_server"
    if not os.path.exists(merlin_server_dir):
        raise ServerInitError("The merlin server was not initialized properly.")

    return merlin_server_dir


@pytest.fixture(scope="session")
def redis_pass(merlin_server_dir):
    """
    Once the merlin server has been initialized, a password file will have been
    written to the merlin server directory. This fixture will grab that password.

    :returns: The raw redis password from the redis.pass file in the merlin_server directory
    """
    print("inside redis pass")
    try:
        with open(f"{merlin_server_dir}/redis.pass", "r") as f:
            redis_password = f.readline().strip()
    except FileNotFoundError as e:
        raise ServerInitError(f"The redis.pass file was not created, likely a problem with the 'merlin server init' command. {e}")
    return redis_password


@pytest.fixture(scope="session")
def redis_pass_celery_format(redis_pass):
    """
    Take the raw redis password and format for use with celery.
    See https://docs.python.org/3/library/urllib.parse.html#urllib.parse.quote for more info.

    :param `redis_pass`: The raw redis password read from the redis.pass file
    :returns: A formatted redis password that can be used with celery
    """
    return quote(redis_pass, safe="")


@pytest.fixture(scope="session")
def redis_server(redis_pass, redis_pass_celery_format):
    """
    Start a redis server instance that runs on localhost:6379. This will yield the
    redis server uri that can be used to create a connection with celery.

    :param `redis_pass`: The raw redis password stored in the redis.pass file
    :param `redis_pass_celery_format`: The redis password formatted to work with celery
    :yields: The value of `redis_pass_celery_format`
    """
    print("inside redis server")
    # Start the local redis server
    try:
        subprocess.run(
            "merlin server start",
            shell=True,
            capture_output=True,
            text=True,
            timeout=5
        )
    except subprocess.TimeoutExpired:
        pass
    
    # Ensure the server started properly
    host = "localhost"
    port = 6379
    db = 0
    username = "default"
    redis_client = redis.Redis(host=host, port=port, db=db, password=redis_pass, username=username)
    if not redis_client.ping():
        raise RedisServerError("The redis server could not be pinged. Check that the server is running with 'ps ux'.")

    # Hand over the redis server url to any other fixtures/tests that need it
    redis_server = f"redis://{username}:{redis_pass_celery_format}@{host}:{port}/{db}"
    yield redis_server

    # Kill the server; don't run this until all tests are done (accomplished with 'yield' above)
    kill_process = subprocess.run("merlin server stop", shell=True, capture_output=True, text=True)
    assert "Merlin server terminated." in kill_process.stderr


@pytest.fixture(scope="session")
def celery_app(redis_server):
    """
    Create the celery app to be used throughout our integration tests.

    :param `redis_server`: The redis server uri we'll use to connect to redis
    """
    print("inside celery app")
    return Celery("test_app", broker=redis_server)


@pytest.fixture(scope="session")
def worker_queue_map():
    """Worker and queue names to be used throughout tests"""
    return {f"test_worker_{i}": f"test_queue_{i}" for i in range(3)}


def start_workers(app, name, queue):
    """
    This is where the workers are actually started. Each worker maintains control of a process until
    we tell it to stop, that's why we have to use the multiprocessing library for this. For each worker,
    the output of it's logs are sent to /tmp/`whoami`/pytest-of-`whoami`/pytest-current/integration_outfiles_current/
    under a file with a name similar to: test_worker_*_<pid>.log.
    NOTE: pytest-current/ will have the results of the most recent test run. If you want to see a previous run
    check under pytest-<integer value>/. HOWEVER, only the 3 most recent test runs will be saved.

    :param `app`: The celery app fixture that's connected to our redis server
    :param `name`: The name of the worker we're going to start
    :param `queue`: The name of the queue that we're going to make this worker watch
    """
    pid = os.getpid()
    app.worker_main(["worker", "-n", name, "-Q", queue, "--concurrency", "1", f"--logfile={name}_{pid}.log"])


def are_workers_ready(app, num_workers, verbose=False) -> bool:
    """
    Check to see if the workers are up and running yet.

    :param `app`: The celery app fixture that's connected to our redis server
    :param `num_workers`: An int representing the number of workers we're looking to have started
    :param `verbose`: If true, enable print statements to show where we're at in execution
    :returns: True if all workers are running. False otherwise.
    """
    app_stats = app.control.inspect().stats()
    if verbose:
        print(f"app_stats: {app_stats}")
    return app_stats is not None and len(app_stats) == num_workers


def wait_for_worker_launch(app, num_workers, verbose=False):
    """
    Poll the workers over a fixed interval of time. If the workers don't show up
    within the time limit then we'll raise a timeout error. Otherwise, the workers
    are up and running and we can continue with our tests.

    :param `app`: The celery app fixture that's connected to our redis server
    :param `num_workers`: An int representing the number of workers we're looking to have started
    :param `verbose`: If true, enable print statements to show where we're at in execution
    """
    # Wait until all workers are ready
    max_wait_time = 2  # Maximum wait time in seconds
    wait_interval = 0.5  # Interval between checks in seconds
    waited_time = 0

    if verbose:
        print("waiting for workers to launch...")

    while not are_workers_ready(app, num_workers, verbose=verbose) and waited_time < max_wait_time:
        sleep(wait_interval)
        waited_time += wait_interval

    # If all workers are not ready after the maximum wait time
    if not are_workers_ready(app, num_workers, verbose=verbose):
        raise TimeoutError("Celery workers did not start within the expected time.")
    
    if verbose:
        print("workers launched")


def shutdown_processes(processes):
    """
    Given a list of processes, shut them all down.

    :param `processes`: A list of processes to terminate
    """
    for process in processes:
        # Try to terminate the process gracefully
        process.terminate()
        process_exit_code = process.join(timeout=3)

        # If it won't terminate then force kill it
        if process_exit_code is None:
            process.kill()


@pytest.fixture(scope="class")
def launch_workers(celery_app, worker_queue_map):
    """
    Launch the workers on the celery app using the worker and queue names
    defined in the worker_queue_map fixture.

    :param `celery_app`: The celery app fixture that's connected to our redis server
    :param `worker_queue_map`: A dict where the keys are worker names and the values are queue names
    """
    print("inside launch workers")
    # Create the processes that will start the workers and store them in a list
    processes = []
    for worker, queue in worker_queue_map.items():
        process = multiprocessing.Process(target=start_workers, args=(celery_app, worker, queue))
        processes.append(process)
        process.start()

    # Ensure that the workers start properly before letting tests use them
    try:
        num_workers = len(worker_queue_map)
        wait_for_worker_launch(celery_app, num_workers, verbose=True)
    except TimeoutError as e:
        # If workers don't launch in time, we need to make sure these processes stop
        shutdown_processes(processes)
        raise e

    # Give control to the tests that need to use workers
    yield

    # Shut down the workers and terminate the processes
    celery_app.control.broadcast("shutdown", destination=list(worker_queue_map.keys()))
    shutdown_processes(processes)
