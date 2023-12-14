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
This module contains pytest fixtures to be used throughout the entire test suite.
"""
import os
from glob import glob
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

SERVER_PASS = "merlin-test-server"


#######################################
#### Helper Functions for Fixtures ####
#######################################


def create_pass_file(pass_filepath: str):
    """
    Check if a password file already exists (it will if the redis server has been started)
    and if it hasn't then create one and write the password to the file.

    :param pass_filepath: The path to the password file that we need to check for/create
    """
    if not os.path.exists(pass_filepath):
        with open(pass_filepath, "w") as pass_file:
            pass_file.write(SERVER_PASS)


def create_encryption_file(key_filepath: str, encryption_key: bytes, app_yaml_filepath: str = None):
    """
    Check if an encryption file already exists (it will if the redis server has been started)
    and if it hasn't then create one and write the encryption key to the file. If an app.yaml
    filepath has been passed to this function then we'll need to update it so that the encryption
    key points to the `key_filepath`.

    :param key_filepath: The path to the file that will store our encryption key
    :param encryption_key: An encryption key to be used for testing
    :param app_yaml_filepath: A path to the app.yaml file that needs to be updated
    """
    if not os.path.exists(key_filepath):
        with open(key_filepath, "w") as key_file:
            key_file.write(encryption_key.decode("utf-8"))

    if app_yaml_filepath is not None:
        # Load up the app.yaml that was created by starting the server
        with open(app_yaml_filepath, "r") as app_yaml_file:
            app_yaml = yaml.load(app_yaml_file, yaml.Loader)
        
        # Modify the path to the encryption key and then save it
        app_yaml["results_backend"]["encryption_key"] = key_filepath
        with open(app_yaml_filepath, "w") as app_yaml_file:
            yaml.dump(app_yaml, app_yaml_file)


def set_config(broker: Dict[str, str], results_backend: Dict[str, str]):
    """
    Given configuration options for the broker and results_backend, update
    the CONFIG object.

    :param broker: A dict of the configuration settings for the broker
    :param results_backend: A dict of configuration settings for the results_backend
    """
    global CONFIG

    # Set the broker configuration for testing
    CONFIG.broker.password = broker["password"]
    CONFIG.broker.port = broker["port"]
    CONFIG.broker.server = broker["server"]
    CONFIG.broker.username = broker["username"]
    CONFIG.broker.vhost = broker["vhost"]
    CONFIG.broker.name = broker["name"]

    # Set the results_backend configuration for testing
    CONFIG.results_backend.password = results_backend["password"]
    CONFIG.results_backend.port = results_backend["port"]
    CONFIG.results_backend.server = results_backend["server"]
    CONFIG.results_backend.username = results_backend["username"]
    CONFIG.results_backend.encryption_key = results_backend["encryption_key"]


#######################################
######### Fixture Definitions #########
#######################################


#######################################
# Loading in Module Specific Fixtures #
#######################################
pytest_plugins = [
    fixture_file.replace("/", ".").replace(".py", "") for fixture_file in glob("tests/fixtures/[!__]*.py", recursive=True)
]

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
    server_dir = f"{temp_output_dir}/merlin_server"
    if not os.path.exists(server_dir):
        os.mkdir(server_dir)
    return server_dir


@pytest.fixture(scope="session")
def redis_server(merlin_server_dir: str, test_encryption_key: bytes) -> str:  # pylint: disable=redefined-outer-name
    """
    Start a redis server instance that runs on localhost:6379. This will yield the
    redis server uri that can be used to create a connection with celery.

    :param merlin_server_dir: The directory to the merlin test server configuration
    :param test_encryption_key: An encryption key to be used for testing
    :yields: The local redis server uri
    """
    with RedisServerManager(merlin_server_dir, SERVER_PASS) as redis_server_manager:
        redis_server_manager.initialize_server()
        redis_server_manager.start_server()
        create_encryption_file(f"{merlin_server_dir}/encrypt_data_key", test_encryption_key, app_yaml_filepath=f"{merlin_server_dir}/app.yaml")
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


@pytest.fixture(scope="function")
def redis_config(merlin_server_dir: str, test_encryption_key: bytes):  # pylint: disable=redefined-outer-name
    """
    This fixture is intended to be used for testing any functionality in the codebase
    that uses the CONFIG object with a Redis broker and results_backend.

    :param merlin_server_dir: The directory to the merlin test server configuration
    :param test_encryption_key: An encryption key to be used for testing
    """
    global CONFIG

    # Create a copy of the CONFIG option so we can reset it after the test
    orig_config = copy(CONFIG)

    # Create a password file and encryption key file (if they don't already exist)
    pass_file = f"{merlin_server_dir}/redis.pass"
    key_file = f"{merlin_server_dir}/encrypt_data_key"
    create_pass_file(pass_file)
    create_encryption_file(key_file, test_encryption_key)

    # Create the broker and results_backend configuration to use
    broker = {
        "cert_reqs": "none",
        "password": pass_file,
        "port": 6379,
        "server": "127.0.0.1",
        "username": "default",
        "vhost": "host4testing",
        "name": "redis",
    }

    results_backend = {
        "cert_reqs": "none",
        "db_num": 0,
        "encryption_key": key_file,
        "password": pass_file,
        "port": 6379,
        "server": "127.0.0.1",
        "username": "default",
        "name": "redis",
    }

    # Set the configuration
    set_config(broker, results_backend)

    # Go run the tests
    yield

    # Reset the configuration
    CONFIG.celery = orig_config.celery
    CONFIG.broker = orig_config.broker
    CONFIG.results_backend = orig_config.results_backend
