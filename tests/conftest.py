##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module contains pytest fixtures to be used throughout the entire test suite.
"""
import os
import sys
from copy import copy
from glob import glob
from time import sleep

import pytest
import yaml
from _pytest.tmpdir import TempPathFactory
from celery import Celery
from redis import Redis

from merlin.config.configfile import CONFIG
from tests.constants import CERT_FILES, SERVER_PASS
from tests.context_managers.celery_workers_manager import CeleryWorkersManager
from tests.context_managers.server_manager import RedisServerManager
from tests.fixture_data_classes import RedisBrokerAndBackend
from tests.fixture_types import (
    FixtureBytes,
    FixtureCallable,
    FixtureCelery,
    FixtureDict,
    FixtureModification,
    FixtureRedis,
    FixtureSignature,
    FixtureStr,
)
from tests.utils import create_cert_files, create_pass_file


# pylint: disable=redefined-outer-name


#######################################
# Loading in Module Specific Fixtures #
#######################################

fixture_glob = os.path.join("tests", "fixtures", "**", "*.py")
pytest_plugins = [
    fixture_file.replace(os.sep, ".").replace(".py", "")
    for fixture_file in glob(fixture_glob, recursive=True)
    if not fixture_file.endswith("__init__.py")
]


#######################################
#### Helper Functions for Fixtures ####
#######################################


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


def setup_redis_config(config_type: str, merlin_server_dir: str):
    """
    Sets up the Redis configuration for either broker or results backend.

    Args:
        config_type: The type of configuration to set up ('broker' or 'results_backend').
        merlin_server_dir: The directory to the merlin test server configuration.
    """
    port = 6379
    name = "redis"
    pass_file = os.path.join(merlin_server_dir, "redis.pass")
    create_pass_file(pass_file)

    if config_type == "broker":
        CONFIG.broker.password = pass_file
        CONFIG.broker.port = port
        CONFIG.broker.name = name
    elif config_type == "results_backend":
        CONFIG.results_backend.password = pass_file
        CONFIG.results_backend.port = port
        CONFIG.results_backend.name = name
    else:
        raise ValueError("Invalid config_type. Must be 'broker' or 'results_backend'.")


#######################################
######### Fixture Definitions #########
#######################################


@pytest.fixture(scope="session")
def path_to_test_specs() -> FixtureStr:
    """
    Fixture to provide the path to the directory containing test specifications.

    This fixture returns the absolute path to the 'test_specs' directory
    within the 'integration' folder of the test directory. It expands
    environment variables and user home directory as necessary.

    Returns:
        The absolute path to the 'test_specs' directory.
    """
    path_to_test_dir = os.path.abspath(os.path.expandvars(os.path.expanduser(os.path.dirname(__file__))))
    return os.path.join(path_to_test_dir, "integration", "test_specs")


@pytest.fixture(scope="session")
def path_to_merlin_codebase() -> FixtureStr:
    """
    Fixture to provide the path to the directory containing the Merlin code.

    This fixture returns the absolute path to the 'merlin' directory at the
    top level of this repository. It expands environment variables and user
    home directory as necessary.

    Returns:
        The absolute path to the 'merlin' directory.
    """
    path_to_test_dir = os.path.abspath(os.path.expandvars(os.path.expanduser(os.path.dirname(__file__))))
    return os.path.join(path_to_test_dir, "..", "merlin")


@pytest.fixture(scope="session")
def create_testing_dir() -> FixtureCallable:
    """
    Fixture to create a temporary testing directory.

    Returns:
        A function that creates the testing directory.
    """

    def _create_testing_dir(base_dir: str, sub_dir: str) -> str:
        """
        Helper function to create a temporary testing directory.

        Args:
            base_dir: The base directory where the testing directory will be created.
            sub_dir: The name of the subdirectory to create.

        Returns:
            The path to the created testing directory.
        """
        testing_dir = os.path.join(base_dir, sub_dir)
        if not os.path.exists(testing_dir):
            os.makedirs(testing_dir)  # Use makedirs to create intermediate directories if needed
        return testing_dir

    return _create_testing_dir


@pytest.fixture(scope="session")
def temp_output_dir(tmp_path_factory: TempPathFactory) -> FixtureStr:
    """
    This fixture will create a temporary directory to store output files of integration tests.
    The temporary directory will be stored at /tmp/`whoami`/pytest-of-`whoami`/. There can be at most
    3 temp directories in this location so upon the 4th test run, the 1st temp directory will be removed.

    :param tmp_path_factory: A built in factory with pytest to help create temp paths for testing
    :yields: The path to the temp output directory we'll use for this test run
    """
    # Log the cwd, then create and move into the temporary one
    cwd = os.getcwd()
    temp_integration_outfile_dir = tmp_path_factory.mktemp(
        f"python_{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}_"
    )
    os.chdir(temp_integration_outfile_dir)

    yield temp_integration_outfile_dir

    # Move back to the directory we started at
    os.chdir(cwd)


@pytest.fixture(scope="session")
def merlin_server_dir(temp_output_dir: FixtureStr) -> FixtureStr:
    """
    The path to the merlin_server directory that will be created by the `redis_server` fixture.

    :param temp_output_dir: The path to the temporary output directory we'll be using for this test run
    :returns: The path to the merlin_server directory that will be created by the `redis_server` fixture
    """
    server_dir = os.path.join(temp_output_dir, "merlin_server")
    if not os.path.exists(server_dir):
        os.mkdir(server_dir)
    return server_dir


@pytest.fixture(scope="session")
def redis_server(merlin_server_dir: FixtureStr, test_encryption_key: FixtureBytes) -> FixtureStr:
    """
    Start a redis server instance that runs on localhost:6379. This will yield the
    redis server uri that can be used to create a connection with celery.

    :param merlin_server_dir: The directory to the merlin test server configuration
    :param test_encryption_key: An encryption key to be used for testing
    :yields: The local redis server uri
    """
    os.environ["CELERY_ENV"] = "test"
    with RedisServerManager(merlin_server_dir, SERVER_PASS) as redis_server_manager:
        redis_server_manager.initialize_server()
        redis_server_manager.start_server()
        create_encryption_file(
            os.path.join(merlin_server_dir, "encrypt_data_key"),
            test_encryption_key,
            app_yaml_filepath=os.path.join(merlin_server_dir, "app.yaml"),
        )
        # Yield the redis_server uri to any fixtures/tests that may need it
        yield redis_server_manager.redis_server_uri
        # The server will be stopped once this context reaches the end of it's execution here


@pytest.fixture(scope="session")
def redis_client(redis_server: FixtureStr) -> FixtureRedis:
    """
    Fixture that provides a Redis client instance for the test session.
    It connects to this client using the url created from the `redis_server`
    fixture.

    Args:
        redis_server: The redis server uri we'll use to connect to redis

    Returns:
        An instance of the Redis client that can be used to interact
            with the Redis server.
    """
    return Redis.from_url(url=redis_server)


@pytest.fixture(scope="session")
def celery_app(redis_server: FixtureStr) -> FixtureCelery:
    """
    Create the celery app to be used throughout our integration tests.

    :param redis_server: The redis server uri we'll use to connect to redis
    :returns: The celery app object we'll use for testing
    """
    os.environ["CELERY_ENV"] = "test"
    return Celery("merlin_test_app", broker=redis_server, backend=redis_server)


@pytest.fixture(scope="session")
def sleep_sig(celery_app: FixtureCelery) -> FixtureSignature:
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
def worker_queue_map() -> FixtureDict[str, str]:
    """
    Worker and queue names to be used throughout tests

    :returns: A dict of dummy worker/queue associations
    """
    return {f"test_worker_{i}": f"test_queue_{i}" for i in range(3)}


@pytest.fixture(scope="class")
def launch_workers(celery_app: FixtureCelery, worker_queue_map: FixtureDict[str, str]):
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
def test_encryption_key() -> FixtureBytes:
    """
    An encryption key to be used for tests that need it.

    :returns: The test encryption key
    """
    return b"Q3vLp07Ljm60ahfU9HwOOnfgGY91lSrUmqcTiP0v9i0="


#######################################
########### CONFIG Fixtures ###########
#######################################
#    These are intended to be used    #
#   either by themselves or together  #
#  For example, you can use a rabbit  #
#  broker config and a redis results  #
#       backend config together       #
#######################################
############ !!!WARNING!!! ############
#   DO NOT USE THE `config` FIXTURE   #
#   IN A TEST; IT HAS UNSET VALUES    #
#######################################


def _config(merlin_server_dir: FixtureStr, test_encryption_key: FixtureBytes):
    """
    Sets up the configuration for testing purposes by modifying the global CONFIG object.

    This helper function prepares the broker and results backend configurations for testing
    by creating necessary encryption key files and resetting the CONFIG object to its
    original state after the tests are executed.

    Args:
        merlin_server_dir: The directory to the merlin test server configuration
        test_encryption_key: An encryption key to be used for testing

    Yields:
        This function yields control back to the test function, allowing tests to run
            with the modified CONFIG settings.
    """
    # Create a copy of the CONFIG option so we can reset it after the test
    orig_config = copy(CONFIG)

    # Create an encryption key file (if it doesn't already exist)
    key_file = os.path.join(merlin_server_dir, "encrypt_data_key")
    create_encryption_file(key_file, test_encryption_key)

    # Set the broker configuration for testing
    CONFIG.broker.password = None  # This will be updated in `redis_broker_config_*` or `rabbit_broker_config`
    CONFIG.broker.port = None  # This will be updated in `redis_broker_config_*` or `rabbit_broker_config`
    CONFIG.broker.name = None  # This will be updated in `redis_broker_config_*` or `rabbit_broker_config`
    CONFIG.broker.server = "127.0.0.1"
    CONFIG.broker.username = "default"
    CONFIG.broker.vhost = "host4testing"
    CONFIG.broker.cert_reqs = "none"

    # Set the results_backend configuration for testing
    CONFIG.results_backend.password = (
        None  # This will be updated in `redis_results_backend_config_function` or `mysql_results_backend_config`
    )
    CONFIG.results_backend.port = None  # This will be updated in `redis_results_backend_config_function`
    CONFIG.results_backend.name = (
        None  # This will be updated in `redis_results_backend_config_function` or `mysql_results_backend_config`
    )
    CONFIG.results_backend.dbname = None  # This will be updated in `mysql_results_backend_config`
    CONFIG.results_backend.server = "127.0.0.1"
    CONFIG.results_backend.username = "default"
    CONFIG.results_backend.cert_reqs = "none"
    CONFIG.results_backend.encryption_key = key_file
    CONFIG.results_backend.db_num = 0

    # Go run the tests
    yield

    # Reset the configuration
    CONFIG.celery = orig_config.celery
    CONFIG.broker = orig_config.broker
    CONFIG.results_backend = orig_config.results_backend


@pytest.fixture(scope="function")
def config_function(merlin_server_dir: FixtureStr, test_encryption_key: FixtureBytes) -> FixtureModification:
    """
    Sets up the configuration for testing with a function scope.

    Warning:
        DO NOT USE THIS FIXTURE IN A TEST, USE ONE OF THE SERVER SPECIFIC CONFIGURATIONS
        (LIKE `redis_broker_config_function`, `rabbit_broker_config`, etc.) INSTEAD.

    This fixture modifies the global CONFIG object to prepare the broker and results backend
    configurations for testing. It creates necessary encryption key files and ensures that
    the original configuration is restored after the tests are executed.

    Args:
        merlin_server_dir: The directory to the merlin test server configuration
        test_encryption_key: An encryption key to be used for testing

    Yields:
        This function yields control back to the test function, allowing tests to run
            with the modified CONFIG settings.
    """
    yield from _config(merlin_server_dir, test_encryption_key)


@pytest.fixture(scope="class")
def config_class(merlin_server_dir: FixtureStr, test_encryption_key: FixtureBytes) -> FixtureModification:
    """
    Sets up the configuration for testing with a class scope.

    Warning:
        DO NOT USE THIS FIXTURE IN A TEST, USE ONE OF THE SERVER SPECIFIC CONFIGURATIONS
        (LIKE `redis_broker_config_class`, `rabbit_broker_config`, etc.) INSTEAD.

    This fixture modifies the global CONFIG object to prepare the broker and results backend
    configurations for testing. It creates necessary encryption key files and ensures that
    the original configuration is restored after the tests are executed.

    Args:
        merlin_server_dir: The directory to the merlin test server configuration
        test_encryption_key: An encryption key to be used for testing

    Yields:
        This function yields control back to the test function, allowing tests to run
            with the modified CONFIG settings.
    """
    yield from _config(merlin_server_dir, test_encryption_key)


@pytest.fixture(scope="function")
def redis_broker_config_function(
    merlin_server_dir: FixtureStr, config_function: FixtureModification  # pylint: disable=redefined-outer-name,unused-argument
) -> FixtureModification:
    """
    Fixture for configuring the Redis broker for testing with a function scope.

    This fixture sets up the CONFIG object to use a Redis broker for testing any functionality
    in the codebase that interacts with the broker. It modifies the configuration to point
    to the specified Redis broker settings.

    Args:
        merlin_server_dir: The directory to the merlin test server configuration.
        config_function: The fixture that sets up most of the CONFIG object for testing.

    Yields:
        This function yields control back to the test function, allowing tests to run
            with the modified CONFIG settings.
    """
    setup_redis_config("broker", merlin_server_dir)
    yield


@pytest.fixture(scope="class")
def redis_broker_config_class(
    merlin_server_dir: FixtureStr, config_class: FixtureModification  # pylint: disable=redefined-outer-name,unused-argument
) -> FixtureModification:
    """
    Fixture for configuring the Redis broker for testing with a class scope.

    This fixture sets up the CONFIG object to use a Redis broker for testing any functionality
    in the codebase that interacts with the broker. It modifies the configuration to point
    to the specified Redis broker settings.

    Args:
        merlin_server_dir: The directory to the merlin test server configuration.
        config_function: The fixture that sets up most of the CONFIG object for testing.

    Yields:
        This function yields control back to the test function, allowing tests to run
            with the modified CONFIG settings.
    """
    setup_redis_config("broker", merlin_server_dir)
    yield


@pytest.fixture(scope="function")
def redis_results_backend_config_function(
    merlin_server_dir: FixtureStr, config_function: FixtureModification  # pylint: disable=redefined-outer-name,unused-argument
) -> FixtureModification:
    """
    Fixture for configuring the Redis results backend for testing with a function scope.

    This fixture sets up the CONFIG object to use a Redis results backend for testing any
    functionality in the codebase that interacts with the results backend. It modifies the
    configuration to point to the specified Redis results backend settings.

    Args:
        merlin_server_dir: The directory to the merlin test server configuration.
        config_function: The fixture that sets up most of the CONFIG object for testing.

    Yields:
        This function yields control back to the test function, allowing tests to run
            with the modified CONFIG settings.
    """
    setup_redis_config("results_backend", merlin_server_dir)
    yield


@pytest.fixture(scope="class")
def redis_results_backend_config_class(
    merlin_server_dir: FixtureStr, config_class: FixtureModification  # pylint: disable=redefined-outer-name,unused-argument
) -> FixtureModification:
    """
    Fixture for configuring the Redis results backend for testing with a class scope.

    This fixture sets up the CONFIG object to use a Redis results backend for testing any
    functionality in the codebase that interacts with the results backend. It modifies the
    configuration to point to the specified Redis results backend settings.

    Args:
        merlin_server_dir: The directory to the merlin test server configuration.
        config_function: The fixture that sets up most of the CONFIG object for testing.

    Yields:
        This function yields control back to the test function, allowing tests to run
            with the modified CONFIG settings.
    """
    setup_redis_config("results_backend", merlin_server_dir)
    yield


@pytest.fixture(scope="function")
def rabbit_broker_config(
    merlin_server_dir: FixtureStr, config_function: FixtureModification  # pylint: disable=redefined-outer-name,unused-argument
) -> FixtureModification:
    """
    This fixture is intended to be used for testing any functionality in the codebase
    that uses the CONFIG object with a RabbitMQ broker.

    :param merlin_server_dir: The directory to the merlin test server configuration
    :param config: The fixture that sets up most of the CONFIG object for testing
    """
    pass_file = os.path.join(merlin_server_dir, "rabbit.pass")
    create_pass_file(pass_file)

    CONFIG.broker.password = pass_file
    CONFIG.broker.port = 5671
    CONFIG.broker.name = "rabbitmq"

    yield


@pytest.fixture(scope="function")
def mysql_results_backend_config(
    merlin_server_dir: FixtureStr, config_function: FixtureModification  # pylint: disable=redefined-outer-name,unused-argument
) -> FixtureModification:
    """
    This fixture is intended to be used for testing any functionality in the codebase
    that uses the CONFIG object with a MySQL results_backend.

    :param merlin_server_dir: The directory to the merlin test server configuration
    :param config: The fixture that sets up most of the CONFIG object for testing
    """
    pass_file = os.path.join(merlin_server_dir, "mysql.pass")
    create_pass_file(pass_file)

    create_cert_files(merlin_server_dir, CERT_FILES)

    CONFIG.results_backend.password = pass_file
    CONFIG.results_backend.name = "mysql"
    CONFIG.results_backend.dbname = "test_mysql_db"
    CONFIG.results_backend.keyfile = CERT_FILES["ssl_key"]
    CONFIG.results_backend.certfile = CERT_FILES["ssl_cert"]
    CONFIG.results_backend.ca_certs = CERT_FILES["ssl_ca"]

    yield


@pytest.fixture(scope="function")
def redis_broker_and_backend_function(
    redis_client: FixtureRedis,
    redis_server: FixtureStr,
    redis_broker_config_function: FixtureModification,
    redis_results_backend_config_function: FixtureModification,
):
    """
    Fixture for setting up Redis broker and backend for function-scoped tests.

    This fixture creates an instance of `RedisBrokerAndBackend`, which
    encapsulates all necessary Redis-related fixtures required for
    establishing connections to Redis as both a broker and a backend
    during function-scoped tests.

    Args:
        redis_client: A fixture that provides a client for interacting with the
            Redis server.
        redis_server: A fixture providing the connection string to the Redis
            server instance.
        redis_broker_config_function: A fixture that modifies the configuration
            to point to the Redis server used as the message broker for
            function-scoped tests.
        redis_results_backend_config_function: A fixture that modifies the
            configuration to point to the Redis server used for storing results
            in function-scoped tests.

    Returns:
        An instance containing the Redis client, server connection string, and
            configuration modifications for both the broker and backend.
    """
    return RedisBrokerAndBackend(
        client=redis_client,
        server=redis_server,
        broker_config=redis_broker_config_function,
        results_backend_config=redis_results_backend_config_function,
    )


@pytest.fixture(scope="class")
def redis_broker_and_backend_class(
    redis_client: FixtureRedis,
    redis_server: FixtureStr,
    redis_broker_config_class: FixtureModification,
    redis_results_backend_config_class: FixtureModification,
) -> RedisBrokerAndBackend:
    """
    Fixture for setting up Redis broker and backend for class-scoped tests.

    This fixture creates an instance of `RedisBrokerAndBackend`, which
    encapsulates all necessary Redis-related fixtures required for
    establishing connections to Redis as both a broker and a backend
    during class-scoped tests.

    Args:
        redis_client: A fixture that provides a client for interacting with the
            Redis server.
        redis_server: A fixture providing the connection string to the Redis
            server instance.
        redis_broker_config_function: A fixture that modifies the configuration
            to point to the Redis server used as the message broker for
            class-scoped tests.
        redis_results_backend_config_function: A fixture that modifies the
            configuration to point to the Redis server used for storing results
            in class-scoped tests.

    Returns:
        An instance containing the Redis client, server connection string, and
            configuration modifications for both the broker and backend.
    """
    return RedisBrokerAndBackend(
        client=redis_client,
        server=redis_server,
        broker_config=redis_broker_config_class,
        results_backend_config=redis_results_backend_config_class,
    )
