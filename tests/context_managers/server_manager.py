##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Module to define functionality for managing the containerized
server used for testing.
"""

import os
import signal
import subprocess
from types import TracebackType
from typing import Type

import redis
import yaml


class RedisServerError(Exception):
    """
    Exception to signal that the server wasn't pinged properly.
    """


class ServerInitError(Exception):
    """
    Exception to signal that there was an error initializing the server.
    """


class RedisServerManager:
    """
    A class to handle the setup and teardown of a containerized redis server.
    This should be treated as a context and used with python's built-in 'with'
    statement. If you use it without this statement, beware that the processes
    spun up here may never be stopped.
    """

    def __init__(self, server_dir: str, redis_pass: str):
        self._redis_pass = redis_pass
        self.server_dir = server_dir
        self.host = "localhost"
        self.port = 6379
        self.database = 0
        self.username = "default"
        self.redis_server_uri = f"redis://{self.username}:{self._redis_pass}@{self.host}:{self.port}/{self.database}"

    def __enter__(self):
        """This magic method is necessary for allowing this class to be used as a context manager"""
        return self

    def __exit__(self, exc_type: Type[Exception], exc_value: Exception, traceback: TracebackType):
        """
        This will always run at the end of a context with statement, even if an error is raised.
        It's a safe way to ensure all of our server gets stopped no matter what.
        """
        self.stop_server()

    def initialize_server(self):
        """
        Initialize the setup for the local redis server. We'll write the folder to:
        /tmp/`whoami`/pytest-of-`whoami`/pytest-current/python_{major}.{minor}.{micro}_current/
        We'll set the password to be 'merlin-test-server' so it'll be easy to shutdown if necessary
        """
        subprocess.run(
            f"merlin server init; merlin server config -pwd {self._redis_pass}", shell=True, capture_output=True, text=True
        )

        # Check that the merlin server was initialized properly
        if not os.path.exists(self.server_dir):
            raise ServerInitError("The merlin server was not initialized properly.")

    def start_server(self):
        """Attempt to start the local redis server."""
        try:
            # Need to set LC_ALL='C' before starting the server or else redis causes a failure
            subprocess.run("export LC_ALL='C'; merlin server start", shell=True, timeout=5)
        except subprocess.TimeoutExpired:
            pass

        # Ensure the server started properly
        redis_client = redis.Redis(
            host=self.host, port=self.port, db=self.database, password=self._redis_pass, username=self.username
        )
        if not redis_client.ping():
            raise RedisServerError("The redis server could not be pinged. Check that the server is running with 'ps ux'.")

    def stop_server(self):
        """Stop the server."""
        # Attempt to stop the server gracefully with `merlin server`
        kill_process = subprocess.run("merlin server stop", shell=True, capture_output=True, text=True)

        # Check that the server was terminated
        if "Merlin server terminated." not in kill_process.stderr:
            # If it wasn't, try to kill the process by using the pid stored in a file created by `merlin server`
            try:
                with open(os.path.join(self.server_dir, "merlin_server.pf"), "r") as process_file:
                    server_process_info = yaml.load(process_file, yaml.Loader)
                    os.kill(int(server_process_info["image_pid"]), signal.SIGKILL)
            # If the file can't be found then let's make sure there's even a redis-server process running
            except FileNotFoundError as exc:
                process_query = subprocess.run("ps ux", shell=True, text=True, capture_output=True)
                # If there is a file running we didn't start it in this test run so we can't kill it
                if "redis-server" in process_query.stdout:
                    raise RedisServerError(
                        "Found an active redis server but cannot stop it since there is no process file (merlin_server.pf). "
                        "Did you start this server before running tests?"
                    ) from exc
                # No else here. If there's no redis-server process found then there's nothing to stop
