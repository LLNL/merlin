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
Module to define functionality for test workers and how to start/stop
them in their own processes.
"""
import multiprocessing
import os
import signal
import subprocess
from time import sleep
from types import TracebackType
from typing import Dict, List, Type

from celery import Celery


class CeleryTestWorkersManager:
    """
    A class to handle the setup and teardown of celery workers.
    This should be treated as a context and used with python's
    built-in 'with' statement. If you use it without this statement,
    beware that the processes spun up here may never be stopped.
    """

    def __init__(self, app: Celery):
        self.app = app
        self.running_workers = []
        self.worker_processes = {}
        self.echo_processes = {}

    def __enter__(self):
        """This magic method is necessary for allowing this class to be used as a context manager."""
        return self

    def __exit__(self, exc_type: Type[Exception], exc_value: Exception, traceback: TracebackType):
        """
        This will always run at the end of a context with statement, even if an error is raised.
        It's a safe way to ensure all of our subprocesses are stopped no matter what.
        """

        # Try to stop everything gracefully first
        self.stop_all_workers()

        # Check that all the worker processes were stopped, otherwise forcefully terminate them
        for worker_process in self.worker_processes.values():
            if worker_process.is_alive():
                worker_process.kill()

        # Check that all the echo processes were stopped, otherwise forcefully terminate them
        ps_proc = subprocess.run("ps ux", shell=True, capture_output=True, text=True)
        for pid in self.echo_processes.values():
            try:
                if str(pid) in ps_proc.stdout:
                    os.kill(pid, signal.SIGKILL)
            except ProcessLookupError as exc:
                raise ProcessLookupError(f"PID {pid} not found. Output of 'ps ux':\n{ps_proc.stdout}") from exc

    def _is_worker_ready(self, worker_name: str, verbose: bool = False) -> bool:
        """
        Check to see if the worker is up and running yet.

        :param worker_name: The name of the worker we're checking on
        :param verbose: If true, enable print statements to show where we're at in execution
        :returns: True if the worker is running. False otherwise.
        """
        ping = self.app.control.inspect().ping(destination=[f"celery@{worker_name}"])
        if verbose:
            print(f"ping: {ping}")
        return ping is not None and f"celery@{worker_name}" in ping

    def _wait_for_worker_launch(self, worker_name: str, verbose: bool = False):
        """
        Poll the worker over a fixed interval of time. If the worker doesn't show up
        within the time limit then we'll raise a timeout error. Otherwise, the worker
        is up and running and we can continue with our tests.

        :param worker_name: The name of the worker we're checking on
        :param verbose: If true, enable print statements to show where we're at in execution
        """
        max_wait_time = 2  # Maximum wait time in seconds
        wait_interval = 0.5  # Interval between checks in seconds
        waited_time = 0
        worker_ready = False

        if verbose:
            print(f"waiting for {worker_name} to launch...")

        # Wait until the worker is ready
        while waited_time < max_wait_time:
            if self._is_worker_ready(worker_name, verbose=verbose):
                worker_ready = True
                break

            sleep(wait_interval)
            waited_time += wait_interval

        if not worker_ready:
            raise TimeoutError("Celery workers did not start within the expected time.")

        if verbose:
            print(f"{worker_name} launched")

    def start_worker(self, worker_launch_cmd: List[str]):
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

        :param worker_launch_cmd: The command to launch a worker
        """
        self.app.worker_main(worker_launch_cmd)

    def launch_worker(self, worker_name: str, queues: List[str], concurrency: int = 1):
        """
        Launch a single worker. We'll add the process that the worker is running in to the list of worker processes.
        We'll also create an echo process to simulate a celery worker command that will show up with 'ps ux'.

        :param worker_name: The name to give to the worker
        :param queues: A list of queues that the worker will be watching
        :param concurrency: The concurrency value of the worker (how many child processes to have the worker spin up)
        """
        # Check to make sure we have a unique worker name so we can track all processes
        if worker_name in self.worker_processes:
            self.stop_all_workers()
            raise ValueError(f"The worker {worker_name} is already running. Choose a different name.")

        # Create the launch command for this worker
        worker_launch_cmd = [
            "worker",
            "-n",
            worker_name,
            "-Q",
            ",".join(queues),
            "--concurrency",
            str(concurrency),
            f"--logfile={worker_name}.log",
            "--loglevel=DEBUG",
        ]

        # Create an echo command to simulate a running celery worker since our celery worker will be spun up in
        # a different process and we won't be able to see it with 'ps ux' like we normally would
        echo_process = subprocess.Popen(  # pylint: disable=consider-using-with
            f"echo 'celery merlin_test_app {' '.join(worker_launch_cmd)}'; sleep inf",
            shell=True,
            preexec_fn=os.setpgrp,  # Make this the parent of the group so we can kill the 'sleep inf' that's spun up
        )
        self.echo_processes[worker_name] = echo_process.pid

        # Start the worker in a separate process since it'll take control of the entire process until we kill it
        worker_process = multiprocessing.Process(target=self.start_worker, args=(worker_launch_cmd,))
        worker_process.start()
        self.worker_processes[worker_name] = worker_process
        self.running_workers.append(worker_name)

        # Wait for the worker to launch properly
        try:
            self._wait_for_worker_launch(worker_name, verbose=False)
        except TimeoutError as exc:
            self.stop_all_workers()
            raise exc

    def launch_workers(self, worker_info: Dict[str, Dict]):
        """
        Launch multiple workers. This will call `launch_worker` to launch each worker
        individually.

        :param worker_info: A dict of worker info with the form
            {"worker_name": {"concurrency": <int>, "queues": <list of queue names>}}
        """
        for worker_name, worker_settings in worker_info.items():
            self.launch_worker(worker_name, worker_settings["queues"], worker_settings["concurrency"])

    def stop_worker(self, worker_name: str):
        """
        Stop a single running worker and its associated processes.

        :param worker_name: The name of the worker to shutdown
        """
        # Send a shutdown signal to the worker
        self.app.control.broadcast("shutdown", destination=[f"celery@{worker_name}"])

        # Try to terminate the process gracefully
        if self.worker_processes[worker_name] is not None:
            self.worker_processes[worker_name].terminate()
            process_exit_code = self.worker_processes[worker_name].join(timeout=3)

            # If it won't terminate then force kill it
            if process_exit_code is None:
                self.worker_processes[worker_name].kill()

        # Terminate the echo process and its sleep inf subprocess
        os.killpg(os.getpgid(self.echo_processes[worker_name]), signal.SIGTERM)
        sleep(2)

    def stop_all_workers(self):
        """
        Stop all of the running workers and the processes associated with them.
        """
        for worker_name in self.running_workers:
            self.stop_worker(worker_name)
