###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.12.1.
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
import logging
import os
import subprocess
import time

import psutil

from merlin.managers.redis_connection import RedisConnectionManager


LOG = logging.getLogger(__name__)


class WorkerStatus:
    running = "Running"
    stalled = "Stalled"
    stopped = "Stopped"
    rebooting = "Rebooting"


WORKER_INFO = {
    "status": WorkerStatus.running,
    "pid": -1,
    "monitored": 1,  # This setting is for debug mode
    "num_unresponsive": 0,
    "processing_work": 1,
}


class CeleryManager:
    def __init__(self, query_frequency: int = 60, query_timeout: float = 0.5, worker_timeout: int = 180):
        """
        Initializer for Celery Manager

        :param query_frequency:     The frequency at which workers will be queried with ping commands
        :param query_timeout:       The timeout for the query pings that are sent to workers
        :param worker_timeout:      The sum total(query_frequency*tries) time before an attempt is made to restart worker.
        """
        self.query_frequency = query_frequency
        self.query_timeout = query_timeout
        self.worker_timeout = worker_timeout

    @staticmethod
    def get_worker_status_redis_connection() -> RedisConnectionManager:
        """Get the redis connection for info regarding the worker and manager status."""
        return RedisConnectionManager(1)

    @staticmethod
    def get_worker_args_redis_connection() -> RedisConnectionManager:
        """Get the redis connection for info regarding the args used to generate each worker."""
        return RedisConnectionManager(2)

    def get_celery_workers_status(self, workers: list) -> dict:
        """
        Get the worker status of a current worker that is being managed

        :param workers: Workers that are checked.
        :return:        The result dictionary for each worker and the response.
        """
        from merlin.celery import app

        celery_app = app.control
        ping_result = celery_app.ping(workers, timeout=self.query_timeout)
        worker_results = {worker: status for d in ping_result for worker, status in d.items()}
        return worker_results

    def stop_celery_worker(self, worker: str) -> bool:
        """
        Stop a celery worker by kill the worker with pid

        :param worker:  Worker that is being stopped.
        :return:        The result of whether a worker was stopped.
        """

        # Get the PID associated with the worker
        with self.get_worker_status_redis_connection() as worker_status_connect:
            worker_pid = int(worker_status_connect.hget(worker, "pid"))
            worker_status = worker_status_connect.hget(worker, "status")

        # TODO be wary of stalled state workers (should not happen since we use psutil.Process.kill())
        # Check to see if the pid exists and worker is set as running
        if worker_status == WorkerStatus.running and psutil.pid_exists(worker_pid):
            # Check to see if the pid is associated with celery
            worker_process = psutil.Process(worker_pid)
            if "celery" in worker_process.name():
                # Kill the pid if both conditions are right
                worker_process.kill()
                return True
        return False

    def restart_celery_worker(self, worker: str) -> bool:
        """
        Restart a celery worker with the same arguements and parameters during its creation

        :param worker:  Worker that is being restarted.
        :return:        The result of whether a worker was restarted.
        """

        # Stop the worker that is currently running (if possible)
        self.stop_celery_worker(worker)

        # Start the worker again with the args saved in redis db
        with self.get_worker_args_redis_connection() as worker_args_connect, self.get_worker_status_redis_connection() as worker_status_connect:
            # Get the args and remove the worker_cmd from the hash set
            args = worker_args_connect.hgetall(worker)
            worker_cmd = args["worker_cmd"]
            del args["worker_cmd"]
            kwargs = args
            for key in args:
                if args[key].startswith("link:"):
                    kwargs[key] = worker_args_connect.hgetall(args[key].split(":", 1)[1])
                elif args[key] == "True":
                    kwargs[key] = True
                elif args[key] == "False":
                    kwargs[key] = False

            # Run the subprocess for the worker and save the PID
            process = subprocess.Popen(worker_cmd, **kwargs)
            worker_status_connect.hset(worker, "pid", process.pid)

        return True

    def run(self):
        """
        Main manager loop for monitoring and managing Celery workers.

        This method continuously monitors the status of Celery workers by
        checking their health and attempting to restart any that are
        unresponsive. It updates the Redis database with the current
        status of the manager and the workers.
        """
        manager_info = {
            "status": "Running",
            "pid": os.getpid(),
        }

        with self.get_worker_status_redis_connection() as redis_connection:
            LOG.debug(f"MANAGER: setting manager key in redis to hold the following info {manager_info}")
            redis_connection.hset("manager", mapping=manager_info)

            # TODO figure out what to do with "processing_work" entry for the merlin monitor
            while True:  # TODO Make it so that it will stop after a list of workers is stopped
                # Get the list of running workers
                workers = redis_connection.keys()
                LOG.debug(f"MANAGER: workers: {workers}")
                workers.remove("manager")
                workers = [worker for worker in workers if int(redis_connection.hget(worker, "monitored"))]
                LOG.info(f"MANAGER: Monitoring {workers} workers")

                # Check/ Ping each worker to see if they are still running
                if workers:
                    worker_results = self.get_celery_workers_status(workers)

                    # If running set the status on redis that it is running
                    LOG.info(f"MANAGER: Responsive workers: {worker_results.keys()}")
                    for worker in list(worker_results.keys()):
                        redis_connection.hset(worker, "status", WorkerStatus.running)

                # If not running attempt to restart it
                for worker in workers:
                    if worker not in worker_results:
                        LOG.info(f"MANAGER: Worker '{worker}' is unresponsive.")
                        # If time where the worker is unresponsive is less than the worker time out then just increment
                        num_unresponsive = int(redis_connection.hget(worker, "num_unresponsive")) + 1
                        if num_unresponsive * self.query_frequency < self.worker_timeout:
                            # Attempt to restart worker
                            LOG.info(f"MANAGER: Attempting to restart worker '{worker}'...")
                            if self.restart_celery_worker(worker):
                                # If successful set the status to running and reset num_unresponsive
                                redis_connection.hset(worker, "status", WorkerStatus.running)
                                redis_connection.hset(worker, "num_unresponsive", 0)
                                LOG.info(f"MANAGER: Worker '{worker}' restarted.")
                            else:
                                # If failed set the status to stalled
                                redis_connection.hset(worker, "status", WorkerStatus.stalled)
                                LOG.error(f"MANAGER: Could not restart worker '{worker}'.")
                        else:
                            redis_connection.hset(worker, "num_unresponsive", num_unresponsive)
                # Sleep for the query_frequency for the next iteration
                time.sleep(self.query_frequency)


if __name__ == "__main__":
    cm = CeleryManager()
    cm.run()
