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

import os
import subprocess
import time

import psutil
import redis


class WorkerStatus:
    running = "Running"
    stalled = "Stalled"
    stopped = "Stopped"
    rebooting = "Rebooting"


WORKER_INFO = {
    "status": WorkerStatus.running,
    "pid": -1,
    "monitored": 1,
    "num_unresponsive": 0,
}


class CeleryManager:
    def __init__(self, query_frequency: int = 60, query_timeout: float = 0.5, worker_timeout: int = 180):
        """
        Initializer for Celery Manager
        @param int query_frequency:     The frequency at which workers will be queried with ping commands
        @param float query_timeout:     The timeout for the query pings that are sent to workers
        @param int worker_timeout:      The sum total(query_frequency*tries) time before an attempt is made to restart worker.
        """
        self.redis_connection = self.get_worker_status_redis_connection()
        self.query_frequency = query_frequency
        self.query_timeout = query_timeout
        self.worker_timeout = worker_timeout

    @staticmethod
    def get_worker_status_redis_connection():
        """
        Get the redis connection for info regarding the worker and manager status.
        """
        return CeleryManager.get_redis_connection(1)

    @staticmethod
    def get_worker_args_redis_connection():
        """
        Get the redis connection for info regarding the args used to generate each worker.
        """
        return CeleryManager.get_redis_connection(2)

    @staticmethod
    def get_redis_connection(db_num):
        """
        Generic redis connection function to get the results backend redis server with a given db number increment.
        :param int db_num:      Increment number for the db from the one provided in the config file.

        :return Redis:          Redis connections object that can be used to access values for the manager.
        """
        from merlin.config.configfile import CONFIG
        from merlin.config.results_backend import get_backend_password

        password_file = CONFIG.results_backend.password
        try:
            password = get_backend_password(password_file)
        except IOError:
            password = CONFIG.results_backend.password
        return redis.Redis(
            host=CONFIG.results_backend.server,
            port=CONFIG.results_backend.port,
            db=CONFIG.results_backend.db_num + db_num,  # Increment db_num to avoid conflicts
            username=CONFIG.results_backend.username,
            password=password,
            decode_responses=True,
        )

    def get_celery_workers_status(self, workers):
        """
        Get the worker status of a current worker that is being managed
        :param CeleryManager self:      CeleryManager attempting the stop.
        :param list workers:            Workers that are checked.

        :return dict:                   The result dictionary for each worker and the response.
        """
        from merlin.celery import app

        celery_app = app.control
        ping_result = celery_app.ping(workers, timeout=self.query_timeout)
        worker_results = {worker: status for d in ping_result for worker, status in d.items()}
        return worker_results

    def stop_celery_worker(self, worker):
        """
        Stop a celery worker by kill the worker with pid
        :param CeleryManager self:      CeleryManager attempting the stop.
        :param str worker:              Worker that is being stopped.

        :return bool:                   The result of whether a worker was stopped.
        """

        # Get the PID associated with the pid
        worker_status_connect = self.get_worker_status_redis_connection()
        worker_pid = int(worker_status_connect.hget(worker, "pid"))
        worker_status = worker_status_connect.hget(worker, "status")
        worker_status_connect.quit()
        # Check to see if the pid exists and worker is set as running
        if worker_status == WorkerStatus.running and psutil.pid_exists(worker_pid):
            # Check to see if the pid is associated with celery
            worker_process = psutil.Process(worker_pid)
            if "celery" in worker_process.name():
                # Kill the pid if both conditions are right
                worker_process.kill()
                return True
        return False

    def restart_celery_worker(self, worker):
        """
        Restart a celery worker with the same arguements and parameters during its creation
        :param CeleryManager self:      CeleryManager attempting the stop.
        :param str worker:              Worker that is being restarted.

        :return bool:                   The result of whether a worker was restarted.
        """

        # Stop the worker that is currently running
        if not self.stop_celery_worker(worker):
            return False
        # Start the worker again with the args saved in redis db
        worker_args_connect = self.get_worker_args_redis_connection()
        worker_status_connect = self.get_worker_status_redis_connection()
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

        worker_args_connect.quit()
        worker_status_connect.quit()

        return True

    # TODO add some logs
    def run(self):
        """
        Main manager loop
        """

        manager_info = {
            "status": "Running",
            "process id": os.getpid(),
        }
        self.redis_connection.hmset(name="manager", mapping=manager_info)

        while True:  # TODO Make it so that it will stop after a list of workers is stopped
            # Get the list of running workers
            workers = self.redis_connection.keys()
            workers.remove("manager")
            workers = [worker for worker in workers if int(self.redis_connection.hget(worker, "monitored"))]
            print(f"Monitoring {workers} workers")

            # Check/ Ping each worker to see if they are still running
            if workers:
                worker_results = self.get_celery_workers_status(workers)

                # If running set the status on redis that it is running
                for worker in list(worker_results.keys()):
                    self.redis_connection.hset(worker, "status", WorkerStatus.running)

            # If not running attempt to restart it
            for worker in workers:
                if worker not in worker_results:
                    # If time where the worker is unresponsive is less than the worker time out then just increment
                    num_unresponsive = int(self.redis_connection.hget(worker, "num_unresponsive")) + 1
                    if num_unresponsive * self.query_frequency < self.worker_timeout:
                        # Attempt to restart worker
                        if self.restart_celery_worker(worker):
                            # If successful set the status to running and reset num_unresponsive
                            self.redis_connection.hset(worker, "status", WorkerStatus.running)
                            self.redis_connection.hset(worker, "num_unresponsive", 0)
                            # If failed set the status to stalled
                            self.redis_connection.hset(worker, "status", WorkerStatus.stalled)
                    else:
                        self.redis_connection.hset(worker, "num_unresponsive", num_unresponsive)
            # Sleep for the query_frequency for the next iteration
            time.sleep(self.query_frequency)


if __name__ == "__main__":
    cm = CeleryManager()
    cm.run()
