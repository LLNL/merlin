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

from merlin.config.configfile import CONFIG
from merlin.config.results_backend import get_backend_password
import os
import redis
import time


class WorkerStatus:
    running = "Running"
    stalled = "Stalled"
    stopped = "Stopped"
    rebooting = "Rebooting"

WORKER_INFO = {
    "status" : WorkerStatus.running,
    "monitored": 1,
    "num_unresponsive": 0,
}

class CeleryManager():

    def __init__(self, query_frequency=60, query_timeout=0.5, worker_timeout=180):
        self.redis_connection = self.get_worker_status_redis_connection()
        self.query_frequency = query_frequency
        self.query_timeout = query_timeout
        self.worker_timeout = worker_timeout
    
    @staticmethod
    def get_worker_status_redis_connection():
        return CeleryManager.get_redis_connection(1)

    @staticmethod
    def get_worker_args_redis_connection():
        return CeleryManager.get_redis_connection(2)

    @staticmethod
    def get_redis_connection(db_num):
        password_file = CONFIG.results_backend.password
        try:
            password = get_backend_password(password_file)
        except IOError:
            password = CONFIG.results_backend.password
        return redis.Redis(host=CONFIG.results_backend.server,
                           port=CONFIG.results_backend.port,
                           db=db_num,
                           username=CONFIG.results_backend.username,
                           password=password)

    def get_celery_worker_status(worker):
        pass

    def restart_celery_worker(worker):
        pass

    def check_pid(pid):        
        """ Check For the existence of a unix pid. """
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        else:
            return True
    
    def run(self):
        manager_info = {
            "status": "Running",
            "process id": os.getpid(),
        }
        self.redis_connection.hmset(name="manager", mapping=manager_info)




        #while True:
        # Get the list of running workers
        workers = [i.decode("ascii") for i in self.redis_connection.keys()]
        workers.remove("manager")
        workers = [worker for worker in workers if int(self.redis_connection.hget(worker, "monitored"))]
        print("Current Monitored Workers", workers)
        
        # Check/ Ping each worker to see if they are still running
        if workers:
            from merlin.celery import app 

            celery_app = app.control
            ping_result = celery_app.ping(workers, timeout=self.query_timeout)
            worker_results = {worker: status for d in ping_result for worker, status in d.items()}
            print("Worker result from ping", worker_results)

            # If running set the status on redis that it is running
            for worker in list(worker_results.keys()):
                self.redis_connection.hset(worker, "status", WorkerStatus.running)

        # If not running attempt to restart it
        for worker in workers:
            if worker not in worker_results:
                # If time where the worker is unresponsive is less than the worker time out then just increment
                num_unresponsive = int(self.redis_connection.hget(worker, "num_unresponsive"))+1
                if num_unresponsive*self.query_frequency < self.worker_timeout:
                    # Attempt to restart worker

                    # If successful set the status to running
                    
                    # If failed set the status to stopped
                    #TODO Try to restart the worker
                    continue
                else:
                    self.redis_connection.hset(worker, "num_unresponsive", num_unresponsive)
        
            #time.sleep(self.query_frequency)
        
if __name__ == "__main__":
    cm = CeleryManager()
    cm.run()