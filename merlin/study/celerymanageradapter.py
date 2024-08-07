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
from merlin.study.celerymanager import CeleryManager, WORKER_INFO, WorkerStatus
import psutil
import subprocess

def add_monitor_workers(workers: list):
    """
    Adds workers to be monitored by the celery manager.
    :param list workers:        A list of tuples which includes (worker_name, pid)
    """
    if workers is None or len(workers) <= 0:
        return
    
    redis_connection = CeleryManager.get_worker_status_redis_connection()
    for worker in workers:
        if redis_connection.exists(worker[0]):
            redis_connection.hset(worker[0], "monitored", 1)
            redis_connection.hset(worker[0], "pid", worker[1])
        worker_info = WORKER_INFO
        worker_info["pid"] = worker[1]
        redis_connection.hmset(name=worker[0], mapping=worker_info)
    redis_connection.quit()

def remove_monitor_workers(workers: list):
    """
    Remove workers from being monitored by the celery manager.
    :param list workers:        A worker names
    """
    if workers is None or len(workers) <= 0:
        return
    redis_connection = CeleryManager.get_worker_status_redis_connection()
    for worker in workers:
        if redis_connection.exists(worker):
            redis_connection.hset(worker, "monitored", 0)
            redis_connection.hset(worker, "status", WorkerStatus.stopped)
            
    redis_connection.quit()

def is_manager_runnning() -> bool:
    """
    Check to see if the manager is running

    :return: True if manager is running and False if not.
    """
    redis_connection = CeleryManager.get_worker_args_redis_connection()
    manager_status = redis_connection.hgetall("manager")
    redis_connection.quit()
    return manager_status["status"] == WorkerStatus.running and psutil.pid_exists(manager_status["pid"])

def run_manager(query_frequency:int = 60, query_timeout:float = 0.5, worker_timeout:int = 180) -> bool:
    celerymanager = CeleryManager(query_frequency=query_frequency,
                                  query_timeout=query_timeout,
                                  worker_timeout=worker_timeout)
    celerymanager.run()
    

def start_manager(query_frequency:int = 60, query_timeout:float = 0.5, worker_timeout:int = 180) -> bool:
    process = subprocess.Popen(f"merlin manager run -qf {query_frequency} -qt {query_timeout} -wt {worker_timeout}".split(),
        start_new_session=True,
        close_fds=True,
        stdout=subprocess.PIPE,
    )
    redis_connection = CeleryManager.get_worker_args_redis_connection()
    redis_connection.hset("manager", "pid", process.pid)
    redis_connection.quit()
    return True

def stop_manager() -> bool:
    redis_connection = CeleryManager.get_worker_args_redis_connection()
    manager_pid = redis_connection.hget("manager", "pid")
    manager_status = redis_connection.hget("manager", "status")
    redis_connection.quit()
    
    # Check to make sure that the manager is running and the pid exists
    if manager_status == WorkerStatus.running and psutil.pid_exists(manager_pid):
        psutil.Process(manager_pid).terminate()
        return True
    return False

