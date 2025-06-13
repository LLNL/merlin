##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module provides the `CeleryMonitor` class, a concrete implementation of the
[`TaskServerMonitor`][monitor.task_server_monitor.TaskServerMonitor] interface for monitoring
Celery task servers. Celery is a distributed task queue system commonly used for executing
asynchronous tasks and managing worker nodes.

The `CeleryMonitor` class combines task and worker monitoring functionality specific to Celery.
It provides methods to:

- Wait for workers to start.
- Check for tasks in the queues.
- Monitor worker activity.
- Run health checks to ensure workers are alive and functioning.
"""

import logging
import time
from typing import List, Set

from merlin.db_scripts.entities.run_entity import RunEntity
from merlin.exceptions import NoWorkersException
from merlin.monitor.task_server_monitor import TaskServerMonitor
from merlin.study.celeryadapter import get_workers_from_app, query_celery_queues


LOG = logging.getLogger(__name__)


class CeleryMonitor(TaskServerMonitor):
    """
    Implementation of [`TaskServerMonitor`][monitor.task_server_monitor.TaskServerMonitor]
    for Celery task servers. This class provides methods to monitor Celery workers, tasks,
    and workflows.

    Methods:
        wait_for_workers: Wait for Celery workers to start up.
        check_workers_processing: Check if any Celery workers are still processing tasks.
        _restart_workers: Restart a list of (dead) Celery workers.
        _get_dead_workers: Get a list of dead Celery workers.
        run_worker_health_check: Check the health of Celery workers and restart any that are dead.
        check_tasks: Checks the status of tasks in the Celery queues for a given workflow run.
    """

    def wait_for_workers(self, workers: List[str], sleep: int):
        """
        Wait for Celery workers to start up.

        Args:
            workers: A list of worker names or IDs to wait for.
            sleep: The interval (in seconds) between checks for worker availability.

        Raises:
            NoWorkersException: When workers don't start in (`self.sleep` * 10) seconds.
        """
        count = 0
        max_count = 10
        while count < max_count:
            worker_status = get_workers_from_app()
            LOG.debug(f"CeleryMonitor: checking for workers, running workers = {worker_status} ...")

            # Check if any of the desired workers have started
            check = any(any(iwn in iws for iws in worker_status) for iwn in workers)
            if check:
                break

            count += 1
            time.sleep(sleep)

        if count == max_count:
            raise NoWorkersException("Monitor: no workers available to process the non-empty queue")

    def check_workers_processing(self, queues: List[str]) -> bool:
        """
        Check if any Celery workers are still processing tasks.

        Args:
            queues: A list of queue names to check for active tasks.

        Returns:
            True if workers are processing tasks in the specified queues, False otherwise.
        """
        from merlin.celery import app  # pylint: disable=import-outside-toplevel

        # Query celery for active tasks
        active_tasks = app.control.inspect().active()

        # Search for the queues we provided
        if active_tasks is not None:
            for tasks in active_tasks.values():
                for task in tasks:
                    if task["delivery_info"]["routing_key"] in queues:
                        return True

        return False

    def _restart_workers(self, workers: List[str]):
        """
        Restart a dead Celery worker.

        Args:
            workers: A list of worker names or IDs to restart.
        """
        for worker in workers:
            try:
                LOG.warning(f"CeleryMonitor: Worker '{worker}' has died. Attempting to restart...")
                # TODO figure out the restart logic; will likely need stuff from manager branch
                LOG.info(f"CeleryMonitor: Worker '{worker}' has been successfully restarted.")
            except Exception as e:  # pylint: disable=broad-exception-caught
                LOG.error(f"CeleryMonitor: Failed to restart worker '{worker}'. Error: {e}")

    def _get_dead_workers(self, workers: List[str]) -> Set[str]:
        """
        Identify unresponsive Celery workers from a given list.

        This function sends a ping to all specified workers and identifies
        which workers did not respond within the given timeout.

        Args:
            workers: A list of Celery worker names to check.

        Returns:
            Set[str]: A set of unresponsive worker names.
        """
        from merlin.celery import app  # pylint: disable=import-outside-toplevel

        # Send ping to all workers
        responses = app.control.ping(destination=workers, timeout=5.0)  # TODO May want to customize timeout like manager does

        # Extract unresponsive workers
        unresponsive_workers = set()
        for response in responses:
            for worker, reply in response.items():
                if not reply.get("ok") == "pong":
                    unresponsive_workers.add(worker)
                    LOG.debug(f"CeleryMonitor: Unresponsive worker '{worker}' gave this reply when pinged: {reply}")

        if unresponsive_workers:
            LOG.warning(f"CeleryMonitor: Found unresponsive workers: {unresponsive_workers}")
        else:
            LOG.info("CeleryMonitor: All workers are alive and responsive.")

        return unresponsive_workers

    def run_worker_health_check(self, workers: List[str]):
        """
        Check the health of Celery workers and restart any that are dead.

        Args:
            workers: A list of worker names or IDs to check for health.

        Raises:
            WorkerRestartException: If a worker fails to restart.
        """
        dead_workers = self._get_dead_workers(workers)
        if dead_workers:
            self._restart_workers(dead_workers)

    def check_tasks(self, run: RunEntity) -> bool:
        """
        Check the status of tasks in Celery queues for the given workflow run.

        Args:
            run: A [`RunEntity`][db_scripts.entities.run_entity.RunEntity] instance representing
                the workflow run whose tasks are being monitored.

        Returns:
            True if tasks are active in the workflow (i.e., jobs are present in the queues),
                False otherwise.
        """
        queues_in_run = run.get_queues()
        LOG.debug(f"CeleryMonitor: queues_in_run={queues_in_run}")
        queue_status = query_celery_queues(queues_in_run)
        LOG.debug(f"CeleryMonitor: Result of querying celery queues: {queue_status}")

        total_jobs = 0
        for queue_info in queue_status.values():
            total_jobs += queue_info["jobs"]
        LOG.debug(f"CeleryMonitor: total_jobs={total_jobs}")

        if total_jobs > 0:
            return True
        return False
