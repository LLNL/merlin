##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module provides the `Monitor` class, which is responsible for monitoring the progress of
Merlin workflows. It ensures that workers are running, tasks are being processed, and workflows
are restarted if needed to prevent hanging. The `Monitor` class uses worker and task monitors
to manage the health and progress of workflows.

The module interacts with the Merlin database to retrieve study and run information and
uses the `monitor_factory` to create monitors for task and worker systems (e.g., Celery).

Exceptions such as Redis timeouts, Kombu operational errors, and other runtime issues are
handled gracefully to ensure that monitoring continues without interruption.
"""

import logging
import subprocess
import time
import traceback

from kombu.exceptions import OperationalError
from redis.exceptions import TimeoutError as RedisTimeoutError

from merlin.db_scripts.entities.run_entity import RunEntity
from merlin.db_scripts.merlin_db import MerlinDatabase
from merlin.exceptions import RestartException
from merlin.monitor.monitor_factory import monitor_factory
from merlin.monitor.task_server_monitor import TaskServerMonitor
from merlin.spec.specification import MerlinSpec
from merlin.utils import verify_dirpath


LOG = logging.getLogger(__name__)


class Monitor:
    """
    The `Monitor` class is responsible for monitoring the progress of Merlin workflows. It ensures
    that workers are running, tasks are being processed, and workflows are restarted if necessary
    to prevent hanging. As a side-effect of the monitor, the users allocation will remain alive for
    however long the monitor lives. The class interacts with the Merlin database to retrieve study
    and run information and uses a task server monitor to help manage workflow health.

    Attributes:
        spec (MerlinSpec): The Merlin specification that defines the workflow.
        sleep (int): The interval (in seconds) between monitoring checks.
        no_restart (bool): If True, the monitor will not try to restart the workflow.
        task_server_monitor (TaskServerMonitor): A monitor for interacting with whichever task server
            that the user is utilizing.
        merlin_db (MerlinDatabase): Interface for accessing and querying the Merlin database.

    Methods:
        monitor_all_runs: Monitors all runs of the current study until they are complete.
        monitor_single_run: Monitors a single run of a study until it completes.
        restart_workflow: Restart a run of a workflow.
    """

    def __init__(self, spec: MerlinSpec, sleep: int, task_server: str, no_restart: bool):
        """
        Initializes the `Monitor` instance with the given Merlin specification, sleep interval,
        and task server type. The task server monitor is created using the
        [`monitor_factory`][monitor.monitor_factory.MonitorFactory].

        Args:
            spec (MerlinSpec): The Merlin specification that defines the workflow.
            sleep (int): The interval (in seconds) between monitoring checks.
            task_server (str): The type of task server being used (e.g., "celery").
            no_restart (bool): If True, the monitor will not try to restart the workflow.
        """
        self.spec: MerlinSpec = spec
        self.sleep: int = sleep
        self.no_restart: bool = no_restart
        self.task_server_monitor: TaskServerMonitor = monitor_factory.create(task_server)
        self.merlin_db = MerlinDatabase()

    def monitor_all_runs(self):
        """
        Monitors all runs of the current study until they are complete. For each run, it checks
        if the run is already complete. If not, it monitors the run until it finishes. This
        method ensures that all runs in the study are processed. This is necessary to be able to
        monitor iterative workflows.

        The method retrieves all runs from the database and iterates through them sequentially.
        If a run is incomplete, it calls [`monitor_single_run`][monitor.monitor.Monitor.monitor_single_run]
        to monitor it until completion.
        """
        study_entity = self.merlin_db.get("study", self.spec.name)

        index = 0
        while True:
            # Always refresh the list at the start of the loop; there could be new runs (think iterative studies)
            all_runs = [self.merlin_db.get("run", run_id) for run_id in study_entity.get_runs()]
            if index >= len(all_runs):  # Break if there are no more runs to process
                break

            run = all_runs[index]
            run_workspace = run.get_workspace()
            LOG.info(f"Monitor: Checking if run with workspace '{run_workspace}' has completed...")

            if run.run_complete:
                LOG.info(
                    f"Monitor: Determined that run with workspace '{run_workspace}' has already completed. "
                    "Moving on to the next run."
                )
                index += 1
                continue

            LOG.info(f"Monitor: Run with workspace '{run_workspace}' has not yet completed.")

            # Monitor the run until it completes
            self.monitor_single_run(run)

            index += 1

    def _check_task_activity(self, run: RunEntity) -> bool:
        """
        Checks whether there is active task activity for the given run.

        This method first checks if there are any tasks in the task server's queues. If not,
        it then checks whether any workers are currently processing tasks. If either of these
        conditions is true, the method considers the workflow to be active and returns True.

        Args:
            run (RunEntity): The run entity representing the workflow run to check for activity.

        Returns:
            True if tasks are in the queues or being processed by workers, False otherwise.
        """
        # Check if any tasks are currently in the queues
        if self.task_server_monitor.check_tasks(run):
            LOG.info("Monitor: Found tasks in queues, keeping allocation alive.")
            return True

        # If no tasks are in the queues, check if workers are processing tasks
        if self.task_server_monitor.check_workers_processing(run.get_queues()):
            LOG.info("Monitor: Found workers processing tasks, keeping allocation alive.")
            return True

        return False

    def _handle_transient_exception(self, exc: Exception):
        """
        Handles transient exceptions that may occur during monitoring.

        This method logs the exception type, message, and full traceback, then
        sleeps for the configured interval before retrying. It is designed to
        gracefully handle recoverable errors such as Redis timeouts or broker issues.

        Args:
            exc (Exception): The exception instance that was caught during execution.
        """
        LOG.warning(f"{exc.__class__.__name__} occurred:\n{exc}")
        LOG.warning(f"Full traceback:\n{traceback.format_exc()}")
        time.sleep(self.sleep)

    def monitor_single_run(self, run: RunEntity):
        """
        Monitors a single run of a study until it completes to ensure that the allocation stays alive
        and workflows are restarted if necessary.

        Args:
            run: A [`RunEntity`][db_scripts.entities.run_entity.RunEntity] instance representing
                the run that's going to be monitored.
        """
        run_workspace = run.get_workspace()
        run_complete = run.run_complete  # Saving this to a variable as it queries the db each time it's called

        LOG.info(f"Monitor: Monitoring run with workspace '{run_workspace}'...")

        # Wait for workers to spin up before checking on tasks
        worker_names = [self.merlin_db.get("logical_worker", worker_id=wid).get_name() for wid in run.get_workers()]
        LOG.info(f"Monitor: Waiting for the following workers to start: {worker_names}...")
        self.task_server_monitor.wait_for_workers(worker_names, self.sleep)
        LOG.info("Monitor: Workers have started.")

        while not run_complete:
            try:
                # Run worker health check (checks for dead workers and restarts them if necessary)
                self.task_server_monitor.run_worker_health_check(run.get_workers())

                # Check if any tasks are currently in the queues or if workers are processing tasks
                active_tasks = self._check_task_activity(run)

                run_complete = run.run_complete  # Re-query db for this value

                # If no tasks are in the queues or being processed by workers and the run is not complete, we have a hanging
                # workflow so restart it
                if not active_tasks and not run_complete:
                    if self.no_restart:
                        LOG.warning(
                            f"Monitor: Determined restart was required for '{run_workspace}' but auto-restart is disabled."
                        )
                    else:
                        self.restart_workflow(run)

                if not run_complete:
                    time.sleep(self.sleep)
            # The below exceptions do not modify the `run_complete` value so the loop should retry
            except (RedisTimeoutError, OperationalError, TimeoutError) as exc:
                self._handle_transient_exception(exc)

        LOG.info(f"Monitor: Run with workspace '{run_workspace}' has completed.")

    def restart_workflow(self, run: RunEntity):
        """
        Restart a run of a workflow.

        Args:
            run: A [`RunEntity`][db_scripts.entities.run_entity.RunEntity] instance representing
                the run that's going to be restarted.

        Raises:
            RestartException: If the workflow restart process fails.
        """
        try:
            run_workspace = verify_dirpath(run.get_workspace())
            LOG.info(f"Monitor: Restarting workflow for run with workspace '{run_workspace}'...")
            restart_proc = subprocess.run(f"merlin restart {run_workspace}", shell=True, capture_output=True, text=True)
            if restart_proc.returncode != 0:
                LOG.error(f"Monitor: Failed to restart workflow: {restart_proc.stderr}")
                raise RestartException(f"Restart process failed with error: {restart_proc.stderr}")
            LOG.info(f"Monitor: Workflow restarted successfully: {restart_proc.stdout}")
        except ValueError:
            LOG.warning(
                f"Monitor: Run with workspace '{run.get_workspace()}' was not found. Ignoring the restart of this workspace."
            )
