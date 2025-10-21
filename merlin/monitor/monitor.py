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

    The monitor supports tracking multiple concurrent runs of the same study, making it suitable for
    iterative workflows and scenarios where multiple runs share the same worker pool. It continuously
    polls the database for active runs and performs health checks on each one during every monitoring
    cycle.

    Attributes:
        spec (MerlinSpec): The Merlin specification that defines the workflow.
        sleep (int): The interval (in seconds) between monitoring checks.
        no_restart (bool): If True, the monitor will not try to restart workflows automatically.
        task_server_monitor (TaskServerMonitor): A monitor for interacting with whichever task server
            that the user is utilizing (e.g., Celery).
        merlin_db (MerlinDatabase): Interface for accessing and querying the Merlin database.

    Methods:
        monitor_all_runs: Monitors all runs of the current study until they are complete.
        monitor_single_run: Monitors a single run of a study until it completes.
        wait_for_workers: Wait for workers to start before proceeding with health checks.
        check_task_activity: Determine if there is active task activity for a run.
        check_run_health: Perform health checks and restart stalled workflows.
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

    def wait_for_workers(self, run: RunEntity):
        """
        Wait for all workers to be ready before proceeding with the health check of the run.

        This method retrieves the logical worker names for the given run from the database and
        delegates to the task server monitor to wait for them to become available. This ensures
        that health checks and task activity monitoring don't begin until the workers are fully
        initialized.

        Args:
            run: A RunEntity instance representing the run to monitor.
        """
        worker_names = [self.merlin_db.get("logical_worker", worker_id=wid).get_name() for wid in run.get_workers()]
        LOG.info(f"Monitor: Waiting for the following workers to start: {worker_names}...")
        self.task_server_monitor.wait_for_workers(worker_names, self.sleep)
        LOG.info("Monitor: Workers have started.")

    def check_task_activity(self, run: RunEntity) -> bool:
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

    def check_run_health(self, run: RunEntity):
        """
        Check the health of a single run and restart if necessary.

        This method performs worker health checks to detect and restart dead workers,
        monitors task activity to determine if the workflow is making progress, and
        automatically restarts stalled workflows (unless auto-restart is disabled).
        
        The health check considers a workflow stalled if there are no tasks in the queues,
        no workers processing tasks, and the run is not marked as complete. This typically
        indicates a workflow that has hung and needs to be restarted.

        Transient exceptions such as Redis timeouts are caught and handled gracefully
        to avoid terminating the monitoring process.
        
        Args:
            run: A RunEntity instance representing the run to monitor.
        """
        try:
            # Run worker health check (checks for dead workers and restarts them if necessary)
            self.task_server_monitor.run_worker_health_check(run.get_workers())

            # Check if any tasks are currently in the queues or if workers are processing tasks
            active_tasks = self.check_task_activity(run)

            # If no tasks are in the queues or being processed by workers and the run is not complete, we have a hanging
            # workflow so restart it
            if not active_tasks and not run.run_complete:
                if self.no_restart:
                    LOG.warning(
                        f"Monitor: Determined restart was required for '{run.get_workspace()}' but auto-restart is disabled."
                    )
                else:
                    self.restart_workflow(run)
        except (RedisTimeoutError, OperationalError, TimeoutError) as exc:
            LOG.warning(f"{exc.__class__.__name__} occurred:\n{exc}")
            LOG.warning(f"Full traceback:\n{traceback.format_exc()}")
            time.sleep(self.sleep)

    def monitor_all_runs(self):
        """
        Monitors all runs of the current study until they are complete.

        This method continuously polls the database for all runs associated with the study,
        filters out completed runs, and performs health checks on all active runs during each
        monitoring cycle. This approach allows the monitor to track multiple concurrent runs
        of the same study, which is essential for iterative workflows and scenarios where
        multiple runs share a common worker pool.

        The method will continue monitoring until all runs have completed. It logs the status
        of completed and active runs during each cycle for visibility into the monitoring process.
        """
        study_entity = self.merlin_db.get("study", self.spec.name)

        while True:
            all_runs = [self.merlin_db.get("run", run_id) 
                       for run_id in study_entity.get_runs()]

            # Filter to complete and incomplete runs
            active_runs = []
            completed_runs = []
            for run in all_runs:
                completed_runs.append(run) if run.run_complete else active_runs.append(run)
            
            # Log completed runs
            if completed_runs:
                completed_workspaces = [run.get_workspace() for run in completed_runs]
                LOG.info(f"Monitor: The following runs have completed: {completed_workspaces}")
            
            # Log active runs
            if active_runs:
                active_workspaces = [run.get_workspace() for run in active_runs]
                LOG.info(f"Monitor: Currently monitoring {len(active_runs)} active run(s): {active_workspaces}")
            else:
                LOG.info("Monitor: No active runs remaining.")
                break
                
            # Check each active run
            for run in active_runs:
                self.wait_for_workers(run)
                self.check_run_health(run)
            
            time.sleep(self.sleep)

    def monitor_single_run(self, run: RunEntity):
        """
        Monitors a single run of a study until it completes.

        This method focuses on monitoring a specific run, continuously performing health
        checks until the run is marked as complete. It ensures that the allocation stays
        alive, workers remain healthy, and stalled workflows are restarted if necessary.

        Unlike [`monitor_all_runs`][monitor.monitor.Monitor.monitor_all_runs], this method
        is designed for scenarios where only a single run needs to be monitored. It follows
        a simpler execution model that doesn't poll for additional runs.

        Args:
            run: A [`RunEntity`][db_scripts.entities.run_entity.RunEntity] instance representing
                the run that's going to be monitored.
        """
        run_workspace = run.get_workspace()

        LOG.info(f"Monitor: Monitoring run with workspace '{run_workspace}'...")

        # Wait for workers to spin up before checking on tasks
        self.wait_for_workers(run)

        while not run.run_complete:
            self.check_run_health(run)
            if not run.run_complete:
                time.sleep(self.sleep)

        LOG.info(f"Monitor: Run with workspace '{run_workspace}' has completed.")
