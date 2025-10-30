##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Manager class for high-level study operations.

This module provides the `StudyManager` class, which handles study-level
operations such as running and cancelling studies. It serves as a facade
over the lower-level components like MerlinStudy, database operations,
and task server interactions.
"""

import logging
from pathlib import Path
from typing import Union

from merlin.common.enums import RunStatus
from merlin.db_scripts.merlin_db import MerlinDatabase
from merlin.exceptions import RunNotFoundError, StudyNotFoundError
from merlin.spec.specification import MerlinSpec
from merlin.study.celeryadapter import purge_celery_tasks, stop_celery_workers
from merlin.utils import verify_filepath


LOG = logging.getLogger(__name__)


class StudyManager:
    """
    High-level manager for Merlin study operations.
    
    This class provides a unified interface for common study operations like
    running and cancelling studies. It coordinates between MerlinStudy, the
    database, and task server components.
    
    Attributes:
        merlin_db: The MerlinDatabase instance for database operations.
        study_identifier: Optional StudyIdentifier for the study being managed.
    
    Methods:
        run: Initialize and run a new study.
        cancel: Cancel an existing study.
        restart: Restart a study from a previous workspace.
    """

    def __init__(self, merlin_db: MerlinDatabase = None):
        """
        Initialize a StudyManager.
        
        Args:
            merlin_db: Optional MerlinDatabase instance. If not provided, creates one.
        """
        self.merlin_db = merlin_db or MerlinDatabase()

    # TODO implement this after refactoring MerlinStudy
    def run(self):
        """
        Given a study, run it.

        NOTE: Not sure what should be passed in as an argument here yet. That'll
        be more clear after refactoring MerlinStudy.
        """

    # TODO when status is refactored/added to database, make it so the task statuses for cancelled runs
    # also show cancelled
    def cancel(
        self,
        spec: MerlinSpec,
        purge_queues: bool = True,
        stop_workers: bool = True,
        mark_runs_cancelled: bool = True,
    ):
        """
        Given a study's specification file, cancel the study.

        When cancelling a study, the following actions should be taken:
        1. Stop the workers for that study
        2. Purge the queues from the study
        3. Mark all runs associated with that study as cancelled

        Each of these options can be disabled individually via flags to this method.

        Args:
            study_identifier (str): The spec file of the study to cancel.
            purge_queues (bool): Whether to purge the queues for the study. Defaults to True.
            stop_workers (bool): Whether to stop the workers for the study. Defaults to True.
            mark_runs_cancelled (bool): Whether to mark all runs as cancelled. Defaults to True.

        Raises:
            StudyNotFoundError: If the study cannot be identified.
        """
        result = {
            "study_name": spec.name,
            "runs_cancelled": 0,
            "queues_purged": [],
            "workers_stopped": [],
        }

        # Step 1: Stop the workers
        if stop_workers:
            # TODO when we refactor `stop-workers`, update this
            worker_names = spec.get_worker_names()
            for worker_name in worker_names:
                if "$" in worker_name:
                    LOG.warning(f"Worker '{worker_name}' is unexpanded. Target provenance spec instead?")
            stop_celery_workers(spec_worker_names=worker_names)

            # TODO when we refactor `stop-workers`, may want to do some extra validation here to ensure
            # all of these workers have actually been stopped
            # - maybe WorkerHandler.stop_workers() should return a list of every worker that was stopped
            result["workers_stopped"] = worker_names
        else:
            LOG.info("Skipping worker shutdown.")

        # Step 2: Purge the queues
        if purge_queues:
            queues = spec.get_queue_list(["all"])
            purge_celery_tasks(",".join(queues), True)

            # TODO should add some way to validate that these queues were actually purged
            result["queues_purged"] = queues
        else:
            LOG.info("Skipping queue purge.")

        # Step 3: Mark runs as cancelled
        if mark_runs_cancelled:
            try:
                study_entity = self.merlin_db.get("study", spec.name)
            except StudyNotFoundError:
                LOG.error(f"Study '{spec.name}' not found in database. Cannot mark runs as cancelled.")

            for run_id in study_entity.get_runs():
                try:
                    run_entity = self.merlin_db.get("run", run_id)
                except RunNotFoundError:
                    LOG.error(f"Run '{run_id}' not found in database. Cannot mark as cancelled.")

                if run_entity.is_active():
                    run_entity.set_status(RunStatus.CANCELLED)
                    result["runs_cancelled"] += 1
        else:
            LOG.info("Skipping marking runs as cancelled.")

        return result

    # TODO implement this on another branch and link to `merlin restart` command
    # TODO after refactoring MerlinStudy, we may want to revisit this?
    def restart(self, workspace: Union[str, Path]):
        """
        Given a study workspace, restart the study.

        Args:
            workspace (Union[str, Path]): The workspace directory of the study to restart.
        """