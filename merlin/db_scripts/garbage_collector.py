##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Automatic garbage collection module for cleaning up stale database entries.

This module provides functionality to identify and remove database entries that
reference non-existent filesystem resources or have other consistency issues.
"""

import logging
import os
from typing import Dict, List

from merlin.db_scripts.merlin_db import MerlinDatabase
from merlin.utils import get_singular_of_entity


LOG = logging.getLogger(__name__)


class DatabaseGarbageCollector:
    """
    Handles automatic cleanup of stale or invalid database entries.
    
    This collector identifies and removes:
    - Runs with non-existent workspaces
    - Orphaned logical workers (not referenced by any run)
    - Orphaned physical workers (not referenced by any logical worker)
    - Studies with no associated runs
    
    Attributes:
        db (MerlinDatabase): The database interface.
        _issues (Dict[str, List[str]]): A dictionary to track issues in the database.

    Methods:
        check_run_workspaces:
            Identify runs whose workspace directories no longer exist on the filesystem.
        
        check_orphaned_workers:
            Identify logical and physical workers that are no longer associated with valid runs.
        
        check_empty_studies:
            Identify studies that have no valid runs associated with them.
        
        cleanup_runs:
            Delete runs with invalid workspaces that were identified during scanning.
        
        cleanup_workers:
            Delete orphaned physical and logical workers in dependency order.
        
        cleanup_studies:
            Delete studies with no valid runs.
        
        generate_report:
            Create a formatted summary of all identified stale entries.
        
        scan:
            Scan the database for stale entries without performing any deletions.
        
        clean:
            Delete all previously identified stale entries with optional confirmation.
        
        scan_and_clean:
            Convenience method that performs both scanning and cleanup in sequence.
    """
    
    def __init__(self, merlin_db: MerlinDatabase = None):
        """
        Initialize the garbage collector.
        
        Args:
            merlin_db: Optional MerlinDatabase instance. Creates one if not provided.
        """
        self.merlin_db = merlin_db or MerlinDatabase()
        self._issues: Dict[str, List[str]] = {
            "runs": [],
            "logical_workers": [],
            "physical_workers": [],
            "studies": []
        }
    
    def _prompt_for_confirmation(self) -> bool:
        """
        Prompt the user for confirmation before deleting entries.
        
        Returns:
            True if user confirms, False otherwise.
        """        
        LOG.warning(
            "[GARBAGE COLLECTOR] WARNING: This will permanently delete stale database entries. "
            "Run with --dry-run first to see what would be deleted."
        )
        response = input("\nContinue? (yes/no): ").strip().lower()
        while response not in ["yes", "y", "no", "n"]:
            response = input("Invalid response. Please enter 'yes' or 'no': ").strip().lower()

        LOG.debug(f"[GARBAGE COLLECTOR] response: {response}")
        return response in ["yes", "y"]
    
    def check_run_workspaces(self):
        """
        Check all runs for valid workspace directories.
    
        Identifies runs whose workspace paths no longer exist on the filesystem.
        These runs are considered stale and are added to the internal issues tracker.
        """
        LOG.info("[GARBAGE COLLECTOR] Checking run workspaces for validity...")

        all_runs = self.merlin_db.get_all("run")
        for run in all_runs:
            workspace = run.get_workspace()
            if not os.path.exists(workspace):
                LOG.debug(f"[GARBAGE COLLECTOR] Run {run.get_id()} has invalid workspace: {workspace}")
                self._issues["runs"].append(run)

        LOG.info(f"[GARBAGE COLLECTOR] Found {len(self._issues["runs"])} runs with invalid workspaces.")
    
    def _check_orphaned_logical_workers(self):
        """
        Check for logical workers not associated with any active runs.

        A logical worker is considered orphaned if:
        - It has no runs associated with it, OR
        - All its runs are invalid (identified in the current scan), OR
        - All its runs no longer exist in the database
        
        Orphaned workers are added to the internal issues tracker.
        """
        LOG.info("[GARBAGE COLLECTOR] Checking for orphaned logical workers...")

        # Get the current invalid run IDs
        invalid_run_ids = [run.get_id() for run in self._issues["runs"]]

        # Get all valid run IDs from the database
        all_runs = self.merlin_db.get_all("run")
        valid_run_ids = {run.get_id() for run in all_runs if run.get_id() not in invalid_run_ids}

        all_logical_workers = self.merlin_db.get_all("logical_worker")
        for worker in all_logical_workers:
            worker_runs = worker.get_runs()
            # Worker is orphaned if:
            # - It has no runs, OR
            # - All its runs are invalid (found in this pass), OR
            # - All its runs don't exist in the database anymore
            if not worker_runs or all(
                run_id in invalid_run_ids or run_id not in valid_run_ids for run_id in worker_runs
            ):
                self._issues["logical_workers"].append(worker)

        LOG.info(f"[GARBAGE COLLECTOR] Found {len(self._issues["logical_workers"])} orphaned logical workers.")
        LOG.debug(f"[GARBAGE COLLECTOR] Orphaned logical workers: {self._issues["logical_workers"]}")

    def _check_orphaned_physical_workers(self):
        """
        Check for physical workers not associated with any active logical workers.

        A physical worker is considered orphaned if:
        - Its parent logical worker is orphaned (identified in the current scan), OR
        - Its parent logical worker no longer exists in the database
        
        Orphaned physical workers are added to the internal issues tracker.
        """
        LOG.info("[GARBAGE COLLECTOR] Checking for orphaned physical workers...")

        # Get the current orphaned logical worker IDs
        orphaned_logical_ids = [worker.get_id() for worker in self._issues["logical_workers"]]

        # Get all valid logical worker IDs from the database
        all_logical_workers = self.merlin_db.get_all("logical_worker")
        valid_logical_ids = {worker.get_id() for worker in all_logical_workers if worker.get_id() not in orphaned_logical_ids}

        all_physical_workers = self.merlin_db.get_all("physical_worker")
        for worker in all_physical_workers:
            logical_worker_id = worker.get_logical_worker_id()
            # Physical worker is orphaned if:
            # - Its logical worker is orphaned (found in this pass), OR
            # - Its logical worker doesn't exist in the database anymore
            if logical_worker_id in orphaned_logical_ids or logical_worker_id not in valid_logical_ids:
                self._issues["physical_workers"].append(worker)

        LOG.info(f"[GARBAGE COLLECTOR] Found {len(self._issues["physical_workers"])} orphaned physical workers.")
        LOG.debug(f"[GARBAGE COLLECTOR] Orphaned physical workers: {self._issues["physical_workers"]}")

    def check_orphaned_workers(self):
        """
        Check for workers not associated with any active runs.

        This method performs a cascading check of both logical and physical workers:
        1. Identifies logical workers that are not referenced by any valid runs
        2. Identifies physical workers that are not referenced by any valid logical workers
        
        The checks are performed in dependency order to properly identify the full chain
        of orphaned entities.
        """
        # Check logical workers
        self._check_orphaned_logical_workers()
        
        # Check physical workers
        self._check_orphaned_physical_workers()
    
    def check_empty_studies(self):
        """
        Check for studies that have no associated runs.

        A study is considered empty if:
        - It has no runs associated with it, OR
        - All its runs are invalid (identified in the current scan), OR
        - All its runs no longer exist in the database
        
        Empty studies are added to the internal issues tracker.
        """
        LOG.info("[GARBAGE COLLECTOR] Checking for empty studies...")

        # Get the current invalid run IDs
        invalid_run_ids = [run.get_id() for run in self._issues["runs"]]
        
        # Get all valid run IDs from the database
        all_runs = self.merlin_db.get_all("run")
        valid_run_ids = {run.get_id() for run in all_runs if run.get_id() not in invalid_run_ids}
        
        all_studies = self.merlin_db.studies.get_all()
        for study in all_studies:
            runs = study.get_runs()
            # Study is empty if:
            # - It has no runs, OR
            # - All its runs are invalid (found in this pass), OR
            # - All its runs don't exist in the database anymore
            if not runs or all(run_id in invalid_run_ids or run_id not in valid_run_ids for run_id in runs):
                LOG.debug(f"[GARBAGE COLLECTOR] Study {study.get_id()} ({study.get_name()}) has no valid runs.")
                self._issues["studies"].append(study)

        LOG.info(f"[GARBAGE COLLECTOR] Found {len(self._issues["studies"])} empty studies.")
    
    def _cleanup_entity(self, entity_type: str):
        """
        Remove entities of a specific type that were identified as stale.
        
        Args:
            entity_type: Type of entity to clean up (runs, logical_workers, physical_workers, studies).
        """
        entity_singular = get_singular_of_entity(entity_type, split_delimiter="_", join_delimiter="_")
        
        if not self._issues[entity_type]:
            LOG.info(f"[GARBAGE COLLECTOR] No stale {entity_type} found.")
            return

        LOG.info(f"[GARBAGE COLLECTOR] Deleting {len(self._issues[entity_type])} {entity_type}...")
        for entity in self._issues[entity_type]:
            entity_id = entity.get_id()
            try:
                if entity_type == "studies":
                    self.merlin_db.delete("study", entity_id, remove_associated_runs=False)
                else:
                    self.merlin_db.delete(entity_singular, entity_id)
                LOG.debug(f"[GARBAGE COLLECTOR] Deleted {entity_singular} {entity_id}")
            except Exception as e:
                LOG.error(f"[GARBAGE COLLECTOR] Failed to delete {entity_singular} {entity_id}: {e}")

    def cleanup_runs(self):
        """Remove runs with invalid workspaces."""
        self._cleanup_entity("runs")

    def cleanup_workers(self):
        """
        Remove orphaned workers (both physical and logical).
        
        This is done in order.
        """
        # Clean up physical workers first (they depend on logical workers)
        self._cleanup_entity("physical_workers")
        
        # Then clean up logical workers
        self._cleanup_entity("logical_workers")

    def cleanup_studies(self):
        """Remove studies with no valid runs."""
        self._cleanup_entity("studies")

    def generate_report(self) -> str:
        """
        Generate a human-readable report of garbage collection results.
        
        Returns:
            Formatted string report.
        """
        report_lines = [
            "=" * 60,
            "Database Garbage Collection Report",
            "=" * 60,
            "",
        ]

        # Invalid Runs section
        report_lines.append(f"Invalid Runs: {len(self._issues['runs'])}")
        if self._issues['runs']:
            for run in self._issues['runs']:
                report_lines.append(f"  - {run.get_workspace()}")
        
        report_lines.append("")
        
        # Orphaned Logical Workers section
        report_lines.append(f"Orphaned Logical Workers: {len(self._issues['logical_workers'])}")
        if self._issues['logical_workers']:
            for worker in self._issues['logical_workers']:
                report_lines.append(f"  - {worker.get_name()} (queues: {', '.join(worker.get_queues())})")
        
        report_lines.append("")
        
        # Orphaned Physical Workers section
        report_lines.append(f"Orphaned Physical Workers: {len(self._issues['physical_workers'])}")
        if self._issues['physical_workers']:
            for worker in self._issues['physical_workers']:
                report_lines.append(f"  - {worker.get_name()} (host: {worker.get_host()})")
        
        report_lines.append("")
        
        # Empty Studies section
        report_lines.append(f"Empty Studies: {len(self._issues['studies'])}")
        if self._issues['studies']:
            for study in self._issues['studies']:
                report_lines.append(f"  - {study.get_name()}")
        
        report_lines.append("=" * 60)
        
        return "\n".join(report_lines)

    def scan(
        self,
        check_runs: bool = True, 
        check_workers: bool = True,
        check_studies: bool = True,
    ):
        """
        Scan the database for stale entries without deleting anything.
        
        Args:
            check_runs: Whether to check for invalid run workspaces.
            check_workers: Whether to check for orphaned workers.
            check_studies: Whether to check for empty studies.
        """
        LOG.info("[GARBAGE COLLECTOR] Scanning database for stale entries...")
        
        # Check phase
        if check_runs:
            self.check_run_workspaces()
        
        if check_workers:
            self.check_orphaned_workers()
        
        if check_studies:
            self.check_empty_studies()

        LOG.info("[GARBAGE COLLECTOR] Scan complete.")
        
        # Report findings
        LOG.info(f"\n{self.generate_report()}")
    
    def clean(
        self,
        check_runs: bool = True, 
        check_workers: bool = True,
        check_studies: bool = True,
        force: bool = False,
    ):
        """
        Delete all previously identified stale entries.
        
        This method should be called after scan(). It will prompt for
        confirmation unless force=True.

        Args:
            check_runs: Whether to check for invalid run workspaces.
            check_workers: Whether to check for orphaned workers.
            check_studies: Whether to check for empty studies.
            force: If True, skip confirmation prompt (use with caution).
        
        Raises:
            ValueError: If no scan has been performed yet.
        """
        total_issues = sum(len(issues) for issues in self._issues.values())
        
        if total_issues == 0:
            LOG.info("[GARBAGE COLLECTOR] No stale entries to clean up. You may need to run scan() first.")
            return
        
        # Get confirmation if needed
        if not force and not self._prompt_for_confirmation():
            LOG.info("[GARBAGE COLLECTOR] Cleanup cancelled.")
            return
        
        LOG.info("[GARBAGE COLLECTOR] Starting cleanup...")
        
        # Clean up in dependency order
        if check_runs:
            self.cleanup_runs()
        if check_workers:
            self.cleanup_workers()
        if check_studies:
            self.cleanup_studies()

        LOG.info("[GARBAGE COLLECTOR] Cleanup complete.")
    
    def scan_and_clean(
        self,
        check_runs: bool = True, 
        check_workers: bool = True,
        check_studies: bool = True,
        force: bool = False,
    ):
        """
        Convenience method that scans for garbage and then cleans it up.
        
        This is equivalent to calling scan() followed by clean().
        
        Args:
            check_runs: Whether to check for invalid run workspaces.
            check_workers: Whether to check for orphaned workers.
            check_studies: Whether to check for empty studies.
            force: If True, skip confirmation prompt (use with caution).
        """
        LOG.info("[GARBAGE COLLECTOR] Starting database garbage collection...")
        
        self.scan(
            check_runs=check_runs,
            check_workers=check_workers,
            check_studies=check_studies,
        )

        self.clean(
            check_runs=check_runs,
            check_workers=check_workers,
            check_studies=check_studies,
            force=force
        )

        LOG.info("[GARBAGE COLLECTOR] Database garbage collection complete.")