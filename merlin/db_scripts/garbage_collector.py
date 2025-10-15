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
import socket
import textwrap
from pathlib import Path
from typing import Dict, List, Union

from merlin.db_scripts.entities.db_entity import DatabaseEntity
from merlin.db_scripts.merlin_db import MerlinDatabase
from merlin.exceptions import RunNotFoundError, StudyNotFoundError, WorkerNotFoundError
from merlin.utils import get_accessible_mounts, get_plural_of_entity


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
        _issues (Dict[str, List[DatabaseEntity]]): A dictionary to track entities with issues
            in the database.

    Methods:
        check_run_workspaces:
            Identify runs whose workspace directories no longer exist on the filesystem.

        check_orphaned_logical_workers:
            Identify logical workers that are not associated with any valid runs.

        check_orphaned_physical_workers:
            Identify physical workers that are not associated with any valid logical workers.

        check_empty_studies:
            Identify studies that have no valid runs associated with them.

        cleanup_runs:
            Delete runs with invalid workspaces that were identified during scanning.

        cleanup_logical_workers:
            Delete orphaned logical workers.

        cleanup_physical_workers:
            Delete orphaned physical workers.

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
        self._issues: Dict[str, List[DatabaseEntity]] = {
            "run": [],
            "logical_worker": [],
            "physical_worker": [],
            "study": [],
            "inaccessible_runs": [],
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
    
    def _is_workspace_on_accessible_mount(self, workspace: Union[str, Path]) -> bool:
        """
        Check if a workspace path is on an accessible mount point (excluding root).

        This purposefully does NOT include '/' as an accessible mount point. We do this
        since every workspace is relative to the root filesystem, and we want to
        specifically check for other mounted filesystems that may not be accessible.
        
        Args:
            workspace: The workspace path to check.
            
        Returns:
            True if workspace is on an accessible mount (excluding root), False otherwise.
        """
        if not isinstance(workspace, Path):
            workspace = Path(workspace)

        accessible_mounts = get_accessible_mounts(exclude_root=True)
        workspace_path = workspace.resolve()
        
        # Check if workspace path starts with any accessible mount
        for mount in sorted(accessible_mounts, key=lambda p: len(str(p)), reverse=True):
            # Sort by length (longest first) to match most specific mount point
            try:
                workspace_path.relative_to(mount)
                # If we get here, workspace is under this mount
                return True
            except ValueError:
                # Not relative to this mount, continue checking
                continue
        
        # If we didn't find any matching mount, it's not accessible or it's on the root filesystem
        LOG.warning(
            f"[GARBAGE COLLECTOR] Workspace '{workspace}' is either on the root filesystem or does not exist "
            f"in a valid mounted file system on current host '{socket.gethostname()}'."
        )
        return False

    def check_run_workspaces(self):
        """
        Check all runs for valid workspace directories.

        Identifies runs whose workspace paths no longer exist on the filesystem.
        These runs are considered stale and are added to the internal issues tracker.
        """
        LOG.info("[GARBAGE COLLECTOR] Checking run workspaces for validity...")

        all_runs = self.merlin_db.runs.get_all()

        for run in all_runs:
            workspace = run.get_workspace()

            # Check if workspace is on an accessible NON-ROOT mount (e.g., a network filesystem).
            # This will be False for workspaces on the local root filesystem.
            is_accessible_mount = self._is_workspace_on_accessible_mount(workspace)
            
            # Check if the workspace physically exists on the current host.
            workspace_exists = os.path.exists(workspace)

            if is_accessible_mount and not workspace_exists:
                # Case 1: Workspace is on an accessible NON-ROOT mount (e.g., /mnt/nfs) but is missing.
                # This is an invalid workspace issue.
                LOG.debug(f"[GARBAGE COLLECTOR] Run {run.get_id()} has invalid workspace on an accessible mount: {workspace}")
                self._issues["run"].append(run)

            elif not is_accessible_mount and not workspace_exists:
                # Case 2: Workspace is NOT on a non-root accessible mount AND does not physically exist on current host.
                # This indicates the workspace is likely on an inaccessible mount *or* it was a local 
                # workspace that was deleted, but we treat this as *potentially* inaccessible 
                # to avoid premature deletion of runs accessible from another host.
                LOG.debug(f"[GARBAGE COLLECTOR] Run '{run.get_id()}' has workspace on potentially inaccessible mount and does not exist: {workspace}")
                self._issues["inaccessible_runs"].append(run)

            # Case 3: Workspace is NOT on a non-root accessible mount, but DOES exist.
            # This is the expected state for a valid workspace on the local root filesystem.
            # No action needed, it's considered valid.

            # Case 4: is_accessible_mount and workspace_exists: Valid non-root workspace. No action.

        LOG.info(f"[GARBAGE COLLECTOR] Found {len(self._issues['run'])} runs with invalid workspaces.")
        if self._issues["inaccessible_runs"]:
            LOG.warning(
                f"[GARBAGE COLLECTOR] Found {len(self._issues['inaccessible_runs'])} runs with workspaces on file systems not accessible " \
                f"from the current host '{socket.gethostname()}'. Run garbage collection from a machine with access to verify these."
            )

    def check_orphaned_logical_workers(self):
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
        invalid_run_ids = [run.get_id() for run in self._issues["run"]]

        # Get all valid run IDs from the database
        all_runs = self.merlin_db.runs.get_all()
        valid_run_ids = {run.get_id() for run in all_runs if run.get_id() not in invalid_run_ids}

        all_logical_workers = self.merlin_db.logical_workers.get_all()
        for worker in all_logical_workers:
            worker_runs = worker.get_runs()
            # Worker is orphaned if:
            # - It has no runs, OR
            # - All its runs are invalid (found in this pass), OR
            # - All its runs don't exist in the database anymore
            if not worker_runs or all(run_id in invalid_run_ids or run_id not in valid_run_ids for run_id in worker_runs):
                self._issues["logical_worker"].append(worker)

        LOG.info(f"[GARBAGE COLLECTOR] Found {len(self._issues['logical_worker'])} orphaned logical workers.")
        LOG.debug(f"[GARBAGE COLLECTOR] Orphaned logical workers: {self._issues['logical_worker']}")

    def check_orphaned_physical_workers(self):
        """
        Check for physical workers not associated with any active logical workers.

        A physical worker is considered orphaned if:
        - Its parent logical worker is orphaned (identified in the current scan), OR
        - Its parent logical worker no longer exists in the database

        Orphaned physical workers are added to the internal issues tracker.
        """
        LOG.info("[GARBAGE COLLECTOR] Checking for orphaned physical workers...")

        # Get the current orphaned logical worker IDs
        orphaned_logical_ids = [worker.get_id() for worker in self._issues["logical_worker"]]

        # Get all valid logical worker IDs from the database
        all_logical_workers = self.merlin_db.logical_workers.get_all()
        valid_logical_ids = {worker.get_id() for worker in all_logical_workers if worker.get_id() not in orphaned_logical_ids}

        all_physical_workers = self.merlin_db.physical_workers.get_all()
        for worker in all_physical_workers:
            logical_worker_id = worker.get_logical_worker_id()
            # Physical worker is orphaned if:
            # - Its logical worker is orphaned (found in this pass), OR
            # - Its logical worker doesn't exist in the database anymore
            if logical_worker_id in orphaned_logical_ids or logical_worker_id not in valid_logical_ids:
                self._issues["physical_worker"].append(worker)

        LOG.info(f"[GARBAGE COLLECTOR] Found {len(self._issues['physical_worker'])} orphaned physical workers.")
        LOG.debug(f"[GARBAGE COLLECTOR] Orphaned physical workers: {self._issues['physical_worker']}")

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
        invalid_run_ids = [run.get_id() for run in self._issues["run"]]

        # Get all valid run IDs from the database
        all_runs = self.merlin_db.runs.get_all()
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
                self._issues["study"].append(study)

        LOG.info(f"[GARBAGE COLLECTOR] Found {len(self._issues['study'])} empty studies.")

    def _cleanup_entity(self, entity_type: str):
        """
        Remove entities of a specific type that were identified as stale.

        Args:
            entity_type: Type of entity to clean up (run, logical_worker, physical_worker, study).
        """
        # Get the plural form of the entity type for logging purposes
        entity_plural = get_plural_of_entity(entity_type, split_delimiter="_", join_delimiter="_")

        if not self._issues[entity_type]:
            LOG.info(f"[GARBAGE COLLECTOR] No stale {entity_plural} found.")
            return

        LOG.info(f"[GARBAGE COLLECTOR] Deleting {len(self._issues[entity_type])} {entity_plural}...")
        for entity in self._issues[entity_type]:
            entity_id = entity.get_id()
            try:
                if entity_type == "study":
                    self.merlin_db.delete(entity_type, entity_id, remove_associated_runs=False)
                else:
                    self.merlin_db.delete(entity_type, entity_id)
                LOG.debug(f"[GARBAGE COLLECTOR] Deleted {entity_type} {entity_id}")
            except (RunNotFoundError, StudyNotFoundError, WorkerNotFoundError) as e:
                LOG.error(f"[GARBAGE COLLECTOR] Failed to delete {entity_type} {entity_id}: {e}")

    def cleanup_runs(self):
        """Remove runs with invalid workspaces."""
        self._cleanup_entity("run")

    def cleanup_logical_workers(self):
        """Remove orphaned logical workers."""
        self._cleanup_entity("logical_worker")

    def cleanup_physical_workers(self):
        """Remove orphaned physical workers."""
        self._cleanup_entity("physical_worker")

    def cleanup_studies(self):
        """Remove studies with no valid runs."""
        self._cleanup_entity("study")

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
        report_lines.append(f"Invalid Runs: {len(self._issues['run'])}")
        if self._issues["run"]:
            for run in self._issues["run"]:
                report_lines.append(f"  - {run.get_workspace()}")

        report_lines.append("")

        # Orphaned Logical Workers section
        report_lines.append(f"Orphaned Logical Workers: {len(self._issues['logical_worker'])}")
        if self._issues["logical_worker"]:
            for worker in self._issues["logical_worker"]:
                report_lines.append(f"  - {worker.get_name()} (queues: {', '.join(worker.get_queues())})")

        report_lines.append("")

        # Orphaned Physical Workers section
        report_lines.append(f"Orphaned Physical Workers: {len(self._issues['physical_worker'])}")
        if self._issues["physical_worker"]:
            for worker in self._issues["physical_worker"]:
                report_lines.append(f"  - {worker.get_name()} (host: {worker.get_host()})")

        report_lines.append("")

        # Empty Studies section
        report_lines.append(f"Empty Studies: {len(self._issues['study'])}")
        if self._issues["study"]:
            for study in self._issues["study"]:
                report_lines.append(f"  - {study.get_name()}")

        report_lines.append("")

        report_lines.append("=" * 60)

        report_lines.append("")

        # Potentially Inaccessible Runs section
        report_lines.append(f"Potentially Inaccessible Runs: {len(self._issues['inaccessible_runs'])}")
        if self._issues["inaccessible_runs"]:
            for run in self._issues["inaccessible_runs"]:
                report_lines.append(f"  - {run.get_workspace()}")
            report_lines.append("")
            inaccessible_message = (
                "You may need to re-run garbage collection on a machine that can access these runs, "
                "or remove them manually if they are local runs being flagged as inaccessible."
            )
            wrapped_message = textwrap.fill(inaccessible_message, width=60)
            report_lines.append(wrapped_message)

        report_lines.append("")

        report_lines.append("=" * 60)

        return "\n".join(report_lines)

    def scan(
        self,
        check_runs: bool = True,
        check_logical_workers: bool = True,
        check_physical_workers: bool = True,
        check_studies: bool = True,
    ):
        """
        Scan the database for stale entries without deleting anything.

        Args:
            check_runs: Whether to check for invalid run workspaces.
            check_logical_workers: Whether to check for orphaned logical workers.
            check_physical_workers: Whether to check for orphaned physical workers.
            check_studies: Whether to check for empty studies.
        """
        LOG.info("[GARBAGE COLLECTOR] Scanning database for stale entries...")

        # Check phase
        if check_runs:
            self.check_run_workspaces()

        if check_logical_workers:
            self.check_orphaned_logical_workers()

        if check_physical_workers:
            self.check_orphaned_physical_workers()

        if check_studies:
            self.check_empty_studies()

        LOG.info("[GARBAGE COLLECTOR] Scan complete.")

        # Report findings
        LOG.info(f"\n{self.generate_report()}")

    def clean(
        self,
        check_runs: bool = True,
        check_logical_workers: bool = True,
        check_physical_workers: bool = True,
        check_studies: bool = True,
        force: bool = False,
    ):
        """
        Delete all previously identified stale entries.

        This method should be called after scan(). It will prompt for
        confirmation unless force=True.

        Args:
            check_runs: Whether to check for invalid run workspaces.
            check_logical_workers: Whether to check for orphaned logical workers.
            check_physical_workers: Whether to check for orphaned physical workers.
            check_studies: Whether to check for empty studies.
            force: If True, skip confirmation prompt (use with caution).
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
        if check_physical_workers:
            self.cleanup_physical_workers()
        if check_logical_workers:
            self.cleanup_logical_workers()
        if check_studies:
            self.cleanup_studies()

        LOG.info("[GARBAGE COLLECTOR] Cleanup complete.")

    def scan_and_clean(
        self,
        check_runs: bool = True,
        check_logical_workers: bool = True,
        check_physical_workers: bool = True,
        check_studies: bool = True,
        force: bool = False,
    ):
        """
        Convenience method that scans for garbage and then cleans it up.

        This is equivalent to calling scan() followed by clean().

        Args:
            check_runs: Whether to check for invalid run workspaces.
            check_logical_workers: Whether to check for orphaned logical workers.
            check_physical_workers: Whether to check for orphaned physical workers.
            check_studies: Whether to check for empty studies.
            force: If True, skip confirmation prompt (use with caution).
        """
        LOG.info("[GARBAGE COLLECTOR] Starting database garbage collection...")

        self.scan(
            check_runs=check_runs,
            check_logical_workers=check_logical_workers,
            check_physical_workers=check_physical_workers,
            check_studies=check_studies,
        )

        self.clean(
            check_runs=check_runs,
            check_logical_workers=check_logical_workers,
            check_physical_workers=check_physical_workers,
            check_studies=check_studies,
            force=force,
        )

        LOG.info("[GARBAGE COLLECTOR] Database garbage collection complete.")
