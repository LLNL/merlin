##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Module for managing entities and their associated runs in the database.

This module provides a mixin class, `RunManagementMixin`, designed to simplify the management
of runs associated with an entity. The mixin can be used by any class that has the necessary
attributes and methods to support run management, such as `reload_data`, `save`, and an
`entity_info` object containing a `runs` list.
"""

from typing import List


class RunManagementMixin:
    """
    Mixin for managing runs associated with an entity.

    This mixin provides utility methods for handling run IDs associated with an entity.
    It assumes that the class using this mixin has the necessary attributes and methods
    to support run management, including `reload_data`, `save`, and an `entity_info` object
    containing a `runs` list.

    Methods:
        get_runs:
            Retrieve the IDs of the runs associated with the entity.

        add_run:
            Add a run ID to the list of runs.

        remove_run:
            Remove a run ID from the list of runs.
    """

    def get_runs(self) -> List[str]:
        """
        Get every run listed in this entity.

        Assumptions:
            - The class using this must have a `reload_data` method
            - The class using this must have an `entity_info` object containing a `runs` list

        Returns:
            A list of run IDs.
        """
        self.reload_data()
        return self.entity_info.runs

    def add_run(self, run_id: str):
        """
        Add a new run ID to the list of runs.

        Assumptions:
            - The class using this must have an `entity_info` object containing a `runs` list
            - The class using this must have a `save` method

        Args:
            run_id: The ID of the run to add.
        """
        self.entity_info.runs.append(run_id)
        self.save()

    def remove_run(self, run_id: str):
        """
        Remove a run ID from the list of runs.

        Does *not* delete the run entity from the database. This will only remove the run's ID
        from the list in this entity.

        Assumptions:
            - The class using this must have a `reload_data` method
            - The class using this must have an `entity_info` object containing a `runs` list
            - The class using this must have a `save` method

        Args:
            run_id: The ID of the run to remove.
        """
        self.reload_data()
        self.entity_info.runs.remove(run_id)
        self.save()

    def construct_run_string(self) -> str:
        """
        Constructs and returns a formatted string representation of all runs associated
        with the current instance.

        Returns:
            A formatted string containing details of all runs.
        """
        from merlin.db_scripts.entities.run_entity import RunEntity  # pylint: disable=import-outside-toplevel

        runs = self.get_runs()
        run_str = ""
        if runs:
            for run_id in runs:
                try:
                    run = RunEntity.load(run_id, self.backend)
                    run_str += f"  - ID: {run.get_id()}\n    Workspace: {run.get_workspace()}\n"
                except Exception:  # pylint: disable=broad-exception-caught
                    run_str += f"  - ID: {run_id} (Error loading run)\n"
        else:
            run_str = "  No runs found.\n"
        return run_str
