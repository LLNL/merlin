##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
SQLite store implementations for Merlin entity models.

This module defines concrete `SQLiteStoreBase` subclasses for managing the
persistence of core entity models used in the Merlin application. Each store
is bound to a specific model and SQLite table, providing CRUD operations and
automated table creation.

See also:
    - merlin.backends.sqlite.sqlite_base_store: Base class
    - merlin.db_scripts.data_models: Data model definitions
"""

from merlin.backends.sqlite.sqlite_store_base import SQLiteStoreBase
from merlin.db_scripts.data_models import LogicalWorkerModel, PhysicalWorkerModel, RunModel, StudyModel


class SQLiteLogicalWorkerStore(SQLiteStoreBase[LogicalWorkerModel]):
    """
    A SQLite-based store for managing [`LogicalWorkerModel`][db_scripts.data_models.LogicalWorkerModel]
    objects.
    """

    def __init__(self):
        """Initialize the `SQLiteLogicalWorkerStore`."""
        super().__init__("logical_worker", LogicalWorkerModel)


class SQLitePhysicalWorkerStore(SQLiteStoreBase[PhysicalWorkerModel]):
    """
    A SQLite-based store for managing [`PhysicalWorkerModel`][db_scripts.data_models.PhysicalWorkerModel]
    objects.
    """

    def __init__(self):
        """Initialize the `SQLitePhysicalWorkerStore`."""
        super().__init__("physical_worker", PhysicalWorkerModel)


class SQLiteRunStore(SQLiteStoreBase[RunModel]):
    """
    A SQLite-based store for managing [`RunModel`][db_scripts.data_models.RunModel]
    objects.
    """

    def __init__(self):
        """Initialize the `SQLiteRunStore`."""
        super().__init__("run", RunModel)


class SQLiteStudyStore(SQLiteStoreBase[StudyModel]):
    """
    A SQLite-based store for managing [`StudyModel`][db_scripts.data_models.StudyModel]
    objects.
    """

    def __init__(self):
        """Initialize the `SQLiteStudyStore`."""
        super().__init__("study", StudyModel)
