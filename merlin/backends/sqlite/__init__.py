##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
SQLite-based backend infrastructure for the Merlin application.

This package provides all components necessary to persist and manage Merlin's core data
entities (such as studies, runs, and workers) using SQLite as the storage backend.
It includes a generic store base, specialized store classes for each entity type,
connection handling logic, and a complete backend implementation conforming to
Merlin's backend interface.

Modules:
    sqlite_backend: Implements the `ResultsBackend` interface using SQLite.
    sqlite_connection: Provides a context-managed SQLite connection with safe configuration.
    sqlite_store_base: Defines a generic base class for entity stores.
    sqlite_stores: Contains concrete SQLite store classes for Merlin models.
"""
