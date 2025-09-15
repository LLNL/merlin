##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Backend infrastructure for the Merlin application.

The `backends` package provides a unified interface and implementations for persisting
and retrieving Merlin's core data entities (such as studies, runs, and workers) across
various storage technologies. It defines an abstract backend interface (`ResultsBackend`)
along with concrete implementations using Redis and SQLite, as well as utility functions,
store abstractions, and a backend factory.

Subpackages:
    redis: Redis-based backend implementation, including store classes and a Redis-backed `ResultsBackend`.
    sqlite: SQLite-based backend implementation, with support for persistent local storage of Merlin data models.

Modules:
    backend_factory: Contains `MerlinBackendFactory`, used to dynamically select and instantiate a backend.
    results_backend: Defines the abstract `ResultsBackend` base class for backend implementations.
    store_base: Provides the abstract `StoreBase` class, the foundation for all store implementations.
    utils: Utility functions for serialization, deserialization, and error handling across backends.

This package serves as the backend layer in Merlin's architecture, enabling flexible,
pluggable data persistence with a consistent API regardless of the underlying storage engine.
"""
