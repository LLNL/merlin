##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Redis-based backend infrastructure for the Merlin application.

This package provides Redis-backed components for persisting and managing the core data
entities in Merlin (such as studies, runs, logical and physical workers). It includes
base store classes, concrete store implementations, and a full backend interface built
on top of Redis.

Modules:
    redis_backend: Implements the `ResultsBackend` interface using Redis.
    redis_store_base: Provides shared base logic for Redis-backed stores.
    redis_stores: Contains entity-specific Redis store classes and mixins.
"""
