##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
The `entity_managers` package contains classes responsible for managing high-level database
operations across all entity types in the Merlin system.

Each manager provides CRUD (Create, Read, Update, Delete) operations tailored to a specific
entity, such as studies, runs, or workers. These managers act as intermediaries between the
underlying data models and higher-level business logic, encapsulating shared behaviors and
coordinating entity relationships, lifecycle events, and cross-references.

Modules:
    entity_manager.py: Defines the abstract base class
        [`EntityManager`][db_scripts.entity_managers.entity_manager.EntityManager],
        which provides shared infrastructure and helper methods for all entity managers.
    logical_worker_manager.py: Manages logical workers, including queue-based identity
        resolution and cleanup of run associations on deletion.
    physical_worker_manager.py: Manages physical workers and handles cleanup of references
        from associated logical workers.
    run_manager.py: Manages run entities and coordinates updates to related studies and
        logical workers.
    study_manager.py: Manages study entities and orchestrates the creation and deletion of
        studies along with their associated runs.

These managers rely on the Merlin results backend and may optionally reference each other
via the central `MerlinDatabase` class to perform coordinated, entity-spanning operations.
"""
