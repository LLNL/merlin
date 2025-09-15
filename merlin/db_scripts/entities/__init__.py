##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
The `entities` package defines database entity classes used throughout Merlin for managing core
components such as studies, runs, and workers. These classes provide a structured interface for
interacting with persisted data, ensuring consistency and maintainability.

At the heart of this package is the abstract base class `DatabaseEntity`, which outlines the
standard methods that all database-backed entities must implement, including save, delete, and
reload operations.

Subpackages:
    - `mixins/`: Contains mixin classes for entities that help reduce shared code.

Modules:
    db_entity.py: Defines the abstract base class [`DatabaseEntity`][db_scripts.entities.db_entity.DatabaseEntity],
        which provides a common interface for all database entity classes.
    logical_worker_entity.py: Implements the
        [`LogicalWorkerEntity`][db_scripts.entities.logical_worker_entity.LogicalWorkerEntity]
        class, representing logical workers and their associated operations.
    physical_worker_entity.py: Defines the
        [`PhysicalWorkerEntity`][db_scripts.entities.physical_worker_entity.PhysicalWorkerEntity]
        class for managing physical workers stored in the database.
    run_entity.py: Implements the [`RunEntity`][db_scripts.entities.run_entity.RunEntity] class,
        which encapsulates database operations related to run records.
    study_entity.py: Defines the [`StudyEntity`][db_scripts.entities.study_entity.StudyEntity] class
        for handling study-related database interactions.
"""
