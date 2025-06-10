##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
The `db_scripts` package provides the core database infrastructure for the Merlin system.

This package defines the models, entities, and management logic required to persist, retrieve,
and manipulate data stored in Merlin's database. It encapsulates both low-level data models and
high-level entity managers, offering a structured, maintainable interface for all database
interactions within the system.

Subpackages:
    - `entities/`: Contains entity classes that wrap database models and expose higher-level
      operations such as `save`, `delete`, and `reload`.
    - `entity_managers/`: Provides manager classes that orchestrate CRUD operations across
      entity types, with support for inter-entity references and cleanup routines.

Modules:
    data_models.py: Defines the dataclasses used to represent raw records in the database,
        such as [`StudyModel`][db_scripts.data_models.StudyModel], [`RunModel`][db_scripts.data_models.RunModel],
        etc.
    db_commands.py: Exposes database-related commands intended for external use (e.g., CLI or scripts),
        allowing users to interact with Merlin's stored data.
    merlin_db.py: Contains the [`MerlinDatabase`][db_scripts.merlin_db.MerlinDatabase] class, which
        aggregates all entity managers and acts as the central access point for database operations
        across the system.
"""
