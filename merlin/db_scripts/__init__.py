###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.12.2.
#
# For details, see https://github.com/LLNL/merlin.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################

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
