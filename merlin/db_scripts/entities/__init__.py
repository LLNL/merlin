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
    logical_worker_entity.py: Implements the [`LogicalWorkerEntity`][db_scripts.entities.logical_worker_entity.LogicalWorkerEntity]
        class, representing logical workers and their associated operations.
    physical_worker_entity.py: Defines the [`PhysicalWorkerEntity`][db_scripts.entities.physical_worker_entity.PhysicalWorkerEntity]
        class for managing physical workers stored in the database.
    run_entity.py: Implements the [`RunEntity`][db_scripts.entities.run_entity.RunEntity] class,
        which encapsulates database operations related to run records.
    study_entity.py: Defines the [`StudyEntity`][db_scripts.entities.study_entity.StudyEntity] class
        for handling study-related database interactions.
"""
