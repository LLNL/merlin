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
The `entity_managers` package contains classes responsible for managing high-level database
operations across all entity types in the Merlin system.

Each manager provides CRUD (Create, Read, Update, Delete) operations tailored to a specific
entity, such as studies, runs, or workers. These managers act as intermediaries between the
underlying data models and higher-level business logic, encapsulating shared behaviors and
coordinating entity relationships, lifecycle events, and cross-references.

Modules:
    entity_manager.py: Defines the abstract base class [`EntityManager`][db_scripts.entity_managers.entity_manager.EntityManager],
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