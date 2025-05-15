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
The `mixins` package provides reusable mixin classes that encapsulate common behaviors
shared across multiple entity types in the Merlin system.

These mixins are designed to be composed into larger classes (e.g., entity models or
entity managers) without enforcing inheritance from a shared base, offering lightweight
extensions to class behavior.

Modules:
    name.py: Defines the [`NameMixin`][db_scripts.mixins.name.NameMixin], which provides name-based
        access and utility methods for entities with a `name` attribute.
    queue_management.py: Defines the [`QueueManagementMixin`][db_scripts.mixins.queue_management.QueueManagementMixin],
        which supports retrieval and filtering of task queues for entities with a `queues` field.
    run_management.py: Defines the [`RunManagementMixin`][db_scripts.mixins.run_management.RunManagementMixin],
        which provides functionality for managing run associations on entities that support saving,
        reloading, and tracking linked run IDs.
"""
