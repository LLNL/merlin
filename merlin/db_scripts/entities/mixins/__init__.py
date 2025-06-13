##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
The `mixins` package provides reusable mixin classes that encapsulate common behaviors
shared across multiple entity types in the Merlin system.

These mixins are designed to be composed into larger classes (e.g., entity models or
entity managers) without enforcing inheritance from a shared base, offering lightweight
extensions to class behavior.

Modules:
    name.py: Defines the [`NameMixin`][db_scripts.entities.mixins.name.NameMixin], which provides name-based
        access and utility methods for entities with a `name` attribute.
    queue_management.py: Defines the
        [`QueueManagementMixin`][db_scripts.entities.mixins.queue_management.QueueManagementMixin],
        which supports retrieval and filtering of task queues for entities with a `queues` field.
    run_management.py: Defines the
        [`RunManagementMixin`][db_scripts.entities.mixins.run_management.RunManagementMixin],
        which provides functionality for managing run associations on entities that support saving,
        reloading, and tracking linked run IDs.
"""
