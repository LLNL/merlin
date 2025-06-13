##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Module for managing task queues associated with entities.

This module provides a mixin class, `QueueManagementMixin`, designed to simplify the
management of task queues associated with an entity. The mixin can be used by any
class that has the necessary attributes to support queue retrieval, specifically an
`entity_info` object containing a `queues` list.
"""

from typing import List


# pylint: disable=too-few-public-methods


class QueueManagementMixin:
    """
    Mixin for managing queues associated with an entity.

    This mixin provides a method for retrieving the task queues assigned to an entity.
    It assumes that the class using this mixin has an `entity_info` object containing
    a `queues` list.

    Methods:
        get_queues:
            Retrieve the task queues assigned to the entity.
    """

    def get_queues(self) -> List[str]:
        """
        Get the queues that this entity is assigned to.

        Assumptions:
            - The class using this must have an `entity_info` object containing a `queues` list

        Returns:
            A list of queue names.
        """
        return self.entity_info.queues
