##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Module for managing entities with names.

This module provides a mixin class, `NameMixin`, designed to simplify the management
of names associated with an entity. The mixin can be used by any class that has the
necessary attributes to support name retrieval, specifically an `entity_info` object
containing a `name` attribute.
"""

# pylint: disable=too-few-public-methods


class NameMixin:
    """
    Mixin for entities that have a name.

    This mixin provides a method for retrieving the name associated with an entity.
    It assumes that the class using this mixin has an `entity_info` object containing
    a `name` attribute.

    Methods:
        get_name:
            Retrieve the name associated with the entity.
    """

    def get_name(self) -> str:
        """
        Get the name of this entity.

        Assumptions:
            - The class using this must have an `entity_info` object containing a `name` attribute

        Returns:
            The name of this entity.
        """
        return self.entity_info.name
