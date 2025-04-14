"""
Module for managing entities with names.

This module provides a mixin class, `NameMixin`, designed to simplify the management
of names associated with an entity. The mixin can be used by any class that has the
necessary attributes to support name retrieval, specifically an `entity_info` object
containing a `name` attribute.
"""


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

    def get_name(self):
        """
        Get the name associated with this logical worker.

        Assumptions:
            - The class using this must have an `entity_info` object containing a `name` attribute

        Returns:
            The name for this logical worker.
        """
        return self.entity_info.name
