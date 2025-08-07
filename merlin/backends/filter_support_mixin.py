##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Provides a mixin class that adds filter-based retrieval support to backend
implementations within the Merlin database system.

This module defines the `FilterSupportMixin`, which can be used by backend
classes that support querying entities using field-based filters. It assumes
the backend implements `_get_store_by_type(store_type)` and that each store
supports a `retrieve_all_filtered(filters)` method.
"""

from typing import Dict, List

from merlin.db_scripts.data_models import BaseDataModel


class FilterSupportMixin:
    """
    Mixin for backends that support retrieving filtered entities from their
    underlying stores.

    This mixin provides a `retrieve_all_filtered` method that dispatches the
    filter query to the appropriate store based on `store_type`. It assumes the
    backend provides a `_get_store_by_type(store_type)` method and that each
    store supports filtering via a `retrieve_all_filtered(filters)` method.

    This class should be inherited alongside a ResultsBackend implementation
    that supports filtering (e.g., SQLite-based backends).
    """

    def retrieve_all_filtered(self, store_type: str, filters: Dict) -> List[BaseDataModel]:
        """
        Retrieve all objects from the specified store that match the given filters.

        Args:
            store_type: The type of store to query (e.g., 'study', 'run', etc.)
            filters: Dictionary of field-value pairs to filter on.

        Returns:
            A list of filtered data model objects.
        """
        store = self._get_store_by_type(store_type)
        return store.retrieve_all_filtered(filters)
