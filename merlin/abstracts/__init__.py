##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
The `abstracts` package provides ABC classes that can be used throughout
Merlin's codebase.

Modules:
    factory: Contains `MerlinBaseFactory`, used to manage pluggable components in Merlin.
"""

from merlin.abstracts.factory import MerlinBaseFactory


__all__ = ["MerlinBaseFactory"]
