##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Message serialization for Merlin task definitions.
"""

from .compressed_json_serializer import CompressedJsonSerializer

__all__ = ['CompressedJsonSerializer']