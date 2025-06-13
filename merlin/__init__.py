##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Merlin: Machine Learning for HPC Workflows.

This module contains the source code for Merlin.
"""

import os
import sys


__version__ = "1.13.0b1"
VERSION = __version__
PATH_TO_PROJ = os.path.join(os.path.dirname(__file__), "")

CLI_MOD = "merlin.main"


def is_using_cli():
    """
    Checks whether the merlin module is currently using the CLI.
    """
    return CLI_MOD in sys.modules
