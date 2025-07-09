##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
Task server interface and implementations for Merlin.

This module provides the pluggable task server architecture that allows
Merlin to work with different task distribution systems while maintaining
a consistent interface.
"""

from merlin.task_servers.task_server_factory import task_server_factory
from merlin.task_servers.task_server_interface import TaskServerInterface

__all__ = ["TaskServerInterface", "task_server_factory"]

