##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""

"""

from abc import ABC, abstractmethod
from typing import List, Dict

from rich.console import Console

from merlin.db_scripts.merlin_db import MerlinDatabase


class WorkerFormatter(ABC):
    """Base formatter for worker query output."""
    
    @abstractmethod
    def format_and_display(self, logical_workers: List, filters: Dict, merlin_db: MerlinDatabase, console: Console = None):
        """Format and display worker information."""
        pass
