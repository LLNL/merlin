##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""

"""

import logging
from typing import Dict, List

from merlin.workers.watchdogs import BaseWorkerWatchdog, CeleryWorkerWatchdog


LOG = logging.getLogger("merlin")


class WorkerWatchdogFactory:
    """
    
    """