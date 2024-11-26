###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.12.2
#
# For details, see https://github.com/LLNL/merlin.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################
"""
This file contains all of the constants used for the status command.
Separating this from status.py and status_renderers.py helps with circular
import issues.
"""

VALID_STATUS_FILTERS = ("INITIALIZED", "RUNNING", "FINISHED", "FAILED", "CANCELLED", "DRY_RUN", "UNKNOWN")
VALID_RETURN_CODES = ("SUCCESS", "SOFT_FAIL", "HARD_FAIL", "STOP_WORKERS", "RESTART", "RETRY", "DRY_SUCCESS", "UNRECOGNIZED")
VALID_EXIT_FILTERS = ("E", "EXIT")
ALL_VALID_FILTERS = VALID_STATUS_FILTERS + VALID_RETURN_CODES + VALID_EXIT_FILTERS + ("MAX_TASKS",)

# Listing worker_name below since it was used in v1.12.0 so if you try to run "merlin status" on a study
# ran with 1.12.0, then you'll need this key here for everything to function
CELERY_KEYS = set(["task_queue", "workers", "worker_name"])
RUN_TIME_STAT_KEYS = set(["avg_run_time", "run_time_std_dev"])
NON_WORKSPACE_KEYS = CELERY_KEYS.union(RUN_TIME_STAT_KEYS)
NON_WORKSPACE_KEYS.add("parameters")
