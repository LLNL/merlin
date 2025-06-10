##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This file contains all of the constants used for the status command.
Separating this from [`status.py`][study.status] and [`status_renderers.py`][study.status_renderers]
helps with circular import issues.
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
