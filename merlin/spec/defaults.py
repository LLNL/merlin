##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module defines the default values for every block in a Merlin specification file.

Merlin specification files are used to configure workflows, tasks, and resources in
Merlin. This module provides the default values for each block in the spec file, ensuring
that workflows can execute even if certain fields are omitted by the user. These defaults
serve as a fallback mechanism to maintain consistency and simplify the configuration process.

This module serves as a reference for the default configuration of Merlin specification files.
"""

DESCRIPTION = {"description": {}}

BATCH = {"batch": {"type": "local", "dry_run": False, "shell": "/bin/bash"}}

ENV = {"env": {"variables": {}, "sources": [], "labels": {}, "dependencies": {}}}

STUDY_STEP_RUN = {"task_queue": "merlin", "shell": "/bin/bash", "max_retries": 30}

PARAMETER = {"global.parameters": {}}

MERLIN = {
    "merlin": {
        "resources": {"task_server": "celery", "overlap": False, "workers": None},
        "samples": None,
    }
}

WORKER = {"steps": ["all"], "nodes": None, "batch": None}

SAMPLES = {
    "generate": {"cmd": "echo 'Insert sample-generating command here'"},
    "level_max_dirs": 25,
}

# Values of the form (step key to search for, default value if no step key found)
VLAUNCHER_VARS = {
    "MERLIN_NODES": ("nodes", 1),
    "MERLIN_PROCS": ("procs", 1),
    "MERLIN_CORES": ("cores per task", 1),
    "MERLIN_GPUS": ("gpus", 0),
}
