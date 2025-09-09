##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module defines all the possible keys for each block in a Merlin specification file.

Merlin specification files are used to configure and manage studies, workflows, and tasks
in Merlin. Each block in the spec file corresponds to a specific aspect of the workflow,
such as batch settings, environment variables, study steps, parameters, and resources.
This module provides sets of valid keys for each block to ensure consistency and validation.

This module serves as a reference for the structure and valid keys of Merlin specification files.
"""

DESCRIPTION = {"description", "name"}

BATCH = {
    "type",
    "bank",
    "queue",
    "dry_run",
    "shell",
    "flux_path",
    "flux_start_opts",
    "flux_exec",
    "flux_exec_workers",
    "launch_pre",
    "launch_args",
    "worker_launch",
    "nodes",
    "walltime",
}

ENV = {"variables", "labels", "sources", "dependencies"}

STUDY_STEP = {"name", "description", "run"}

STUDY_STEP_RUN = {
    "cmd",
    "restart",
    "task_queue",
    "shell",
    "max_retries",
    "depends",
    "nodes",
    "procs",
    "cores per task",
    "gpus per task",
    "batch",
    "slurm",
    "lsf",
    "bind",
    "num resource set",
    "launch_distribution",
    "exit_on_error",
    "flux",
    "walltime",
}

PARAMETER = {"values", "label"}

MERLIN_RESOURCES = {"task_server", "overlap", "workers"}

MERLIN = {"resources", "samples"}

WORKER = {"steps", "nodes", "batch", "args", "machines"}

SAMPLES = {"generate", "level_max_dirs", "file", "column_labels"}
