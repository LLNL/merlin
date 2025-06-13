##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
The `study` package contains functionality for defining, managing, and monitoring studies
in Merlin. A study represents a collection of tasks, steps, and workflows that can be
executed on distributed systems using various schedulers.

Modules:
    batch.py: Parses the batch section of the YAML specification, supporting worker
        launches for schedulers like Slurm, LSF, and Flux.
    celeryadapter.py: Provides an adapter for integrating with the Celery Distributed
        Task Queue, enabling distributed task execution.
    dag.py: Defines the Merlin `DAG` class, which represents the Directed Acyclic Graph
        structure of a study's workflow.
    script_adapter.py: Contains functionality for adapting bash scripts to work with
        supported schedulers, including Flux, LSF, and Slurm.
    status_constants.py: Defines constants used by the `status` module and its renderers,
        helping to avoid circular import issues.
    status_renderers.py: Handles the creation of formatted, task-by-task status displays
        for studies.
    status.py: Implements functionality for retrieving and displaying the statuses of studies.
    step.py: Contains the logic for representing and managing individual steps in a study.
    study.py: Implements the core logic for defining and managing a study as a whole.
"""
