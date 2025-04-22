###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.12.2.
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
