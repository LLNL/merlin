###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.11.0.
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
"""This module defines all the keys possible in each block of a merlin spec file"""

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
