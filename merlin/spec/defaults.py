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
"""This module defines the default values of every block in the merlin spec"""

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
