###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.10.1.
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
"""This module holds variables that will be used to test against output from calls to status methods"""
import os


# Global path variables for files we'll need during these status tests
PATH_TO_TEST_FILES = f"{os.path.dirname(__file__)}"
SPEC_PATH = f"{PATH_TO_TEST_FILES}/status_test_spec.yaml"
VALID_WORKSPACE = "status_test_study_20230717-162921"
DUMMY_WORKSPACE = "status_test_study_20230713-000000"
VALID_WORKSPACE_PATH = f"{PATH_TO_TEST_FILES}/{VALID_WORKSPACE}"
DUMMY_WORKSPACE_PATH = f"{PATH_TO_TEST_FILES}/{DUMMY_WORKSPACE}"

# These globals are variables that will be tested against to ensure correct output
FULL_STEP_TRACKER = {
    "started_steps": ["just_samples", "just_parameters", "params_and_samples", "fail_step", "cancel_step"],
    "unstarted_steps": ["unstarted_step"],
}
TASKS_PER_STEP = {
    "just_samples": 5,
    "just_parameters": 2,
    "params_and_samples": 10,
    "fail_step": 1,
    "cancel_step": 1,
    "unstarted_step": 1,
}
REAL_STEP_NAME_MAP = {
    "just_samples": ["just_samples"],
    "just_parameters": ["just_parameters_GREET.hello.LEAVE.goodbye", "just_parameters_GREET.hola.LEAVE.adios"],
    "params_and_samples": ["params_and_samples_GREET.hello", "params_and_samples_GREET.hola"],
    "fail_step": ["fail_step"],
    "cancel_step": ["cancel_step"],
}
NUM_ALL_REQUESTED_STATUSES = sum(TASKS_PER_STEP.values()) - TASKS_PER_STEP["unstarted_step"]

# This is the requested statuses with just the failed step
REQUESTED_STATUSES_JUST_FAILED_STEP = {
    "fail_step": {
        "Cmd Parameters": None,
        "Restart Parameters": None,
        "Task Queue": "fail_queue",
        "Worker Name": "other_worker",
        "fail_step": {
            "Status": "FAILED",
            "Return Code": "MERLIN_SOFT_FAIL",
            "Elapsed Time": "0d:00h:00m:00s",
            "Run Time": "0d:00h:00m:00s",
            "Restarts": 0,
        },
    }
}

# This is the requested statuses with just the cancelled step
REQUESTED_STATUSES_JUST_CANCELLED_STEP = {
    "cancel_step": {
        "Cmd Parameters": None,
        "Restart Parameters": None,
        "Task Queue": "cancel_queue",
        "Worker Name": "other_worker",
        "cancel_step": {
            "Status": "CANCELLED",
            "Return Code": "MERLIN_STOP_WORKERS",
            "Elapsed Time": "0d:00h:00m:00s",
            "Run Time": "0d:00h:00m:00s",
            "Restarts": 0,
        },
    }
}

# This is the requested statuses with both the failed step and the cancelled step
REQUESTED_STATUSES_FAIL_AND_CANCEL = {
    "fail_step": {
        "Cmd Parameters": None,
        "Restart Parameters": None,
        "Task Queue": "fail_queue",
        "Worker Name": "other_worker",
        "fail_step": {
            "Status": "FAILED",
            "Return Code": "MERLIN_SOFT_FAIL",
            "Elapsed Time": "0d:00h:00m:00s",
            "Run Time": "0d:00h:00m:00s",
            "Restarts": 0,
        },
    },
    "cancel_step": {
        "Cmd Parameters": None,
        "Restart Parameters": None,
        "Task Queue": "cancel_queue",
        "Worker Name": "other_worker",
        "cancel_step": {
            "Status": "CANCELLED",
            "Return Code": "MERLIN_STOP_WORKERS",
            "Elapsed Time": "0d:00h:00m:00s",
            "Run Time": "0d:00h:00m:00s",
            "Restarts": 0,
        },
    },
}

# This variable holds the result of applying a max_tasks limit of 3 to the VALID_WORKSPACE
REQUESTED_STATUSES_WITH_MAX_TASKS = {
    "just_parameters_GREET.hello.LEAVE.goodbye": {
        "Cmd Parameters": "GREET:hello",
        "Restart Parameters": "LEAVE:goodbye",
        "Task Queue": "just_parameters_queue",
        "Worker Name": "other_worker",
        "just_parameters/GREET.hello.LEAVE.goodbye": {
            "Status": "FINISHED",
            "Return Code": "MERLIN_SUCCESS",
            "Elapsed Time": "0d:00h:00m:00s",
            "Run Time": "0d:00h:00m:00s",
            "Restarts": 0,
        },
    },
    "just_parameters_GREET.hola.LEAVE.adios": {
        "Cmd Parameters": "GREET:hola",
        "Restart Parameters": "LEAVE:adios",
        "Task Queue": "just_parameters_queue",
        "Worker Name": "other_worker",
        "just_parameters/GREET.hola.LEAVE.adios": {
            "Status": "FINISHED",
            "Return Code": "MERLIN_SUCCESS",
            "Elapsed Time": "0d:00h:00m:00s",
            "Run Time": "0d:00h:00m:00s",
            "Restarts": 0,
        },
    },
    "just_samples": {
        "Cmd Parameters": None,
        "Restart Parameters": None,
        "Task Queue": "just_samples_queue",
        "Worker Name": "sample_worker",
        "just_samples/00": {
            "Status": "FINISHED",
            "Return Code": "MERLIN_SUCCESS",
            "Elapsed Time": "0d:00h:00m:00s",
            "Run Time": "0d:00h:00m:00s",
            "Restarts": 0,
        },
    },
}

# This variable holds the state_info dict of every step from VALID_WORKSPACE
# i.e. the format returned by the display() method when run in test_mode
DISPLAY_INFO = {
    "just_samples": {
        "FINISHED": {"count": 5, "color": "\033[38;2;0;158;115m", "fill": "█"},
        "CANCELLED": {"count": 0, "color": "\033[38;2;240;228;66m", "fill": "/"},
        "FAILED": {"count": 0, "color": "\033[38;2;213;94;0m", "fill": "⣿"},
        "UNKNOWN": {"count": 0, "color": "\033[38;2;102;102;102m", "fill": "?"},
        "INITIALIZED": {"count": 0, "color": "\033[38;2;86;180;233m"},
        "RUNNING": {"count": 0, "color": "\033[38;2;0;114;178m"},
        "DRY_RUN": {"count": 0, "color": "\033[38;2;230;159;0m", "fill": "\\"},
        "TOTAL_TASKS": {"total": 5},
        "WORKER_NAME": {"name": "sample_worker"},
        "TASK_QUEUE": {"name": "just_samples_queue"},
    },
    "just_parameters": {
        "FINISHED": {"count": 2, "color": "\033[38;2;0;158;115m", "fill": "█"},
        "CANCELLED": {"count": 0, "color": "\033[38;2;240;228;66m", "fill": "/"},
        "FAILED": {"count": 0, "color": "\033[38;2;213;94;0m", "fill": "⣿"},
        "UNKNOWN": {"count": 0, "color": "\033[38;2;102;102;102m", "fill": "?"},
        "INITIALIZED": {"count": 0, "color": "\033[38;2;86;180;233m"},
        "RUNNING": {"count": 0, "color": "\033[38;2;0;114;178m"},
        "DRY_RUN": {"count": 0, "color": "\033[38;2;230;159;0m", "fill": "\\"},
        "TOTAL_TASKS": {"total": 2},
        "WORKER_NAME": {"name": "other_worker"},
        "TASK_QUEUE": {"name": "just_parameters_queue"},
    },
    "params_and_samples": {
        "FINISHED": {"count": 10, "color": "\033[38;2;0;158;115m", "fill": "█"},
        "CANCELLED": {"count": 0, "color": "\033[38;2;240;228;66m", "fill": "/"},
        "FAILED": {"count": 0, "color": "\033[38;2;213;94;0m", "fill": "⣿"},
        "UNKNOWN": {"count": 0, "color": "\033[38;2;102;102;102m", "fill": "?"},
        "INITIALIZED": {"count": 0, "color": "\033[38;2;86;180;233m"},
        "RUNNING": {"count": 0, "color": "\033[38;2;0;114;178m"},
        "DRY_RUN": {"count": 0, "color": "\033[38;2;230;159;0m", "fill": "\\"},
        "TOTAL_TASKS": {"total": 10},
        "WORKER_NAME": {"name": "sample_worker"},
        "TASK_QUEUE": {"name": "both_queue"},
    },
    "fail_step": {
        "FINISHED": {"count": 0, "color": "\033[38;2;0;158;115m", "fill": "█"},
        "CANCELLED": {"count": 0, "color": "\033[38;2;240;228;66m", "fill": "/"},
        "FAILED": {"count": 1, "color": "\033[38;2;213;94;0m", "fill": "⣿"},
        "UNKNOWN": {"count": 0, "color": "\033[38;2;102;102;102m", "fill": "?"},
        "INITIALIZED": {"count": 0, "color": "\033[38;2;86;180;233m"},
        "RUNNING": {"count": 0, "color": "\033[38;2;0;114;178m"},
        "DRY_RUN": {"count": 0, "color": "\033[38;2;230;159;0m", "fill": "\\"},
        "TOTAL_TASKS": {"total": 1},
        "WORKER_NAME": {"name": "other_worker"},
        "TASK_QUEUE": {"name": "fail_queue"},
    },
    "cancel_step": {
        "FINISHED": {"count": 0, "color": "\033[38;2;0;158;115m", "fill": "█"},
        "CANCELLED": {"count": 1, "color": "\033[38;2;240;228;66m", "fill": "/"},
        "FAILED": {"count": 0, "color": "\033[38;2;213;94;0m", "fill": "⣿"},
        "UNKNOWN": {"count": 0, "color": "\033[38;2;102;102;102m", "fill": "?"},
        "INITIALIZED": {"count": 0, "color": "\033[38;2;86;180;233m"},
        "RUNNING": {"count": 0, "color": "\033[38;2;0;114;178m"},
        "DRY_RUN": {"count": 0, "color": "\033[38;2;230;159;0m", "fill": "\\"},
        "TOTAL_TASKS": {"total": 1},
        "WORKER_NAME": {"name": "other_worker"},
        "TASK_QUEUE": {"name": "cancel_queue"},
    },
    "unstarted_step": "UNSTARTED",
}

# This variable holds every status from the VALID_WORKSPACE in the format used when we first load them in
# i.e. the format loaded in by load_requested_statuses()
ALL_REQUESTED_STATUSES = {
    "just_parameters_GREET.hello.LEAVE.goodbye": {
        "Cmd Parameters": "GREET:hello",
        "Restart Parameters": "LEAVE:goodbye",
        "Task Queue": "just_parameters_queue",
        "Worker Name": "other_worker",
        "just_parameters/GREET.hello.LEAVE.goodbye": {
            "Status": "FINISHED",
            "Return Code": "MERLIN_SUCCESS",
            "Elapsed Time": "0d:00h:00m:00s",
            "Run Time": "0d:00h:00m:00s",
            "Restarts": 0,
        },
    },
    "just_parameters_GREET.hola.LEAVE.adios": {
        "Cmd Parameters": "GREET:hola",
        "Restart Parameters": "LEAVE:adios",
        "Task Queue": "just_parameters_queue",
        "Worker Name": "other_worker",
        "just_parameters/GREET.hola.LEAVE.adios": {
            "Status": "FINISHED",
            "Return Code": "MERLIN_SUCCESS",
            "Elapsed Time": "0d:00h:00m:00s",
            "Run Time": "0d:00h:00m:00s",
            "Restarts": 0,
        },
    },
    "just_samples": {
        "Cmd Parameters": None,
        "Restart Parameters": None,
        "Task Queue": "just_samples_queue",
        "Worker Name": "sample_worker",
        "just_samples/00": {
            "Status": "FINISHED",
            "Return Code": "MERLIN_SUCCESS",
            "Elapsed Time": "0d:00h:00m:00s",
            "Run Time": "0d:00h:00m:00s",
            "Restarts": 0,
        },
        "just_samples/01": {
            "Status": "FINISHED",
            "Return Code": "MERLIN_SUCCESS",
            "Elapsed Time": "0d:00h:00m:00s",
            "Run Time": "0d:00h:00m:00s",
            "Restarts": 0,
        },
        "just_samples/02": {
            "Status": "FINISHED",
            "Return Code": "MERLIN_SUCCESS",
            "Elapsed Time": "0d:00h:00m:00s",
            "Run Time": "0d:00h:00m:00s",
            "Restarts": 0,
        },
        "just_samples/03": {
            "Status": "FINISHED",
            "Return Code": "MERLIN_SUCCESS",
            "Elapsed Time": "0d:00h:00m:00s",
            "Run Time": "0d:00h:00m:00s",
            "Restarts": 0,
        },
        "just_samples/04": {
            "Status": "FINISHED",
            "Return Code": "MERLIN_SUCCESS",
            "Elapsed Time": "0d:00h:00m:00s",
            "Run Time": "0d:00h:00m:00s",
            "Restarts": 0,
        },
    },
    "params_and_samples_GREET.hello": {
        "Cmd Parameters": "GREET:hello",
        "Restart Parameters": None,
        "Task Queue": "both_queue",
        "Worker Name": "sample_worker",
        "params_and_samples/GREET.hello/00": {
            "Status": "FINISHED",
            "Return Code": "MERLIN_SUCCESS",
            "Elapsed Time": "0d:00h:00m:00s",
            "Run Time": "0d:00h:00m:00s",
            "Restarts": 0,
        },
        "params_and_samples/GREET.hello/01": {
            "Status": "FINISHED",
            "Return Code": "MERLIN_SUCCESS",
            "Elapsed Time": "0d:00h:00m:00s",
            "Run Time": "0d:00h:00m:00s",
            "Restarts": 0,
        },
        "params_and_samples/GREET.hello/02": {
            "Status": "FINISHED",
            "Return Code": "MERLIN_SUCCESS",
            "Elapsed Time": "0d:00h:00m:00s",
            "Run Time": "0d:00h:00m:00s",
            "Restarts": 0,
        },
        "params_and_samples/GREET.hello/03": {
            "Status": "FINISHED",
            "Return Code": "MERLIN_SUCCESS",
            "Elapsed Time": "0d:00h:00m:00s",
            "Run Time": "0d:00h:00m:00s",
            "Restarts": 0,
        },
        "params_and_samples/GREET.hello/04": {
            "Status": "FINISHED",
            "Return Code": "MERLIN_SUCCESS",
            "Elapsed Time": "0d:00h:00m:00s",
            "Run Time": "0d:00h:00m:00s",
            "Restarts": 0,
        },
    },
    "params_and_samples_GREET.hola": {
        "Cmd Parameters": "GREET:hola",
        "Restart Parameters": None,
        "Task Queue": "both_queue",
        "Worker Name": "sample_worker",
        "params_and_samples/GREET.hola/00": {
            "Status": "FINISHED",
            "Return Code": "MERLIN_SUCCESS",
            "Elapsed Time": "0d:00h:00m:00s",
            "Run Time": "0d:00h:00m:00s",
            "Restarts": 0,
        },
        "params_and_samples/GREET.hola/01": {
            "Status": "FINISHED",
            "Return Code": "MERLIN_SUCCESS",
            "Elapsed Time": "0d:00h:00m:00s",
            "Run Time": "0d:00h:00m:00s",
            "Restarts": 0,
        },
        "params_and_samples/GREET.hola/02": {
            "Status": "FINISHED",
            "Return Code": "MERLIN_SUCCESS",
            "Elapsed Time": "0d:00h:00m:00s",
            "Run Time": "0d:00h:00m:00s",
            "Restarts": 0,
        },
        "params_and_samples/GREET.hola/03": {
            "Status": "FINISHED",
            "Return Code": "MERLIN_SUCCESS",
            "Elapsed Time": "0d:00h:00m:00s",
            "Run Time": "0d:00h:00m:00s",
            "Restarts": 0,
        },
        "params_and_samples/GREET.hola/04": {
            "Status": "FINISHED",
            "Return Code": "MERLIN_SUCCESS",
            "Elapsed Time": "0d:00h:00m:00s",
            "Run Time": "0d:00h:00m:00s",
            "Restarts": 0,
        },
    },
    "fail_step": {
        "Cmd Parameters": None,
        "Restart Parameters": None,
        "Task Queue": "fail_queue",
        "Worker Name": "other_worker",
        "fail_step": {
            "Status": "FAILED",
            "Return Code": "MERLIN_SOFT_FAIL",
            "Elapsed Time": "0d:00h:00m:00s",
            "Run Time": "0d:00h:00m:00s",
            "Restarts": 0,
        },
    },
    "cancel_step": {
        "Cmd Parameters": None,
        "Restart Parameters": None,
        "Task Queue": "cancel_queue",
        "Worker Name": "other_worker",
        "cancel_step": {
            "Status": "CANCELLED",
            "Return Code": "MERLIN_STOP_WORKERS",
            "Elapsed Time": "0d:00h:00m:00s",
            "Run Time": "0d:00h:00m:00s",
            "Restarts": 0,
        },
    },
}

# This variable holds every status from the VALID_WORKSPACE in the format used for displaying/dumping statuses
# i.e. the format returned by format_status_for_display()
ALL_FORMATTED_STATUSES = {
    "Step Name": [
        "just_parameters_GREET.hello.LEAVE.goodbye",
        "just_parameters_GREET.hola.LEAVE.adios",
        "just_samples",
        "just_samples",
        "just_samples",
        "just_samples",
        "just_samples",
        "params_and_samples_GREET.hello",
        "params_and_samples_GREET.hello",
        "params_and_samples_GREET.hello",
        "params_and_samples_GREET.hello",
        "params_and_samples_GREET.hello",
        "params_and_samples_GREET.hola",
        "params_and_samples_GREET.hola",
        "params_and_samples_GREET.hola",
        "params_and_samples_GREET.hola",
        "params_and_samples_GREET.hola",
        "fail_step",
        "cancel_step",
    ],
    "Step Workspace": [
        "just_parameters/GREET.hello.LEAVE.goodbye",
        "just_parameters/GREET.hola.LEAVE.adios",
        "just_samples/00",
        "just_samples/01",
        "just_samples/02",
        "just_samples/03",
        "just_samples/04",
        "params_and_samples/GREET.hello/00",
        "params_and_samples/GREET.hello/01",
        "params_and_samples/GREET.hello/02",
        "params_and_samples/GREET.hello/03",
        "params_and_samples/GREET.hello/04",
        "params_and_samples/GREET.hola/00",
        "params_and_samples/GREET.hola/01",
        "params_and_samples/GREET.hola/02",
        "params_and_samples/GREET.hola/03",
        "params_and_samples/GREET.hola/04",
        "fail_step",
        "cancel_step",
    ],
    "Status": [
        "FINISHED",
        "FINISHED",
        "FINISHED",
        "FINISHED",
        "FINISHED",
        "FINISHED",
        "FINISHED",
        "FINISHED",
        "FINISHED",
        "FINISHED",
        "FINISHED",
        "FINISHED",
        "FINISHED",
        "FINISHED",
        "FINISHED",
        "FINISHED",
        "FINISHED",
        "FAILED",
        "CANCELLED",
    ],
    "Return Code": [
        "MERLIN_SUCCESS",
        "MERLIN_SUCCESS",
        "MERLIN_SUCCESS",
        "MERLIN_SUCCESS",
        "MERLIN_SUCCESS",
        "MERLIN_SUCCESS",
        "MERLIN_SUCCESS",
        "MERLIN_SUCCESS",
        "MERLIN_SUCCESS",
        "MERLIN_SUCCESS",
        "MERLIN_SUCCESS",
        "MERLIN_SUCCESS",
        "MERLIN_SUCCESS",
        "MERLIN_SUCCESS",
        "MERLIN_SUCCESS",
        "MERLIN_SUCCESS",
        "MERLIN_SUCCESS",
        "MERLIN_SOFT_FAIL",
        "MERLIN_STOP_WORKERS",
    ],
    "Elapsed Time": [
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
    ],
    "Run Time": [
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
    ],
    "Restarts": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    "Cmd Parameters": [
        "GREET:hello",
        "GREET:hola",
        "-------",
        "-------",
        "-------",
        "-------",
        "-------",
        "GREET:hello",
        "GREET:hello",
        "GREET:hello",
        "GREET:hello",
        "GREET:hello",
        "GREET:hola",
        "GREET:hola",
        "GREET:hola",
        "GREET:hola",
        "GREET:hola",
        "-------",
        "-------",
    ],
    "Restart Parameters": [
        "LEAVE:goodbye",
        "LEAVE:adios",
        "-------",
        "-------",
        "-------",
        "-------",
        "-------",
        "-------",
        "-------",
        "-------",
        "-------",
        "-------",
        "-------",
        "-------",
        "-------",
        "-------",
        "-------",
        "-------",
        "-------",
    ],
    "Task Queue": [
        "just_parameters_queue",
        "just_parameters_queue",
        "just_samples_queue",
        "just_samples_queue",
        "just_samples_queue",
        "just_samples_queue",
        "just_samples_queue",
        "both_queue",
        "both_queue",
        "both_queue",
        "both_queue",
        "both_queue",
        "both_queue",
        "both_queue",
        "both_queue",
        "both_queue",
        "both_queue",
        "fail_queue",
        "cancel_queue",
    ],
    "Worker Name": [
        "other_worker",
        "other_worker",
        "sample_worker",
        "sample_worker",
        "sample_worker",
        "sample_worker",
        "sample_worker",
        "sample_worker",
        "sample_worker",
        "sample_worker",
        "sample_worker",
        "sample_worker",
        "sample_worker",
        "sample_worker",
        "sample_worker",
        "sample_worker",
        "sample_worker",
        "other_worker",
        "other_worker",
    ],
}
