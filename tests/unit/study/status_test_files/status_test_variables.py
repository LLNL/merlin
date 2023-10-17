###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.10.2.
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
MERLIN_INFO_PATH = f"{VALID_WORKSPACE_PATH}/merlin_info"
EXPANDED_SPEC_PATH = f"{MERLIN_INFO_PATH}/status_test_spec.expanded.yaml"
SAMPLES_PATH = f"{MERLIN_INFO_PATH}/samples.csv"

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
NUM_ALL_REQUESTED_STATUSES = sum(TASKS_PER_STEP.values()) - TASKS_PER_STEP["unstarted_step"]

# This is the requested statuses with just the failed step
REQUESTED_STATUSES_JUST_FAILED_STEP = {
    "fail_step": {
        "parameters": {"cmd": None, "restart": None},
        "task_queue": "fail_queue",
        "worker_name": "other_worker",
        "fail_step": {
            "status": "FAILED",
            "return_code": "MERLIN_SOFT_FAIL",
            "elapsed_time": "0d:00h:00m:00s",
            "run_time": "0d:00h:00m:00s",
            "restarts": 0,
        },
    }
}

# This is the requested statuses with just the cancelled step
REQUESTED_STATUSES_JUST_CANCELLED_STEP = {
    "cancel_step": {
        "parameters": {"cmd": None, "restart": None},
        "task_queue": "cancel_queue",
        "worker_name": "other_worker",
        "cancel_step": {
            "status": "CANCELLED",
            "return_code": "MERLIN_STOP_WORKERS",
            "elapsed_time": "0d:00h:00m:00s",
            "run_time": "0d:00h:00m:00s",
            "restarts": 0,
        },
    }
}

# This is the requested statuses with both the failed step and the cancelled step
REQUESTED_STATUSES_FAIL_AND_CANCEL = {
    "fail_step": {
        "parameters": {"cmd": None, "restart": None},
        "task_queue": "fail_queue",
        "worker_name": "other_worker",
        "fail_step": {
            "status": "FAILED",
            "return_code": "MERLIN_SOFT_FAIL",
            "elapsed_time": "0d:00h:00m:00s",
            "run_time": "0d:00h:00m:00s",
            "restarts": 0,
        },
    },
    "cancel_step": {
        "parameters": {"cmd": None, "restart": None},
        "task_queue": "cancel_queue",
        "worker_name": "other_worker",
        "cancel_step": {
            "status": "CANCELLED",
            "return_code": "MERLIN_STOP_WORKERS",
            "elapsed_time": "0d:00h:00m:00s",
            "run_time": "0d:00h:00m:00s",
            "restarts": 0,
        },
    },
}

FORMATTED_STATUSES_FAIL_AND_CANCEL = {
    "step_name": ["fail_step", "cancel_step"],
    "step_workspace": ["fail_step", "cancel_step"],
    "status": ["FAILED", "CANCELLED"],
    "return_code": ["MERLIN_SOFT_FAIL", "MERLIN_STOP_WORKERS"],
    "elapsed_time": ["0d:00h:00m:00s", "0d:00h:00m:00s"],
    "run_time": ["0d:00h:00m:00s", "0d:00h:00m:00s"],
    "restarts": [0, 0],
    "cmd_parameters": ["-------", "-------"],
    "restart_parameters": ["-------", "-------"],
    "task_queue": ["fail_queue", "cancel_queue"],
    "worker_name": ["other_worker", "other_worker"],
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
        "DRY RUN": {"count": 0, "color": "\033[38;2;230;159;0m", "fill": "\\"},
        "TOTAL TASKS": {"total": 5},
        "WORKER NAME": {"name": "sample_worker"},
        "TASK QUEUE": {"name": "just_samples_queue"},
        "AVG RUN TIME": "01m:30s",
        "RUN TIME STD DEV": "±21s",
    },
    "just_parameters": {
        "FINISHED": {"count": 2, "color": "\033[38;2;0;158;115m", "fill": "█"},
        "CANCELLED": {"count": 0, "color": "\033[38;2;240;228;66m", "fill": "/"},
        "FAILED": {"count": 0, "color": "\033[38;2;213;94;0m", "fill": "⣿"},
        "UNKNOWN": {"count": 0, "color": "\033[38;2;102;102;102m", "fill": "?"},
        "INITIALIZED": {"count": 0, "color": "\033[38;2;86;180;233m"},
        "RUNNING": {"count": 0, "color": "\033[38;2;0;114;178m"},
        "DRY RUN": {"count": 0, "color": "\033[38;2;230;159;0m", "fill": "\\"},
        "TOTAL TASKS": {"total": 2},
        "WORKER NAME": {"name": "other_worker"},
        "TASK QUEUE": {"name": "just_parameters_queue"},
        "AVG RUN TIME": "01m:15s",
        "RUN TIME STD DEV": "±15s",
    },
    "params_and_samples": {
        "FINISHED": {"count": 10, "color": "\033[38;2;0;158;115m", "fill": "█"},
        "CANCELLED": {"count": 0, "color": "\033[38;2;240;228;66m", "fill": "/"},
        "FAILED": {"count": 0, "color": "\033[38;2;213;94;0m", "fill": "⣿"},
        "UNKNOWN": {"count": 0, "color": "\033[38;2;102;102;102m", "fill": "?"},
        "INITIALIZED": {"count": 0, "color": "\033[38;2;86;180;233m"},
        "RUNNING": {"count": 0, "color": "\033[38;2;0;114;178m"},
        "DRY RUN": {"count": 0, "color": "\033[38;2;230;159;0m", "fill": "\\"},
        "TOTAL TASKS": {"total": 10},
        "WORKER NAME": {"name": "sample_worker"},
        "TASK QUEUE": {"name": "both_queue"},
        "AVG RUN TIME": "16s",
        "RUN TIME STD DEV": "±06s",
    },
    "fail_step": {
        "FINISHED": {"count": 0, "color": "\033[38;2;0;158;115m", "fill": "█"},
        "CANCELLED": {"count": 0, "color": "\033[38;2;240;228;66m", "fill": "/"},
        "FAILED": {"count": 1, "color": "\033[38;2;213;94;0m", "fill": "⣿"},
        "UNKNOWN": {"count": 0, "color": "\033[38;2;102;102;102m", "fill": "?"},
        "INITIALIZED": {"count": 0, "color": "\033[38;2;86;180;233m"},
        "RUNNING": {"count": 0, "color": "\033[38;2;0;114;178m"},
        "DRY RUN": {"count": 0, "color": "\033[38;2;230;159;0m", "fill": "\\"},
        "TOTAL TASKS": {"total": 1},
        "WORKER NAME": {"name": "other_worker"},
        "TASK QUEUE": {"name": "fail_queue"},
        "AVG RUN TIME": "00s",
        "RUN TIME STD DEV": "±00s",
    },
    "cancel_step": {
        "FINISHED": {"count": 0, "color": "\033[38;2;0;158;115m", "fill": "█"},
        "CANCELLED": {"count": 1, "color": "\033[38;2;240;228;66m", "fill": "/"},
        "FAILED": {"count": 0, "color": "\033[38;2;213;94;0m", "fill": "⣿"},
        "UNKNOWN": {"count": 0, "color": "\033[38;2;102;102;102m", "fill": "?"},
        "INITIALIZED": {"count": 0, "color": "\033[38;2;86;180;233m"},
        "RUNNING": {"count": 0, "color": "\033[38;2;0;114;178m"},
        "DRY RUN": {"count": 0, "color": "\033[38;2;230;159;0m", "fill": "\\"},
        "TOTAL TASKS": {"total": 1},
        "WORKER NAME": {"name": "other_worker"},
        "TASK QUEUE": {"name": "cancel_queue"},
        "AVG RUN TIME": "00s",
        "RUN TIME STD DEV": "±00s",
    },
    "unstarted_step": "UNSTARTED",
}

RUN_TIME_INFO = {
    "just_parameters": {
        "avg_run_time": "01m:15s",
        "run_time_std_dev": "±15s",
    },
    "just_samples": {
        "avg_run_time": "01m:30s",
        "run_time_std_dev": "±21s",
    },
    "params_and_samples": {
        "avg_run_time": "16s",
        "run_time_std_dev": "±06s",
    },
    "fail_step": {
        "avg_run_time": "00s",
        "run_time_std_dev": "±00s",
    },
    "cancel_step": {
        "avg_run_time": "00s",
        "run_time_std_dev": "±00s",
    },
}

# This variable holds every status from the VALID_WORKSPACE in the format used when we first load them in
# i.e. the format loaded in by load_requested_statuses()
ALL_REQUESTED_STATUSES = {
    "just_parameters_GREET.hello.LEAVE.goodbye": {
        "parameters": {"cmd": {"GREET": "hello"}, "restart": {"LEAVE": "goodbye"}},
        "task_queue": "just_parameters_queue",
        "worker_name": "other_worker",
        "just_parameters/GREET.hello.LEAVE.goodbye": {
            "status": "FINISHED",
            "return_code": "MERLIN_SUCCESS",
            "elapsed_time": "0d:00h:02m:00s",
            "run_time": "0d:00h:01m:30s",
            "restarts": 0,
        },
    },
    "just_parameters_GREET.hola.LEAVE.adios": {
        "parameters": {"cmd": {"GREET": "hola"}, "restart": {"LEAVE": "adios"}},
        "task_queue": "just_parameters_queue",
        "worker_name": "other_worker",
        "just_parameters/GREET.hola.LEAVE.adios": {
            "status": "FINISHED",
            "return_code": "MERLIN_SUCCESS",
            "elapsed_time": "0d:00h:01m:00s",
            "run_time": "0d:00h:01m:00s",
            "restarts": 0,
        },
    },
    "just_samples": {
        "parameters": {"cmd": None, "restart": None},
        "task_queue": "just_samples_queue",
        "worker_name": "sample_worker",
        "just_samples/00": {
            "status": "FINISHED",
            "return_code": "MERLIN_SUCCESS",
            "elapsed_time": "0d:00h:02m:00s",
            "run_time": "0d:00h:01m:00s",
            "restarts": 0,
        },
        "just_samples/01": {
            "status": "FINISHED",
            "return_code": "MERLIN_SUCCESS",
            "elapsed_time": "0d:00h:02m:00s",
            "run_time": "0d:00h:01m:15s",
            "restarts": 0,
        },
        "just_samples/02": {
            "status": "FINISHED",
            "return_code": "MERLIN_SUCCESS",
            "elapsed_time": "0d:00h:02m:00s",
            "run_time": "0d:00h:01m:30s",
            "restarts": 0,
        },
        "just_samples/03": {
            "status": "FINISHED",
            "return_code": "MERLIN_SUCCESS",
            "elapsed_time": "0d:00h:02m:00s",
            "run_time": "0d:00h:01m:45s",
            "restarts": 0,
        },
        "just_samples/04": {
            "status": "FINISHED",
            "return_code": "MERLIN_SUCCESS",
            "elapsed_time": "0d:00h:02m:00s",
            "run_time": "0d:00h:02m:00s",
            "restarts": 0,
        },
    },
    "params_and_samples_GREET.hello": {
        "parameters": {"cmd": {"GREET": "hello"}, "restart": None},
        "task_queue": "both_queue",
        "worker_name": "sample_worker",
        "params_and_samples/GREET.hello/00": {
            "status": "FINISHED",
            "return_code": "MERLIN_SUCCESS",
            "elapsed_time": "0d:00h:00m:15s",
            "run_time": "0d:00h:00m:10s",
            "restarts": 0,
        },
        "params_and_samples/GREET.hello/01": {
            "status": "FINISHED",
            "return_code": "MERLIN_SUCCESS",
            "elapsed_time": "0d:00h:00m:15s",
            "run_time": "0d:00h:00m:11s",
            "restarts": 0,
        },
        "params_and_samples/GREET.hello/02": {
            "status": "FINISHED",
            "return_code": "MERLIN_SUCCESS",
            "elapsed_time": "0d:00h:00m:15s",
            "run_time": "0d:00h:00m:12s",
            "restarts": 0,
        },
        "params_and_samples/GREET.hello/03": {
            "status": "FINISHED",
            "return_code": "MERLIN_SUCCESS",
            "elapsed_time": "0d:00h:00m:15s",
            "run_time": "0d:00h:00m:13s",
            "restarts": 0,
        },
        "params_and_samples/GREET.hello/04": {
            "status": "FINISHED",
            "return_code": "MERLIN_SUCCESS",
            "elapsed_time": "0d:00h:00m:15s",
            "run_time": "0d:00h:00m:14s",
            "restarts": 0,
        },
    },
    "params_and_samples_GREET.hola": {
        "parameters": {"cmd": {"GREET": "hola"}, "restart": None},
        "task_queue": "both_queue",
        "worker_name": "sample_worker",
        "params_and_samples/GREET.hola/00": {
            "status": "FINISHED",
            "return_code": "MERLIN_SUCCESS",
            "elapsed_time": "0d:00h:00m:30s",
            "run_time": "0d:00h:00m:10s",
            "restarts": 0,
        },
        "params_and_samples/GREET.hola/01": {
            "status": "FINISHED",
            "return_code": "MERLIN_SUCCESS",
            "elapsed_time": "0d:00h:00m:30s",
            "run_time": "0d:00h:00m:18s",
            "restarts": 0,
        },
        "params_and_samples/GREET.hola/02": {
            "status": "FINISHED",
            "return_code": "MERLIN_SUCCESS",
            "elapsed_time": "0d:00h:00m:30s",
            "run_time": "0d:00h:00m:23s",
            "restarts": 0,
        },
        "params_and_samples/GREET.hola/03": {
            "status": "FINISHED",
            "return_code": "MERLIN_SUCCESS",
            "elapsed_time": "0d:00h:00m:30s",
            "run_time": "0d:00h:00m:29s",
            "restarts": 0,
        },
        "params_and_samples/GREET.hola/04": {
            "status": "FINISHED",
            "return_code": "MERLIN_SUCCESS",
            "elapsed_time": "0d:00h:00m:30s",
            "run_time": "0d:00h:00m:16s",
            "restarts": 0,
        },
    },
    "fail_step": {
        "parameters": {"cmd": None, "restart": None},
        "task_queue": "fail_queue",
        "worker_name": "other_worker",
        "fail_step": {
            "status": "FAILED",
            "return_code": "MERLIN_SOFT_FAIL",
            "elapsed_time": "0d:00h:00m:00s",
            "run_time": "0d:00h:00m:00s",
            "restarts": 0,
        },
    },
    "cancel_step": {
        "parameters": {"cmd": None, "restart": None},
        "task_queue": "cancel_queue",
        "worker_name": "other_worker",
        "cancel_step": {
            "status": "CANCELLED",
            "return_code": "MERLIN_STOP_WORKERS",
            "elapsed_time": "0d:00h:00m:00s",
            "run_time": "0d:00h:00m:00s",
            "restarts": 0,
        },
    },
}

# This variable holds every status from the VALID_WORKSPACE in the format used for displaying/dumping statuses
# i.e. the format returned by format_status_for_csv()
ALL_FORMATTED_STATUSES = {
    "step_name": [
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
    "step_workspace": [
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
    "status": [
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
    "return_code": [
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
    "elapsed_time": [
        "0d:00h:02m:00s",
        "0d:00h:01m:00s",
        "0d:00h:02m:00s",
        "0d:00h:02m:00s",
        "0d:00h:02m:00s",
        "0d:00h:02m:00s",
        "0d:00h:02m:00s",
        "0d:00h:00m:15s",
        "0d:00h:00m:15s",
        "0d:00h:00m:15s",
        "0d:00h:00m:15s",
        "0d:00h:00m:15s",
        "0d:00h:00m:30s",
        "0d:00h:00m:30s",
        "0d:00h:00m:30s",
        "0d:00h:00m:30s",
        "0d:00h:00m:30s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
    ],
    "run_time": [
        "0d:00h:01m:30s",
        "0d:00h:01m:00s",
        "0d:00h:01m:00s",
        "0d:00h:01m:15s",
        "0d:00h:01m:30s",
        "0d:00h:01m:45s",
        "0d:00h:02m:00s",
        "0d:00h:00m:10s",
        "0d:00h:00m:11s",
        "0d:00h:00m:12s",
        "0d:00h:00m:13s",
        "0d:00h:00m:14s",
        "0d:00h:00m:10s",
        "0d:00h:00m:18s",
        "0d:00h:00m:23s",
        "0d:00h:00m:29s",
        "0d:00h:00m:16s",
        "0d:00h:00m:00s",
        "0d:00h:00m:00s",
    ],
    "restarts": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    "cmd_parameters": [
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
    "restart_parameters": [
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
    "task_queue": [
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
    "worker_name": [
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
