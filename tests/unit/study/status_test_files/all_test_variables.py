"""This module holds all of the variables that will be used to test against output from calls to status methods"""
import json
import os

# Global path variables for files we'll need during these status tests
PATH_TO_TEST_FILES = f"{os.path.dirname(__file__)}"
SPEC_PATH = f"{PATH_TO_TEST_FILES}/status_test_spec.yaml"
VALID_WORKSPACE = "status_test_study_20230717-162921"
DUMMY_WORKSPACE = "status_test_study_20230713-000000"
VALID_WORKSPACE_PATH = f"{PATH_TO_TEST_FILES}/{VALID_WORKSPACE}"
DUMMY_WORKSPACE_PATH = f"{PATH_TO_TEST_FILES}/{DUMMY_WORKSPACE}"

# These globals are variables that will be tested against to ensure correct output
FULL_STEP_TRACKER = {"started_steps": ["just_samples", "just_parameters", "params_and_samples", "fail_step", "cancel_step"], "unstarted_steps": ["unstarted_step"]}
TASKS_PER_STEP = {"just_samples": 5, "just_parameters": 2, "params_and_samples": 10, "fail_step": 1, "cancel_step": 1, "unstarted_step": 1}
REAL_STEP_NAME_MAP = {
    "just_samples": ["just_samples"],
    "just_parameters": ["just_parameters_GREET.hello.LEAVE.goodbye", "just_parameters_GREET.hola.LEAVE.adios"],
    "params_and_samples": ["params_and_samples_GREET.hello", "params_and_samples_GREET.hola"],
    "fail_step": ["fail_step"],
    "cancel_step": ["cancel_step"],
}

# The all_statuses.json file holds every status from the VALID_WORKSPACE in the format used when we first load them in
# i.e. the format returned by get_requested_statuses()
with open(f"{PATH_TO_TEST_FILES}/all_statuses.json", "r") as all_statuses_file:
    ALL_REQUESTED_STATUSES = json.load(all_statuses_file)

NUM_ALL_REQUESTED_STATUSES = sum(TASKS_PER_STEP.values()) - TASKS_PER_STEP["unstarted_step"]

# The all_statuses_formatted.json file holds every status from the VALID_WORKSPACE in the format used for displaying/dumping statuses
# i.e. the format returned by format_status_for_display()
with open(f"{PATH_TO_TEST_FILES}/all_statuses_formatted.json", "r") as all_statuses_formatted_file:
    ALL_FORMATTED_STATUSES = json.load(all_statuses_formatted_file)

# The status_display_info.json file holds the state_info dict of every step from VALID_WORKSPACE
# i.e. the format returned by the display() method when run in test_mode
with open(f"{PATH_TO_TEST_FILES}/status_display_info.json", "r") as status_display_info_file:
    DISPLAY_INFO = json.load(status_display_info_file)