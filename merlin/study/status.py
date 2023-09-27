###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.10.0
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
"""This module handles all the functionality of getting the statuses of studies"""
import json
import logging
import os
import re
from argparse import Namespace
from datetime import datetime
from glob import glob
from typing import Dict, List, Optional, Tuple

import numpy as np
from filelock import FileLock, Timeout
from tabulate import tabulate

from merlin.common.dumper import dump_handler
from merlin.display import ANSI_COLORS, display_status_summary
from merlin.spec.expansion import get_spec_with_expansion
from merlin.utils import (
    convert_timestring,
    convert_to_timedelta,
    dict_deep_merge,
    pretty_format_HMS,
    verify_dirpath,
    ws_time_to_dt,
)


LOG = logging.getLogger(__name__)
VALID_STATUS_FILTERS = ("INITIALIZED", "RUNNING", "FINISHED", "FAILED", "CANCELLED", "DRY_RUN", "UNKNOWN")
VALID_RETURN_CODES = ("SUCCESS", "SOFT_FAIL", "HARD_FAIL", "STOP_WORKERS", "RESTART", "RETRY", "DRY_SUCCESS", "UNRECOGNIZED")
VALID_EXIT_FILTERS = ("E", "EXIT")
ALL_VALID_FILTERS = VALID_STATUS_FILTERS + VALID_RETURN_CODES + VALID_EXIT_FILTERS + ("MAX_TASKS",)
CELERY_KEYS = set(["task_queue", "worker_name"])
RUN_TIME_STAT_KEYS = set(["avg_run_time", "run_time_std_dev"])
NON_WORKSPACE_KEYS = CELERY_KEYS.union(RUN_TIME_STAT_KEYS)


class Status:
    """
    This class handles everything to do with status besides displaying it.
    Display functionality is handled in display.py.
    """

    def __init__(self, args: Namespace, spec_display: bool, file_or_ws: str):
        # Save the args to this class instance and check if the steps filter was given
        self.args = args

        # Load in the workspace path and spec object
        if spec_display:
            self.workspace, self.spec = self._load_from_spec(file_or_ws)
        else:
            self.workspace = file_or_ws
            self.spec = self._load_from_workspace()

        # Verify the filter args (this will only do something for DetailedStatus)
        self._verify_filter_args()

        # Create a step tracker that will tell us which steps have started/not started
        self.step_tracker = self.get_steps_to_display()

        # Create a tasks per step mapping in order to give accurate totals for each step
        self.tasks_per_step = self.spec.get_tasks_per_step()

        # Variable to store the statuses that the user wants
        self.requested_statuses = {}
        self.load_requested_statuses()

    def _verify_filter_args(self):
        """
        This is an abstract method since we'll need to verify filter args for DetailedStatus
        but not for Status.
        """

    def _get_latest_study(self, studies: List[str]) -> str:
        """
        Given a list of studies, get the latest one.

        :param `studies`: A list of studies to sort through
        :returns: The latest study in the list provided
        """
        # We can assume the newest study is the last one to be added to the list of potential studies
        newest_study = studies[-1]
        newest_timestring = newest_study[-15:]
        newest_study_date = ws_time_to_dt(newest_timestring)

        # Check that the newest study somehow isn't the last entry
        for study in studies:
            temp_timestring = study[-15:]
            date_to_check = ws_time_to_dt(temp_timestring)
            if date_to_check > newest_study_date:
                newest_study = study
                newest_study_date = date_to_check

        return newest_study

    def _obtain_study(self, study_output_dir: str, num_studies: int, potential_studies: List[Tuple[int, str]]) -> str:
        """
        Grab the study that the user wants to view the status of based on a list of potential studies provided.

        :param `study_output_dir`: A string representing the output path of a study; equivalent to $(OUTPUT_PATH)
        :param `num_studies`: The number of potential studies we found
        :param `potential_studies`: The list of potential studies we found;
                                    Each entry is of the form (index, potential_study_name)
        :returns: A directory path to the study that the user wants
                  to view the status of ("study_output_dir/selected_potential_study")
        """
        study_to_check = f"{study_output_dir}/"
        if num_studies == 0:
            raise ValueError("Could not find any potential studies.")
        if num_studies > 1:
            # Get the latest study
            if self.args.no_prompts:
                LOG.info("Choosing the latest study...")
                potential_studies = [study for _, study in potential_studies]
                latest_study = self._get_latest_study(potential_studies)
                LOG.info(f"Chose {latest_study}")
                study_to_check += latest_study
            # Ask the user which study to view
            else:
                print(f"Found {num_studies} potential studies:")
                print(tabulate(potential_studies, headers=["Index", "Study Name"]))
                prompt = "Which study would you like to view the status of? Use the index on the left: "
                index = -1
                while index < 1 or index > num_studies:
                    try:
                        index = int(input(prompt))
                        if index < 1 or index > num_studies:
                            raise ValueError
                    except ValueError:
                        print(
                            f"{ANSI_COLORS['RED']}Input must be an integer between 1 "
                            f"and {num_studies}.{ANSI_COLORS['RESET']}"
                        )
                        prompt = "Enter a different index: "
                study_to_check += potential_studies[index - 1][1]
        else:
            # Only one study was found so we'll just assume that's the one the user wants
            study_to_check += potential_studies[0][1]

        return study_to_check

    def _load_from_spec(self, filepath: str) -> Tuple[str, "MerlinSpec"]:  # noqa: F821 pylint: disable=R0914
        """
        Get the desired workspace from the user and load up it's yaml spec
        for further processing.

        :param `filepath`: The filepath to a spec given by the user
        :returns: The workspace of the study we'll check the status for and a MerlinSpec
                object loaded in from the workspace's merlin_info subdirectory.
        """
        # Get the output path of the study that was given to us
        # Case where the output path is left out of the spec file
        if self.args.spec_provided.output_path == "":
            output_path = os.path.dirname(filepath)
        # Case where output path is absolute
        elif self.args.spec_provided.output_path.startswith("/"):
            output_path = self.args.spec_provided.output_path
        # Case where output path is relative to the specroot
        else:
            output_path = f"{os.path.dirname(filepath)}/{self.args.spec_provided.output_path}"

        LOG.debug(f"Verifying output path: {output_path}...")
        study_output_dir = verify_dirpath(output_path)
        LOG.debug(f"Output path verified. Expanded version: {study_output_dir}")

        # Build a list of potential study output directories
        study_output_subdirs = next(os.walk(study_output_dir))[1]
        timestamp_regex = r"\d{8}-\d{6}"
        potential_studies = []
        num_studies = 0
        LOG.debug(f"All subdirs in output path: {study_output_subdirs}")
        for subdir in study_output_subdirs:
            match = re.search(rf"{self.args.spec_provided.name}_{timestamp_regex}", subdir)
            if match:
                potential_studies.append((num_studies + 1, subdir))
                num_studies += 1
        LOG.debug(f"Potential studies: {potential_studies}")

        # Obtain the correct study to view the status of based on the list of potential studies we just built
        LOG.debug("Obtaining a study to view the status of...")
        study_to_check = self._obtain_study(study_output_dir, num_studies, potential_studies)
        LOG.debug(f"Selected '{study_to_check}' for viewing.")

        # Verify the directory that the user selected is a merlin study output directory
        if "merlin_info" not in next(os.walk(study_to_check))[1]:
            raise ValueError(
                f"The merlin_info subdirectory was not found. {study_to_check} may not be a Merlin study output directory."
            )

        # Grab the spec saved to the merlin info directory in case something
        # in the current spec has changed since starting the study
        expanded_spec_options = glob(f"{study_to_check}/merlin_info/*.expanded.yaml")
        if len(expanded_spec_options) > 1:
            raise ValueError(f"Multiple expanded spec options found in the {study_to_check}/merlin_info/ directory")
        if len(expanded_spec_options) < 1:
            raise ValueError(f"No expanded spec options found in the {study_to_check}/merlin_info/ directory")

        LOG.debug(f"Creating a spec object from '{expanded_spec_options[0]}'...")
        actual_spec = get_spec_with_expansion(expanded_spec_options[0])
        LOG.debug("Spec object created.")

        return study_to_check, actual_spec

    def _load_from_workspace(self) -> "MerlinSpec":  # noqa: F821
        """
        Create a MerlinSpec object based on the spec file in the workspace.

        :returns: A MerlinSpec object loaded from the workspace provided by the user
        """
        # Grab the spec file from the directory provided
        expanded_spec_options = glob(f"{self.workspace}/merlin_info/*.expanded.yaml")
        if len(expanded_spec_options) > 1:
            raise ValueError(f"Multiple expanded spec options found in the {self.workspace}/merlin_info/ directory")
        if len(expanded_spec_options) < 1:
            raise ValueError(f"No expanded spec options found in the {self.workspace}/merlin_info/ directory")

        # Create a MerlinSpec object from the expanded spec we grabbed
        LOG.debug(f"Creating a spec object from '{expanded_spec_options[0]}'...")
        spec = get_spec_with_expansion(expanded_spec_options[0])
        LOG.debug("Spec object created.")

        return spec

    def _create_step_tracker(self, steps_to_check: List[str]) -> Dict[str, List[str]]:
        """
        Creates a dictionary of started and unstarted steps that we
        will display the status for.

        :param `steps_to_check`: A list of steps to view the status of
        :returns: A dictionary mapping of started and unstarted steps. Values are lists of step names.
        """
        step_tracker = {"started_steps": [], "unstarted_steps": []}
        started_steps = next(os.walk(self.workspace))[1]
        started_steps.remove("merlin_info")

        LOG.debug(f"All started steps: {started_steps}")

        for sstep in started_steps:
            if sstep in steps_to_check:
                step_tracker["started_steps"].append(sstep)
                steps_to_check.remove(sstep)
        step_tracker["unstarted_steps"] = steps_to_check

        LOG.debug(f"Started steps after (potentially) filtering: {step_tracker['started_steps']}")
        LOG.debug(f"Unstarted steps: {step_tracker['unstarted_steps']}")

        return step_tracker

    def get_steps_to_display(self) -> Dict[str, List[str]]:
        """
        Generates a list of steps to display the status for based on information
        provided to the merlin status command by the user.

        :returns: A dictionary of started and unstarted steps for us to display the status of
        """
        existing_steps = self.spec.get_study_step_names()

        LOG.debug(f"existing steps: {existing_steps}")
        LOG.debug("Building step tracker based on existing steps...")

        # Filter the steps to display status for by started/unstarted
        step_tracker = self._create_step_tracker(existing_steps)

        LOG.debug("Step tracker created.")

        return step_tracker

    @property
    def num_requested_statuses(self):
        """
        Count the number of task statuses in a the requested_statuses dict.
        We need to ignore non workspace keys when we count.
        """
        num_statuses = 0
        for step_name in self.step_tracker["started_steps"]:
            for status_key, status_info in self.requested_statuses[step_name].items():
                if status_key in RUN_TIME_STAT_KEYS:
                    continue
                num_statuses += len(status_info.keys() - NON_WORKSPACE_KEYS)
        return num_statuses

    def get_step_statuses(self, step_workspace: str, started_step_name: str) -> Dict[str, List[str]]:
        """
        Given a step workspace and the name of the step, read in all the statuses
        for the step and return them in a dict.

        :param `step_workspace`: The path to the step we're going to read statuses from
        :param `started_step_name`: The name of the started step that we're getting statuses from
        :returns: A dict of statuses for the given step
        """
        step_statuses = {started_step_name: {}}
        num_statuses_read = 0

        # Traverse the step workspace and look for MERLIN_STATUS files
        LOG.info(f"Traversing '{step_workspace}' to find MERLIN_STATUS.json files...")
        for root, _, _ in os.walk(step_workspace):
            # Search for a status file
            status_filepath = os.path.join(root, "MERLIN_STATUS.json")
            matching_files = glob(status_filepath)
            if matching_files:
                LOG.debug(f"Found status file at '{status_filepath}'")
                # Read in the statuses and count how many statuses we read
                lock = FileLock(f"{root}/status.lock")  # pylint: disable=E0110
                statuses_read = read_status(status_filepath, lock)
                for status_info in statuses_read.values():
                    num_statuses_read += len(status_info.keys() - NON_WORKSPACE_KEYS)

                # Merge the statuses we read with the dict tracking all statuses for this step
                dict_deep_merge(step_statuses[started_step_name], statuses_read)

        LOG.info(
            f"Done traversing '{step_workspace}'. Read in {num_statuses_read} "
            f"{'statuses' if num_statuses_read != 1 else 'status'}."
        )

        # Calculate run time average and standard deviation for this step
        step_statuses = self.get_runtime_avg_std_dev(step_statuses, started_step_name)

        return step_statuses

    def load_requested_statuses(self):
        """
        Populate the requested_statuses dict with the statuses from the study.
        """
        LOG.info(f"Reading task statuses from {self.workspace}")

        # Read in all statuses from the started steps the user wants to see
        for sstep in self.step_tracker["started_steps"]:
            step_workspace = f"{self.workspace}/{sstep}"
            step_statuses = self.get_step_statuses(step_workspace, sstep)
            dict_deep_merge(self.requested_statuses, step_statuses)

        # Count how many statuses in total that we just read in
        LOG.info(f"Read in {self.num_requested_statuses} statuses total.")

    def get_runtime_avg_std_dev(self, step_statuses: Dict, step_name: str) -> Dict:
        """
        Calculate the mean and standard deviation for the runtime of each step.
        Add this to the state information once calculated.

        :param `step_statuses`: A dict of step status information that we'll parse for run times
        :param `step_name`: The name of the step
        :returns: An updated dict of step status info with run time avg and std dev
        """
        # Initialize a list to track all existing runtimes
        run_times_in_seconds = []

        # This outer loop will only loop once
        LOG.info(f"Calculating run time avg and std dev for {step_name}...")
        for _, overall_step_info in step_statuses[step_name].items():
            for step_info_key, step_status_info in overall_step_info.items():
                # Ignore non-workspace keys and any run times that have been yet to be calculated
                if step_info_key in NON_WORKSPACE_KEYS or step_status_info["run_time"] == "--:--:--":
                    LOG.debug(f"Skipping {step_info_key}.")
                    continue

                # Parse the runtime value, convert it to seconds, and add it to the lsit of existing run times
                run_time = step_status_info["run_time"].replace("d", "").replace("h", "").replace("m", "").replace("s", "")
                run_time_tdelta = convert_to_timedelta(run_time)
                run_times_in_seconds.append(run_time_tdelta.total_seconds())

        # Using the list of existing run times, calculate avg and std dev
        LOG.debug(f"Using the following run times for our calculations: {run_times_in_seconds}")
        np_run_times_in_seconds = np.array(run_times_in_seconds)
        run_time_mean = round(np.mean(np_run_times_in_seconds))
        run_time_std_dev = round(np.std(np_run_times_in_seconds))
        LOG.debug(f"Run time avg in seconds: {run_time_mean}")
        LOG.debug(f"Run time std dev in seconds: {run_time_std_dev}")

        # Pretty format the avg and std dev and store them as new entries to the status information for the step
        step_statuses[step_name]["avg_run_time"] = pretty_format_HMS(convert_timestring(run_time_mean))
        step_statuses[step_name]["run_time_std_dev"] = f"±{pretty_format_HMS(convert_timestring(run_time_std_dev))}"
        LOG.info(f"Run time avg and std dev for {step_name} calculated.")

        return step_statuses

    def display(self, test_mode=False) -> Dict:
        """
        Displays the high level summary of the status.

        :param `test_mode`: If true, run this in testing mode and don't print any output
        :returns: A dict that will be empty if test_mode is False. Otherwise, the dict will
                  contain the status info that would be displayed.
        """
        return display_status_summary(self, NON_WORKSPACE_KEYS, test_mode=test_mode)

    def format_json_dump(self, date: datetime) -> Dict:
        """
        Build the dict of statuses to dump to the json file.

        :param `date`: A timestamp for us to mark when this status occurred
        :returns: A dictionary that's ready to dump to a json outfile
        """
        # Statuses are already in json format so we'll just add a timestamp for the dump here
        return {date: self.requested_statuses}

    def format_csv_dump(self, date: datetime) -> Dict:
        """
        Add the timestamp to the statuses to write.

        :param `date`: A timestamp for us to mark when this status occurred
        :returns: A dict equivalent of formatted statuses with a timestamp entry at the start of the dict.
        """
        # Reformat the statuses to a new dict where the keys are the column labels and rows are the values
        LOG.debug("Formatting statuses for csv dump...")
        statuses_to_write = self.format_status_for_display()
        LOG.debug("Statuses formatted.")

        # Add date entries as the first column then update this dict with the statuses we just reformatted
        statuses_with_timestamp = {"time_of_status": [date] * len(statuses_to_write["step_name"])}
        statuses_with_timestamp.update(statuses_to_write)

        return statuses_with_timestamp

    def dump(self):
        """
        Dump the status information to a file.
        """
        # Get a timestamp for this dump
        date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Handle different file types
        if self.args.dump.endswith(".csv"):
            # Build the lists of information/labels we'll need
            dump_info = self.format_csv_dump(date)
        elif self.args.dump.endswith(".json"):
            # Build the dict of info to dump to the json file
            dump_info = self.format_json_dump(date)

        # Dump the information
        dump_handler(self.args.dump, dump_info)

    def format_status_for_display(self) -> Dict:
        """
        Reformat our statuses to display so they can use Maestro's status renderer layouts.

        :returns: A formatted dictionary where each key is a column and the values are the rows
                  of information to display for that column.
        """
        reformatted_statuses = {
            "step_name": [],
            "step_workspace": [],
            "status": [],
            "return_code": [],
            "elapsed_time": [],
            "run_time": [],
            "restarts": [],
            "task_queue": [],
            "worker_name": [],
        }

        # We only care about started steps since unstarted steps won't have any status to report
        for step_name in self.step_tracker["started_steps"]:
            # Obtain and loop through all statuses
            step_statuses = self.requested_statuses[step_name]
            for full_step_name, overall_step_info in step_statuses.items():
                if full_step_name in RUN_TIME_STAT_KEYS:
                    continue

                # Get the number of statuses for this step so we know how many entries there should be
                num_statuses = len(overall_step_info.keys() - NON_WORKSPACE_KEYS)

                # Loop through information for each step
                for step_info_key, step_info_value in overall_step_info.items():
                    # Format celery specific keys
                    if step_info_key in CELERY_KEYS:
                        # Set the val_to_add value based on if a value exists for the key
                        val_to_add = step_info_value if step_info_value else "-------"
                        # Add the val_to_add entry for each row
                        key_entries = [val_to_add] * num_statuses
                        reformatted_statuses[step_info_key].extend(key_entries)

                    # Format workspace keys
                    else:
                        # Put the step name and workspace in each entry
                        reformatted_statuses["step_name"].append(step_name)
                        reformatted_statuses["step_workspace"].append(step_info_key)

                        # Add the rest of the information for each task (status, return code, elapsed & run time, num restarts)
                        for key, val in step_info_value.items():
                            reformatted_statuses[key].append(val)

        # For local runs, there will be no task queue or worker name so delete these entries
        for celery_specific_key in ("task_queue", "worker_name"):
            if not reformatted_statuses[celery_specific_key]:
                del reformatted_statuses[celery_specific_key]

        return reformatted_statuses


class DetailedStatus(Status):
    pass


def read_status(status_filepath: str, lock: FileLock, display_fnf_message: Optional[bool] = True) -> Dict:
    """
    Locks the status file for reading and returns its contents.

    :param `status_filepath`: The path to the status file that we'll read from
    :param `lock`: A FileLock object that we'll use to lock the file
    :param `display_fnf_message`: If True, display the file not found warning. Otherwise don't.
    :returns: A dict of the contents in the status file
    """
    try:
        # The status files will need locks when reading to avoid race conditions
        with lock.acquire(timeout=10):
            with open(status_filepath, "r") as status_file:
                statuses_read = json.load(status_file)
    # Handle timeouts
    except Timeout:
        LOG.warning(f"Timed out when trying to read status from {status_filepath}")
        statuses_read = {}
    # Handle FNF errors
    except FileNotFoundError:
        if display_fnf_message:
            LOG.warning(f"Could not find {status_filepath}")
        statuses_read = {}

    return statuses_read
