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
from copy import deepcopy
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union

from filelock import FileLock, Timeout

from merlin import display
from merlin.common.dumper import dump_handler
from merlin.config.configfile import CONFIG
from merlin.study.status_renderers import status_renderer_factory
from merlin.utils import dict_deep_merge, ws_time_to_td


LOG = logging.getLogger(__name__)
VALID_STATUS_FILTERS = ("INITIALIZED", "RUNNING", "FINISHED", "FAILED", "CANCELLED", "DRY_RUN", "UNKNOWN")
VALID_RETURN_CODES = ("SUCCESS", "SOFT_FAIL", "HARD_FAIL", "STOP_WORKERS", "RESTART", "RETRY", "DRY_SUCCESS", "UNRECOGNIZED")
VALID_EXIT_FILTERS = ("E", "EXIT")
ALL_VALID_FILTERS = VALID_STATUS_FILTERS + VALID_RETURN_CODES + VALID_EXIT_FILTERS + ("MAX_TASKS",)
NON_WORKSPACE_KEYS = set(["Cmd Parameters", "Restart Parameters", "Task Queue", "Worker Name"])


class Status:
    """
    This class handles everything to do with status besides displaying it.
    Display functionality is handled in display.py.
    """

    def __init__(self, args: "Namespace", spec_display: bool, file_or_ws: str):  # noqa: F821
        # Save the args to this class instance and check if the steps filter was given
        self.args = args
        self.steps_filter_provided = True if "all" not in args.steps else False

        # Load in the workspace path and spec object
        if spec_display:
            self.workspace, self.spec = self._load_from_spec(file_or_ws)
        else:
            self.workspace = file_or_ws
            self.spec = self._load_from_workspace()

        # Verify the args provided by the user
        self._verify_filter_args()

        # Create a step tracker that will tell us which steps we'll display that have started/not started
        self.step_tracker = self.get_steps_to_display()

        # Create a tasks per step mapping in order to give accurate totals for each step
        self.tasks_per_step = self.spec.get_tasks_per_step()

        # This attribute will store a map between the overall step name and the real step names
        # that are created with parameters (e.g. step name is hello and uses a "GREET: hello" parameter
        # so the real step name is hello_GREET.hello)
        self.real_step_name_map = {}

        # Variable to store the statuses that the user wants
        self.requested_statuses = {}
        self.get_requested_statuses()

    def _get_latest_study(self, studies: List[str]) -> str:
        """
        Given a list of studies, get the latest one.

        :param `studies`: A list of studies to sort through
        :returns: The latest study in the list provided
        """
        # We can assume the newest study is the last one to be added to the list of potential studies
        newest_study = studies[-1]
        newest_timestring = newest_study[-15:]
        newest_study_date = ws_time_to_td(newest_timestring)

        # Check that the newest study somehow isn't the last entry
        for study in studies:
            temp_timestring = study[-15:]
            date_to_check = ws_time_to_td(temp_timestring)
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
                print(f"Found {num_studies-1} potential studies:")
                display.tabulate_info(potential_studies, headers=["Index", "Study Name"])
                prompt = "Which study would you like to view the status of? Use the index on the left: "
                index = -1
                while index < 1 or index > num_studies:
                    try:
                        index = int(input(prompt))
                        if index < 1 or index > num_studies:
                            raise ValueError
                    except ValueError:
                        print(
                            f"{display.ANSI_COLORS['RED']}Input must be an integer between 1 "
                            f"and {num_studies}.{display.ANSI_COLORS['RESET']}"
                        )
                        prompt = "Enter a different index: "
                study_to_check += potential_studies[index - 1][1]
        else:
            # Only one study was found so we'll just assume that's the one the user wants
            study_to_check += potential_studies[0][1]

        return study_to_check

    def _load_from_spec(self, filepath: str) -> Tuple[str, "MerlinSpec"]:  # noqa: F821
        """
        Get the desired workspace from the user and load up it's yaml spec
        for further processing.

        :param `filepath`: The filepath to a spec given by the user
        :returns: The workspace of the study we'll check the status for and a MerlinSpec
                object loaded in from the workspace's merlin_info subdirectory.
        """
        from merlin.main import get_merlin_spec_with_override, get_spec_with_expansion, verify_dirpath  # pylint: disable=C0415

        # Get the spec and the output path for the spec
        self.args.specification = filepath
        spec_provided, _ = get_merlin_spec_with_override(self.args)

        # Case where the output path is left out of the spec file
        if spec_provided.output_path == "":
            output_path = os.path.dirname(filepath)
        # Case where output path is absolute
        elif spec_provided.output_path.startswith("/"):
            output_path = spec_provided.output_path
        # Case where output path is relative to the specroot
        else:
            output_path = f"{os.path.dirname(filepath)}/{spec_provided.output_path}"

        study_output_dir = verify_dirpath(output_path)

        # Build a list of potential study output directories
        study_output_subdirs = next(os.walk(study_output_dir))[1]
        timestamp_regex = r"\d{8}-\d{6}"
        potential_studies = []
        num_studies = 0
        for subdir in study_output_subdirs:
            match = re.search(rf"{spec_provided.name}_{timestamp_regex}", subdir)
            if match:
                potential_studies.append((num_studies + 1, subdir))
                num_studies += 1

        # Obtain the correct study to view the status of based on the list of potential studies we just built
        study_to_check = self._obtain_study(study_output_dir, num_studies, potential_studies)

        # Verify the directory that the user selected is a merlin study output directory
        if "merlin_info" not in next(os.walk(study_to_check))[1]:
            LOG.error(
                f"The merlin_info subdirectory was not found. {study_to_check} may not be a Merlin study output directory."
            )

        # Grab the spec saved to the merlin info directory in case something
        # in the current spec has changed since starting the study
        actual_spec = get_spec_with_expansion(f"{study_to_check}/merlin_info/{spec_provided.name}.expanded.yaml")

        return study_to_check, actual_spec

    def _load_from_workspace(self) -> "MerlinSpec":  # noqa: F821
        """
        Create a MerlinSpec object based on the spec file in the workspace.

        :returns: A MerlinSpec object loaded from the workspace provided by the user
        """
        from merlin.main import get_spec_with_expansion, verify_dirpath  # pylint: disable=C0415

        # Grab the spec file from the directory provided
        info_dir = verify_dirpath(f"{self.workspace}/merlin_info")
        spec_file = ""
        for _, _, files in os.walk(info_dir):
            for f in files:  # pylint: disable=C0103
                if f.endswith(".expanded.yaml"):
                    spec_file = f"{info_dir}/{f}"
                    break
            break

        # Make sure we got a spec file and load it in
        if not spec_file:
            LOG.error(f"Spec file not found in {info_dir}. Cannot display status.")
            return None
        spec = get_spec_with_expansion(spec_file)

        return spec

    def _verify_filters(self, filters_to_check: List[str], valid_options: Union[List, Tuple], warning_msg: Optional[str] = ""):
        """
        Check each filter in a list of filters provided by the user against a list of valid options.
        If the filter is invalid, remove it from the list of filters.

        :param `filters_to_check`: A list of filters provided by the user
        :param `valid_options`: A list of valid options for this particular filter
        :param `warning_msg`: An optional warning message to attach to output
        """
        for filter_arg in filters_to_check[:]:
            if filter_arg not in valid_options:
                LOG.warning(f"The filter {filter_arg} is invalid. {warning_msg}")
                filters_to_check.remove(filter_arg)

    def _verify_filter_args(self):
        """
        Verify that our filters are all valid and able to be used.

        :param `args`: The CLI arguments provided via the user
        :param `spec`: A MerlinSpec object loaded from the expanded spec in the output study directory
        """
        # Ensure the steps are valid
        if "all" not in self.args.steps:
            existing_steps = self.spec.get_study_step_names()
            self._verify_filters(
                self.args.steps, existing_steps, warning_msg="Removing this step from the list of steps to filter by..."
            )

        # Make sure max_tasks is a positive int
        if self.args.max_tasks and self.args.max_tasks < 1:
            LOG.warning("The value of --max-tasks must be greater than 0. Ignoring --max-tasks...")
            self.args.max_tasks = None

        # Make sure task_status is valid
        if self.args.task_status:
            self.args.task_status = [x.upper() for x in self.args.task_status]
            self._verify_filters(
                self.args.task_status,
                VALID_STATUS_FILTERS,
                warning_msg="Removing this status from the list of statuses to filter by...",
            )

        # Ensure return_code is valid
        if self.args.return_code:
            # TODO remove this code block and uncomment the one below once you've
            # implemented entries for restarts/retries
            idx = 0
            for ret_code_provided in self.args.return_code[:]:
                ret_code_provided = ret_code_provided.upper()
                if ret_code_provided in ("RETRY", "RESTART"):
                    LOG.warning(f"The {ret_code_provided} filter is coming soon. Ignoring this filter for now...")
                    self.args.return_code.remove(ret_code_provided)
                else:
                    self.args.return_code[idx] = f"MERLIN_{ret_code_provided}"
                    idx += 1

            # self.args.return_code = [f"MERLIN_{x.upper()}" for x in self.args.return_code]
            valid_return_codes = [f"MERLIN_{x}" for x in VALID_RETURN_CODES]
            self._verify_filters(
                self.args.return_code,
                valid_return_codes,
                warning_msg="Removing this code from the list of return codes to filter by...",
            )

        # Ensure every task queue provided exists
        if self.args.task_queues:
            existing_queues = self.spec.get_queue_list(["all"], omit_tag=True)
            self._verify_filters(
                self.args.task_queues,
                existing_queues,
                warning_msg="Removing this queue from the list of queues to filter by...",
            )

        # Ensure every worker provided exists
        if self.args.workers:
            worker_names = self.spec.get_worker_names()
            self._verify_filters(
                self.args.workers, worker_names, warning_msg="Removing this worker from the list of workers to filter by..."
            )

    def _process_workers(self):
        """
        Modifies the list of steps to display status for based on
        the list of workers provided by the user.
        """
        # Remove duplicates
        workers_provided = list(set(self.args.workers))

        # Get a map between workers and steps
        worker_step_map = self.spec.get_worker_step_map()

        # Append steps associated with each worker provided
        for worker_provided in workers_provided:
            # Check for invalid workers
            if worker_provided not in worker_step_map:
                LOG.warning(f"Worker with name {worker_provided} does not exist for this study.")
            else:
                for step in worker_step_map[worker_provided]:
                    if step not in self.args.steps:
                        self.args.steps.append(step)

    def _process_task_queue(self):
        """
        Modifies the list of steps to display status for based on
        the list of task queues provided by the user.
        """
        # Remove duplicate queues
        queues_provided = list(set(self.args.task_queues))

        # Get a map between queues and steps
        queue_step_relationship = self.spec.get_queue_step_relationship()

        # Append steps associated with each task queue provided
        for queue_provided in queues_provided:
            # Check for invalid task queues
            queue_with_celery_tag = f"{CONFIG.celery.queue_tag}{queue_provided}"
            if queue_with_celery_tag not in queue_step_relationship:
                LOG.warning(f"Task queue with name {queue_provided} does not exist for this study.")
            else:
                for step in queue_step_relationship[queue_with_celery_tag]:
                    if step not in self.args.steps:
                        self.args.steps.append(step)

    def _create_step_tracker(self) -> Dict[str, List[str]]:
        """
        Creates a dictionary of started and unstarted steps that we
        will display the status for.

        :param `workspace`: The output directory for a study
        :param `steps_to_check`: A list of steps to view the status of
        :returns: A dictionary mapping of started and unstarted steps. Values are lists of step names.
        """
        step_tracker = {"started_steps": [], "unstarted_steps": []}
        started_steps = next(os.walk(self.workspace))[1]
        started_steps.remove("merlin_info")
        steps_to_check = self.args.steps.copy()

        for sstep in started_steps:
            if sstep in steps_to_check:
                step_tracker["started_steps"].append(sstep)
                steps_to_check.remove(sstep)
        step_tracker["unstarted_steps"] = steps_to_check

        return step_tracker

    def _remove_steps_without_statuses(self):
        """
        After applying filters, there's a chance that certain steps will still exist
        in self.requested_statuses but won't have any tasks to view the status of so
        we'll remove those here.
        """
        result = deepcopy(self.requested_statuses)
        for step_name, overall_step_info in self.requested_statuses.items():
            sub_step_workspaces = sorted(list(overall_step_info.keys() - NON_WORKSPACE_KEYS))
            if len(sub_step_workspaces) == 0:
                del result[step_name]

        self.requested_statuses = result

    def get_steps_to_display(self) -> Dict[str, List[str]]:
        """
        Generates a list of steps to display the status for based on information
        provided to the merlin status command by the user.

        :returns: A dictionary of started and unstarted steps for us to display the status of
        """
        existing_steps = self.spec.get_study_step_names()

        if ("all" in self.args.steps) and (not self.args.task_queues) and (not self.args.workers):
            self.args.steps = existing_steps
        else:
            # This won't matter anymore since task_queues or workers is not None here
            if "all" in self.args.steps:
                self.args.steps = []

            # Add steps to start based on task queues and/or workers provided
            if self.args.task_queues:
                self._process_task_queue()
            if self.args.workers:
                self._process_workers()

            # Sort the steps to start by the order they show up in the study
            for i, estep in enumerate(existing_steps):
                if estep in self.args.steps:
                    self.args.steps.remove(estep)
                    self.args.steps.insert(i, estep)

        # Filter the steps to display status for by started/unstarted
        step_tracker = self._create_step_tracker()

        return step_tracker

    @property
    def num_requested_statuses(self):
        """
        Count the number of task statuses in a the requested_statuses dict.
        We need to ignore non workspace keys when we count.
        """
        num_statuses = 0
        for status_info in self.requested_statuses.values():
            num_statuses += len(status_info.keys() - NON_WORKSPACE_KEYS)
        return num_statuses

    def apply_filters(self, filter_types: List[str], filters: List[str]):
        """
        Given a list of filters, filter the dict of requested statuses by them.

        :param `filter_types`: A list of str denoting the types of filters we're applying
        :param `filters`: A list of filters to apply to the dict of statuses we read in
        """
        LOG.info(f"Filtering tasks using these filters: {filters}")

        # Create a deep copy of the dict so we can make changes to it while we iterate
        result = deepcopy(self.requested_statuses)
        for step_name, overall_step_info in self.requested_statuses.items():
            for sub_step_workspace, task_status_info in overall_step_info.items():
                # Ignore non workspace keys
                if sub_step_workspace in NON_WORKSPACE_KEYS:
                    continue

                # Search for our filters
                found_a_match = False
                for filter_type in filter_types:
                    if task_status_info[filter_type] in filters:
                        found_a_match = True
                        break

                # If our filters aren't a match for this task then delete it
                if not found_a_match:
                    del result[step_name][sub_step_workspace]

        # Get the number of tasks found with our filters
        self.requested_statuses = result
        self._remove_steps_without_statuses()
        LOG.info(f"Found {self.num_requested_statuses} tasks matching your filters.")

        # If no tasks were found set the status dict to empty
        if self.num_requested_statuses == 0:
            self.requested_statuses = {}

    def apply_max_tasks_limit(self):
        """
        Given a number representing the maximum amount of tasks to display, filter the dict of statuses
        to that number.
        """
        # Make sure the max_tasks variable is set to a reasonable number and store that value
        if self.args.max_tasks and self.args.max_tasks > self.num_requested_statuses:
            self.args.max_tasks = self.num_requested_statuses
        max_tasks = self.args.max_tasks

        new_status_dict = {}
        for step_name, overall_step_info in self.requested_statuses.items():
            new_status_dict[step_name] = {}
            sub_step_workspaces = sorted(list(overall_step_info.keys() - NON_WORKSPACE_KEYS))

            # If there are more status entries than max_tasks will allow then we need to remove some
            if len(sub_step_workspaces) > self.args.max_tasks:
                workspaces_to_delete = set(sub_step_workspaces) - set(sub_step_workspaces[: self.args.max_tasks])
                for ws_to_delete in workspaces_to_delete:
                    del overall_step_info[ws_to_delete]
                self.args.max_tasks = 0
            # Otherwise, subtract how many tasks there are in this step from max_tasks
            else:
                self.args.max_tasks -= len(sub_step_workspaces)

            # Merge in the task statuses that we're allowing
            dict_deep_merge(new_status_dict[step_name], overall_step_info)

        LOG.info(f"Limited the number of tasks to display to {max_tasks} tasks.")

        # Set the new requested statuses with the max_tasks limit and remove steps without statuses
        self.requested_statuses = new_status_dict
        self._remove_steps_without_statuses()

        # Reset max_tasks
        self.args.max_tasks = max_tasks

    def get_step_statuses(self, step_workspace: str, started_step_name: str) -> Dict[str, List[str]]:
        """
        Given a step workspace and the name of the step, read in all the statuses
        for the step and return them in a dict.

        :param `step_workspace`: The path to the step we're going to read statuses from
        :param `started_step_name`: The name of the started step that we're getting statuses from
        :returns: A dict of statuses for the given step
        """
        step_statuses = {}
        num_statuses_read = 0

        # Traverse the step workspace and look for MERLIN_STATUS files
        for root, _, files in os.walk(step_workspace):
            if "MERLIN_STATUS.json" in files:
                status_filepath = f"{root}/MERLIN_STATUS.json"
                lock = FileLock(f"{root}/status.lock")  # pylint: disable=E0110
                statuses_read = read_status(status_filepath, lock)

                # Count the number of statuses we just read
                for step_name, status_info in statuses_read.items():
                    if started_step_name not in self.real_step_name_map:
                        self.real_step_name_map[started_step_name] = [step_name]
                    else:
                        if step_name not in self.real_step_name_map[started_step_name]:
                            self.real_step_name_map[started_step_name].append(step_name)
                    num_statuses_read += len(status_info.keys() - NON_WORKSPACE_KEYS)

                # Merge the statuses we read with the dict tracking all statuses for this step
                dict_deep_merge(step_statuses, statuses_read)

            # If we've read all the statuses then we're done
            if num_statuses_read == self.tasks_per_step[started_step_name]:
                break
            # This shouldn't get hit
            if num_statuses_read > self.tasks_per_step[started_step_name]:
                LOG.error(
                    f"Read {num_statuses_read} statuses when there should "
                    f"only be {self.tasks_per_step[started_step_name]} tasks in total."
                )
                break

        return step_statuses

    def get_requested_statuses(self):
        """
        Populate the requested_statuses dict with statuses that the user is looking to find.
        Filters for steps, task queues, workers will have already been applied
        when creating the step_tracker attribute. Remaining filters will be applied here.
        """
        LOG.info(f"Reading task statuses from {self.workspace}")

        # Read in all statuses from the started steps the user wants to see
        for sstep in self.step_tracker["started_steps"]:
            step_workspace = f"{self.workspace}/{sstep}"
            step_statuses = self.get_step_statuses(step_workspace, sstep)
            dict_deep_merge(self.requested_statuses, step_statuses)

        # Count how many statuses in total that we just read in
        LOG.info(f"Read in {self.num_requested_statuses} statuses.")

        # Build a list of filters provided
        filter_types = set()
        filters = []
        if self.args.task_status:
            filter_types.add("Status")
            filters += self.args.task_status
        if self.args.return_code:
            filter_types.add("Return Code")
            filters += self.args.return_code

        # Apply the filters if necessary
        if filters:
            self.apply_filters(list(filter_types), filters)

        # Limit the number of tasks to display if necessary
        if self.args.max_tasks and self.args.max_tasks > 0:
            self.apply_max_tasks_limit()

    def query_task_by_task_status(self):
        """
        Displays a task-by-task view of the status based on user filter(s).
        """
        # Check if there's any filters remaining after the verification process
        filters_remaining = any(
            [
                self.args.task_queues,
                self.args.workers,
                self.args.task_status,
                self.args.return_code,
                self.steps_filter_provided,
                self.args.max_tasks,
            ]
        )

        # If there's no filters left, there's nothing to display
        if not filters_remaining:
            LOG.warning("No filters remaining, cannot find tasks to display.")
        else:
            # Check that there's statuses found and display them
            if self.requested_statuses:
                display.display_status_task_by_task(self)

    def query_summary_status(self):
        """Displays the high level summary of the status"""
        display.display_status_summary(self)

    def get_user_filters(self) -> List[str]:
        """
        Get a filter on the statuses to display from the user. Possible options
        for filtering:
            - A str MAX_TASKS -> will ask the user for another input that's equivalent to the --max-tasks flag
            - A list of statuses -> equivalent to the --task-status flag
            - A list of return codes -> equivalent to the --return-code flag
            - An exit keyword to leave the filter prompt without filtering

        :returns: A list of strings to filter by
        """
        # Build the filter options
        filter_info = {
            "Filter Type": [
                "Put a limit on the number of tasks to display",
                "Filter by status",
                "Filter by return code",
                "Exit without filtering",
            ],
            "Description": [
                "Enter 'MAX_TASKS'",
                f"Enter a comma separated list of the following statuses you'd like to see: {VALID_STATUS_FILTERS}",
                f"Enter a comma separated list of the following return codes you'd like to see: {VALID_RETURN_CODES}",
                f"Enter one of the following: {VALID_EXIT_FILTERS}",
            ],
            "Example": ["MAX_TASKS", "FAILED, CANCELLED", "SOFT_FAIL, RETRY", "EXIT"],
        }

        # Display the filter options
        filter_option_renderer = status_renderer_factory.get_renderer("table", disable_theme=True, disable_pager=True)
        filter_option_renderer.layout(status_data=filter_info)
        filter_option_renderer.render()

        # Obtain and validate the filter provided by the user
        invalid_filter = True
        while invalid_filter:
            user_filters = input("How would you like to filter the tasks? ")
            # Remove spaces and split user filters by commas
            user_filters = user_filters.replace(" ", "")
            user_filters = user_filters.split(",")

            # Ensure every filter is valid
            for i, entry in enumerate(user_filters):
                entry = entry.upper()
                if entry not in ALL_VALID_FILTERS:
                    invalid_filter = True
                    print(f"Invalid input: {entry}. Input must be one of the following {ALL_VALID_FILTERS}")
                    break
                invalid_filter = False
                user_filters[i] = entry

        return user_filters

    def get_user_max_tasks(self) -> int:
        """
        Get a limit for the amount of tasks to display from the user.

        :returns: An int representing the max amount of tasks to display
        """
        invalid_input = True

        while invalid_input:
            try:
                user_max_tasks = int(input("What limit would you like to set? (must be an integer greater than 0) "))
                if user_max_tasks > 0:
                    invalid_input = False
            except ValueError:
                continue

        return user_max_tasks

    def filter_via_prompts(self):
        """
        Interact with the user to manage how many/which tasks are displayed. This helps to
        prevent us from overloading the terminal by displaying a bazillion tasks at once.
        """
        # Initialize a list to store the filter types we're going to apply
        filter_types = []

        # Get the filters from the user
        user_filters = self.get_user_filters()

        # TODO remove this once restart/retry functionality is implemented
        if "RESTART" in user_filters:
            LOG.warning("The RESTART filter is coming soon. Ignoring this filter for now...")
            user_filters.remove("RESTART")
        if "RETRY" in user_filters:
            LOG.warning("The RETRY filter is coming soon. Ignoring this filter for now...")
            user_filters.remove("RETRY")

        # Variable to track whether the user wants to stop filtering
        exit_without_filtering = False

        # Process the filters
        max_tasks_found = False
        for i, user_filter in enumerate(user_filters):
            # Case 1: Exit command found, stop filtering
            if user_filter in ("E", "EXIT"):
                exit_without_filtering = True
            # Case 2: MAX_TASKS command found, get the limit from the user
            elif user_filter == "MAX_TASKS":
                max_tasks_found = True
            # Case 3: Status filter provided, add it to the list of filter types
            elif user_filter in VALID_STATUS_FILTERS and "Status" not in filter_types:
                filter_types.append("Status")
            # Case 4: Return Code filter provided, add it to the list of filter types and add the MERLIN prefix
            elif user_filter in VALID_RETURN_CODES:
                user_filters[i] = f"MERLIN_{user_filter}"
                if "Return Code" not in filter_types:
                    filter_types.append("Return Code")

        # Remove the MAX_TASKS entry so we don't try to filter using it
        try:
            user_filters.remove("MAX_TASKS")
        except ValueError:
            pass

        # Apply the filters and tell the user how many tasks match the filters (if necessary)
        if not exit_without_filtering and user_filters:
            self.apply_filters(filter_types, user_filters)

        # Apply max tasks limit (if necessary)
        if max_tasks_found:
            user_max_tasks = self.get_user_max_tasks()
            self.args.max_tasks = user_max_tasks
            self.apply_max_tasks_limit()

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
        statuses_to_write = self.format_status_for_display()

        # Add date entries as the first column then update this dict with the statuses we just reformatted
        statuses_with_timestamp = {"Time of Status": [date] * len(statuses_to_write["Step Name"])}
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

        :param `statuses_to_format`: A dict of statuses that we need to reformat
        :returns: A formatted dictionary where each key is a column and the values are the rows
                  of information to display for that column.
        """
        reformatted_statuses = {
            "Step Name": [],
            "Step Workspace": [],
            "Status": [],
            "Return Code": [],
            "Elapsed Time": [],
            "Run Time": [],
            "Restarts": [],
            "Cmd Parameters": [],
            "Restart Parameters": [],
            "Task Queue": [],
            "Worker Name": [],
        }

        # Loop through all statuses
        for step_name, overall_step_info in self.requested_statuses.items():
            for sub_step_workspace, task_status_info in overall_step_info.items():
                # Ignore non workspace keys for now
                if sub_step_workspace in NON_WORKSPACE_KEYS:
                    continue

                # Put the step name and workspace in each entry
                reformatted_statuses["Step Name"].append(step_name)
                reformatted_statuses["Step Workspace"].append(sub_step_workspace)

                # Add the rest of the information for each task (status, return code, elapsed & run time, num restarts)
                for key, val in task_status_info.items():
                    reformatted_statuses[key].append(val)

            # Handle the non workspace keys
            num_statuses = len(overall_step_info.keys() - NON_WORKSPACE_KEYS)
            for key in NON_WORKSPACE_KEYS:
                try:
                    # Set the val_to_add value based on if a value exists for the key
                    val_to_add = "-------"
                    if overall_step_info[key]:
                        val_to_add = overall_step_info[key]
                except KeyError:
                    # This key error will happen for Task Queue and Worker Name columns on local runs
                    # So just remove that entry in the reformatted statuses dict if necessary
                    if key in reformatted_statuses:
                        del reformatted_statuses[key]
                    continue
                # Add the val_to_add entry for each row
                key_entries = [val_to_add] * num_statuses
                reformatted_statuses[key].extend(key_entries)

        return reformatted_statuses


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
