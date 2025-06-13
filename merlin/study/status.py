##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""This module handles all the functionality of getting the statuses of studies."""
import json
import logging
import os
import re
from argparse import Namespace
from copy import deepcopy
from datetime import datetime
from glob import glob
from traceback import print_exception
from typing import Any, Dict, List, Set, Tuple, Union

import numpy as np
from filelock import FileLock, Timeout
from maestrowf.utils import get_duration
from tabulate import tabulate

from merlin.common.dumper import dump_handler
from merlin.display import ANSI_COLORS, display_status_summary, display_status_task_by_task
from merlin.spec.expansion import get_spec_with_expansion
from merlin.spec.specification import MerlinSpec
from merlin.study.status_constants import (
    ALL_VALID_FILTERS,
    CELERY_KEYS,
    NON_WORKSPACE_KEYS,
    VALID_EXIT_FILTERS,
    VALID_RETURN_CODES,
    VALID_STATUS_FILTERS,
)
from merlin.study.status_renderers import status_renderer_factory
from merlin.utils import (
    apply_list_of_regex,
    convert_timestring,
    convert_to_timedelta,
    dict_deep_merge,
    pretty_format_hms,
    verify_dirpath,
    ws_time_to_dt,
)


LOG = logging.getLogger(__name__)


class Status:
    """
    Handles the management and retrieval of status information for studies.

    This class is responsible for loading specifications, tracking the status of steps,
    calculating runtime statistics, and formatting status information for output in
    various formats (JSON, CSV). It interacts with the file system to read status files
    and provides methods to display and dump status information.

    Attributes:
        args (Namespace): Command-line arguments provided by the user.
        full_step_name_map (Dict[str, Set[str]]): A mapping of overall step names to full step names.
        num_requested_statuses (int): Counts the number of task statuses in the `requested_statuses`
            dictionary.
        requested_statuses (Dict): A dictionary storing the statuses that the user wants to view.
        run_time_info (Dict[str, Dict]): A dictionary storing runtime statistics for each step.
        spec (spec.specification.MerlinSpec): A [`MerlinSpec`][spec.specification.MerlinSpec]
            object loaded from the workspace or spec file.
        step_tracker (Dict[str, List[str]]): A dictionary tracking started and unstarted steps.
        tasks_per_step (Dict[str, int]): A mapping of tasks per step for accurate totals.
        workspace (str): The path to the workspace containing study data.

    Methods:
        display: Displays a high-level summary of the status.
        dump: Dumps the status information to a specified file.
        format_csv_dump: Prepares the dictionary of statuses for CSV output.
        format_json_dump: Prepares the dictionary of statuses for JSON output.
        format_status_for_csv: Reformats statuses into a dictionary suitable for CSV output.
        get_runtime_avg_std_dev: Calculates and stores the average and standard deviation of
            runtimes for a step.
        get_step_statuses: Reads and returns the statuses for a given step.
        get_steps_to_display: Generates a list of steps to display the status for.
        load_requested_statuses: Populates the `requested_statuses` dictionary with statuses
            from the study.
    """

    def __init__(self, args: Namespace, spec_display: bool, file_or_ws: str):
        """
        Initializes the `Status` object, which manages and retrieves status information for studies.

        Args:
            args: Command-line arguments provided by the user, including filters and options
                for displaying or dumping status information.
            spec_display: A flag indicating whether the status should be loaded from a specification
                file (`True`) or from a workspace (`False`).
            file_or_ws: The path to the specification file or workspace, depending on the value of
                `spec_display`.
        """
        # Save the args to this class instance and check if the steps filter was given
        self.args: Namespace = args

        # Load in the workspace path and spec object
        self.workspace: str
        self.spec: MerlinSpec
        if spec_display:
            self.workspace, self.spec = self._load_from_spec(file_or_ws)
        else:
            self.workspace = file_or_ws
            self.spec = self._load_from_workspace()

        # Verify the filter args (this will only do something for DetailedStatus)
        self._verify_filter_args()

        # Create a step tracker that will tell us which steps have started/not started
        self.step_tracker: Dict[str, List[str]] = self.get_steps_to_display()

        # Create a tasks per step mapping in order to give accurate totals for each step
        self.tasks_per_step: Dict[str, int] = self.spec.get_tasks_per_step()

        # This attribute will store a map between the overall step name and the full step names
        # that are created with parameters (e.g. step name is hello and uses a "GREET: hello" parameter
        # so the real step name is hello_GREET.hello)
        self.full_step_name_map: Dict[str, Set[str]] = {}

        # Variable to store run time information for each step
        self.run_time_info: Dict[str, Dict] = {}

        # Variable to store the statuses that the user wants
        self.requested_statuses: Dict = {}
        self.load_requested_statuses()

    def _print_requested_statuses(self):
        """
        Print the requested statuses stored in the `requested_statuses` dictionary.

        This helper method iterates through the `requested_statuses` attribute, which contains
        information about the statuses of various steps. It prints the step names along with
        their corresponding status information. Non-workspace keys are printed directly, while
        workspace-related keys are further detailed by their status keys and values.
        """
        print("self.requested_statuses:")
        for step_name, overall_step_info in self.requested_statuses.items():
            print(f"\t{step_name}:")
            for key, val in overall_step_info.items():
                if key in NON_WORKSPACE_KEYS:
                    print(f"\t\t{key}: {val}")
                else:
                    print(f"\t\t{key}:")
                    for status_key, status_val in val.items():
                        print(f"\t\t\t{status_key}: {status_val}")

    def _verify_filter_args(self):
        """
        Verify the filter arguments for the status retrieval.

        This is an abstract method intended to be implemented in subclasses, such as
        [`DetailedStatus`][study.status.DetailedStatus]. The method will ensure that
        the filter arguments provided for retrieving statuses are valid and meet the
        necessary criteria. The implementation details will depend on the specific
        requirements of the subclass.
        """

    def _get_latest_study(self, studies: List[str]) -> str:
        """
        Retrieve the latest study from a list of studies.

        This method examines a list of study identifiers and determines which one is the latest
        based on the timestamp embedded in the study names. It assumes that the newest study is
        represented by the last entry in the list but verifies this assumption by comparing the
        timestamps of all studies.

        The method extracts the timestamp from the last 15 characters of each study identifier,
        converts it to a datetime object, and compares it to find the most recent study.

        Args:
            studies: A list of study identifiers to evaluate.

        Returns:
            The identifier of the latest study.

        Example:
            ```python
            >>> self._get_latest_study(["study_20231101-174102", "study_20231101-182044", "study_20231101-163327"])
            'study_20231101-182044'
            ```
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

        This method checks the number of potential studies found and either selects the latest study
        automatically or prompts the user to choose from the available options. It constructs the
        directory path to the selected study.

        Args:
            study_output_dir: A string representing the output path of a study; equivalent to $(OUTPUT_PATH).
            num_studies: The number of potential studies found.
            potential_studies: A list of potential studies found, where each entry is of the form (index,
                potential_study_name).

        Returns:
            A directory path to the study that the user wants to view the status of, formatted as
                "study_output_dir/selected_potential_study".

        Raises:
            ValueError: If no potential studies are found or if the user input is invalid.
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

    def _load_from_spec(self, filepath: str) -> Tuple[str, MerlinSpec]:  # pylint: disable=R0914
        """
        Get the desired workspace from the user and load its YAML spec for further processing.

        This method verifies the output path based on user input or the spec file and builds a list
        of potential study output directories. It then calls another method to obtain the study to
        check the status for and loads the corresponding spec.

        Args:
            filepath: The filepath to a spec provided by the user.

        Returns:
            A tuple containing the workspace of the study to check the status for and a
                [`MerlinSpec`][spec.specification.MerlinSpec] object loaded from the workspace's
                merlin_info subdirectory.

        Raises:
            ValueError: If the specified output directory does not contain a merlin_info subdirectory,
                or if multiple or no expanded spec options are found in the directory.
        """
        # If the user provided a new output path to look in, use that
        if self.args.output_path is not None:
            output_path = self.args.output_path
        # Otherwise, use the output path of the study that was given to us
        else:
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

    def _load_from_workspace(self) -> MerlinSpec:
        """
        Create a [`MerlinSpec`][spec.specification.MerlinSpec] object based on the expanded spec file
        in the workspace.

        Returns:
            spec.specification.MerlinSpec: A [`MerlinSpec`][spec.specification.MerlinSpec] object loaded
                from the workspace provided by the user.

        Raises:
            ValueError: If multiple or no expanded spec options are found in the workspace's merlin_info directory.
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
        Creates a dictionary of started and unstarted steps to display their status.

        This method checks the workspace for steps that have been started and compares them
        against a provided list of steps to determine which steps are started and which are
        unstarted. It returns a dictionary categorizing the steps accordingly.

        Args:
            steps_to_check: A list of step names to check the status of.

        Returns:
            A dictionary with two keys:\n
                - "started_steps": A list of steps that have been started.
                - "unstarted_steps": A list of steps that have not been started.
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
        Generates a dictionary of steps to display their status based on user input
        provided to the merlin status command.

        This method retrieves the names of existing steps from the study specification
        and creates a step tracker to categorize them into started and unstarted steps.

        Returns:
            A dictionary with two keys:\n
                - `started_steps`: A list of steps that have been started.
                - `unstarted_steps`: A list of steps that have not been started.

        Example:
            ```python
            >>> self.get_steps_to_display()
            {"started_steps": ["step1"], "unstarted_steps": ["step2", "step3"]}
            ```
        """
        existing_steps = self.spec.get_study_step_names()

        LOG.debug(f"existing steps: {existing_steps}")
        LOG.debug("Building step tracker based on existing steps...")

        # Filter the steps to display status for by started/unstarted
        step_tracker = self._create_step_tracker(existing_steps)

        LOG.debug("Step tracker created.")

        return step_tracker

    @property
    def num_requested_statuses(self) -> int:
        """
        Counts the number of task statuses in the requested_statuses dictionary,
        excluding non-workspace keys.

        Returns:
            The count of requested task statuses that are not non-workspace keys.
        """
        num_statuses = 0
        for overall_step_info in self.requested_statuses.values():
            num_statuses += len(overall_step_info.keys() - NON_WORKSPACE_KEYS)

        return num_statuses

    def get_step_statuses(self, step_workspace: str, started_step_name: str) -> Dict[str, List[str]]:
        """
        Reads the statuses for a specified step from the given step workspace.

        This method traverses the specified step workspace directory to locate
        `MERLIN_STATUS.json` files, reads their contents, and aggregates the statuses
        into a dictionary. It also tracks the full names of the steps and counts
        the number of statuses read.

        Args:
            step_workspace: The path to the step directory from which to read statuses.
            started_step_name: The name of the step for which statuses are being gathered.

        Returns:
            A dictionary containing the statuses for the specified step, where each key is a full
                step name and the value is a list of status information.
        """
        step_statuses = {}
        num_statuses_read = 0

        self.full_step_name_map[started_step_name] = set()

        # Traverse the step workspace and look for MERLIN_STATUS files
        LOG.debug(f"Traversing '{step_workspace}' to find MERLIN_STATUS.json files...")
        for root, dirs, _ in os.walk(step_workspace, topdown=True):
            # Look for nested workspaces and skip them
            timestamp_regex = r"\d{8}-\d{6}$"
            curr_dir = os.path.split(root)[1]
            dirs[:] = [d for d in dirs if not re.search(timestamp_regex, curr_dir)]

            # Search for a status file
            status_filepath = os.path.join(root, "MERLIN_STATUS.json")
            matching_files = glob(status_filepath)
            if matching_files:
                LOG.debug(f"Found status file at '{status_filepath}'")
                # Read in the statuses
                statuses_read = read_status(status_filepath, f"{root}/status.lock")

                # Merge the statuses we read with the dict tracking all statuses for this step
                dict_deep_merge(step_statuses, statuses_read, conflict_handler=status_conflict_handler)

                # Add full step name to the tracker and count number of statuses we just read in
                for full_step_name, status_info in statuses_read.items():
                    self.full_step_name_map[started_step_name].add(full_step_name)
                    num_statuses_read += len(status_info.keys() - NON_WORKSPACE_KEYS)

                    # Make sure there aren't any duplicate workers
                    if "workers" in step_statuses[full_step_name]:
                        step_statuses[full_step_name]["workers"] = list(set(step_statuses[full_step_name]["workers"]))

        LOG.debug(
            f"Done traversing '{step_workspace}'. Read in {num_statuses_read} "
            f"{'statuses' if num_statuses_read != 1 else 'status'}."
        )

        return step_statuses

    def load_requested_statuses(self):
        """
        Populates the `requested_statuses` dictionary with statuses from the study.

        This method iterates through the started steps in the step tracker,
        retrieves their statuses using the
        [`get_step_statuses`][study.status.Status.get_step_statuses] method, and merges
        these statuses into the `requested_statuses` dictionary. It also calculates
        the average and standard deviation of the run times for each step.
        """
        LOG.info(f"Reading task statuses from {self.workspace}")

        # Read in all statuses from the started steps the user wants to see
        for sstep in self.step_tracker["started_steps"]:
            step_workspace = f"{self.workspace}/{sstep}"
            step_statuses = self.get_step_statuses(step_workspace, sstep)
            dict_deep_merge(self.requested_statuses, step_statuses, conflict_handler=status_conflict_handler)

            # Calculate run time average and standard deviation for this step
            self.get_runtime_avg_std_dev(step_statuses, sstep)

        # Count how many statuses in total that we just read in
        LOG.info(f"Read in {self.num_requested_statuses} statuses total.")

    def get_runtime_avg_std_dev(self, step_statuses: Dict, step_name: str):
        """
        Calculates the average and standard deviation of the runtime for a specified step.

        This method parses the provided step status information to extract runtime values,
        computes the mean and standard deviation of these runtimes, and updates the state
        information with the calculated values. The runtimes are expected to be in a specific
        format (e.g., "1h30m15s") and are converted to seconds for the calculations.

        Args:
            step_statuses: A dictionary containing step status information, where each
                entry includes runtime data to be parsed.
            step_name: The name of the step for which the average and standard deviation
                of the runtime are being calculated.
        """
        # Initialize a list to track all existing runtimes
        run_times_in_seconds = []

        # This outer loop will only loop once
        LOG.debug(f"Calculating run time avg and std dev for step '{step_name}'...")
        for overall_step_info in step_statuses.values():
            for step_info_key, step_status_info in overall_step_info.items():
                # Ignore non-workspace keys
                if step_info_key in NON_WORKSPACE_KEYS:
                    continue

                # Ignore any run times that have yet to be calculated
                if step_status_info["run_time"] == "--:--:--":
                    LOG.debug(f"Skipping {step_info_key} since the run time is empty.")
                    continue

                # Parse the runtime value, convert it to seconds, and add it to the lsit of existing run times
                run_time = step_status_info["run_time"].replace("d", "").replace("h", "").replace("m", "").replace("s", "")
                run_time_tdelta = convert_to_timedelta(run_time)
                run_times_in_seconds.append(run_time_tdelta.total_seconds())

        # Using the list of existing run times, calculate avg and std dev
        LOG.debug(f"Using the following run times for our calculations: {run_times_in_seconds}")
        self.run_time_info[step_name] = {}
        if len(run_times_in_seconds) == 0:
            self.run_time_info[step_name]["avg_run_time"] = "--"
            self.run_time_info[step_name]["run_time_std_dev"] = "±--"
        else:
            np_run_times_in_seconds = np.array(run_times_in_seconds)
            run_time_mean = round(np.mean(np_run_times_in_seconds))
            run_time_std_dev = round(np.std(np_run_times_in_seconds))
            LOG.debug(f"Run time avg in seconds: {run_time_mean}")
            LOG.debug(f"Run time std dev in seconds: {run_time_std_dev}")

            # Pretty format the avg and std dev and store them as new entries in the run time info
            self.run_time_info[step_name]["avg_run_time"] = pretty_format_hms(convert_timestring(run_time_mean))
            self.run_time_info[step_name]["run_time_std_dev"] = f"±{pretty_format_hms(convert_timestring(run_time_std_dev))}"
            LOG.debug(f"Run time avg and std dev for step '{step_name}' calculated.")

    def display(self, test_mode: bool = False) -> Dict:
        """
        Displays a high-level summary of the status.

        This method provides an overview of the current status of the workflow. If
        `test_mode` is enabled, it will not print any output but will return the
        status information in a dictionary.

        Args:
            test_mode: If true, run this in testing mode and don't print any output.

        Returns:
            An empty dictionary if `test_mode` is False; otherwise, a dictionary containing
                the status information that would be displayed.
        """
        return display_status_summary(self, NON_WORKSPACE_KEYS, test_mode=test_mode)

    def format_json_dump(self, date: datetime) -> Dict:
        """
        Builds a dictionary of statuses to dump to a JSON file.

        This method prepares the status information for serialization by adding a timestamp
        to the existing status data.

        Args:
            date: A timestamp marking when this status occurred.

        Returns:
            A dictionary ready to be dumped to a JSON file, containing the timestamp
                and the requested statuses.
        """
        # Statuses are already in json format so we'll just add a timestamp for the dump here
        return {date: self.requested_statuses}

    def format_csv_dump(self, date: datetime) -> Dict:
        """
        Adds a timestamp to the statuses for CSV output.

        This method reformats the status information into a structure suitable for CSV
        output, including a timestamp entry as the first column.

        Args:
            date: A timestamp marking when this status occurred.

        Returns:
            A dictionary equivalent of formatted statuses with a timestamp entry
                at the start of the dictionary.
        """
        # Reformat the statuses to a new dict where the keys are the column labels and rows are the values
        LOG.debug("Formatting statuses for csv dump...")
        statuses_to_write = self.format_status_for_csv()
        LOG.debug("Statuses formatted.")

        # Add date entries as the first column then update this dict with the statuses we just reformatted
        statuses_with_timestamp = {"time_of_status": [date] * len(statuses_to_write["step_name"])}
        statuses_with_timestamp.update(statuses_to_write)

        return statuses_with_timestamp

    def dump(self):
        """
        Dumps the status information to a file.

        This method handles the creation of a timestamp and determines the appropriate
        file format (CSV or JSON) for dumping the status information. It then calls
        the appropriate formatting method and writes the data to the specified file.
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

    def format_status_for_csv(self) -> Dict:
        """
        Reformats statuses for CSV output to comply with
        [Maestro's status renderer layouts](https://maestrowf.readthedocs.io/en/latest/Maestro/reference_guide/api_reference/index.html).

        This method transforms the status information into a dictionary format where each
        key represents a column label and the corresponding values are the rows of information
        to display for that column.

        Returns:
            A formatted dictionary where each key is a column and the values are the
                rows of information to display for that column.
        """
        reformatted_statuses = {
            "step_name": [],
            "step_workspace": [],
            "status": [],
            "return_code": [],
            "elapsed_time": [],
            "run_time": [],
            "restarts": [],
            "cmd_parameters": [],
            "restart_parameters": [],
            "task_queue": [],
            "workers": [],
        }

        # We only care about started steps since unstarted steps won't have any status to report
        for step_name, overall_step_info in self.requested_statuses.items():
            # Get the number of statuses for this step so we know how many entries there should be
            num_statuses = len(overall_step_info.keys() - NON_WORKSPACE_KEYS)

            # Loop through information for each step
            for step_info_key, step_info_value in overall_step_info.items():
                # Skip the workers entry at the top level;
                # this will be added in the else statement below on a task-by-task basis
                if step_info_key in ("workers", "worker_name"):
                    continue
                # Format task queue entry
                if step_info_key == "task_queue":
                    # Set the val_to_add value based on if a value exists for the key
                    val_to_add = step_info_value if step_info_value else "-------"
                    # Add the val_to_add entry for each row
                    key_entries = [val_to_add] * num_statuses
                    reformatted_statuses[step_info_key].extend(key_entries)

                # Format parameters
                elif step_info_key == "parameters":
                    for cmd_type in ("cmd", "restart"):
                        reformatted_statuses_key = f"{cmd_type}_parameters"
                        # Set the val_to_add value based on if a value exists for the key
                        if step_info_value[cmd_type] is not None:
                            param_str = ";".join(
                                [f"{token}:{param_val}" for token, param_val in step_info_value[cmd_type].items()]
                            )
                        else:
                            param_str = "-------"
                        # Add the parameter string for each row in this step
                        reformatted_statuses[reformatted_statuses_key].extend([param_str] * num_statuses)

                # Format workspace keys
                else:
                    # Put the step name and workspace in each entry
                    reformatted_statuses["step_name"].append(step_name)
                    reformatted_statuses["step_workspace"].append(step_info_key)

                    # Add the rest of the information for each task (status, return code, elapsed & run time, num restarts)
                    for key, val in step_info_value.items():
                        if key == "workers":
                            reformatted_statuses[key].append(", ".join(val))
                        else:
                            reformatted_statuses[key].append(val)

        # For local runs, there will be no task queue or worker name so delete these entries
        for celery_specific_key in CELERY_KEYS:
            try:
                if not reformatted_statuses[celery_specific_key]:
                    del reformatted_statuses[celery_specific_key]
            except KeyError:
                pass

        return reformatted_statuses


class DetailedStatus(Status):
    """
    This class handles obtaining and filtering requested statuses from the user.
    It inherits from the [`Status`][study.status.Status] class and provides
    additional functionality for filtering and displaying task statuses based on
    user-defined criteria.

    Attributes:
        args (Namespace): A namespace containing user-defined arguments for filtering.
        num_requested_statuses (int): The number of task statuses in the `requested_statuses` dictionary.
        requested_statuses (Dict): A dictionary holding the statuses requested by the user.
        spec (spec.specification.MerlinSpec): A [`MerlinSpec`][spec.specification.MerlinSpec]
            object loaded from the workspace or spec file.
        steps_filter_provided (bool): Indicates if a specific steps filter was provided.

    Methods:
        apply_filters: Applies user-defined filters to the requested statuses.
        apply_max_tasks_limit: Limits the number of tasks displayed based on the user-defined maximum.
        display: Displays a task-by-task view of the status based on user filters.
        filter_via_prompts: Interacts with the user to manage task display filters.
        get_steps_to_display: Generates a list of steps to display the status for.
        get_user_filters: Prompts the user for filters to apply to the statuses.
        get_user_max_tasks: Prompts the user for a maximum task limit to display.
        load_requested_statuses: Populates the requested statuses dictionary based on user-defined filters.
    """

    def __init__(self, args: Namespace, spec_display: bool, file_or_ws: str):
        """
        Initializes the `DetailedStatus` object, extending the functionality of the `Status` class
        to include filtering and detailed task-by-task status handling.

        Args:
            args: Command-line arguments provided by the user, including options
                for filtering, displaying, or dumping detailed task statuses.
            spec_display: A flag indicating whether the status should be loaded from a
                specification file (`True`) or from a workspace (`False`).
            file_or_ws: The path to the specification file or workspace, depending on the
                value of `spec_display`.
        """
        args_copy = Namespace(**vars(args))
        super().__init__(args, spec_display, file_or_ws)

        # Need to set this environment value for the pager functionality to work
        if not args.disable_pager:
            os.environ["MANPAGER"] = "less -r"

        # Check if the steps filter was given
        self.steps_filter_provided: bool = "all" not in args_copy.steps

    def _verify_filters(
        self,
        filters_to_check: List[str],
        valid_options: Union[List, Tuple],
        suppress_warnings: bool,
        warning_msg: str = "",
    ):
        """
        Verify and validate a list of user-provided filters against a set of valid options.

        This method checks each filter in the `filters_to_check` list to determine if it is present
        in the `valid_options`. If a filter is found to be invalid (i.e., not in `valid_options`),
        it is removed from the `filters_to_check` list. Depending on the value of `suppress_warnings`,
        a warning message may be logged for each invalid filter.

        Args:
            filters_to_check: A list of filters provided by the user that need to be validated.
            valid_options: A list or tuple of valid options against which the filters will be checked.
            suppress_warnings: A boolean flag indicating whether to suppress warning messages.
                If True, no warnings will be logged for invalid filters.
            warning_msg: An optional string that provides additional context for the warning message
                logged when an invalid filter is detected. Default is an empty string.
        """
        for filter_arg in filters_to_check[:]:
            if filter_arg not in valid_options:
                if not suppress_warnings:
                    LOG.warning(f"The filter '{filter_arg}' is invalid. {warning_msg}")
                filters_to_check.remove(filter_arg)

    def _verify_filter_args(self, suppress_warnings: bool = False):
        """
        Verify the validity of filter arguments used in the current context.

        This method checks various filter arguments, including steps, max_tasks, task_status,
        return_code, task_queues, and workers, to ensure they are valid and can be used.
        Invalid filters are removed from their respective lists, and warnings may be logged
        based on the `suppress_warnings` flag.

        Args:
            suppress_warnings: If True, suppress logging of warnings for invalid filters.
        """
        # Ensure the steps are valid
        if "all" not in self.args.steps:
            LOG.debug(f"args.steps before verification: {self.args.steps}")
            existing_steps = self.spec.get_study_step_names()
            self._verify_filters(
                self.args.steps,
                existing_steps,
                suppress_warnings,
                warning_msg="Removing this step from the list of steps to filter by...",
            )
            LOG.debug(f"args.steps after verification: {self.args.steps}")

        # Make sure max_tasks is a positive int
        if self.args.max_tasks is not None:
            LOG.debug(f"args.max_tasks before verification: {self.args.max_tasks}")
            if self.args.max_tasks < 1 or not isinstance(self.args.max_tasks, int):
                if not suppress_warnings:
                    LOG.warning("The value of --max-tasks must be an integer greater than 0. Ignoring --max-tasks...")
                self.args.max_tasks = None
            LOG.debug(f"args.max_tasks after verification: {self.args.max_tasks}")

        # Make sure task_status is valid
        if self.args.task_status:
            LOG.debug(f"args.task_status before verificaiton: {self.args.task_status}")
            self.args.task_status = [x.upper() for x in self.args.task_status]
            self._verify_filters(
                self.args.task_status,
                VALID_STATUS_FILTERS,
                suppress_warnings,
                warning_msg="Removing this status from the list of statuses to filter by...",
            )
            LOG.debug(f"args.task_status after verification: {self.args.task_status}")

        # Ensure return_code is valid
        if self.args.return_code:
            LOG.debug(f"args.return_code before verification: {self.args.return_code}")
            # TODO remove this code block and uncomment the line below once you've
            # implemented entries for restarts/retries
            idx = 0
            for ret_code_provided in self.args.return_code[:]:
                ret_code_provided = ret_code_provided.upper()
                if ret_code_provided in ("RETRY", "RESTART"):
                    if not suppress_warnings:
                        LOG.warning(f"The {ret_code_provided} filter is coming soon. Ignoring this filter for now...")
                    self.args.return_code.remove(ret_code_provided)
                else:
                    self.args.return_code[idx] = ret_code_provided
                    idx += 1

            # self.args.return_code = [ret_code.upper() for ret_code in self.args.return_code]
            self._verify_filters(
                self.args.return_code,
                VALID_RETURN_CODES,
                suppress_warnings,
                warning_msg="Removing this code from the list of return codes to filter by...",
            )
            LOG.debug(f"args.return_code after verification: {self.args.return_code}")

        # Ensure every task queue provided exists
        if self.args.task_queues:
            LOG.debug(f"args.task_queues before verification: {self.args.task_queues}")
            existing_queues = self.spec.get_queue_list(["all"], omit_tag=True)
            self._verify_filters(
                self.args.task_queues,
                existing_queues,
                suppress_warnings,
                warning_msg="Removing this queue from the list of queues to filter by...",
            )
            LOG.debug(f"args.task_queues after verification: {self.args.task_queues}")

        # Ensure every worker provided exists
        if self.args.workers:
            LOG.debug(f"args.workers before verification: {self.args.workers}")
            worker_names = self.spec.get_worker_names()
            self._verify_filters(
                self.args.workers,
                worker_names,
                suppress_warnings,
                warning_msg="Removing this worker from the list of workers to filter by...",
            )
            LOG.debug(f"args.workers after verification: {self.args.workers}")

    def _process_task_queue(self):
        """
        Modify the list of steps to display status for based on the provided task queues.

        This method processes the task queues specified by the user, removing any duplicates
        and checking for their validity. It updates the list of steps to include those associated
        with the valid task queues. If a provided task queue does not exist, a warning is logged.
        """
        from merlin.config.configfile import CONFIG  # pylint: disable=C0415

        LOG.debug("Processing task_queues filter...")
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

        LOG.debug(f"Steps after task_queues filter: {self.args.steps}")

    def get_steps_to_display(self) -> Dict[str, List[str]]:
        """
        Generate a dictionary of steps to display the status for based on user-provided filters.

        This method processes the `--steps` and `--task-queues` options from the `merlin
        detailed-status` command. It determines which steps should be included in the status
        display based on the existing steps in the study and the specified filters.

        Returns:
            A dictionary containing two lists:\n
                - `started`: A list of steps that have been started.
                - `unstarted`: A list of steps that have not yet been started.
        """
        existing_steps = self.spec.get_study_step_names()

        LOG.debug(f"existing steps: {existing_steps}")

        if ("all" in self.args.steps) and (not self.args.task_queues):
            LOG.debug("The steps and task_queues filters weren't provided. Setting steps to be all existing steps.")
            self.args.steps = existing_steps
        else:
            # This won't matter anymore since task_queues is not None here
            if "all" in self.args.steps:
                self.args.steps = []

            # Add steps to start based on task queues provided
            if self.args.task_queues:
                self._process_task_queue()

            # Sort the steps to start by the order they show up in the study
            for i, estep in enumerate(existing_steps):
                if estep in self.args.steps:
                    self.args.steps.remove(estep)
                    self.args.steps.insert(i, estep)

        LOG.debug(f"Building detailed step tracker based on these steps: {self.args.steps}")

        # Filter the steps to display status for by started/unstarted
        step_tracker = self._create_step_tracker(self.args.steps.copy())

        return step_tracker

    def _remove_steps_without_statuses(self):
        """
        Remove steps from the requested statuses that do not have any associated tasks.

        This method iterates through the `requested_statuses` dictionary and checks each step
        for associated sub-steps. If a step does not have any valid sub-step workspaces (i.e.,
        it has no tasks to view the status of), it is removed from the `requested_statuses`.

        Note:
            After applying filters, there's a chance that certain steps will still exist
            in self.requested_statuses but won't have any tasks to view the status of. That's
            why this method is necessary.
        """
        result = deepcopy(self.requested_statuses)

        for step_name, overall_step_info in self.requested_statuses.items():
            sub_step_workspaces = sorted(list(overall_step_info.keys() - NON_WORKSPACE_KEYS))
            if len(sub_step_workspaces) == 0:
                LOG.debug(f"Removing step '{step_name}' from the requested_statuses dict since it didn't match our filters.")
                del result[step_name]

        self.requested_statuses = result

    def _search_for_filter(self, filter_to_apply: List[str], entry_to_search: Union[List[str], str]) -> bool:
        """
        Search an entry to see if the specified filters apply to it.

        This method checks if any of the provided filters match the given entry or entries.

        Args:
            filter_to_apply: A list of filters to search for.
            entry_to_search: A list or string of entries to search for the filters in.

        Returns:
            True if a filter was found in the entry; False otherwise.
        """
        if not isinstance(entry_to_search, list):
            entry_to_search = [entry_to_search]

        filter_matches = []
        apply_list_of_regex(filter_to_apply, entry_to_search, filter_matches, display_warning=False)
        if len(filter_matches) != 0:
            return True
        return False

    def apply_filters(self):
        """
        Apply filters based on the provided command-line arguments for workers, return code,
        and task status, as well as enforce a maximum task limit if specified.

        This method processes the `requested_statuses` to filter out entries that do not match
        the specified criteria. It ensures that the filtering is done in-place to optimize performance
        and avoid a two-pass algorithm, which can be inefficient with a large number of statuses.
        """
        if self.args.max_tasks is not None:
            # Make sure the max_tasks variable is set to a reasonable number and store that value
            if self.args.max_tasks > self.num_requested_statuses:
                LOG.warning(
                    f"'max_tasks' was set to {self.args.max_tasks} but only {self.num_requested_statuses} statuses exist. "
                    f"Setting 'max_tasks' to {self.num_requested_statuses}."
                )
                self.args.max_tasks = self.num_requested_statuses

        # Establish a map between keys and filters; Only create a key/val pair here if the filter is not None
        filter_key_map = {
            key: value
            for key, value in zip(
                ["status", "return_code", "workers"], [self.args.task_status, self.args.return_code, self.args.workers]
            )
            if value is not None
        }

        matches_found = 0
        filtered_statuses = {}
        for step_name, overall_step_info in self.requested_statuses.items():
            filtered_statuses[step_name] = {}
            # Add the non-workspace keys to the filtered_status dict so we
            # don't accidentally miss any of this information while filtering
            for non_ws_key in NON_WORKSPACE_KEYS:
                try:
                    filtered_statuses[step_name][non_ws_key] = overall_step_info[non_ws_key]
                except KeyError:
                    LOG.debug(
                        f"Tried to add {non_ws_key} to filtered_statuses dict "
                        f"but it was not found in requested_statuses[{step_name}]"
                    )

            # Go through the actual statuses and filter them as necessary
            for sub_step_workspace, task_status_info in overall_step_info.items():
                # Ignore non workspace keys
                if sub_step_workspace in NON_WORKSPACE_KEYS:
                    continue

                found_a_match = False

                # Check all of our filters to see if this specific entry matches them all
                filter_match = [False for _ in range(len(filter_key_map))]
                for i, (filter_key, filter_to_apply) in enumerate(filter_key_map.items()):
                    filter_match[i] = self._search_for_filter(filter_to_apply, task_status_info[filter_key])

                found_a_match = any(filter_match)

                # If a match is found, increment the number of matches found and compare against args.max_tasks limit
                if found_a_match:
                    matches_found += 1
                    filtered_statuses[step_name][sub_step_workspace] = task_status_info
                    # If we've hit the limit set by args.max_tasks, break out of the inner loop
                    if matches_found == self.args.max_tasks:
                        break
                else:
                    LOG.debug(f"No matching filter for '{sub_step_workspace}'.")

            # If we've hit the limit set by args.max_tasks, break out of the outer loop
            if matches_found == self.args.max_tasks:
                break

        LOG.debug(f"result after applying filters: {filtered_statuses}")
        LOG.info(f"Found {matches_found} tasks matching your filters.")

        # Set our requested statuses to the new filtered statuses
        self.requested_statuses = filtered_statuses
        self._remove_steps_without_statuses()

        # If no tasks were found set the status dict to empty
        if self.num_requested_statuses == 0:
            self.requested_statuses = {}

        if self.args.max_tasks is not None:
            LOG.info(f"Limited the number of tasks to display to {self.args.max_tasks} tasks.")

    def apply_max_tasks_limit(self):
        """
        Filter the dictionary of statuses to ensure that the number of displayed tasks does not exceed
        the specified maximum limit.

        This method checks the current value of `max_tasks` and adjusts it if it exceeds the number
        of available statuses. It then iterates through the `requested_statuses`, removing excess
        entries to comply with the `max_tasks` limit. The method also merges the allowed task statuses
        into a new dictionary and updates the `requested_statuses` accordingly.
        """
        # Make sure the max_tasks variable is set to a reasonable number and store that value
        if self.args.max_tasks > self.num_requested_statuses:
            LOG.debug(
                f"'max_tasks' was set to {self.args.max_tasks} but only {self.num_requested_statuses} statuses exist. "
                f"Setting 'max_tasks' to {self.num_requested_statuses}."
            )
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
            dict_deep_merge(new_status_dict[step_name], overall_step_info, conflict_handler=status_conflict_handler)

        LOG.info(f"Limited the number of tasks to display to {max_tasks} tasks.")

        # Set the new requested statuses with the max_tasks limit and remove steps without statuses
        self.requested_statuses = new_status_dict
        self._remove_steps_without_statuses()

        # Reset max_tasks
        self.args.max_tasks = max_tasks

    def load_requested_statuses(self):
        """
        Populate the requested_statuses dict with statuses that the user is looking to find.
        Filters for steps, task queues, workers will have already been applied
        when creating the step_tracker attribute. Remaining filters will be applied here.
        """
        # Grab all the statuses based on our step tracker
        super().load_requested_statuses()

        # Determine if there are filters to apply
        filters_to_apply = (
            (self.args.return_code is not None) or (self.args.task_status is not None) or (self.args.workers is not None)
        )

        # Case where there are filters to apply
        if filters_to_apply:
            self.apply_filters()  # This will also apply max_tasks if it's provided too
        # Case where there are no filters but there is a max tasks limit set
        elif self.args.max_tasks is not None:
            self.apply_max_tasks_limit()

    def get_user_filters(self) -> bool:
        """
        Prompt the user to specify filters for the statuses to display. The user can choose from
        several filtering options, including setting a maximum number of tasks, filtering by status,
        return code, or worker, or exiting the filter prompt without applying any filters.

        The method displays available filter options and their descriptions, then collects and
        validates the user's input. If the user provides valid filters, they are stored in the
        corresponding attributes. If the user opts to exit, the method returns True; otherwise,
        it returns False.

        Possible filtering options include:\n
        - A string "MAX_TASKS" to request a limit on the number of tasks.
        - A list of statuses to filter by, corresponding to the `--task-status` flag.
        - A list of return codes to filter by, corresponding to the `--return-code` flag.
        - A list of workers to filter by, corresponding to the `--workers` flag.
        - An exit keyword to leave the filter prompt without applying any filters.

        Returns:
            True if the user chooses to exit without filtering; False otherwise.
        """
        valid_workers = tuple(self.spec.get_worker_names())

        # Build the filter options
        filter_info = {
            "Filter Type": [
                "Put a limit on the number of tasks to display",
                "Filter by status",
                "Filter by return code",
                "Filter by workers",
                "Exit without filtering",
            ],
            "Description": [
                "Enter 'MAX_TASKS'",
                f"Enter a comma separated list of the following statuses you'd like to see: {VALID_STATUS_FILTERS}",
                f"Enter a comma separated list of the following return codes you'd like to see: {VALID_RETURN_CODES}",
                f"Enter a comma separated list of the following workers from your spec: {valid_workers}",
                f"Enter one of the following: {VALID_EXIT_FILTERS}",
            ],
            "Example": ["MAX_TASKS", "FAILED, CANCELLED", "SOFT_FAIL, RETRY", "default_worker, other_worker", "EXIT"],
        }

        # Display the filter options
        filter_option_renderer = status_renderer_factory.get_renderer("table", disable_theme=True, disable_pager=True)
        filter_option_renderer.layout(status_data=filter_info)
        filter_option_renderer.render()

        # Obtain and validate the filter provided by the user
        invalid_filter = True
        exit_requested = False
        while invalid_filter:
            user_filters = input("How would you like to filter the tasks? ")

            # Remove spaces and split user filters by commas
            user_filters = user_filters.replace(" ", "")
            user_filters = user_filters.split(",")

            # Variables to help track our filters
            status_filters = []
            return_code_filters = []
            worker_filters = []
            max_task_requested = False

            # Ensure every filter is valid
            for entry in user_filters:
                invalid_filter = False
                orig_entry = entry
                entry = entry.upper()

                if entry in VALID_STATUS_FILTERS:
                    status_filters.append(entry)
                elif entry in VALID_RETURN_CODES:
                    return_code_filters.append(entry)
                elif orig_entry in valid_workers:
                    worker_filters.append(orig_entry)
                elif entry == "MAX_TASKS":
                    max_task_requested = True
                elif entry in VALID_EXIT_FILTERS:
                    LOG.info(f"The exit filter '{entry}' was provided. Exiting without filtering.")
                    exit_requested = True
                    break
                else:
                    invalid_filter = True
                    print(f"Invalid input: {entry}. Input must be one of the following {ALL_VALID_FILTERS + valid_workers}")
                    break

        if exit_requested:
            return True

        # Set the filters provided by the user
        self.args.task_status = status_filters if len(status_filters) > 0 else None
        self.args.return_code = return_code_filters if len(return_code_filters) > 0 else None
        self.args.workers = worker_filters if len(worker_filters) > 0 else None

        # Set the max_tasks value if it was requested
        if max_task_requested:
            self.get_user_max_tasks()

        return False

    def get_user_max_tasks(self):
        """
        Prompt the user to specify a maximum limit for the number of tasks to display.

        The method repeatedly requests input from the user until a valid integer greater than 0
        is provided. Once a valid input is received, it sets the `max_tasks` attribute in the
        `args` object to the specified limit.

        This method ensures that the user input is validated and handles any exceptions
        related to invalid input types or values.

        Raises:
            ValueError: If the input is not a valid integer greater than 0.
        """
        invalid_input = True

        while invalid_input:
            try:
                user_max_tasks = int(input("What limit would you like to set? (must be an integer greater than 0) "))
                if user_max_tasks > 0:
                    invalid_input = False
                else:
                    raise ValueError
            except ValueError:
                print("Invalid input. The limit must be an integer greater than 0.")
                continue

        self.args.max_tasks = user_max_tasks

    def filter_via_prompts(self):
        """
        Interact with the user to determine how many and which tasks should be displayed,
        preventing terminal overload by limiting the output to a manageable number of tasks.

        This method prompts the user for filtering options, including task statuses, return codes,
        and worker specifications. It also handles the case where the user opts to exit without
        applying any filters. If filters are provided, it applies them accordingly.

        Warning:
            The method includes specific handling for the "RESTART" and "RETRY" return codes,
            which are currently not implemented, and issues warnings if these filters are selected.
        """
        # Get the filters from the user
        exit_without_filtering = self.get_user_filters()

        if not exit_without_filtering:
            # TODO remove this once restart/retry functionality is implemented
            if self.args.return_code is not None:
                if "RESTART" in self.args.return_code:
                    LOG.warning("The RESTART filter is coming soon. Ignoring this filter for now...")
                    self.args.return_code.remove("RESTART")
                if "RETRY" in self.args.return_code:
                    LOG.warning("The RETRY filter is coming soon. Ignoring this filter for now...")
                    self.args.return_code.remove("RETRY")

            # If any status, return code, or workers filters were given, apply them
            if any(
                list_var is not None and len(list_var) != 0
                for list_var in [self.args.return_code, self.args.task_status, self.args.workers]
            ):
                self.apply_filters()  # This will also apply max_tasks if it's provided too
            # If just max_tasks was given, apply the limit and nothing else
            elif self.args.max_tasks is not None:
                self.apply_max_task_limit()

    def display(self, test_mode: bool = False):
        """
        Displays a task-by-task view of the statuses based on the user-defined filters.

        This method checks for any requested statuses and, if found, invokes the
        `display_status_task_by_task` function to present the tasks accordingly.
        If no statuses are available to display, it logs a warning message.

        Args:
            test_mode: If set to True, the method runs in testing mode, suppressing
                any output to the terminal. This is useful for unit testing or debugging
                without cluttering the output.
        """
        # Check that there's statuses found and display them
        if self.requested_statuses:
            display_status_task_by_task(self, test_mode=test_mode)
        else:
            LOG.warning("No statuses to display.")


# Pylint complains that args is unused but we can ignore that
def status_conflict_handler(*args, **kwargs) -> Any:  # pylint: disable=W0613
    """
    Handles conflicts that arise when merging two status files by applying specific merge rules
    to conflicting values.

    This function is designed to be used during the merging process of status entries, where
    conflicting values may exist. It defines how to resolve these conflicts based on predefined
    rules, ensuring that the merged dictionary maintains integrity and clarity.

    The merge rules currently implemented are:\n
    - **string-concatenate**: Concatenates the two conflicting string values.
    - **use-dict_b-and-log-debug**: Uses the value from dict_b and logs a debug message indicating
      the conflict.
    - **use-longest-time**: Chooses the longest time value between the two conflicting entries,
      converting them to a timedelta for comparison.
    - **use-max**: Selects the maximum integer value from the two conflicting entries.

    If a key does not have a defined merge rule, a warning is logged, and the function returns None.

    The function expects the following keyword arguments:\n
    - `dict_a_val`: The conflicting value from the dictionary that we are merging into (dict_a).
    - `dict_b_val`: The conflicting value from the dictionary that we are merging from (dict_b).
    - `key`: The key in each dictionary that has a conflict.
    - `path`: The current path in the dictionary tree during the merge process.

    Returns:
        The resolved value to merge into dict_a at the specified key.
    """
    # Grab the arguments passed into this function
    dict_a_val = kwargs.get("dict_a_val", None)
    dict_b_val = kwargs.get("dict_b_val", None)
    key = kwargs.get("key", None)
    path = kwargs.get("path", None)

    merge_rules = {
        "task_queue": "string-concatenate",
        "worker_name": "string-concatenate",
        "status": "use-dict_b-and-log-debug",
        "return_code": "use-dict_b-and-log-debug",
        "elapsed_time": "use-longest-time",
        "run_time": "use-longest-time",
        "restarts": "use-max",
    }

    # TODO
    # - make status tracking more modular (see https://lc.llnl.gov/gitlab/weave/merlin/-/issues/58)
    # - once it's more modular, move the below code and the above merge_rules dict to a property in
    #   one of the new status classes (the one that has condensing maybe? or upstream from that?)

    # params = self.spec.get_parameters()
    # for token in params.parameters:
    #     merge_rules[token] = "use-dict_b-and-log-debug"

    # Set parameter token key rules (commented for loop would be better but it's
    # only possible if this conflict handler is contained within Status object; however,
    # since this function needs to be imported outside of this file we can't do that)
    if path is not None and "parameters" in path:
        merge_rules[key] = "use-dict_b-and-log-debug"

    try:
        merge_rule = merge_rules[key]
    except KeyError:
        LOG.warning(f"The key '{key}' does not have a merge rule defined. Setting this merge to None.")
        return None

    merge_val = None

    if merge_rule == "string-concatenate":
        merge_val = f"{dict_a_val}, {dict_b_val}"
    elif merge_rule == "use-dict_b-and-log-debug":
        LOG.debug(
            f"Conflict at key '{key}' while merging status files. Using the updated value. "
            "This could lead to incorrect status information, you may want to re-run in debug mode and "
            "check the files in the output directory for this task."
        )
        merge_val = dict_b_val
    elif merge_rule == "use-longest-time":
        if dict_a_val == "--:--:--":
            merge_val = dict_b_val
        elif dict_b_val == "--:--:--":
            merge_val = dict_a_val
        else:
            dict_a_time = convert_to_timedelta(dict_a_val)
            dict_b_time = convert_to_timedelta(dict_b_val)
            merge_val = get_duration(max(dict_a_time, dict_b_time))
    elif merge_rule == "use-max":
        merge_val = max(dict_a_val, dict_b_val)
    else:
        LOG.warning(f"The merge_rule '{merge_rule}' was provided but it has no implementation.")

    return merge_val


def read_status(
    status_filepath: str, lock_file: str, display_fnf_message: bool = True, raise_errors: bool = False, timeout: int = 10
) -> Dict:
    """
    Locks the status file for reading and returns its contents.

    This function attempts to read the contents of a status file while ensuring that the file is
    locked to prevent race conditions. It handles various exceptions that may occur during the
    reading process, including file not found errors and JSON decoding errors.

    Args:
        status_filepath: The path to the status file that will be read.
        lock_file: The path to the lock file used to create a FileLock.
        display_fnf_message: If True, displays a warning message if the file is not found.
        raise_errors: If True, raises exceptions when errors occur.
        timeout: The maximum time (in seconds) to hold the lock before timing out.

    Returns:
        A dictionary containing the contents of the status file.

    Raises:
        Timeout: If the lock acquisition times out.
        FileNotFoundError: If the status file does not exist and `raise_errors` is True.
        json.decoder.JSONDecodeError: If the status file is empty or contains invalid JSON and `raise_errors` is True.
        Exception: Any other exceptions that occur during the reading process if `raise_errors` is True.
    """
    statuses_read = {}

    # Pylint complains that we're instantiating an abstract class but this is correct usage
    lock = FileLock(lock_file)  # pylint: disable=abstract-class-instantiated
    try:
        # The status files will need locks when reading to avoid race conditions
        with lock.acquire(timeout=timeout):
            with open(status_filepath, "r") as status_file:
                statuses_read = json.load(status_file)
    # Handle timeouts
    except Timeout as to_exc:
        LOG.warning(f"Timed out when trying to read status from '{status_filepath}'")
        if raise_errors:
            raise to_exc
    # Handle FNF errors
    except FileNotFoundError as fnf_exc:
        if display_fnf_message:
            LOG.warning(f"Could not find '{status_filepath}'")
        if raise_errors:
            raise fnf_exc
    # Handle JSONDecode errors (this is likely due to an empty status file)
    except json.decoder.JSONDecodeError as json_exc:
        LOG.warning(f"JSONDecodeError raised when trying to read status from '{status_filepath}'")
        if raise_errors:
            raise json_exc
    # Catch all exceptions so that we don't crash the workers
    except Exception as exc:  # pylint: disable=broad-except
        LOG.warning(
            f"An exception was raised while trying to read status from '{status_filepath}'!\n"
            f"{print_exception(type(exc), exc, exc.__traceback__)}"
        )
        if raise_errors:
            raise exc

    return statuses_read


def write_status(status_to_write: Dict, status_filepath: str, lock_file: str, timeout: int = 10):
    """
    Locks the status file for writing and writes the provided status to the file.

    This function ensures that the status file is locked during the write operation to prevent
    race conditions. It does not catch errors during the writing process, as it is important to
    be aware of any issues that may arise.

    Args:
        status_to_write: The status data to write to the status file.
        status_filepath: The path to the status file where the status will be written.
        lock_file: The path to the lock file used to create a FileLock for the write operation.
        timeout: The maximum time (in seconds) to hold the lock before timing out.

    Raises:
        Exception: Any exceptions that occur during the writing process will be logged, but not caught.
    """
    # Pylint complains that we're instantiating an abstract class but this is correct usage
    try:
        lock = FileLock(lock_file)  # pylint: disable=abstract-class-instantiated
        with lock.acquire(timeout=timeout):
            with open(status_filepath, "w") as status_file:
                json.dump(status_to_write, status_file)
    # Catch all exceptions so that we don't crash the workers
    except Exception as exc:  # pylint: disable=broad-except
        LOG.warning(
            f"An exception was raised while trying to write status to '{status_filepath}'!\n"
            f"{print_exception(type(exc), exc, exc.__traceback__)}"
        )
