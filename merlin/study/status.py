###############################################################################
# Copyright (c) 2022, Lawrence Livermore National Security, LLC.
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
import csv
import logging
import os
import re
from collections import deque
from pathlib import Path
from typing import Dict, List, Tuple, Union

from merlin import display
from merlin.config.configfile import CONFIG
from merlin.utils import ws_time_to_td

LOG = logging.getLogger(__name__)


def query_status(args: "Argparse Namespace", spec_display: bool, file_or_ws: str):
    """
    The overarching function to query the status of a study.

    :param `args`: The CLI arguments provided via the user
    :param `spec_display`: A boolean. If this is True, the user provided a spec file. If this is False,
                           the user provided a workspace directory.
    :param `file_or_ws`: If spec_display is True then this is a filepath to a spec file. Otherwise,
                         this is a dirpath to a study's output directory (workspace).
    """
    # Load in the spec file from either the spec provided by the user or the workspace
    if spec_display:
        workspace, spec = load_from_spec(args, file_or_ws)
    else:
        spec = load_from_workspace(file_or_ws)
        workspace = file_or_ws 

    # Verify the filter args (if any)
    verify_filter_args(args, spec)

    # Get a dict of started and unstarted steps to display and a dict representing the number of tasks per step
    step_tracker = get_steps_to_display(workspace, spec, args.steps, args.task_queues, args.workers)
    tasks_per_step = spec.get_tasks_per_step()

    # Determine whether to display a low level or high level display
    filters = [args.task_queues, args.workers, args.task_status, args.max_tasks, ("all" not in args.steps)]
    low_lvl = any(filters)

    # If we're dumping to csv or showing low level task-by-task info
    if args.csv or low_lvl:
        # Get the statuses requested by the user and insert column titles
        all_statuses = get_all_statuses(step_tracker["started_steps"], tasks_per_step, workspace, args)
        statuses_with_labels = insert_column_titles(all_statuses)

        if args.csv:
            dump_status_to_csv(statuses_with_labels, args.csv)
        else:
            display.display_low_lvl_status(statuses_with_labels, step_tracker["unstarted_steps"], workspace, args)
    else:
        # Display the high level status summary
        display.display_high_lvl_status(tasks_per_step, step_tracker, workspace, args.cb_help)


def insert_column_titles(statuses: List[List[str]]) -> List[List[str]]:
    """
    Given a list of statuses, prepend a list of column titles.
    Using deque rather than list.insert we make this an O(1) operation.

    :param `statuses`: A list of statuses where each status is contained within its own list
    :returns: The modified list with column titles
    """
    # Store column titles in a list and add additional column titles if necessary
    column_titles = ["Step Name", "Status", "Return Code", "Elapsed Time", "Run Time", "Restarts", "Step Workspace"]
    try:
        # If celery workers were used, the length of each status entry will be 9
        len_status_entry = len(statuses[0])
        if len_status_entry == 9:
            column_titles.append("Task Queue")
            column_titles.append("Worker Name")
    except IndexError:
        pass

    # Insert column titles at head of list in O(1) time
    statuses = deque(statuses)
    statuses.appendleft(column_titles)
    statuses = list(statuses)

    return statuses


def dump_status_to_csv(statuses_to_write: List[List[str]], csv_file: str):
    """
    Write the statuses to a csv file.

    :param `statuses_to_write`: A list of statuses to write to the csv file
    :param `csv_file`: The name of the csv file we're going to write to
    :side effect: A csv file is created or appended to
    """
    # Get the correct filemode
    if os.path.exists(csv_file):
        fmode = "a"
    else:
        fmode = "w"
    
    # If we have statuses to write, create a csv writer object and write to the csv file
    if len(statuses_to_write) > 1:
        LOG.info(f"{'Writing' if fmode == 'w' else 'Appending'} {len(statuses_to_write) - 1} statuses to {csv_file}...")
        with open(csv_file, fmode) as f:
            csv_writer = csv.writer(f)
            # If we're writing a new csv file, add the column titles
            if fmode == "w":
                csv_writer.writerow(statuses_to_write[0])
            csv_writer.writerows(statuses_to_write[1:])
        LOG.info("Writing complete.")
    else:
        LOG.error(f"No statuses found to write.")


def get_all_statuses(started_steps: List[str], tasks_per_step: Dict[str, int], workspace: str, args: "Namespace") -> List[List[str]]:
    """
    Get all the statuses that the user is looking for. Filters for task queue and workers will have already been applied
    when creating the started_steps list. The filters for task status and max tasks will be applied here.

    :param `started_steps`: A list of started steps that the user wants information from
    :param `tasks_per_step`: A dictionary to keep track of how many tasks are needed for each step in the workflow
    :param `workspace`: The output directory path for the study
    :param `args`: The arguments from the user provided by the CLI
    :returns: A list of all statuses matching the users filter(s). Each status is contained within its own list.
    """
    # Read in all statuses from the started steps the user wants to see, and split each status by spaces
    LOG.info(f"Reading task statuses from {workspace}")
    all_statuses = []
    for sstep in started_steps:
        step_workspace = f"{workspace}/{sstep}"
        step_statuses = get_step_statuses(step_workspace, tasks_per_step[sstep])
        for step in step_statuses:
            all_statuses.append(step.split(" "))

    # Filter the statuses we read in by task status if necessary
    if args.task_status:
        LOG.info(f"Filtering tasks by the following statuses: {args.task_status}")
        for entry in all_statuses[:]:
            if entry[1] not in args.task_status:
                all_statuses.remove(entry)
        if len(all_statuses) == 0:
            LOG.error(f"No tasks found matching the filters {args.task_status}.")

    LOG.info(f"Found {len(all_statuses)} tasks matching your filters.")

    # Limit the number of statuses to display if necessary
    if args.max_tasks:
        if args.max_tasks > len(all_statuses):
            args.max_tasks = len(all_statuses)
        LOG.info(f"Max tasks filter was provided. Limiting the number of statuses to display to {args.max_tasks}.")
        all_statuses = all_statuses[:args.max_tasks]

    return all_statuses


def get_step_statuses(step_workspace: str, total_tasks: int, args: "Namespace" = None) -> List[str]:
    """
    Given a step workspace and the total number of tasks for the step, read in all the statuses
    for the step and return them in a list.

    :param `step_workspace`: The path to the step we're going to read statuses from
    :param `total_tasks`: The total number of tasks for the step
    :returns: A list of statuses for the given step
    """
    step_statuses = []
    num_statuses_read = 0
    # Count number of tasks in each state
    for root, dirs, files in os.walk(step_workspace):
        if "MERLIN_STATUS" in files:
            # Read in the statuses for the tasks in this step
            status_file = f"{root}/MERLIN_STATUS"
            statuses_read = read_status(status_file).split("\n")
            statuses_read.remove('')
            step_statuses.extend(statuses_read)
            num_statuses_read += len(statuses_read)
        # To help save time, exit this loop if we've read all the task statuses for this step
        if num_statuses_read == total_tasks:
            break
    return step_statuses


def verify_filter_args(args: "Argparse Namespace", spec: "MerlinSpec"):
    """
    Verify that our filters are all valid and able to be used.
    :param `args`: The CLI arguments provided via the user
    :param `spec`: A MerlinSpec object loaded from the expanded spec in the output study directory
    """
    if "all" not in args.steps:
        existing_steps = spec.get_study_step_names()
        for step in args.steps[:]:
            if step not in existing_steps:
                LOG.warning(f"The step {step} was not found in the study {spec.name}. Removing this step from the list of steps to filter by...")
                args.steps.remove(step)

    # Make sure max_tasks is a positive int
    if args.max_tasks and args.max_tasks < 1:
        LOG.warning(f"The value of --max-tasks must be greater than 0. Ignoring --max-tasks...")
        args.max_tasks = None
    
    # Make sure task_status is valid
    if args.task_status:
        args.task_status = [x.upper() for x in args.task_status]
        valid_task_statuses = ("INITIALIZED", "RUNNING", "FINISHED", "FAILED", "RESTART", "CANCELLED", "UNKNOWN")
        for status_filter in args.task_status[:]:
            if status_filter not in valid_task_statuses:
                LOG.warning(f"The status filter {status_filter} is invalid. Valid statuses are any of the following: {valid_task_statuses}. Ignoring {status_filter} as a filter...")
                args.task_status.remove(status_filter)

    # Ensure every task queue provided exists
    if args.task_queues:
        existing_queues = spec.get_queue_list(["all"], omit_tag=True)
        for queue in args.task_queues[:]:
            if queue not in existing_queues:
                LOG.warning(f"The queue {queue} is not an existing queue. Removing this queue from the list of queues to filter by...")
                args.task_queues.remove(queue)
    
    # Ensure every worker provided exists
    if args.workers:
        worker_names = spec.get_worker_names()
        for worker in args.workers[:]:
            if worker not in worker_names:
                LOG.warning(f"The worker {worker} cannot be found. Removing this worker from the list of workers to filter by...")
                args.workers.remove(worker)


def get_latest_study(studies: List[str]) -> str:
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


def obtain_study(study_output_dir: str, num_studies: int, potential_studies: List[Tuple[int, str]], no_prompts: bool) -> str:
    """
    Grab the study that the user wants to view the status of based on a list of potential studies provided.

    :param `study_output_dir`: A string representing the output path of a study; equivalent to $(OUTPUT_PATH)
    :param `num_studies`: The number of potential studies we found
    :param `potential_studies`: The list of potential studies we found; Each entry is of the form (index, potential_study_name)
    :param `no_prompts`: A CLI flag for the status command representing whether the user wants CLI prompts or not
    :returns: A directory path to the study that the user wants to view the status of ("study_output_dir/selected_potential_study")
    """
    study_to_check = f"{study_output_dir}/"
    if num_studies == 0:
        raise ValueError("Could not find any potential studies.")
    if num_studies > 1:
        # Get the latest study
        if no_prompts:
            LOG.info("Choosing the latest study...")
            potential_studies = [study for _, study in potential_studies]
            latest_study = get_latest_study(potential_studies)
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
                        print(f"{display.ANSI_COLORS['RED']}Input must be an integer between 1 and {num_studies}.{display.ANSI_COLORS['RESET']}")
                        prompt = "Enter a different index: "
            study_to_check += potential_studies[index-1][1]
    else:
        # Only one study was found so we'll just assume that's the one the user wants
        study_to_check += potential_studies[0][1]
    
    return study_to_check


def load_from_spec(args: "Argparse Namespace", filepath: str) -> Tuple[str, "MerlinSpec"]:
    """
    Get the desired workspace from the user and load up it's yaml spec
    for further processing.

    :param `args`: The namespace given by ArgumentParser with flag info
    :param `filepath`: The filepath to a spec given by the user

    :returns: The workspace of the study we'll check the status for and a MerlinSpec
              object loaded in from the workspace's merlin_info subdirectory.
    """
    from merlin.main import get_spec_with_expansion, get_merlin_spec_with_override, verify_dirpath

    # Get the spec and the output path for the spec
    args.specification = filepath
    spec_provided, _ = get_merlin_spec_with_override(args)
    if spec_provided.output_path:
        study_output_dir = verify_dirpath(spec_provided.output_path)
    else:
        study_output_dir = verify_dirpath(".")

    # Build a list of potential study output directories
    study_output_subdirs = next(os.walk(study_output_dir))[1]
    timestamp_regex = r"\d{8}-\d{6}"
    potential_studies = []
    num_studies = 0
    for subdir in study_output_subdirs:
        match = re.search(rf"{spec_provided.name}_{timestamp_regex}", subdir)
        if match:
            potential_studies.append((num_studies+1, subdir))
            num_studies += 1

    # Obtain the correct study that the user wants to view the status of based on the list of potential studies we just built
    study_to_check = obtain_study(study_output_dir, num_studies, potential_studies, args.no_prompts)

    # Verify the directory that the user selected is a merlin study output directory
    if "merlin_info" not in next(os.walk(study_to_check))[1]:
        LOG.error(f"The merlin_info subdirectory was not found. {study_to_check} may not be a Merlin study output directory.")

    # Grab the spec saved to the merlin info directory in case something in the current spec has changed since starting the study
    actual_spec = get_spec_with_expansion(f"{study_to_check}/merlin_info/{spec_provided.name}.expanded.yaml")

    return study_to_check, actual_spec


def load_from_workspace(workspace: str) -> "MerlinSpec":
    """
    Create a MerlinSpec object based on the spec file in the workspace.

    :param `workspace`: A directory path to an existing study's workspace
    :returns: A MerlinSpec object loaded from the workspace provided by the user
    """
    from merlin.main import verify_dirpath, get_spec_with_expansion

    # Grab the spec file from the directory provided
    info_dir = verify_dirpath(f"{workspace}/merlin_info")
    spec_file = ""
    for root, dirs, files in os.walk(info_dir):
        for f in files:
            if f.endswith(".expanded.yaml"):
                spec_file = f"{info_dir}/{f}"
                break
        break

    # Make sure we got a spec file and load it in
    if not spec_file:
        LOG.error(f"Spec file not found in {info_dir}. Cannot display status.")
        return
    spec = get_spec_with_expansion(spec_file)

    return spec


def process_workers(spec: "MerlinSpec", workers_provided: List[str], steps_to_check: List[str]):
    """
    Modifies the list of steps to display status for based on
    the list of workers provided by the user.

    :param `spec`: MerlinSpec object for a study
    :param `workers_provided`: A list of workers provided by the user
    :param `steps_to_check`: A list of steps to view the status for
    """
    # Remove duplicates
    workers_provided = list(set(workers_provided))

    # Get a map between workers and steps
    worker_step_map = spec.get_worker_step_map()

    # Append steps associated with each worker provided
    for wp in workers_provided:
        # Check for invalid workers
        if wp not in worker_step_map:
            LOG.warning(f"Worker with name {wp} does not exist for this study.")
        else:
            for step in worker_step_map[wp]:
                if step not in steps_to_check:
                    steps_to_check.append(step)


def process_task_queue(spec: "MerlinSpec", queues_provided: List[str], steps_to_check: List[str]):
    """
    Modifies the list of steps to display status for based on
    the list of task queues provided by the user.

    :param `spec`: MerlinSpec object for a study
    :param `queues_provided`: A list of task queues provided by the user
    :param `steps_to_check`: A list of steps to view the status for
    """
    # Remove duplicate queues
    queues_provided = list(set(queues_provided))

    # Get a map between queues and steps
    queue_step_relationship = spec.get_queue_step_relationship()

    # Append steps associated with each task queue provided
    for qp in queues_provided:
        # Check for invalid task queues
        if f"{CONFIG.celery.queue_tag}{qp}" not in queue_step_relationship:
            LOG.warning(f"Task queue with name {qp} does not exist for this study.")
        else:
            for step in queue_step_relationship[f"{CONFIG.celery.queue_tag}{qp}"]:
                if step not in steps_to_check:
                    steps_to_check.append(step)


def create_step_tracker(workspace: str, steps_to_check: List[str]) -> Dict[str, List[str]]:
    """
    Creates a dictionary of started and unstarted steps that we
    will display the status for.

    :param `workspace`: The output directory for a study
    :param `steps_to_check`: A list of steps to view the status of
    :returns: A dictionary mapping of started and unstarted steps. Values are lists of step names.
    """
    step_tracker = {"started_steps": [], "unstarted_steps": []}
    started_steps = next(os.walk(workspace))[1]
    started_steps.remove("merlin_info")

    for sstep in started_steps:
        if sstep in steps_to_check:
            step_tracker["started_steps"].append(sstep)
            steps_to_check.remove(sstep)
    step_tracker["unstarted_steps"] = steps_to_check

    return step_tracker


def get_steps_to_display(workspace: str, spec: "MerlinSpec", steps: List[str], task_queues: List[str], workers: List[str]) -> Dict[str, List[str]]:
    """
    Generates a list of steps to display the status for based on information
    provided to the merlin status command by the user.

    :param `workspace`: The output directory for a study
    :param `spec`: MerlinSpec object for a study
    :param `steps`: A list of steps to view the status of
    :param `task_queues`: A list of task_queues to view the status of
    :param `workers`: A list of workers to view the status of tasks they're running
    :returns: A dictionary of started and unstarted steps for us to display the status of
    """
    existing_steps = spec.get_study_step_names()

    if ("all" in steps) and (not task_queues) and (not workers):
        steps = existing_steps
    else:
        # This won't matter anymore since task_queues or workers is not None here
        if "all" in steps:
            steps = []

        # Add steps to start based on task queues and/or workers provided
        if task_queues:
            process_task_queue(spec, task_queues, steps)
        if workers:
            process_workers(spec, workers, steps)

        # Sort the steps to start by the order they show up in the study
        idx = 0
        for estep in existing_steps:
            if estep in steps:
                steps.remove(estep)
                steps.insert(idx, estep)
                idx += 1

    # Filter the steps to display status for by started/unstarted
    step_tracker = create_step_tracker(workspace, steps)

    return step_tracker


def read_status(status_file: Union[Path, str], display_fnf_message=True) -> str:
    """
    Locks the status file for reading and returns its contents.

    :param `status_file`: The path to the status file that we'll read from
    :param `display_fnf_message`: If True, display the file not found warning. Otherwise don't.
    :returns: The contents of the status file
    """
    # Convert workspace to a Path object if necessary and obtain a filelock object for this status file
    if isinstance(status_file, str):
        status_file = Path(status_file)
    status = ""
    try:
        status = status_file.read_text()
    except FileNotFoundError:
        if display_fnf_message:
            LOG.warning(f"File not found: {status_file}.")
        status = "FNF"
    return status