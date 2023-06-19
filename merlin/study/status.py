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
import logging
import os
import re
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Union

from merlin import display
from merlin.common.dumper import Dumper
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

    # Verify the filter args (if any) and determine whether any filters remain after verification
    filters = [args.task_queues, args.workers, args.task_status, ("all" not in args.steps)]
    filters_provided = any(filters + [args.max_tasks])
    verify_filter_args(args, spec)
    filters_remaining = any(filters + [args.max_tasks])

    # If the user provided filters but the verification process removed them all, don't display anything
    if filters_provided != filters_remaining:
        LOG.warning("No filters remaining, cannot find tasks to display.")
    else:
        # Get a dict of started and unstarted steps to display and a dict representing the number of tasks per step
        step_tracker = get_steps_to_display(workspace, spec, args.steps, args.task_queues, args.workers)
        tasks_per_step = spec.get_tasks_per_step()

        # If we're dumping to an output file or showing low level task-by-task info
        if args.dump or filters_remaining:
            # Get the statuses requested by the user
            requested_statuses = get_requested_statuses(step_tracker["started_steps"], tasks_per_step, workspace, args)

            # Check that there's statuses found
            if requested_statuses and len(requested_statuses["Step Name"]) > 0:
                if args.dump:
                    dump_handler(requested_statuses, args.dump)
                else:
                    display.display_low_lvl_status(requested_statuses, step_tracker["unstarted_steps"], workspace, args)
        else:
            # Display the high level status summary
            display.display_high_lvl_status(tasks_per_step, step_tracker, workspace, args.cb_help)


def get_column_labels(statuses: List[List[str]]) -> List[str]:
    """
    Build a list of column labels for the status entries.
    :param `statuses`: A list of lists of statuses. Each status is contained within its own list.
    :returns: A list of column labels
    """
    # Store column titles in a list and add additional column titles if necessary
    column_labels = ["Step Name", "Step Workspace", "Status", "Return Code", "Elapsed Time", "Run Time", "Restarts", "Cmd Parameters", "Restart Parameters"]
    try:
        # If celery workers were used, the length of each status entry will be 10
        len_status_entry = len(statuses[0])
        if len_status_entry == 11:
            column_labels.append("Task Queue")
            column_labels.append("Worker Name")
    except IndexError:
        pass

    return column_labels


def insert_column_titles(statuses: List[List[str]]) -> List[List[str]]:
    """
    Given a list of statuses, prepend a list of column titles.
    Using deque rather than list.insert we make this an O(1) operation.

    :param `statuses`: A list of statuses where each status is contained within its own list
    :returns: The modified list with column titles
    """
    column_labels = get_column_labels(statuses)

    # Insert column titles at head of list in O(1) time
    statuses = deque(statuses)
    statuses.appendleft(column_labels)
    statuses = list(statuses)

    return statuses


def build_csv_status(statuses_to_write: Dict[str, List], date: str) -> Dict[str, List]:
    """
    Add the timestamp to the statuses to write.

    :param `statuses_to_write`: A dict of statuses to write to the csv file
    :param `date`: A timestamp for us to mark when this status occurred
    :returns: A dict equivalent to `statuses_to_write` with a timestamp entry at the start of the dict.
    """
    # Build the list of column labels
    statuses_with_timestamp = {"Time of Status": [date] * len(statuses_to_write["Step Name"])}
    statuses_with_timestamp.update(statuses_to_write)
    return statuses_with_timestamp

    
def build_json_status(statuses_to_write: Dict[str, List], date: str) -> Dict:
    """
    Build the dict of statuses to dump to the json file.

    :param `statuses_to_write`: A dict of statuses to write to the json file
    :param `date`: A timestamp for us to mark when this status occurred
    :returns: A dictionary that's ready to dump to a json outfile
    """
    # Build a dict for the new json entry we'll write
    json_to_dump = {date: {}}
    for i, step in enumerate(statuses_to_write["Step Name"]):
        # Create an entry for this step if it doesn't exist yet
        if step not in json_to_dump[date]:
            json_to_dump[date][step] = {}

            # If we have a queue and worker to display add that here
            if "Task Queue" in statuses_to_write:
                json_to_dump[date][step]["queue"] = statuses_to_write["Task Queue"][i]
            if "Worker Name" in statuses_to_write:
                json_to_dump[date][step]["worker"] = statuses_to_write["Worker Name"][i]
            
            # Add parameter entries as necessary
            if statuses_to_write["Cmd Parameters"][i] != '-------':
                json_to_dump[date][step]["cmd parameters"] = {}
            if statuses_to_write["Restart Parameters"][i] != '-------':
                json_to_dump[date][step]["restart parameters"] = {}

            # Populate the parameter entries
            param_keys = ("Cmd Parameters", "Restart Parameters")
            for param_key in param_keys:
                lower_param_key = param_key.lower()
                if lower_param_key in json_to_dump[date][step]:
                    for param in statuses_to_write[param_key][i].split(";"):
                        split_param = param.split(":")
                        token = split_param[0]
                        value = split_param[1]
                        json_to_dump[date][step][lower_param_key][token] = value
        
        # Format the information for each workspace here
        workspace_dict = {
            statuses_to_write["Step Workspace"][i]: {
                "status": statuses_to_write["Status"][i],
                "return_code": statuses_to_write["Return Code"][i],
                "elapsed_time": statuses_to_write["Elapsed Time"][i],
                "run_time": statuses_to_write["Run Time"][i],
                "restarts": statuses_to_write["Restarts"][i],
            }
        }

        # Update the overarching json dump with the workspace status info
        json_to_dump[date][step].update(workspace_dict)

    return json_to_dump


def build_csv_queue_info(query_return: List[Tuple[str, int, int]], date: str) -> Dict[str, List]:
    """
    Build the lists of column labels and queue info to write to the csv file.

    :param `query_return`: The output of query_status
    :param `date`: A timestamp for us to mark when this status occurred
    :returns: A dict of queue information to dump to csv
    """
    # Build the list of labels if necessary
    csv_to_dump = {"Time": [date]}
    for name, jobs, consumers in query_return:
        csv_to_dump[f"{name}:tasks"] = [str(jobs)]
        csv_to_dump[f"{name}:consumers"] = [str(consumers)]
    
    return csv_to_dump

    
def build_json_queue_info(query_return: List[Tuple[str, int, int]], date: str) -> Dict:
    """
    Build the dict of queue info to dump to the json file.

    :param `query_return`: The output of query_status
    :param `date`: A timestamp for us to mark when this status occurred
    :returns: A dictionary that's ready to dump to a json outfile
    """
    # Get the datetime so we can track different entries and initalize a new json entry
    json_to_dump = {date: {}}

    # Add info for each queue (name)
    for name, jobs, consumers in query_return:
        json_to_dump[date][name] = {"tasks": jobs, "consumers": consumers}

    return json_to_dump


def dump_handler(info_to_write: Union[Dict[str, List], List[Tuple[str, int, int]]], dump_file: str, queue_dump: bool = False):
    """
    Dump the information about a study to a file.

    :param `info_to_write`: If dumping statuses, this will be a dict of statuses to dump.
                            If dumping queue info, this will be a list of tuples representing queue information.
    :param `dump_file`: The name of the file we're going to write to
    :param `queue_dump`: If True, we're dumping queue information. Otherwise, we're dumping status information.
    """
    # Create a dumper object to help us write to dump_file
    dumper = Dumper(dump_file)

    # Get the correct file write mode
    if os.path.exists(dump_file):
        fmode = "a"
    else:
        fmode = "w"
    
    # Get a timestamp for this dump
    date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    write_type = 'Writing' if fmode == 'w' else 'Appending'
    LOG.info(f"{write_type} {'queue information' if queue_dump else 'statuses'} to {dump_file}...")

    # Handle different file types
    if dump_file.endswith(".csv"):
        # Build the lists of information/labels we'll need
        if queue_dump:
            dump_info = build_csv_queue_info(info_to_write, date)
        else:
            dump_info = build_csv_status(info_to_write, date)
    elif dump_file.endswith(".json"):
        # Build the dict of info to dump to the json file
        if queue_dump:
            dump_info = build_json_queue_info(info_to_write, date)
        else:
            dump_info = build_json_status(info_to_write, date)
    
    # Write the output
    dumper.write(dump_info, fmode)
    LOG.info(f"{write_type} complete.")


def status_list_to_dict(list_of_statuses: List[List[str]]) -> Dict[str, List]:
    """
    Convert a list of statuses (where each status is contained in it's own list) to a dictionary
    of statuses.
    :param `list_of_statuses`: A list of list of statuses
    :returns: A dict of statuses where each key is a column label and each val are the values in that column
    """
    labels = get_column_labels(list_of_statuses)
    status_dict = {k: [] for k in labels}
    for status_entry in list_of_statuses:
        for key, val in zip(labels, status_entry):
            status_dict[key].append(val)
    return status_dict


def _filter_by_status(status_dict: Dict[str, List], status_filters: List[str]):
    """
    Given a list of status filters, filter the dict of statuses by them.
    :param `status_dict`: A dict statuses that we're going to display.
    :param `status_filters`: A list of filters to apply to the dict of statuses
    """
    LOG.info(f"Filtering tasks by the following statuses: {status_filters}")
    # Create a list of tuples for each status entry
    status_list = list(zip(*status_dict.values()))

    # If the status of a task doesn't match the filters provided by the user, remove that entry
    for status_entry in status_list[:]:
        if status_entry[2] not in status_filters:
            status_list.remove(status_entry)
    if len(status_list) == 0:
        LOG.error(f"No tasks found matching the task status filters {status_filters}.")

    # Revert the status back to dict form and update the status dict with the changes
    status_dict.update(status_list_to_dict(status_list))

    # If updating the status dict resulted in no steps being found then we may need to manually reset the task queue and worker name entries
    if not status_dict["Step Name"]:
        if "Task Queue" in status_dict:
            status_dict["Task Queue"] = []
        if "Worker Name" in status_dict:
            status_dict["Worker Name"] = []


def _filter_by_max_tasks(status_dict: Dict[str, List], max_tasks: int):
    """
    Given a number representing the maximum amount of tasks to display, filter the dict of statuses
    to that number.
    :param `status_dict`: A dict statuses that we're going to display.
    :param `max_tasks`: An int representing the max number of tasks to display
    """
    LOG.info(f"Max tasks filter was provided. Displaying at most {max_tasks} tasks.")
    if max_tasks > len(status_dict["Step Name"]):
        max_tasks = len(status_dict["Step Name"])
    for key, val in status_dict.items():
        status_dict[key] = val[:max_tasks]


def get_requested_statuses(started_steps: List[str], tasks_per_step: Dict[str, int], workspace: str, args: "Namespace") -> List[List[str]]:
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
    requested_statuses = {}
    for sstep in started_steps:
        step_workspace = f"{workspace}/{sstep}"
        step_statuses = get_step_statuses(step_workspace, tasks_per_step[sstep])
        if not requested_statuses:
            requested_statuses.update(step_statuses)
        else:
            for key, val in step_statuses.items():
                requested_statuses[key].extend(val)

    # Filter the statuses we read in by task status if necessary
    if args.task_status:
        _filter_by_status(requested_statuses, args.task_status)

    # Get the number of tasks found with our filters
    num_tasks_found = 0
    if requested_statuses:
        num_tasks_found = len(requested_statuses['Step Name'])

    LOG.info(f"Found {num_tasks_found} tasks matching your filters.")

    # Limit the number of statuses to display if necessary
    if args.max_tasks:
        _filter_by_max_tasks(requested_statuses, args.max_tasks)

    return requested_statuses


def get_step_statuses(step_workspace: str, total_tasks: int, args: "Namespace" = None) -> Dict[str, List[str]]:
    """
    Given a step workspace and the total number of tasks for the step, read in all the statuses
    for the step and return them in a dict.

    :param `step_workspace`: The path to the step we're going to read statuses from
    :param `total_tasks`: The total number of tasks for the step
    :returns: A dict of statuses for the given step
    """
    from filelock import FileLock, Timeout  # pylint: disable=C0415
    step_statuses = []
    num_statuses_read = 0
    # Count number of tasks in each state
    for root, dirs, files in os.walk(step_workspace):
        if "MERLIN_STATUS" in files:
            # Read in the statuses for the tasks in this step
            status_file = f"{root}/MERLIN_STATUS"
            lock = FileLock(f"{root}/status.lock")
            try:
                with lock.acquire(timeout=10):
                    statuses_read = read_status(status_file).split("\n")
            except Timeout:
                LOG.warning(f"Timed out when trying to read status from {status_file}")
                statuses_read = []
            statuses_read.remove('')
            step_statuses.extend(statuses_read)
            num_statuses_read += len(statuses_read)
        # To help save time, exit this loop if we've read all the task statuses for this step
        if num_statuses_read == total_tasks:
            break

    # Move the statuses into a dict
    step_statuses = [status.split() for status in step_statuses]
    status_dict = status_list_to_dict(step_statuses)

    return status_dict


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
    from merlin.main import get_spec_with_expansion, get_merlin_spec_with_override, verify_dirpath  # pylint: disable=C0415

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
    from merlin.main import verify_dirpath, get_spec_with_expansion  # pylint: disable=C0415

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