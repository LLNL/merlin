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
import logging
import os
from pathlib import Path
from typing import Dict, List, Tuple, Union

from filelock import Timeout

from merlin import display, router
from merlin.config.configfile import CONFIG
from merlin.utils import get_parent_dir, get_sibling_dirs, obtain_filelock, ws_time_to_td

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

    # The old merlin status command
    if args.queue_info:
        ret = router.query_status("celery", spec, args.steps)
        for name, jobs, consumers in ret:
            print(f"{name:30} - Workers: {consumers:10} - Queued Tasks: {jobs:10}")
        if args.csv is not None:
            router.dump_status(ret, args.csv)
        return

    # Get a dict of started and unstarted steps to display and a dict representing the number of tasks per step
    step_tracker = get_steps_to_display(workspace, spec, args.steps, args.task_queues, args.workers)
    # tasks_per_step = get_tasks_per_step(spec)
    tasks_per_step = spec.get_tasks_per_step()

    # Determine whether to display a low level or high level display
    low_lvl = True if args.task_queues or args.workers or ("all" not in args.steps) else False

    # Verify the filter args (if any)
    verify_filter_args(args)

    # Display the status
    display.display_status(workspace, tasks_per_step, step_tracker, low_lvl, args)


def verify_filter_args(args: "Argparse Namespace"):
    """
    Verify that our filters are all valid and able to be used.
    :param `args`: The CLI arguments provided via the user
    """
    # Ignore invalid use of filters
    if "all" in args.steps:
        if args.max_tasks:
            LOG.warning("Can only use the --max-tasks flag with the --steps flag. Ignoring --max-tasks...")
            args.max_tasks = None
        if args.task_status:
            LOG.warning("Can only use the --task-status flag with the --steps flag. Ignoring --task-status...")
            args.task_status = None

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


def load_from_spec(args: "Argparse Namespace", filepath: str) -> Tuple[str, "MerlinSpec"]:
    """
    Get the desired workspace from the user and load up it's yaml spec
    for further processing.

    :param `args`: The namespace given by ArgumentParser with flag info
    :param `filepath`: The filepath to a spec given by the user

    :returns: The workspace of the study we'll check the status for and a MerlinSpec
              object loaded in from the workspace's merlin_info subdirectory.
    """
    from merlin.main import verify_dirpath, get_spec_with_expansion

    # Get the spec and the output path for the spec
    spec_provided = get_spec_with_expansion(filepath)

    if spec_provided.output_path:
        study_output_dir = verify_dirpath(spec_provided.output_path)
    else:
        study_output_dir = verify_dirpath(".")

    # Build a list of potential study output directories
    study_output_subdirs = next(os.walk(study_output_dir))[1]
    potential_studies = []
    num_studies = 1
    for subdir in study_output_subdirs:
        if subdir.startswith(spec_provided.name):
            potential_studies.append((num_studies, subdir))
            num_studies += 1

    # Obtain the correct study that the user wants to view the status of
    # based on the list of potential studies we just built
    study_to_check = f"{study_output_dir}/"
    if num_studies == 1:
        raise ValueError("Could not find any potential studies.")
    if num_studies > 2:
        # Get the latest study
        if args.no_prompts:
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
            while index < 1 or index > num_studies-1:
                try:
                    index = int(input(prompt))
                    if index < 1 or index > num_studies-1:
                        raise ValueError
                except ValueError:
                    print(f"{display.ANSI_COLORS['RED']}Input must be an integer between 1 and {num_studies-1}.{display.ANSI_COLORS['RESET']}")
                    prompt = "Enter a different index: "
            study_to_check += potential_studies[index-1][1]
    else:
        study_to_check += potential_studies[0][1]

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
            print(f"{display.ANSI_COLORS['YELLOW']}Worker with name {wp} does not exist for this study.{display.ANSI_COLORS['RESET']}")
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
            print(f"{display.ANSI_COLORS['YELLOW']}Task queue with name {qp} does not exist for this study.{display.ANSI_COLORS['RESET']}")
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
        steps_to_check = existing_steps
    else:
        # This won't matter anymore since task_queues or workers is not None here
        if "all" in steps:
            steps = []

         # Build a list of steps to get the status for based on steps, task_queues, and workers flags
        steps_to_check = []

        # Loop through all steps provided by user and if they exist, add them to our list
        # of steps to get the status for
        for step in steps:
            if step not in existing_steps:
                LOG.warning(f"Could not find {step} in this study with name {spec.name}.")
            else:
                steps_to_check.append(step)

        # Add steps to start based on task queues and/or workers provided
        if task_queues:
            process_task_queue(spec, task_queues, steps_to_check)
        if workers:
            process_workers(spec, workers, steps_to_check)

        # Sort the steps to start by the order they show up in the study
        idx = 0
        for estep in existing_steps:
            if estep in steps_to_check:
                steps_to_check.remove(estep)
                steps_to_check.insert(idx, estep)
                idx += 1

    # Filter the steps to display status for by started/unstarted
    step_tracker = create_step_tracker(workspace, steps_to_check)

    return step_tracker


def status_file_exists(workspace: Union[Path, str]) -> bool:
    """
    Check if the status file in the workspace exists.

    :param `workspace`: The path to the workspace that we're looking for a status file in
    :returns: A boolean denoting whether the status file exists or not
    """
    # Convert workspace to a Path object and obtain a filelock object if necessary
    if isinstance(workspace, str):
        workspace = Path(workspace)
    lock = obtain_filelock(workspace, "status.lock")
    lock_file = workspace / "status.lock"

    # Lock the file and check if the status file exists
    try:
        with lock.acquire(timeout=5):
            status_file = workspace / "MERLIN_STATUS"
            result = status_file.exists()
    except Timeout:
        LOG.warning("Timed out while checking if the status file exists.")
    lock_file.unlink()

    return result

def write_status(workspace: Union[Path, str], status: str, timeout_message=None):
    """
    Locks the status file for writing and writes the task status to it.

    :param `workspace`: The path to the workspace that has the status file we'll write to
    :param `status`: The status we'll write to the status file
    :param `timeout_message`: An optional parameter for defining a custom timeout message
    """
    # Convert workspace to a Path object if necessary and obtain a filelock object for this status file
    if isinstance(workspace, str):
        workspace = Path(workspace)
    lock = obtain_filelock(workspace, "status.lock")
    lock_file = workspace / "status.lock"

    # Lock the file and write the task status to the status cache file
    try:
        with lock.acquire(timeout=5):
            status_file = workspace / "MERLIN_STATUS"
            status_file.write_text(status)
    except Timeout:
        if not timeout_message:
            timeout_message = "Timed out when writing status to file."
        LOG.warning(timeout_message)
    lock_file.unlink()


def read_status(workspace: Union[Path, str], timeout_message=None, delete_after_read=False) -> str:
    """
    Locks the status file for reading and returns its contents.

    :param `workspace`: The path to the workspace that has the status file we'll read from
    :param `timeout_message`: An optional parameter for defining a custom timeout message
    :param `delete_after_read`: An optional parameter to delete the status file after reading from it
    :returns: The contents of the status file located at `workspace`
    """
    # Convert workspace to a Path object if necessary and obtain a filelock object for this status file
    if isinstance(workspace, str):
        workspace = Path(workspace)
    lock = obtain_filelock(workspace, "status.lock")
    lock_file = workspace / "status.lock"

    # Initialize a variable to store the status(es) we'll read in
    status = ""

    # Lock the file and read in the status files contents
    try:
        with lock.acquire(timeout=5):
            status_file = workspace / "MERLIN_STATUS"
            status = status_file.read_text()
            # Delete the status file after reading it if necessary
            if delete_after_read:
                status_file.unlink()
    except Timeout:
        if not timeout_message:
            timeout_message = "Timed out when reading from status file."
        LOG.warning(timeout_message)
    except FileNotFoundError:
        LOG.warning(f"File not found: {status_file}. Deleting associated lock file.")
        # If the file wasn't found, we don't need the lock file anymore
        delete_after_read = True
    # Delete the lock file
    lock_file.unlink()

    return status


def get_statuses_to_condense(dirs_to_check: List[Path], total_num_siblings: int) -> List[List[str]]:
    """
    This function reads in statuses for condensing and checks if we're ready to condense at the same time.
    If we're ready to condense, this will return a list of statuses to condense. Otherwise, this will return
    an empty list.

    Conditions needed for condensing:
    1. Number of sibling directories that currently exist must be equal to the total number of sibling directories
       that will eventually be created
    2. Status files exist in all sibling directories
    3. (Only if this is a leaf directory) Every status file has a status that's not 'INITIALIZED' or 'RUNNING'

    :param `dirs_to_check`: A list of sibling directories to read/check status files of
    :param `total_num_siblings`: The total number of sibling directories that will eventually be created
    :returns: A list of statuses to condense if all conditions are met for condensing. Otherwise an empty list.
    """
    # Checking against condition 1
    if len(dirs_to_check) == total_num_siblings:
        all_statuses = []
        for directory in dirs_to_check:
            # Checking against condition 2
            if not status_file_exists(directory):
                return []
            
            # Read in the status and append it to the list of all statuses needed for condensing
            timeout_message = "Timed out when checking if condensing status files is needed."
            status = read_status(directory, timeout_message=timeout_message)
            all_statuses.append(status)

            # If we only have one status, this is a leaf (i.e. no condensing has been done yet) so check the state
            status = status.split("\n")
            status.remove("")
            if len(status) == 1:
                # Checking against condition 3
                status = status[0].split(" ")
                try:
                    # status[1] is the state of the task
                    if status[1] in ("INITIALIZED", "RUNNING"):
                        return []
                except IndexError:
                    return []

        # If we get here then all conditions are satisfied and we need to condense
        return all_statuses
    return []


def condense_status_files(workspace: str, top_lvl_dir: str, param_length: int, uses_params: bool, hierarchy_children: Dict[str, int] = None,):
    """
    Condense status files up the hierarchy (if necessary).

    :param `workspace`: The path to the task that's currently being worked on. Will serve as our starting point for condensing.
    :param `top_lvl_dir`: The path to the top level directory of the step we're condensing files for. Will serve as our end point
                          for condensing.
    :param `param_length`: The number of values in each global parameter
    :param `uses_params`: True if the step we're condensing for uses parameters. False, otherwise.
    :param `hierarchy_children`: A dict where the keys are the sample index paths and the values are the number of child directories
                                 in that path. Used for making sure the number of sibling directories that exist is correct before
                                 we start condensing. ONLY NEEDED FOR A STEP USING SAMPLES.
    """
    # Get the directory to stop our condensing at and initialize the current directory variable
    current_dir = Path(workspace)
    write_timeout_message = "Timed out when writing condensed status up to the parent status file."
    stop_condensing = False

    while str(current_dir) != top_lvl_dir and not stop_condensing:
        # Get the sibling directories (including the current dir) and the parent directory
        sibling_dirs = get_sibling_dirs(current_dir, include_path=True)
        parent_dir = get_parent_dir(current_dir)

        # Get the total number of children that the parent directory will eventually have
        if str(parent_dir) == top_lvl_dir and uses_params:
            total_num_siblings = param_length
        else:
            # This else statement will only get hit if the step we're condensing for uses samples
            for sample_path, total_num_siblings in hierarchy_children.items():
                if str(parent_dir).endswith(sample_path):
                    break
        
        # Lock the parent directory for condensing
        parent_lock = obtain_filelock(parent_dir, "condense.lock")
        try:
            with parent_lock.acquire(timeout=5):
                statuses_to_condense = get_statuses_to_condense(sibling_dirs, total_num_siblings)
                # If we have statuses to condense then condense them
                if statuses_to_condense:
                    # Delete the status files (and their associated lock files) we're about to condense
                    for directory in sibling_dirs:
                        status_file = directory / "MERLIN_STATUS"
                        status_lock = directory / "status.lock"
                        lock = obtain_filelock(directory, "status.lock")
                        try:
                            with lock.acquire(timeout=5):
                                status_file.unlink()
                        except Timeout:
                            LOG.warning(f"Timed out while unlinking {status_file}")
                        status_lock.unlink()
                    # Condense the statuses and then write it to a new status file in the parent directory
                    condensed_status = "".join(statuses_to_condense)
                    write_status(parent_dir, condensed_status, timeout_message=write_timeout_message)
                # If we don't have statuses to condense then we can stop looping
                else:
                    stop_condensing = True
        except Timeout:
            LOG.warning("Timed out when checking if condensing is needed")
        # Remove the condense lock to free up file space
        parent_lock_file = parent_dir / "condense.lock"
        parent_lock_file.unlink()
        
        # Move the current dir up a level for the next iteration
        current_dir = parent_dir
