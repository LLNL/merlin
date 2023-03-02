###############################################################################
# Copyright (c) 2022, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.9.1.
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
import os

from merlin import display
from merlin.config.configfile import CONFIG


def load_from_spec(args, filepath):
    """
    Get the desired workspace from the user and load up it's yaml spec
    for further processing.

    :param `args`: The namespace given by ArgumentParser with flag info (Namespace)
    :param `filepath`: The filepath to a spec given by the user (Str)

    :returns: The desired workspace to check the study for and a MerlinSpec
              object loaded in from the workspace's merlin_info subdirectory.
              (Tuple(Str, MerlinSpec))
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
        LOG.error(f"Could not find any potential studies.")
    if num_studies > 2:
        print(f"Found {num_studies-1} potential studies:")
        display.tabulate_info(potential_studies, headers=["Index", "Study Name"])
        prompt = "Which study would you like to view the status of? Use the index on the left: "
        index = -1
        while index < 1 or index > num_studies-1:
            try:
                user_input = input(prompt)
                index = int(user_input)
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


def load_from_workspace(workspace):
    """
    Create a MerlinSpec object based on the spec file in the workspace.

    :param `workspace`: A directory path to the existing merlin study output (Str)

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
    actual_spec = get_spec_with_expansion(spec_file)

    return actual_spec


def process_workers(spec, workers_provided, steps_to_check):
    """
    Modifies the list of steps to display status for based on
    the list of workers provided by the user.

    :param `spec`: MerlinSpec object for a study (MerlinSpec)
    :param `workers_provided`: A list of workers provided by the user (List)
    :param `steps_to_check`: A list of steps to view the status for (List)
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


def process_task_queue(spec, queues_provided, steps_to_check):
    """
    Modifies the list of steps to display status for based on
    the list of task queues provided by the user.

    :param `spec`: MerlinSpec object for a study (MerlinSpec)
    :param `queues_provided`: A list of task queues provided by the user (List)
    :param `steps_to_check`: A list of steps to view the status for (List)
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


def create_step_tracker(workspace, steps_to_check):
    """
    Creates a dictionary of started and unstarted steps that we
    will display the status for.

    :param `workspace`: The output directory for a study (str)
    :param `steps_to_check`: A list of steps to view the status of (List)
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


def get_steps_to_display(workspace, spec, steps, task_queues, workers):
    """
    Generates a list of steps to display the status for based on information
    provided to the merlin status command by the user.

    :param `workspace`: The output directory for a study (str)
    :param `spec`: MerlinSpec object for a study (MerlinSpec)
    :param `steps`: A list of steps to view the status of (List)
    :param `task_queues`: A list of task_queues to view the status of (List)
    :param `workers`: A list of workers to view the status of tasks they're running (List)
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