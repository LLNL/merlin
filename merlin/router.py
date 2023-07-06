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

"""
This module routes actions from the Merlin CLI to the appropriate tasking
logic.

This module is intended to help keep the task management layer (i.e., Celery)
decoupled from the logic the tasks are running.
"""
import logging
import os
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from merlin.common.dumper import dump_handler
from merlin.study.celeryadapter import (
    celerize_queues,
    create_celery_config,
    get_queues,
    get_workers_from_app,
    purge_celery_tasks,
    query_celery_queues,
    query_celery_workers,
    run_celery,
    start_celery_workers,
    stop_celery_workers,
)


try:
    from importlib import resources
except ImportError:
    import importlib_resources as resources


LOG = logging.getLogger(__name__)

# TODO go through this file and find a way to make a common return format to main.py
# Also, if that doesn't fix them, look into the pylint errors that have been disabled
# and try to resolve them


def run_task_server(study, run_mode=None):
    """
    Creates the task server interface for communicating the tasks.

    :param `study`: The MerlinStudy object
    :param `run_mode`: The type of run mode, e.g. local, batch
    """
    if study.expanded_spec.merlin["resources"]["task_server"] == "celery":
        run_celery(study, run_mode)
    else:
        LOG.error("Celery is not specified as the task server!")


def launch_workers(spec, steps, worker_args="", disable_logs=False, just_return_command=False):
    """
    Launches workers for the specified study.

    :param `specs`: Tuple of (YAMLSpecification, MerlinSpec)
    :param `steps`: The steps in the spec to tie the workers to
    :param `worker_args`: Optional arguments for the workers
    :param `disable_logs`: Boolean flag to disable the worker logs from celery
    :param `just_return_command`: Don't execute, just return the command
    """
    if spec.merlin["resources"]["task_server"] == "celery":  # pylint: disable=R1705
        # Start workers
        cproc = start_celery_workers(spec, steps, worker_args, disable_logs, just_return_command)
        return cproc
    else:
        LOG.error("Celery is not specified as the task server!")
        return "No workers started"


def purge_tasks(task_server, spec, force, steps):
    """
    Purges all tasks.

    :param `task_server`: The task server from which to purge tasks.
    :param `spec`: A MerlinSpec object
    :param `force`: Purge without asking for confirmation
    :param `steps`: Space-separated list of stepnames defining queues to purge,
        default is all steps
    """
    LOG.info(f"Purging queues for steps = {steps}")

    if task_server == "celery":  # pylint: disable=R1705
        queues = spec.make_queue_string(steps)
        # Purge tasks
        return purge_celery_tasks(queues, force)
    else:
        LOG.error("Celery is not specified as the task server!")
        return -1


def build_csv_queue_info(query_return: List[Tuple[str, int, int]], date: str) -> Dict[str, List]:
    """
    Build the lists of column labels and queue info to write to the csv file.

    :param `query_return`: The output of query_queues
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

    :param `query_return`: The output of query_queues
    :param `date`: A timestamp for us to mark when this status occurred
    :returns: A dictionary that's ready to dump to a json outfile
    """
    # Get the datetime so we can track different entries and initalize a new json entry
    json_to_dump = {date: {}}

    # Add info for each queue (name)
    for name, jobs, consumers in query_return:
        json_to_dump[date][name] = {"tasks": jobs, "consumers": consumers}

    return json_to_dump


def dump_queue_info(query_return: List[Tuple[str, int, int]], dump_file: str):
    """
    Format the information we're going to dump in a way that the Dumper class can
    understand and add a timestamp to the info.

    :param `query_return`: The output of query_queues
    :param `dump_file`: The filepath of the file we'll dump queue info to
    """
    # Get a timestamp for this dump
    date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Handle different file types
    if dump_file.endswith(".csv"):
        # Build the lists of information/labels we'll need
        dump_info = build_csv_queue_info(query_return, date)
    elif dump_file.endswith(".json"):
        # Build the dict of info to dump to the json file
        dump_info = build_json_queue_info(query_return, date)

    # Dump the information
    dump_handler(dump_file, dump_info)


def query_queues(
    task_server: str,
    spec: "MerlinSpec",  # noqa: F821
    steps: List[str],
    specific_queues: List[str],
    verbose: Optional[bool] = True,
):
    """
    Queries status of queues.

    :param `task_server`: The task server from which to purge tasks.
    :param `spec`: A MerlinSpec object or None
    :param `steps`: Spaced-separated list of stepnames to query. Default is all
    :param `specific_queues`: A list of queue names to query or None
    :param `verbose`: A bool to determine whether to output log statements or not
    """
    from merlin.celery import app  # pylint: disable=C0415

    if task_server == "celery":  # pylint: disable=R1705
        queues = set()
        # If the user provided a spec file, get the queues from that spec
        if spec:
            if verbose:
                LOG.info(f"Querying queues for steps = {steps}")
            queues = set(spec.get_queue_list(steps))

        # If the user provided specific queues, search for those
        if specific_queues:
            if verbose:
                LOG.info(f"Filtering queues to query by these specific queues: {specific_queues}")
            # Add the queue tag from celery if necessary
            celerize_queues(specific_queues)
            # Remove any potential duplicates and create a new set to store the queues we'll want to check
            specific_queues = set(specific_queues)
            specific_queues_to_check = set()

            for specific_queue in specific_queues:
                # If the user provided the --specification flag too then we'll need to check that this
                # specific queue exists in the spec that they provided
                add_specific_queue = True
                if spec and specific_queue not in queues:
                    LOG.warning(
                        f"Either couldn't find {specific_queue} in the existing queues for the spec file provided or this queue doesn't go with the steps provided with the --steps option. Ignoring this queue."
                    )
                    add_specific_queue = False

                # Add the full queue name to the set of queues we'll check
                if add_specific_queue:
                    specific_queues_to_check.add(specific_queue)
            queues = specific_queues_to_check

        # Default behavior with no options provided; display active queues
        if not spec and not specific_queues:
            if verbose:
                LOG.info("Querying active queues")
            existing_queues, _ = get_queues(app)

            # Check if there's any active queues currently
            if len(existing_queues) == 0:
                LOG.warning("No active queues found. Are your workers running yet?")
                return []

            # Set the queues we're going to check to be all existing queues by default
            queues = existing_queues.keys()

        # Query the queues
        return query_celery_queues(queues)
    else:
        LOG.error("Celery is not specified as the task server!")
        return []


def query_workers(task_server, spec_worker_names, queues, workers_regex):
    """
    Gets info from workers.

    :param `task_server`: The task server to query.
    """
    LOG.info("Searching for workers...")

    if task_server == "celery":
        query_celery_workers(spec_worker_names, queues, workers_regex)
    else:
        LOG.error("Celery is not specified as the task server!")


def get_workers(task_server):
    """Get all workers.

    :param `task_server`: The task server to query.
    :return: A list of all connected workers
    :rtype: list
    """
    if task_server == "celery":  # pylint: disable=R1705
        return get_workers_from_app()
    else:
        LOG.error("Celery is not specified as the task server!")
        return []


def stop_workers(task_server, spec_worker_names, queues, workers_regex):
    """
    Stops workers.

    :param `task_server`: The task server from which to stop workers.
    :param `spec_worker_names`: Worker names to stop, drawn from a spec.
    :param `queues`     : The queues to stop
    :param `workers_regex`    : Regex for workers to stop
    """
    LOG.info("Stopping workers...")

    if task_server == "celery":  # pylint: disable=R1705
        # Stop workers
        stop_celery_workers(queues, spec_worker_names, workers_regex)
    else:
        LOG.error("Celery is not specified as the task server!")


def create_config(task_server: str, config_dir: str, broker: str, test: str) -> None:
    """
    Create a config for the given task server.

    :param [str] `task_server`: The task server from which to stop workers.
    :param [str] `config_dir`: Optional directory to install the config.
    :param [str] `broker`: string indicated the broker, used to check for redis.
    :param [str] `test`: string indicating if the app.yaml is used for testing.
    """
    if test:
        LOG.info("Creating test config ...")
    else:
        LOG.info("Creating config ...")

    if not os.path.isdir(config_dir):
        os.makedirs(config_dir)

    if task_server == "celery":
        config_file = "app.yaml"
        data_config_file = "app.yaml"
        if broker == "redis":
            data_config_file = "app_redis.yaml"
        elif test:
            data_config_file = "app_test.yaml"
        with resources.path("merlin.data.celery", data_config_file) as data_file:
            create_celery_config(config_dir, config_file, data_file)
    else:
        LOG.error("Only celery can be configured currently.")


def check_merlin_status(args, spec):
    """
    Function to check merlin workers and queues to keep
    the allocation alive

    :param `args`: parsed CLI arguments
    :param `spec`: the parsed spec.yaml
    """
    queue_status = query_queues(args.task_server, spec, args.steps, None, verbose=False)

    total_jobs = 0
    total_consumers = 0
    for _, jobs, consumers in queue_status:
        total_jobs += jobs
        total_consumers += consumers

    if total_jobs > 0 and total_consumers == 0:
        # Determine if any of the workers are on this allocation
        worker_names = spec.get_worker_names()

        # Loop until workers are detected.
        count = 0
        max_count = 10
        while count < max_count:
            # This list will include strings comprised of the worker name with the hostname e.g. worker_name@host.
            worker_status = get_workers(args.task_server)
            LOG.info(f"Monitor: checking for workers, running workers = {worker_status} ...")

            check = any(any(iwn in iws for iws in worker_status) for iwn in worker_names)
            if check:
                break

            count += 1
            time.sleep(args.sleep)

        if count == max_count:
            LOG.error("Monitor: no workers available to process the non-empty queue")
            total_jobs = 0

    return total_jobs
