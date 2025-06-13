##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module routes actions from the Merlin CLI to the appropriate tasking
logic.

This module is intended to help keep the task management layer (i.e., Celery)
decoupled from the logic the tasks are running.
"""
import logging
import time
from argparse import Namespace
from typing import Dict, List, Tuple

from merlin.exceptions import NoWorkersException
from merlin.spec.specification import MerlinSpec
from merlin.study.celeryadapter import (
    build_set_of_queues,
    check_celery_workers_processing,
    dump_celery_queue_info,
    get_active_celery_queues,
    get_workers_from_app,
    purge_celery_tasks,
    query_celery_queues,
    query_celery_workers,
    run_celery,
    start_celery_workers,
    stop_celery_workers,
)
from merlin.study.study import MerlinStudy


LOG = logging.getLogger(__name__)

# TODO go through this file and find a way to make a common return format to main.py
# Also, if that doesn't fix them, look into the pylint errors that have been disabled
# and try to resolve them


def run_task_server(study: MerlinStudy, run_mode: str = None):
    """
    Creates the task server interface for managing task communications.

    This function determines which server to send tasks to. It checks if
    Celery is set as the task server; if not, it logs an error message.
    The run mode can be specified to determine how tasks should be executed.

    Args:
        study (study.study.MerlinStudy): The study object representing the
            current experiment setup, containing configuration details for
            the task server.
        run_mode: The type of run mode to use for task execution. This can
            include options such as 'local' or 'batch'.
    """
    if study.expanded_spec.merlin["resources"]["task_server"] == "celery":
        run_celery(study, run_mode)
    else:
        LOG.error("Celery is not specified as the task server!")


def launch_workers(
    spec: MerlinSpec,
    steps: List[str],
    worker_args: str = "",
    disable_logs: bool = False,
    just_return_command: bool = False,
) -> str:
    """
    Launches workers for the specified study based on the provided
    specification and steps.

    This function checks if Celery is configured as the task server
    and initiates the specified workers accordingly. It provides options
    for additional worker arguments, logging control, and command-only
    execution without launching the workers.

    Args:
        spec (spec.specification.MerlinSpec): Specification details
            necessary for launching the workers.
        steps: The specific steps in the specification that the workers
            will be associated with.
        worker_args: Additional arguments to be passed to the workers.
            Defaults to an empty string.
        disable_logs: Flag to disable logging during worker execution.
            Defaults to False.
        just_return_command: If True, the function will not execute the
            command but will return it instead. Defaults to False.

    Returns:
        A string of the worker launch command(s).
    """
    if spec.merlin["resources"]["task_server"] == "celery":  # pylint: disable=R1705
        # Start workers
        cproc = start_celery_workers(spec, steps, worker_args, disable_logs, just_return_command)
        return cproc
    else:
        LOG.error("Celery is not specified as the task server!")
        return "No workers started"


def purge_tasks(task_server: str, spec: MerlinSpec, force: bool, steps: List[str]) -> int:
    """
    Purges all tasks from the specified task server.

    This function removes tasks from the designated queues associated
    with the specified steps. It operates without confirmation if
    the `force` parameter is set to True. The function logs the
    steps being purged and checks if Celery is the configured task
    server before proceeding.

    Args:
        task_server: The task server from which to purge tasks.
        spec (spec.specification.MerlinSpec): A
            [`MerlinSpec`][spec.specification.MerlinSpec] object
            containing the configuration needed to generate queue
            specifications.
        force: If True, purge the tasks without any confirmation prompt.
        steps: A space-separated list of step names that define
            which queues to purge. If not specified, defaults to purging
            all steps.

    Returns:
        The result of the purge operation; -1 if the task server is not
            supported (i.e., not Celery).
    """
    LOG.info(f"Purging queues for steps = {steps}")

    if task_server == "celery":  # pylint: disable=R1705
        queues = spec.make_queue_string(steps)
        # Purge tasks
        return purge_celery_tasks(queues, force)
    else:
        LOG.error("Celery is not specified as the task server!")
        return -1


def dump_queue_info(task_server: str, query_return: List[Tuple[str, int, int]], dump_file: str):
    """
    Formats and dumps queue information for the specified task server.

    This function prepares the queue data returned from the queue
    query and formats it in a way that the [`Dumper`][common.dumper.Dumper]
    class can process. It also adds a timestamp to the information before
    dumping it to the specified file.

    Args:
        task_server: The task server from which to query queues.
        query_return: The output from the [`query_queues`][router.query_queues]
            function, containing tuples of queue information.
        dump_file: The filepath where the queue information will be dumped.
    """
    if task_server == "celery":
        dump_celery_queue_info(query_return, dump_file)
    else:
        LOG.error("Celery is not specified as the task server!")


def query_queues(
    task_server: str,
    spec: MerlinSpec,
    steps: List[str],
    specific_queues: List[str],
    verbose: bool = True,
) -> Dict[str, Dict[str, int]]:
    """
    Queries the status of queues from the specified task server.

    This function checks the status of queues tied to a given task server,
    building a list of queues based on the provided steps and specific queue
    names. It supports querying Celery task servers and returns the results
    in a structured format. Logging behavior can be controlled with the verbose
    parameter.

    Args:
        task_server: The task server from which to query queues.
        spec (spec.specification.MerlinSpec): A
            [`MerlinSpec`][spec.specification.MerlinSpec] object used to define
            the configuration of queues. Can also be None.
        steps: A space-separated list of step names to query. Default is to query
            all available steps if this is empty.
        specific_queues: A list of specific queue names to query. Can be empty or
            None to query all relevant queues.
        verbose: If True, enables logging of query operations. Defaults to True.

    Returns:
        A dictionary where the keys are queue names and the values are dictionaries
            containing the number of workers (consumers) and tasks (jobs) attached
            to each queue.
    """
    if task_server == "celery":  # pylint: disable=R1705
        # Build a set of queues to query and query them
        queues = build_set_of_queues(spec, steps, specific_queues, verbose=verbose)
        return query_celery_queues(queues)
    else:
        LOG.error("Celery is not specified as the task server!")
        return {}


def query_workers(task_server: str, spec_worker_names: List[str], queues: List[str], workers_regex: str):
    """
    Retrieves information from workers associated with the specified task server.

    Args:
        task_server: The task server to query.
        spec_worker_names: A list of specific worker names to query.
        queues: A list of queues to search for associated workers.
        workers_regex: A regex pattern used to filter worker names during the query.
    """
    LOG.info("Searching for workers...")

    if task_server == "celery":
        query_celery_workers(spec_worker_names, queues, workers_regex)
    else:
        LOG.error("Celery is not specified as the task server!")


def get_workers(task_server: str) -> List[str]:
    """
    This function queries the designated task server to obtain a list of all
    workers that are currently connected.

    Args:
        task_server: The task server to query.

    Returns:
        A list of all connected workers. If the task server is not supported,
            an empty list is returned.
    """
    if task_server == "celery":  # pylint: disable=R1705
        return get_workers_from_app()
    else:
        LOG.error("Celery is not specified as the task server!")
        return []


def stop_workers(task_server: str, spec_worker_names: List[str], queues: List[str], workers_regex: str):
    """
    This function sends a command to stop workers that match the specified
    criteria from the designated task server.

    Args:
        task_server: The task server from which to stop workers.
        spec_worker_names: A list of worker names to stop, as defined
            in a specification.
        queues: A list of queues from which to stop associated workers.
        workers_regex: A regex pattern used to filter the workers to stop.
    """
    LOG.info("Stopping workers...")

    if task_server == "celery":  # pylint: disable=R1705
        # Stop workers
        stop_celery_workers(queues, spec_worker_names, workers_regex)
    else:
        LOG.error("Celery is not specified as the task server!")


# TODO in Merlin 1.14 delete all of the below functions since we're deprecating the old version of the monitor
# and a lot of this stuff is in the new monitor classes


def get_active_queues(task_server: str) -> Dict[str, List[str]]:
    """
    Retrieve a dictionary of active queues and their associated workers for the specified task server.

    This function queries the given task server for its active queues and gathers
    information about which workers are currently monitoring these queues. It supports
    the 'celery' task server and returns a structured dictionary containing the queue
    names as keys and lists of worker names as values.

    Args:
        task_server: The task server to query for active queues.

    Returns:
        A dictionary where:\n
            - The keys are the names of the active queues.
            - The values are lists of worker names that are currently attached to those queues.
    """
    active_queues = {}

    if task_server == "celery":
        from merlin.celery import app  # pylint: disable=C0415

        active_queues, _ = get_active_celery_queues(app)
    else:
        LOG.error("Only celery can be configured currently.")

    return active_queues


def wait_for_workers(sleep: int, task_server: str, spec: MerlinSpec):  # noqa
    """
    Wait for workers to start up by checking their status at regular intervals.

    This function monitors the specified task server for the startup of worker processes.
    It checks for the existence of the expected workers up to 10 times, sleeping for a
    specified number of seconds between each check. If no workers are detected after
    the maximum number of attempts, it raises an error to terminate the monitoring
    process, indicating a potential issue with the task server.

    Args:
        sleep: The number of seconds to pause between each check for worker status.
        task_server: The task server from which to query for worker status.
        spec (spec.specification.MerlinSpec): An instance of the
            [`MerlinSpec`][spec.specification.MerlinSpec] class that contains the
            specification for the workers being monitored.

    Raises:
        NoWorkersException: If no workers are detected after the maximum number of checks.
    """
    # Get the names of the workers that we're looking for
    worker_names = spec.get_worker_names()
    LOG.info(f"Checking for the following workers: {worker_names}")

    # Loop until workers are detected
    count = 0
    max_count = 10
    while count < max_count:
        # This list will include strings comprised of the worker name with the hostname e.g. worker_name@host.
        worker_status = get_workers(task_server)
        LOG.info(f"Monitor: checking for workers, running workers = {worker_status} ...")

        # Check to see if any of the workers we're looking for in 'worker_names' have started
        check = any(any(iwn in iws for iws in worker_status) for iwn in worker_names)
        if check:
            break

        # Increment count and sleep until the next check
        count += 1
        time.sleep(sleep)

    # If no workers were started in time, raise an exception to stop the monitor
    if count == max_count:
        raise NoWorkersException("Monitor: no workers available to process the non-empty queue")


def check_workers_processing(queues_in_spec: List[str], task_server: str) -> bool:
    """
    Check if any workers are still processing tasks by querying the task server.

    Args:
        queues_in_spec: A list of queue names to check for active tasks.
        task_server: The task server from which to query the processing status.

    Returns:
        True if workers are still processing tasks, False otherwise.
    """
    result = False

    if task_server == "celery":
        from merlin.celery import app  # pylint: disable=import-outside-toplevel

        result = check_celery_workers_processing(queues_in_spec, app)
    else:
        LOG.error("Celery is not specified as the task server!")

    return result


def check_merlin_status(args: Namespace, spec: MerlinSpec) -> bool:
    """
    Function to check Merlin workers and queues to keep the allocation alive.

    This function monitors the status of workers and jobs within the specified task server
    and the provided Merlin specification. It checks for active tasks and workers, ensuring
    that the allocation remains valid.

    Args:
        args: Parsed command-line interface arguments, including task server
            specifications and sleep duration.
        spec (spec.specification.MerlinSpec): The parsed spec.yaml as a
            [`MerlinSpec`][spec.specification.MerlinSpec] object, containing queue
            and worker definitions.

    Returns:
        True if there are still tasks being processed, False otherwise.
    """
    # Initialize the variable to track if there are still active tasks
    active_tasks = False

    # Get info about jobs and workers in our spec from celery
    queue_status = query_queues(args.task_server, spec, args.steps, None, verbose=False)
    LOG.debug(f"Monitor: queue_status: {queue_status}")

    # Count the number of jobs that are active
    # (Adding up the number of consumers in the same way is inaccurate so we won't do that)
    total_jobs = 0
    for queue_info in queue_status.values():
        total_jobs += queue_info["jobs"]

    # Get the queues defined in the spec
    queues_in_spec = spec.get_queue_list(["all"] if args.steps is None else args.steps)
    LOG.debug(f"Monitor: queues_in_spec: {queues_in_spec}")

    # Get the active queues and the workers that are watching them
    active_queues = get_active_queues(args.task_server)
    LOG.debug(f"Monitor: active_queues: {active_queues}")

    # Count the number of workers that are active
    consumers = set()
    for active_queue, workers_on_queue in active_queues.items():
        if active_queue in queues_in_spec:
            consumers |= set(workers_on_queue)
    LOG.debug(f"Monitor: consumers found: {consumers}")
    total_consumers = len(consumers)

    LOG.info(f"Monitor: found {total_jobs} jobs in queues and {total_consumers} workers alive")

    # If there are no workers, wait for the workers to start
    if total_consumers == 0:
        wait_for_workers(args.sleep, args.task_server, spec=spec)

    # If we're here, workers have started and jobs should be queued
    if total_jobs > 0:
        active_tasks = True
    # If there are no jobs left, see if any workers are still processing them
    elif total_jobs == 0:
        active_tasks = check_workers_processing(queues_in_spec, args.task_server)

    LOG.debug(f"Monitor: active_tasks: {active_tasks}")
    return active_tasks
