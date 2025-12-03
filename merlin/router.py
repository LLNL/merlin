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
from typing import Dict, List, Tuple

from merlin.spec.specification import MerlinSpec
from merlin.study.celeryadapter import (
    build_set_of_queues,
    dump_celery_queue_info,
    purge_celery_tasks,
    query_celery_queues,
    run_celery,
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
