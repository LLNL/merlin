##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""
This module provides an adapter to the Celery Distributed Task Queue.
"""
import logging
import os
import socket
import subprocess
import time
from contextlib import suppress
from datetime import datetime
from types import SimpleNamespace
from typing import Dict, List, Set, Tuple

from amqp.exceptions import ChannelError
from celery import Celery
from tabulate import tabulate

from merlin.common.dumper import dump_handler
from merlin.config import Config
from merlin.spec.specification import MerlinSpec
from merlin.study.batch import batch_check_parallel, batch_worker_launch
from merlin.study.study import MerlinStudy
from merlin.utils import apply_list_of_regex, check_machines, get_procs, get_yaml_var, is_running


LOG = logging.getLogger(__name__)

# TODO figure out a better way to handle the import of celery app and CONFIG


def run_celery(study: MerlinStudy, run_mode: str = None):
    """
    Run the given [`MerlinStudy`][study.study.MerlinStudy] object with optional
    Celery configuration.

    This function executes the provided [`MerlinStudy`][study.study.MerlinStudy]
    object. If the `run_mode` is set to "local", it configures Celery to run in
    local mode (without utilizing workers). Otherwise, it connects to the Celery
    server to queue tasks.

    Args:
        study (study.study.MerlinStudy): The study object to be executed.
        run_mode: The mode in which to run the study. If set to "local",
            Celery runs locally.
    """
    # Only import celery stuff if we want celery in charge
    # Pylint complains about circular import between merlin.common.tasks -> merlin.router -> merlin.study.celeryadapter
    # For now I think this is still the best way to do this so we'll ignore it
    from merlin.celery import app  # pylint: disable=C0415
    from merlin.common.tasks import queue_merlin_study  # pylint: disable=C0415, R0401

    adapter_config = study.get_adapter_config(override_type="local")

    if run_mode == "local":
        app.conf.task_always_eager = True
        app.conf.task_eager_propogates = True
    else:
        # Check for server
        app.connection().connect()

    # Send the tasks to the server
    queue_merlin_study(study, adapter_config)


def get_running_queues(celery_app_name: str, test_mode: bool = False) -> List[str]:
    """
    Check for running Celery workers and retrieve their associated queues.

    This function inspects currently running processes to identify active
    Celery workers. It extracts queue names from the `-Q` tag in the
    command line of the worker processes. The returned list contains
    only unique Celery queue names. This function must be executed
    on the allocation where the workers are running.

    Note:
        Unlike [`get_active_celery_queues`][study.celeryadapter.get_active_celery_queues],
        this function does _not_ go through the application's server.

    Args:
        celery_app_name: The name of the Celery app (typically "merlin"
            unless in test mode).
        test_mode: If True, the function runs in test mode.

    Returns:
        A unique list of Celery queue names with workers attached to them.
    """
    running_queues = []

    if not is_running(f"{celery_app_name} worker"):
        return running_queues

    proc_name = "celery" if not test_mode else "sh"
    procs = get_procs(proc_name)
    for _, lcmd in procs:
        lcmd = list(filter(None, lcmd))
        cmdline = " ".join(lcmd)
        if "-Q" in cmdline:
            if test_mode:
                echo_cmd = lcmd.pop(2)
                lcmd.extend(echo_cmd.split())
            running_queues.extend(lcmd[lcmd.index("-Q") + 1].split(","))

    running_queues = list(set(running_queues))

    return running_queues


def get_active_celery_queues(app: Celery) -> Tuple[Dict[str, List[str]], List[str]]:
    """
    Retrieve all active queues and their associated workers for a Celery application.

    This function queries the application's server to obtain a comprehensive
    view of active queues and the workers connected to them. It returns a
    dictionary where each key is a queue name and the value is a list of
    workers attached to that queue. Additionally, it provides a list of all
    active workers in the application.

    Note:
        Unlike [`get_running_queues`][study.celeryadapter.get_running_queues],
        this function goes through the application's server.

    Args:
        app: The Celery application instance.

    Returns:
        A tuple containing:\n
            - A dictionary mapping queue names to lists of workers connected to them.
            - A list of all active workers in the application.

    Example:
        ```python
        from merlin.celery import app
        queues, workers = get_active_celery_queues(app)
        queue_names = list(queues)
        workers_on_q0 = queues[queue_names[0]]
        workers_not_on_q0 = [worker for worker in workers if worker not in workers_on_q0]
        ```
    """
    i = app.control.inspect()
    active_workers = i.active_queues()
    if active_workers is None:
        active_workers = {}
    queues = {}
    for worker in active_workers:
        for my_queue in active_workers[worker]:
            try:
                queues[my_queue["name"]].append(worker)
            except KeyError:
                queues[my_queue["name"]] = [worker]
    return queues, [*active_workers]


def get_active_workers(app: Celery) -> Dict[str, List[str]]:
    """
    Retrieve a mapping of active workers to their associated queues for a Celery application.

    This function serves as the inverse of
    [`get_active_celery_queues()`][study.celeryadapter.get_active_celery_queues]. It constructs
    a dictionary where each key is a worker's name and the corresponding value is a
    list of queues that the worker is connected to. This allows for easy identification
    of which queues are being handled by each worker.

    Args:
        app: The Celery application instance.

    Returns:
        A dictionary mapping active worker names to lists of queue names they are
            attached to. If no active workers are found, an empty dictionary is returned.
    """
    # Get the information we need from celery
    i = app.control.inspect()
    active_workers = i.active_queues()
    if active_workers is None:
        active_workers = {}

    # Build the mapping dictionary
    worker_queue_map = {}
    for worker, queues in active_workers.items():
        for queue in queues:
            if worker in worker_queue_map:
                worker_queue_map[worker].append(queue["name"])
            else:
                worker_queue_map[worker] = [queue["name"]]

    return worker_queue_map


def celerize_queues(queues: List[str], config: SimpleNamespace = None):
    """
    Prepend a queue tag to each queue in the provided list to conform to Celery's
    queue naming requirements.

    This function modifies the input list of queues by adding a specified queue tag
    from the configuration. If no configuration is provided, it defaults to using
    the global configuration settings.

    Args:
        queues: A list of queue names that need the queue tag prepended.
        config: A SimpleNamespace of configuration settings. If not provided, the
            function will use the default configuration.
    """
    if config is None:
        from merlin.config.configfile import CONFIG as config  # pylint: disable=C0415

    for i, queue in enumerate(queues):
        queues[i] = f"{config.celery.queue_tag}{queue}"


def _build_output_table(worker_list: List[str], output_table: List[Tuple[str, str]]):
    """
    Construct an output table for displaying the status of workers and their associated queues.

    This helper function populates the provided output table with entries for each worker
    in the given worker list. It retrieves the mapping of active workers to their queues
    and formats the data accordingly.

    Args:
        worker_list: A list of worker names to be included in the output table.
        output_table: A list of tuples where each entry will be of the form
            (worker name, associated queues).
    """
    from merlin.celery import app  # pylint: disable=C0415

    # Get a mapping between workers and the queues they're watching
    worker_queue_map = get_active_workers(app)

    # Loop through the list of workers and add an entry in the table
    # of the form (worker name, queues attached to this worker)
    for worker in worker_list:
        if "celery@" not in worker:
            worker = f"celery@{worker}"
        output_table.append((worker, ", ".join(worker_queue_map[worker])))


def query_celery_workers(spec_worker_names: List[str], queues: List[str], workers_regex: List[str]):
    """
    Query and filter existing Celery workers based on specified criteria,
    and print a table of the workers along with their associated queues.

    This function retrieves the list of active Celery workers and filters them
    according to the provided specifications, including worker names from a
    spec file, specific queues, and regular expressions for worker names.
    It then constructs and displays a table of the matching workers and their
    associated queues.

    Args:
        spec_worker_names: A list of worker names defined in a spec file
            to filter the workers.
        queues: A list of queues to filter the workers by.
        workers_regex: A list of regular expressions to filter the worker names.
    """
    from merlin.celery import app  # pylint: disable=C0415

    # Ping all workers and grab which ones are running
    workers = get_workers_from_app()
    if not workers:
        LOG.warning("No workers found!")
        return

    # Remove prepended celery tag while we filter
    workers = [worker.replace("celery@", "") for worker in workers]
    workers_to_query = []

    # --queues flag
    if queues:
        # Get a mapping between queues and the workers watching them
        queue_worker_map, _ = get_active_celery_queues(app)
        # Remove duplicates and prepend the celery queue tag to all queues
        queues = list(set(queues))
        celerize_queues(queues)
        # Add the workers associated to each queue to the list of workers we're
        # going to query
        for queue in queues:
            try:
                workers_to_query.extend(queue_worker_map[queue])
            except KeyError:
                LOG.warning(f"No workers connected to {queue}.")

    # --spec flag
    if spec_worker_names:
        apply_list_of_regex(spec_worker_names, workers, workers_to_query)

    # --workers flag
    if workers_regex:
        apply_list_of_regex(workers_regex, workers, workers_to_query)

    # Remove any potential duplicates
    workers = set(workers)
    workers_to_query = set(workers_to_query)

    # If there were filters and nothing was found then we can't display a table
    if (queues or spec_worker_names or workers_regex) and not workers_to_query:
        LOG.warning("No workers found that match your filters.")
        return

    # Build the output table based on our filters
    table = []
    if workers_to_query:
        _build_output_table(workers_to_query, table)
    else:
        _build_output_table(workers, table)

    # Display the output table
    LOG.info("Found these connected workers:")
    print(tabulate(table, headers=["Workers", "Queues"]))
    print()


def build_csv_queue_info(query_return: List[Tuple[str, int, int]], date: str) -> Dict[str, List]:
    """
    Construct a dictionary containing queue information and column labels
    for writing to a CSV file.

    This function processes the output from the [`query_queues`][router.query_queues]
    function and organizes the data into a format suitable for CSV export. It includes
    a timestamp to indicate when the status was recorded.

    Args:
        query_return: The output from the [`query_queues`][router.query_queues] function,
            containing queue names and their associated statistics.
        date: A timestamp indicating when the queue status was recorded.

    Returns:
        A dictionary where keys are column labels and values are lists containing the
            corresponding queue information, formatted for CSV output.
    """
    # Build the list of labels if necessary
    csv_to_dump = {"time": [date]}
    for queue_name, queue_stats in query_return.items():
        csv_to_dump[f"{queue_name}:tasks"] = [str(queue_stats["jobs"])]
        csv_to_dump[f"{queue_name}:consumers"] = [str(queue_stats["consumers"])]

    return csv_to_dump


def build_json_queue_info(query_return: List[Tuple[str, int, int]], date: str) -> Dict:
    """
    Construct a dictionary containing queue information for JSON export.

    This function processes the output from the [`query_queues`][router.query_queues]
    function and organizes the data into a structured format suitable for JSON
    serialization. It includes a timestamp to indicate when the queue status was
    recorded.

    Args:
        query_return: The output from the [`query_queues`][router.query_queues]
            function, containing queue names and their associated statistics.
        date: A timestamp indicating when the queue status was recorded.

    Returns:
        A dictionary structured for JSON output, where the keys are timestamps
            and the values are dictionaries containing queue names and their
            corresponding statistics (tasks and consumers).
    """
    # Get the datetime so we can track different entries and initalize a new json entry
    json_to_dump = {date: {}}

    # Add info for each queue (name)
    for queue_name, queue_stats in query_return.items():
        json_to_dump[date][queue_name] = {"tasks": queue_stats["jobs"], "consumers": queue_stats["consumers"]}

    return json_to_dump


def dump_celery_queue_info(query_return: List[Tuple[str, int, int]], dump_file: str):
    """
    Format and dump Celery queue information to a specified file.

    This function processes the output from the `query_queues` function, formats
    the data according to the file type (CSV or JSON), and adds a timestamp
    to the information before writing it to the specified file.

    Args:
        query_return: The output from the [`query_queues`][router.query_queues]
            function, containing queue names and their associated statistics.
        dump_file: The filepath of the file where the queue information
            will be written. The file extension determines the format (CSV or JSON).
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


def _get_specific_queues(queues: Set[str], specific_queues: List[str], spec: MerlinSpec, verbose: bool = True) -> Set[str]:
    """
    Retrieve a set of specific queues requested by the user, filtering out those that do not exist.

    This function checks a provided list of specific queues against a set of existing queues
    (from a [`MerlinSpec`][spec.specification.MerlinSpec] object) and returns a set of queues
    that are found. If a queue is not found in the existing set, it will be excluded from the
    results. The function also logs messages based on the verbosity setting.

    Args:
        queues: A set of existing queues, which may be empty or populated from the `spec`
            object.
        specific_queues: A list of specific queue names to search for.
        spec (spec.specification.MerlinSpec): A [`MerlinSpec`][spec.specification.MerlinSpec]
            object that may provide context for the search. Can be None.
        verbose: If True, log messages will be displayed.

    Returns:
        A set containing the specific queues that were found in the existing queues.
    """
    if verbose:
        LOG.info(f"Filtering queues to query by these specific queues: {specific_queues}")

    # Add the queue tag from celery if necessary
    celerize_queues(specific_queues)
    # Remove any potential duplicates and create a new set to store the queues we'll want to check
    specific_queues = set(specific_queues)
    specific_queues_to_check = set()

    for specific_queue in specific_queues:
        # If the user provided the --spec flag too then we'll need to check that this
        # specific queue exists in the spec that they provided
        add_specific_queue = True
        if spec and specific_queue not in queues:
            if verbose:
                LOG.warning(
                    f"Either couldn't find {specific_queue} in the existing queues for the spec file provided or "
                    "this queue doesn't go with the steps provided with the --steps option. Ignoring this queue."
                )
            add_specific_queue = False

        # Add the full queue name to the set of queues we'll check
        if add_specific_queue:
            specific_queues_to_check.add(specific_queue)

    return specific_queues_to_check


def build_set_of_queues(
    spec: MerlinSpec,
    steps: List[str],
    specific_queues: List[str],
    verbose: bool = True,
    app: Celery = None,
) -> Set[str]:
    """
    Construct a set of queues to query based on the provided parameters.

    This function builds a set of queues by querying a [`MerlinSpec`][spec.specification.MerlinSpec]
    object for queues associated with specified steps and/or filtering for specific queue names.
    If no spec or specific queues are provided, it defaults to querying active queues from the Celery
    application.

    Args:
        spec (spec.specification.MerlinSpec): A [`MerlinSpec`][spec.specification.MerlinSpec]
            object that defines the context for the query. Can be None.
        steps: A list of step names to query. If empty, all steps are considered.
        specific_queues: A list of specific queue names to filter. Can be None.
        verbose: If True, log statements will be output. Defaults to True.
        app: A Celery application instance. If None, it will be imported.

    Returns:
        A set of queue names to investigate based on the provided parameters.
    """
    if app is None:
        from merlin.celery import app  # pylint: disable=C0415

    queues = set()
    # If the user provided a spec file, get the queues from that spec
    if spec:
        if verbose:
            LOG.info(f"Querying queues for steps = {steps}")
        queues = set(spec.get_queue_list(steps))

    # If the user provided specific queues, search for those
    if specific_queues:
        queues = _get_specific_queues(queues, specific_queues, spec, verbose=verbose)

    # Default behavior with no options provided; display active queues
    if not spec and not specific_queues:
        if verbose:
            LOG.info("Querying active queues")
        existing_queues, _ = get_active_celery_queues(app)

        # Check if there's any active queues currently
        if len(existing_queues) == 0:
            if verbose:
                LOG.warning("No active queues found. Are your workers running yet?")
            return set()

        # Set the queues we're going to check to be all existing queues by default
        queues = set(existing_queues.keys())

    return queues


def query_celery_queues(queues: List[str], app: Celery = None, config: Config = None) -> Dict[str, Dict[str, int]]:
    """
    Retrieve information about the number of jobs and consumers for specified Celery queues.

    This function constructs a dictionary containing details about the number of jobs
    and consumers associated with each queue provided in the input list. It connects
    to the Celery application to gather this information, handling both Redis and
    RabbitMQ brokers.

    Notes:
        - If the specified queue does not exist or has no jobs, it will be handled gracefully.
        - For Redis brokers, the function counts consumers by inspecting active queues
          since Redis does not track consumers like RabbitMQ does.

    Args:
        queues: A list of queue names for which to gather information.
        app: The Celery application instance. Defaults to None, which triggers an import
            for testing purposes.
        config (config.Config): A configuration object containing broker details.
            Defaults to None, which also triggers an import for testing.

    Returns:
        A dictionary where each key is a queue name and the value is another dictionary
            containing:\n
            - `jobs`: The number of jobs in the queue.
            - `consumers`: The number of consumers attached to the queue.
    """
    if app is None:
        from merlin.celery import app  # pylint: disable=C0415
    if config is None:
        from merlin.config.configfile import CONFIG as config  # pylint: disable=C0415

    # Initialize the dictionary with the info we want about our queues
    queue_info = {queue: {"consumers": 0, "jobs": 0} for queue in queues}

    # Open a connection via our Celery app
    with app.connection() as conn:
        # Open a channel inside our connection
        with conn.channel() as channel:
            # Loop through all the queues we're searching for
            for queue in queues:
                try:
                    # Count the number of jobs and consumers for each queue
                    _, queue_info[queue]["jobs"], queue_info[queue]["consumers"] = channel.queue_declare(
                        queue=queue, passive=True
                    )
                # Redis likes to throw this error when a queue we're looking for has no jobs
                except ChannelError:
                    pass

    # Redis doesn't keep track of consumers attached to queues like rabbit does
    # so we have to count this ourselves here
    if config.broker.name in ("rediss", "redis"):
        # Get a dict of active queues by querying the celery app
        active_queues = app.control.inspect().active_queues()
        if active_queues is not None:
            # Loop through each active queue that was found
            for active_queue_list in active_queues.values():
                # Loop through each queue that each worker is watching
                for active_queue in active_queue_list:
                    # If this is a queue we're looking for, increment the consumer count
                    if active_queue["name"] in queues:
                        queue_info[active_queue["name"]]["consumers"] += 1

    return queue_info


def get_workers_from_app() -> List[str]:
    """
    Retrieve a list of all workers connected to the Celery application.

    This function uses the Celery control interface to inspect the current state
    of the application and returns a list of workers that are currently connected.
    If no workers are found, an empty list is returned.

    Returns:
        A list of worker names that are currently connected to the Celery application.
            If no workers are connected, an empty list is returned.
    """
    from merlin.celery import app  # pylint: disable=C0415

    i = app.control.inspect()
    workers = i.ping()
    if workers is None:
        return []
    return [*workers]


def check_celery_workers_processing(queues_in_spec: List[str], app: Celery) -> bool:
    """
    Check if any Celery workers are currently processing tasks from specified queues.

    This function queries the Celery application to determine if there are any active
    tasks being processed by workers for the given list of queues. It returns a boolean
    indicating whether any tasks are currently active.

    Args:
        queues_in_spec: A list of queue names to check for active tasks.
        app: The Celery application instance used for querying.

    Returns:
        True if any workers are processing tasks in the specified queues; False
            otherwise.
    """
    # Query celery for active tasks
    active_tasks = app.control.inspect().active()

    # Search for the queues we provided if necessary
    if active_tasks is not None:
        for tasks in active_tasks.values():
            for task in tasks:
                if task["delivery_info"]["routing_key"] in queues_in_spec:
                    return True

    return False


def _get_workers_to_start(spec: MerlinSpec, steps: List[str]) -> Set[str]:
    """
    Determine the set of workers to start based on the specified steps.

    This helper function retrieves a mapping of steps to their corresponding workers
    from a [`MerlinSpec`][spec.specification.MerlinSpec] object and returns a unique
    set of workers that should be started for the provided list of steps. If a step
    is not found in the mapping, a warning is logged.

    Args:
        spec (spec.specification.MerlinSpec): An instance of the
            [`MerlinSpec`][spec.specification.MerlinSpec] class that contains the
            mapping of steps to workers.
        steps: A list of steps for which workers need to be started.

    Returns:
        A set of unique workers to be started based on the specified steps.
    """
    workers_to_start = []
    step_worker_map = spec.get_step_worker_map()
    for step in steps:
        try:
            workers_to_start.extend(step_worker_map[step])
        except KeyError:
            LOG.warning(f"Cannot start workers for step: {step}. This step was not found.")

    workers_to_start = set(workers_to_start)
    LOG.debug(f"workers_to_start: {workers_to_start}")

    return workers_to_start


def _create_kwargs(spec: MerlinSpec) -> Tuple[Dict[str, str], Dict]:
    """
    Construct the keyword arguments for launching a worker process.

    This helper function creates a dictionary of keyword arguments that will be
    passed to `subprocess.Popen` when launching a worker. It retrieves the
    environment variables defined in a [`MerlinSpec`][spec.specification.MerlinSpec]
    object and updates the shell environment accordingly.

    Args:
        spec (spec.specification.MerlinSpec): An instance of the MerlinSpec class
            that contains environment specifications.

    Returns:
        A tuple containing:
            - A dictionary of keyword arguments for `subprocess.Popen`, including
              the updated environment.
            - A dictionary of variables defined in the spec, or None if no variables
              were defined.
    """
    # Get the environment from the spec and the shell
    spec_env = spec.environment
    shell_env = os.environ.copy()
    yaml_vars = None

    # If the environment from the spec has anything in it,
    # read in the variables and save them to the shell environment
    if spec_env:
        yaml_vars = get_yaml_var(spec_env, "variables", {})
        for var_name, var_val in yaml_vars.items():
            shell_env[str(var_name)] = str(var_val)
            # For expandvars
            os.environ[str(var_name)] = str(var_val)

    # Create the kwargs dict
    kwargs = {"env": shell_env, "shell": True, "universal_newlines": True}
    return kwargs, yaml_vars


def _get_steps_to_start(wsteps: List[str], steps: List[str], steps_provided: bool) -> List[str]:
    """
    Identify the steps for which workers should be started.

    This function determines which steps to initiate based on the steps
    associated with a worker and the user-provided steps. If specific steps
    are provided by the user, only those steps that match the worker's steps
    will be included. If no specific steps are provided, all worker-associated
    steps will be returned.

    Args:
        wsteps: A list of steps that are associated with a worker.
        steps: A list of steps specified by the user to start workers for.
        steps_provided: A boolean indicating whether the user provided
            specific steps to start.

    Returns:
        A list of steps for which workers should be started.
    """
    steps_to_start = []
    if steps_provided:
        for wstep in wsteps:
            if wstep in steps:
                steps_to_start.append(wstep)
    else:
        steps_to_start.extend(wsteps)

    return steps_to_start


def start_celery_workers(
    spec: MerlinSpec, steps: List[str], celery_args: str, disable_logs: bool, just_return_command: bool
) -> str:  # pylint: disable=R0914,R0915
    """
    Start Celery workers based on the provided specifications and steps.

    This function initializes and starts Celery workers for the specified steps
    in the given [`MerlinSpec`][spec.specification.MerlinSpec]. It constructs
    the necessary command-line arguments and handles the launching of subprocesses
    for each worker. If the `just_return_command` flag is set to `True`, it will
    return the command(s) to start the workers without actually launching them.

    Args:
        spec (spec.specification.MerlinSpec): A [`MerlinSpec`][spec.specification.MerlinSpec]
            object representing the study configuration.
        steps: A list of steps for which to start workers.
        celery_args: A string of additional arguments to pass to the Celery workers.
        disable_logs: A flag to disable logging for the Celery workers.
        just_return_command: If `True`, returns the launch command(s) without starting the workers.

    Returns:
        A string containing all the worker launch commands.

    Side Effects:
        - Starts subprocesses for each worker that is launched, so long as `just_return_command`
          is not True.

    Example:
        Below is an example configuration for Merlin workers:

        ```yaml
        merlin:
          resources:
            task_server: celery
            overlap: False
            workers:
                simworkers:
                    args: -O fair --prefetch-multiplier 1 -E -l info --concurrency 4
                    steps: [run, data]
                    nodes: 1
                    machine: [hostA, hostB]
        ```
    """
    if not just_return_command:
        LOG.info("Starting workers")

    overlap = spec.merlin["resources"]["overlap"]
    workers = spec.merlin["resources"]["workers"]

    # Build kwargs dict for subprocess.Popen to use when we launch the worker
    kwargs, yenv = _create_kwargs(spec)

    worker_list = []
    local_queues = []

    # Get the workers we need to start if we're only starting certain steps
    steps_provided = False if "all" in steps else True  # pylint: disable=R1719
    if steps_provided:
        workers_to_start = _get_workers_to_start(spec, steps)

    for worker_name, worker_val in workers.items():
        # Only triggered if --steps flag provided
        if steps_provided and worker_name not in workers_to_start:
            continue

        skip_loop_step: bool = examine_and_log_machines(worker_val, yenv)
        if skip_loop_step:
            continue

        worker_args = get_yaml_var(worker_val, "args", celery_args)
        with suppress(KeyError):
            if worker_val["args"] is None:
                worker_args = ""

        worker_nodes = get_yaml_var(worker_val, "nodes", None)
        worker_batch = get_yaml_var(worker_val, "batch", None)

        # Get the correct steps to start workers for
        wsteps = get_yaml_var(worker_val, "steps", steps)
        steps_to_start = _get_steps_to_start(wsteps, steps, steps_provided)
        queues = spec.make_queue_string(steps_to_start)

        # Check for missing arguments
        worker_args = verify_args(spec, worker_args, worker_name, overlap, disable_logs=disable_logs)

        # Add a per worker log file (debug)
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("Redirecting worker output to individual log files")
            worker_args += " --logfile %p.%i"

        # Get the celery command & add it to the batch launch command
        celery_com = get_celery_cmd(queues, worker_args=worker_args, just_return_command=True)
        celery_cmd = os.path.expandvars(celery_com)
        worker_cmd = batch_worker_launch(spec, celery_cmd, nodes=worker_nodes, batch=worker_batch)
        worker_cmd = os.path.expandvars(worker_cmd)

        LOG.debug(f"worker cmd={worker_cmd}")

        if just_return_command:
            worker_list = ""
            print(worker_cmd)
            continue

        # Get the running queues
        running_queues = []
        running_queues.extend(local_queues)
        queues = queues.split(",")
        if not overlap:
            running_queues.extend(get_running_queues("merlin"))
            # Cache the queues from this worker to use to test
            # for existing queues in any subsequent workers.
            # If overlap is True, then do not check the local queues.
            # This will allow multiple workers to pull from the same
            # queue.
            local_queues.extend(queues)

        # Search for already existing queues and log a warning if we try to start one that already exists
        found = []
        for q in queues:  # pylint: disable=C0103
            if q in running_queues:
                found.append(q)
        if found:
            LOG.warning(
                f"A celery worker named '{worker_name}' is already configured/running for queue(s) = {' '.join(found)}"
            )
            continue

        # Start the worker
        launch_celery_worker(worker_cmd, worker_list, kwargs)

    # Return a string with the worker commands for logging
    return str(worker_list)


def examine_and_log_machines(worker_val: Dict, yenv: Dict[str, str]) -> bool:
    """
    Determine if a worker should be skipped based on machine availability and log any errors.

    This function checks the specified machines for a worker and determines
    whether the worker can be started. If the machines are not available,
    it logs an error message regarding the output path for the Celery worker.
    If the environment variables (`yenv`) are not provided or do not specify
    an output path, a warning is logged.

    Args:
        worker_val: A dictionary containing worker configuration, including
            the list of machines associated with the worker.
        yenv: A dictionary of environment variables that may include the
            output path for logging.

    Returns:
        Returns `True` if the worker should be skipped (i.e., machines are
            unavailable), otherwise returns `False`.
    """
    worker_machines = get_yaml_var(worker_val, "machines", None)
    if worker_machines:
        LOG.debug(f"check machines = {check_machines(worker_machines)}")
        if not check_machines(worker_machines):
            return True

        if yenv:
            output_path = get_yaml_var(yenv, "OUTPUT_PATH", None)
            if output_path and not os.path.exists(output_path):
                hostname = socket.gethostname()
                LOG.error(f"The output path, {output_path}, is not accessible on this host, {hostname}")
        else:
            LOG.warning(
                "The env:variables section does not have an OUTPUT_PATH specified, multi-machine checks cannot be performed."
            )
        return False
    return False


def verify_args(spec: MerlinSpec, worker_args: str, worker_name: str, overlap: bool, disable_logs: bool = False) -> str:
    """
    Validate and enhance the arguments passed to a Celery worker for completeness.

    This function checks the provided worker arguments to ensure that they include
    recommended settings for running parallel tasks. It adds default values for
    concurrency, prefetch multiplier, and logging level if they are not specified.
    Additionally, it generates a unique worker name based on the current time if
    the `-n` argument is not provided.

    Args:
        spec (spec.specification.MerlinSpec): A [`MerlinSpec`][spec.specification.MerlinSpec]
            object containing the study configuration.
        worker_args: A string of arguments passed to the worker that may need validation.
        worker_name: The name of the worker, used for generating a unique worker identifier.
        overlap: A flag indicating whether multiple workers can overlap in their queue processing.
        disable_logs: A flag to disable logging configuration for the worker.

    Returns:
        The validated and potentially modified worker arguments string.
    """
    parallel = batch_check_parallel(spec)
    if parallel:
        if "--concurrency" not in worker_args:
            LOG.warning("The worker arg --concurrency [1-4] is recommended when running parallel tasks")
        if "--prefetch-multiplier" not in worker_args:
            LOG.warning("The worker arg --prefetch-multiplier 1 is recommended when running parallel tasks")
        if "fair" not in worker_args:
            LOG.warning("The worker arg -O fair is recommended when running parallel tasks")

    if "-n" not in worker_args:
        nhash = ""
        if overlap:
            nhash = time.strftime("%Y%m%d-%H%M%S")
        # TODO: Once flux fixes their bug, change this back to %h
        # %h in Celery is short for hostname including domain name
        worker_args += f" -n {worker_name}{nhash}.%%h"

    if not disable_logs and "-l" not in worker_args:
        worker_args += f" -l {logging.getLevelName(LOG.getEffectiveLevel())}"

    return worker_args


def launch_celery_worker(worker_cmd: str, worker_list: List[str], kwargs: Dict):
    """
    Launch a Celery worker using the specified command and parameters.

    This function executes the provided Celery command to start a worker as a
    subprocess. It appends the command to the given list of worker commands
    for tracking purposes. If the worker fails to start, an error is logged.

    Args:
        worker_cmd: The command string used to launch the Celery worker.
        worker_list: A list that will be updated to include the launched
            worker command for tracking active workers.
        kwargs: A dictionary of additional keyword arguments to pass to
            `subprocess.Popen`, allowing for customization of the subprocess
            behavior.

    Raises:
        Exception: If the worker fails to start, an error is logged, and the
            exception is re-raised.

    Side Effects:
        - Launches a Celery worker process in the background.
        - Modifies the `worker_list` by appending the launched worker command.
    """
    try:
        subprocess.Popen(worker_cmd, **kwargs)  # pylint: disable=R1732
        worker_list.append(worker_cmd)
    except Exception as e:  # pylint: disable=C0103
        LOG.error(f"Cannot start celery workers, {e}")
        raise


def get_celery_cmd(queue_names: str, worker_args: str = "", just_return_command: bool = False) -> str:
    """
    Construct the command to launch Celery workers for the specified queues.

    This function generates a command string that can be used to start Celery
    workers associated with the provided queue names. It allows for optional
    worker arguments to be included and can return the command without executing it.

    Args:
        queue_names: A comma-separated string of the queue name(s) to which the worker
            will be associated.
        worker_args: Additional command-line arguments for the Celery worker.
        just_return_command: If True, the function will return the constructed command
            without executing it.

    Returns:
        The constructed command string for launching the Celery worker. If
            `just_return_command` is True, returns the command; otherwise, returns an
            empty string.
    """
    worker_command = " ".join(["celery -A merlin worker", worker_args, "-Q", queue_names])
    if just_return_command:
        return worker_command
    # If we get down here, this only runs celery locally the user would need to
    # add all of the flux config themselves.
    return ""


def purge_celery_tasks(queues: str, force: bool) -> int:
    """
    Purge Celery tasks from the specified queues.

    This function constructs and executes a command to purge tasks from the
    specified Celery queues. If the `force` parameter is set to True, the
    purge operation will be executed without prompting for confirmation.

    Args:
        queues: A comma-separated string of the queue name(s) from which
            tasks should be purged.
        force: If True, the purge operation will be executed without asking
            for user confirmation.

    Returns:
        The return code from the subprocess execution. A return code of
            0 indicates success, while any non-zero value indicates an error
            occurred during the purge operation.
    """
    # This version will purge all queues.
    # from merlin.celery import app
    # app.control.purge()
    force_com = ""
    if force:
        force_com = " -f "
    purge_command = " ".join(["celery -A merlin purge", force_com, "-Q", queues])
    LOG.debug(purge_command)
    return subprocess.run(purge_command, shell=True).returncode


def stop_celery_workers(
    queues: List[str] = None, spec_worker_names: List[str] = None, worker_regex: List[str] = None
):  # pylint: disable=R0912
    """
    Send a stop command to Celery workers.

    This function sends a shutdown command to Celery workers associated with
    specified queues. By default, it stops all connected workers, but it can
    be configured to target specific workers based on queue names or regular
    expression patterns.

    Args:
        queues: A list of queue names to which the stop command will be sent.
            If None, all connected workers across all queues will be stopped.
        spec_worker_names: A list of specific worker names to stop, in addition
            to those matching the `worker_regex`.
        worker_regex: A regular expression string used to match worker names.
            If None, no regex filtering will be applied.

    Side Effects:
        - Broadcasts a shutdown signal to Celery workers

    Example:
        ```python
        stop_celery_workers(queues=['hello'], worker_regex='celery@*my_machine*')
        stop_celery_workers()
        ```
    """
    from merlin.celery import app  # pylint: disable=C0415

    LOG.debug(f"Sending stop to queues: {queues}, worker_regex: {worker_regex}, spec_worker_names: {spec_worker_names}")
    active_queues, _ = get_active_celery_queues(app)

    # If not specified, get all the queues
    if queues is None:
        queues = [*active_queues]
    # Celery adds the queue tag in front of each queue so we add that here
    else:
        celerize_queues(queues)

    # Find the set of all workers attached to all of those queues
    all_workers = set()
    for queue in queues:
        try:
            all_workers.update(active_queues[queue])
            LOG.debug(f"Workers attached to queue {queue}: {active_queues[queue]}")
        except KeyError:
            LOG.warning(f"No workers are connected to queue {queue}")

    all_workers = list(all_workers)

    LOG.debug(f"Pre-filter worker stop list: {all_workers}")

    # Stop workers with no flags
    if (spec_worker_names is None or len(spec_worker_names) == 0) and worker_regex is None:
        workers_to_stop = list(all_workers)
    # Flag handling
    else:
        workers_to_stop = []
        # --spec flag
        if (spec_worker_names is not None) and len(spec_worker_names) > 0:
            apply_list_of_regex(spec_worker_names, all_workers, workers_to_stop)
        # --workers flag
        if worker_regex is not None:
            LOG.debug(f"Searching for workers to stop based on the following regex's: {worker_regex}")
            apply_list_of_regex(worker_regex, all_workers, workers_to_stop)

    # Remove duplicates
    workers_to_stop = list(set(workers_to_stop))
    LOG.debug(f"Post-filter worker stop list: {workers_to_stop}")

    if workers_to_stop:
        LOG.info(f"Sending stop to these workers: {workers_to_stop}")
        # Send the shutdown signal
        app.control.broadcast("shutdown", destination=workers_to_stop)
    else:
        LOG.warning("No workers found to stop")
