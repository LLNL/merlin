###############################################################################
# Copyright (c) 2023, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.11.0.
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
This module provides an adapter to the Celery Distributed Task Queue.
"""
import logging
import os
import socket
import subprocess
import time
from contextlib import suppress

from merlin.study.batch import batch_check_parallel, batch_worker_launch
from merlin.utils import apply_list_of_regex, check_machines, get_procs, get_yaml_var, is_running


LOG = logging.getLogger(__name__)


def run_celery(study, run_mode=None):
    """
    Run the given MerlinStudy object. If the run mode is set to "local"
    configure Celery to run locally (without workers).
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


def get_running_queues():
    """
    Check for running celery workers with -Q queues
    and return a unique list of the queues

    Must be run on the allocation where the workers are running
    """
    running_queues = []

    if not is_running("celery worker"):
        return running_queues

    procs = get_procs("celery")
    for _, lcmd in procs:
        lcmd = list(filter(None, lcmd))
        cmdline = " ".join(lcmd)
        if "-Q" in cmdline:
            running_queues.extend(lcmd[lcmd.index("-Q") + 1].split(","))

    running_queues = list(set(running_queues))

    return running_queues


def get_queues(app):
    """Get all active queues and workers for a celery application.

    Unlike get_running_queues, this goes through the application's server.
    Also returns a dictionary with entries for each worker attached to
    the given queues.

    :param `celery.Celery` app: the celery application

    :return: queues dictionary with connected workers, all workers
    :rtype: (dict of lists of strings, list of strings)

    :example:

    >>> from merlin.celery import app
    >>> queues, workers = get_queues(app)
    >>> queue_names = [*queues]
    >>> workers_on_q0 = queues[queue_names[0]]
    >>> workers_not_on_q0 = [worker for worker in workers
                             if worker not in workers_on_q0]
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


def get_active_workers(app):
    """
    This is the inverse of get_queues() defined above. This function
    builds a dict where the keys are worker names and the values are lists
    of queues attached to the worker.

    :param `app`: The celery application
    :returns: A dict mapping active workers to queues
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


def celerize_queues(queues):
    """
    Celery requires a queue tag to be prepended to their
    queues so this function will 'celerize' every queue in
    a list you provide it by prepending the queue tag.

    :param `queues`: A list of queues that need the queue
                     tag prepended.
    """
    from merlin.config.configfile import CONFIG  # pylint: disable=C0415

    for i, queue in enumerate(queues):
        queues[i] = f"{CONFIG.celery.queue_tag}{queue}"


def _build_output_table(worker_list, output_table):
    """
    Helper function for query-status that will build a table
    that we'll use as output.

    :param `worker_list`: A list of workers to add to the table
    :param `output_table`: A list of tuples where each entry is
                           of the form (worker name, associated queues)
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


def query_celery_workers(spec_worker_names, queues, workers_regex):
    """
    Look for existing celery workers. Filter by spec, queues, or
    worker names if provided by user. At the end, print a table
    of workers and their associated queues.

    :param `spec_worker_names`: The worker names defined in a spec file
    :param `queues`: A list of queues to filter by
    :param `workers_regex`: A list of regexs to filter by
    """
    from merlin.celery import app  # pylint: disable=C0415
    from merlin.display import tabulate_info  # pylint: disable=C0415

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
        queue_worker_map, _ = get_queues(app)
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
    tabulate_info(table, headers=["Workers", "Queues"])
    print()


def query_celery_queues(queues):
    """Return stats for queues specified.

    Send results to the log.
    """
    from merlin.celery import app  # pylint: disable=C0415

    connection = app.connection()
    found_queues = []
    try:
        channel = connection.channel()
        for queue in queues:
            try:
                name, jobs, consumers = channel.queue_declare(queue=queue, passive=True)
                found_queues.append((name, jobs, consumers))
            except Exception as e:  # pylint: disable=C0103,W0718
                LOG.warning(f"Cannot find queue {queue} on server.{e}")
    finally:
        connection.close()
    return found_queues


def get_workers_from_app():
    """Get all workers connected to a celery application.

    :param `celery.Celery` app: the celery application
    :return: A list of all connected workers
    :rtype: list
    """
    from merlin.celery import app  # pylint: disable=C0415

    i = app.control.inspect()
    workers = i.ping()
    if workers is None:
        return []
    return [*workers]


def _get_workers_to_start(spec, steps):
    """
    Helper function to return a set of workers to start based on
    the steps provided by the user.

    :param `spec`: A MerlinSpec object
    :param `steps`: A list of steps to start workers for

    :returns: A set of workers to start
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


def _create_kwargs(spec):
    """
    Helper function to handle creating the kwargs dict that
    we'll pass to subprocess.Popen when we launch the worker.

    :param `spec`: A MerlinSpec object
    :returns: A tuple where the first entry is the kwargs and
              the second entry is variables defined in the spec
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


def _get_steps_to_start(wsteps, steps, steps_provided):
    """
    Determine which steps to start workers for.

    :param `wsteps`: A list of steps associated with a worker
    :param `steps`: A list of steps to start provided by the user
    :param `steps`: A bool representing whether the user gave specific
                    steps to start or not
    :returns: A list of steps to start workers for
    """
    steps_to_start = []
    if steps_provided:
        for wstep in wsteps:
            if wstep in steps:
                steps_to_start.append(wstep)
    else:
        steps_to_start.extend(wsteps)

    return steps_to_start


def start_celery_workers(spec, steps, celery_args, disable_logs, just_return_command):  # pylint: disable=R0914,R0915
    """Start the celery workers on the allocation

    :param MerlinSpec spec:             A MerlinSpec object representing our study
    :param list steps:                  A list of steps to start workers for
    :param str celery_args:             A string of arguments to provide to the celery workers
    :param bool disable_logs:           A boolean flag to turn off the celery logs for the workers
    :param bool just_return_command:    When True, workers aren't started and just the launch command(s)
                                        are returned
    :side effect:                       Starts subprocesses for each worker we launch
    :returns:                           A string of all the worker launch commands
    ...

    example config:

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
            running_queues.extend(get_running_queues())
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


def examine_and_log_machines(worker_val, yenv) -> bool:
    """
    Examines whether a worker should be skipped in a step of start_celery_workers(), logs errors in output path for a celery
    worker.
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


def verify_args(spec, worker_args, worker_name, overlap, disable_logs=False):
    """Examines the args passed to a worker for completeness."""
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


def launch_celery_worker(worker_cmd, worker_list, kwargs):
    """
    Using the worker launch command provided, launch a celery worker.
    :param str worker_cmd:      The celery command to launch a worker
    :param list worker_list:    A list of worker launch commands
    :param dict kwargs:         A dictionary containing additional keyword args to provide
                                to subprocess.Popen

    :side effect:               Launches a celery worker via a subprocess
    """
    try:
        _ = subprocess.Popen(worker_cmd, **kwargs)  # pylint: disable=R1732
        worker_list.append(worker_cmd)
    except Exception as e:  # pylint: disable=C0103
        LOG.error(f"Cannot start celery workers, {e}")
        raise


def get_celery_cmd(queue_names, worker_args="", just_return_command=False):
    """
    Get the appropriate command to launch celery workers for the specified MerlinStudy.
    queue_names         The name(s) of the queue(s) to associate a worker with
    worker_args         Optional celery arguments for the workers
    just_return_command Don't execute, just return the command
    """
    worker_command = " ".join(["celery -A merlin worker", worker_args, "-Q", queue_names])
    if just_return_command:
        return worker_command
    # If we get down here, this only runs celery locally the user would need to
    # add all of the flux config themselves.
    return ""


def purge_celery_tasks(queues, force):
    """
    Purge celery tasks for the specified spec file.

    queues              Which queues to purge
    force               Purge without asking for confirmation
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


def stop_celery_workers(queues=None, spec_worker_names=None, worker_regex=None):  # pylint: disable=R0912
    """Send a stop command to celery workers.

    Default behavior is to stop all connected workers.
    As options can downselect to only workers on certain queues and/or that
    match a regular expression.

    :param list queues: The queues to send stop signals to. If None: stop all
    :param list spec_worker_names: Worker names read from a spec to stop, in addition to worker_regex matches.
    :param str worker_regex: The regex string to match worker names. If None:
    :return: Return code from stop command

    :example:

    >>> stop_celery_workers(queues=['hello'], worker_regex='celery@*my_machine*')

    >>> stop_celery_workers()

    """
    from merlin.celery import app  # pylint: disable=C0415

    LOG.debug(f"Sending stop to queues: {queues}, worker_regex: {worker_regex}, spec_worker_names: {spec_worker_names}")
    active_queues, _ = get_queues(app)

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
        app.control.broadcast("shutdown", destination=workers_to_stop)
    else:
        LOG.warning("No workers found to stop")


def create_celery_config(config_dir, data_file_name, data_file_path):
    """
    Command to setup default celery merlin config.

    :param `config_dir`: The directory to create the config file.
    :param `data_file_name`: The name of the config file.
    :param `data_file_path`: The full data file path.
    """
    # This will need to come from the server interface
    MERLIN_CONFIG = os.path.join(config_dir, data_file_name)  # pylint: disable=C0103

    if os.path.isfile(MERLIN_CONFIG):
        from merlin.common.security import encrypt  # pylint: disable=C0415

        encrypt.init_key()
        LOG.info(f"The config file already exists, {MERLIN_CONFIG}")
        return

    with open(MERLIN_CONFIG, "w") as outfile, open(data_file_path, "r") as infile:
        outfile.write(infile.read())

    if not os.path.isfile(MERLIN_CONFIG):
        LOG.error(f"Cannot create config file {MERLIN_CONFIG}")

    LOG.info(f"The file {MERLIN_CONFIG} is ready to be edited for your system.")

    from merlin.common.security import encrypt  # pylint: disable=C0415

    encrypt.init_key()
