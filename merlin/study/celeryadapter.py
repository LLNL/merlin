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

"""
This module provides an adapter to the Celery Distributed Task Queue.
"""
import logging
import os
import socket
import subprocess
import time
from contextlib import suppress

from merlin.config.configfile import CONFIG
from merlin.study.batch import batch_check_parallel, batch_worker_launch
from merlin.utils import check_machines, get_procs, get_yaml_var, is_running, regex_list_filter


LOG = logging.getLogger(__name__)


def run_celery(study, run_mode=None):
    """
    Run the given MerlinStudy object. If the run mode is set to "local"
    configure Celery to run locally (without workers).
    """
    # Only import celery stuff if we want celery in charge
    from merlin.celery import app
    from merlin.common.tasks import queue_merlin_study

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


def query_celery_workers():
    """Look for existing celery workers.

    Send results to the log.
    """
    workers = get_workers_from_app()
    if workers:
        LOG.info("Found these connected workers:")
        for worker in workers:
            LOG.info(worker)
    else:
        LOG.warning("No workers found!")


def query_celery_queues(queues):
    """Return stats for queues specified.

    Send results to the log.
    """
    from merlin.celery import app

    found_queues = []
    with app.connection() as conn:
        with conn.channel() as channel:
            for queue in queues:
                try:
                    name, jobs, consumers = channel.queue_declare(queue=queue, passive=True)
                    found_queues.append((name, jobs, consumers))
                except Exception as e:
                    LOG.warning(f"Cannot find queue {queue} on server.{e}")
    return found_queues


def get_workers_from_app():
    """Get all workers connected to a celery application.

    :param `celery.Celery` app: the celery application
    :return: A list of all connected workers
    :rtype: list
    """
    from merlin.celery import app

    i = app.control.inspect()
    workers = i.ping()
    if workers is None:
        return []
    return [*workers]


def start_celery_workers(spec, steps, celery_args, disable_logs, just_return_command):
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
    study_name = spec.description["name"]

    # Build kwargs dict for subprocess.Popen to use when we launch the worker
    senv = spec.environment
    spenv = os.environ.copy()
    yenv = None
    if senv:
        yenv = get_yaml_var(senv, "variables", {})
        for k, v in yenv.items():
            spenv[str(k)] = str(v)
            # For expandvars
            os.environ[str(k)] = str(v)
    kwargs = {"env": spenv, "shell": True, "universal_newlines": True}

    worker_list = []
    local_queues = []

    #############################################
    # Start a worker to track the status of tasks
    #############################################
    status_worker_args = ""
    parallel = batch_check_parallel(spec)
    if parallel:
        status_worker_args = "--concurrency 1 --prefetch-multiplier 1 -O fair"

    if not disable_logs and LOG.isEnabledFor(logging.DEBUG):
        LOG.debug("Redirecting worker output to individual log files")
        status_worker_args += " --logfile %p.%i"
    
    status_worker_args = verify_args(spec, status_worker_args, f"{study_name}_status_worker", False, disable_logs=disable_logs)

    status_queue = f"{CONFIG.celery.queue_tag}{study_name}_status_queue"
    # Append this whether overlap is True or not since we don't want any other workers pulling from this queue
    local_queues.append(status_queue)

    # Get the celery worker launch command & append it to the batch launch
    celery_status_cmd = get_celery_cmd(spec, queue_names=status_queue, worker_args=status_worker_args, just_return_command=True)
    worker_status_cmd = os.path.expandvars(batch_worker_launch(spec, celery_status_cmd))

    LOG.debug(f"worker_status_cmd: {worker_status_cmd}")

    if just_return_command:
        print(worker_status_cmd)
    else:
        # Start the status worker
        launch_celery_worker(worker_status_cmd, worker_list, kwargs)

    # Get the workers we need to start if we're only starting certain steps
    steps_provided = False if "all" in steps else True
    if steps_provided:
        workers_to_start = []
        step_worker_map = spec.get_step_worker_map()
        for step in steps:
            try:
                workers_to_start.extend(step_worker_map[step])
            except KeyError:
                LOG.warning(f"Cannot start workers for step: {step}. This step was not found.")
    
        workers_to_start = set(workers_to_start)
        LOG.debug(f"workers_to_start: {workers_to_start}")

    ##################################################################
    # Start the workers defined in the merlin section of the yaml file
    ##################################################################
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
        steps_to_start = []
        if steps_provided:
            for step in wsteps:
                if step in steps:
                    steps_to_start.append(step)
        else:
            steps_to_start.extend(wsteps)
        queues = spec.make_queue_string(steps_to_start)

        # Check for missing arguments
        worker_args = verify_args(spec, worker_args, worker_name, overlap, disable_logs=disable_logs)

        # Add a per worker log file (debug)
        if LOG.isEnabledFor(logging.DEBUG):
            LOG.debug("Redirecting worker output to individual log files")
            worker_args += " --logfile %p.%i"

        # Get the celery command & add it to the batch launch command
        celery_com = get_celery_cmd(spec, queues, worker_args=worker_args, just_return_command=True)
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
        for q in queues:
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
        LOG.debug("check machines = ", check_machines(worker_machines))
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
    else:
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
        _ = subprocess.Popen(worker_cmd, **kwargs)
        worker_list.append(worker_cmd)
    except Exception as e:
        LOG.error(f"Cannot start celery workers, {e}")
        raise


def get_celery_cmd(spec, queue_names, worker_args="", just_return_command=False):
    """
    Get the appropriate command to launch celery workers for the specified MerlinStudy.

    spec                MerlinSpec object
    queue_names         The name(s) of the queue(s) to associate a worker with
    worker_args         Optional celery arguments for the workers
    just_return_command Don't execute, just return the command
    """
    worker_command = " ".join(["celery -A merlin worker", worker_args, "-Q", queue_names])
    if just_return_command:
        return worker_command
    else:
        # This only runs celery locally the user would need to
        # add all of the flux config themselves.
        pass


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


def stop_celery_workers(queues=None, spec_worker_names=None, worker_regex=None):
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
    from merlin.celery import app

    LOG.debug(f"Sending stop to queues: {queues}, worker_regex: {worker_regex}, spec_worker_names: {spec_worker_names}")
    active_queues, _ = get_queues(app)

    # If not specified, get all the queues
    if queues is None or len(queues) == 0:
        queues = [*active_queues]

    # Find the set of all workers attached to all of those queues
    all_workers = set()
    for queue in queues:
        try:
            all_workers.update(active_queues[queue])
            LOG.debug(f"Workers attached to queue {queue}: {active_queues[queue]}")
        except KeyError:
            LOG.warning(f"No workers are connected to queue {queue}")

    all_workers = list(all_workers)
    LOG.debug(f"all_workers: {all_workers}")

    if (spec_worker_names is None or len(spec_worker_names) == 0) and worker_regex is None:
        workers_to_stop = list(all_workers)
    else:
        workers_to_stop = []
        if (spec_worker_names is not None) and len(spec_worker_names) > 0:
            for worker_name in spec_worker_names:
                LOG.debug(f"Result of regex_list_filter for {worker_name}: {regex_list_filter(worker_name, all_workers, match=False)}")
                workers_to_stop += regex_list_filter(worker_name, all_workers, match=False)
        if worker_regex is not None:
            LOG.debug(f"Result of regex_list_filter: {regex_list_filter(worker_regex, all_workers, match=False)}")
            workers_to_stop += regex_list_filter(worker_regex, all_workers, match=False)

    LOG.debug(f"workers_to_stop: {workers_to_stop}")
    if workers_to_stop:
        LOG.info(f"Sending stop to these workers: {workers_to_stop}")
        return app.control.broadcast("shutdown", destination=workers_to_stop)
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
    MERLIN_CONFIG = os.path.join(config_dir, data_file_name)

    if os.path.isfile(MERLIN_CONFIG):
        from merlin.common.security import encrypt

        encrypt.init_key()
        LOG.info(f"The config file already exists, {MERLIN_CONFIG}")
        return

    with open(MERLIN_CONFIG, "w") as outfile, open(data_file_path, "r") as infile:
        outfile.write(infile.read())

    if not os.path.isfile(MERLIN_CONFIG):
        LOG.error(f"Cannot create config file {MERLIN_CONFIG}")

    LOG.info(f"The file {MERLIN_CONFIG} is ready to be edited for your system.")

    from merlin.common.security import encrypt

    encrypt.init_key()
