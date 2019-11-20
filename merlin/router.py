###############################################################################
# Copyright (c) 2019, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.0.0.
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
from contextlib import suppress

from merlin.study.celeryadapter import (
    create_celery_config,
    purge_celery_tasks,
    query_celery_workers,
    run_celery,
    start_celery_workers,
    stop_celery_workers,
)
from merlin.templates.generator import setup_template


try:
    import importlib.resources as resources
except ImportError:
    import importlib_resources as resources


LOG = logging.getLogger(__name__)


def run_task_server(study, run_mode=None):
    """
    Creates the task server interface for communicating the tasks.

    :param `study`: The MerlinStudy object
    :param `run_mode`: The type of run mode, e.g. local, batch
    """
    if study.spec.merlin["resources"]["task_server"] == "celery":
        run_celery(study, run_mode)
    else:
        LOG.error("Celery is not specified as the task server!")


def launch_workers(spec, steps, worker_args="", just_return_command=False):
    """
    Launches workers for the specified study.

    :param `specs`: Tuple of (YAMLSpecification, MerlinSpec)
    :param `steps`: The steps in the spec to tie the workers to
    :param `worker_args`: Optional arguments for the workers
    :param `just_return_command`: Don't execute, just return the command
    """
    if spec.merlin["resources"]["task_server"] == "celery":
        # Start workers
        cproc = start_celery_workers(spec, steps, worker_args, just_return_command)
        return cproc
    else:
        LOG.error("Celery is not specified as the task server!")


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

    if task_server == "celery":
        queues = spec.make_queue_string(steps)
        # Purge tasks
        return purge_celery_tasks(queues, force)
    else:
        LOG.error("Celery is not specified as the task server!")


def query_workers(task_server):
    """
    Gets info from workers.

    :param `task_server`: The task server to query.
    """
    LOG.info(f"Searching for workers...")

    if task_server == "celery":
        # Stop workers
        return query_celery_workers()
    else:
        LOG.error("Celery is not specified as the task server!")


def stop_workers(task_server, queues, workers):
    """
    Stops workers.

    :param `task_server`: The task server from which to stop workers.
    :param `queues`     : The queues to stop
    :param `workers`    : Regex for workers to stop
    """
    LOG.info(f"Stopping workers...")

    if task_server == "celery":
        # Stop workers
        return stop_celery_workers(queues, workers)
    else:
        LOG.error("Celery is not specified as the task server!")


def route_for_task(name, args, kwargs, options, task=None, **kw):
    """
    Custom task router for queues
    """
    if ":" in name:
        queue, _ = name.split(":")
        return {"queue": queue}


def create_config(task_server, config_dir):
    """
    Create a config for the given task server.

    :param `task_server`: The task server from which to stop workers.
    :param `config_dir`: Optional directory to install the config.
    """
    LOG.info(f"Creating config ...")

    if not os.path.isdir(config_dir):
        os.makedirs(config_dir)

    if task_server == "celery":
        config_file = "app.yaml"
        with resources.path("merlin.data.celery", config_file) as data_file:
            create_celery_config(config_dir, config_file, data_file)
    else:
        LOG.error("Only celery can be configured currently.")


def templates(template_name, outdir):
    """
    Setup a Merlin template spec.

    :param template_name: Then name of the template to copy into the workspace.
    :param outdir: The directory to copy the template to.
    """
    with suppress(FileExistsError):
        os.makedirs(outdir)

    template = setup_template(template_name, outdir)
