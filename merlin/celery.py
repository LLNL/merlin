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

"""Updated celery configuration."""
from __future__ import absolute_import, print_function

import logging
import os
from typing import Dict, Optional, Union

import billiard
import psutil
from celery import Celery
from celery.signals import worker_process_init

import merlin.common.security.encrypt_backend_traffic
from merlin.config import broker, celeryconfig, results_backend
from merlin.config.configfile import CONFIG
from merlin.config.utils import Priority, get_priority
from merlin.utils import nested_namespace_to_dicts


LOG: logging.Logger = logging.getLogger(__name__)


# This function has to have specific args/return values for celery so ignore pylint
def route_for_task(name, args, kwargs, options, task=None, **kw):  # pylint: disable=W0613,R1710
    """
    Custom task router for queues
    """
    if ":" in name:
        queue, _ = name.split(":")
        return {"queue": queue}


merlin.common.security.encrypt_backend_traffic.set_backend_funcs()


BROKER_SSL: bool = True
RESULTS_SSL: bool = False
BROKER_URI: Optional[str] = ""
RESULTS_BACKEND_URI: Optional[str] = ""
try:
    BROKER_URI = broker.get_connection_string()
    LOG.debug("broker: %s", broker.get_connection_string(include_password=False))
    BROKER_SSL = broker.get_ssl_config()
    LOG.debug("broker_ssl = %s", BROKER_SSL)
    RESULTS_BACKEND_URI = results_backend.get_connection_string()
    RESULTS_SSL = results_backend.get_ssl_config(celery_check=True)
    LOG.debug("results: %s", results_backend.get_connection_string(include_password=False))
    LOG.debug("results: redis_backed_use_ssl = %s", RESULTS_SSL)
except ValueError:
    # These variables won't be set if running with '--local'.
    BROKER_URI = None
    RESULTS_BACKEND_URI = None

# initialize app with essential properties
app: Celery = Celery(
    "merlin",
    broker=BROKER_URI,
    backend=RESULTS_BACKEND_URI,
    broker_use_ssl=BROKER_SSL,
    redis_backend_use_ssl=RESULTS_SSL,
    task_routes=(route_for_task,),
)

# set task priority defaults to prioritize workflow tasks over task-expansion tasks
task_priority_defaults: Dict[str, Union[int, Priority]] = {
    "task_queue_max_priority": 10,
    "task_default_priority": get_priority(Priority.MID),
}
if CONFIG.broker.name.lower() == "redis":
    app.conf.broker_transport_options = {
        "priority_steps": list(range(1, 11)),
        "queue_order_strategy": "priority",
    }
app.conf.update(**task_priority_defaults)

# load merlin config defaults
app.conf.update(**celeryconfig.DICT)

# load config overrides from app.yaml
if (
    not hasattr(CONFIG.celery, "override")
    or (CONFIG.celery.override is None)
    or (not nested_namespace_to_dicts(CONFIG.celery.override))  # only true if len == 0
):
    LOG.debug("Skipping celery config override; 'celery.override' field is empty.")
else:
    override_dict: Dict = nested_namespace_to_dicts(CONFIG.celery.override)
    override_str: str = ""
    i: int = 0
    for k, v in override_dict.items():
        if k not in str(app.conf.__dict__):
            raise ValueError(f"'{k}' is not a celery configuration.")
        override_str += f"\t{k}:\t{v}"
        if i != len(override_dict) - 1:
            override_str += "\n"
        i += 1
    LOG.info(
        "Overriding default celery config with 'celery.override' in 'app.yaml':\n%s",
        override_str,
    )
    app.conf.update(**override_dict)

# auto-discover tasks
app.autodiscover_tasks(["merlin.common"])


# Pylint believes the args are unused, I believe they're used after decoration
@worker_process_init.connect()
def setup(**kwargs):  # pylint: disable=W0613
    """
    Set affinity for the worker on startup (works on toss3 nodes)

    :param `**kwargs`: keyword arguments
    """
    if "CELERY_AFFINITY" in os.environ and int(os.environ["CELERY_AFFINITY"]) > 1:
        # Number of cpus between workers.
        cpu_skip: int = int(os.environ["CELERY_AFFINITY"])
        npu: int = psutil.cpu_count()
        process: psutil.Process = psutil.Process()
        # pylint is upset that typing accesses a protected class, ignoring W0212
        # pylint is upset that billiard doesn't have a current_process() method - it does
        current: billiard.process._MainProcess = billiard.current_process()  # pylint: disable=W0212, E1101
        prefork_id: int = current._identity[0] - 1  # pylint: disable=W0212  # range 0:nworkers-1
        cpu_slot: int = (prefork_id * cpu_skip) % npu
        process.cpu_affinity(list(range(cpu_slot, cpu_slot + cpu_skip)))
