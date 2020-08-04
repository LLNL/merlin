###############################################################################
# Copyright (c) 2019, Lawrence Livermore National Security, LLC.
# Produced at the Lawrence Livermore National Laboratory
# Written by the Merlin dev team, listed in the CONTRIBUTORS file.
# <merlin@llnl.gov>
#
# LLNL-CODE-797170
# All rights reserved.
# This file is part of Merlin, Version: 1.7.2.
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

import billiard
import psutil
from celery import Celery
from celery.signals import worker_process_init

import merlin.common.security.encrypt_backend_traffic
from merlin.config import broker, celeryconfig, results_backend
from merlin.config.configfile import CONFIG
from merlin.router import route_for_task
from merlin.utils import nested_namespace_to_dicts


LOG = logging.getLogger(__name__)

merlin.common.security.encrypt_backend_traffic.set_backend_funcs()

broker_ssl = True
results_ssl = False
try:
    BROKER_URI = broker.get_connection_string()
    LOG.debug(f"broker: {broker.get_connection_string(include_password=False)}")
    broker_ssl = broker.get_ssl_config()
    LOG.debug(f"broker_ssl = {broker_ssl}")
    RESULTS_BACKEND_URI = results_backend.get_connection_string()
    results_ssl = results_backend.get_ssl_config(celery_check=True)
    LOG.debug(
        f"results: {results_backend.get_connection_string(include_password=False)}"
    )
    LOG.debug(f"results: redis_backed_use_ssl = {results_ssl}")
except ValueError:
    # These variables won't be set if running with '--local'.
    BROKER_URI = None
    RESULTS_BACKEND_URI = None

# initialize app with essential properties
app = Celery(
    "merlin",
    broker=BROKER_URI,
    backend=RESULTS_BACKEND_URI,
    broker_use_ssl=broker_ssl,
    redis_backend_use_ssl=results_ssl,
    task_routes=(route_for_task,),
)

# load merlin config defaults
app.conf.update(**celeryconfig.DICT)

# load config overrides from app.yaml
if (
    (not hasattr(CONFIG.celery, "override"))
    or (CONFIG.celery.override is None)
    or (len(nested_namespace_to_dicts(CONFIG.celery.override)) == 0)
):
    LOG.debug("Skipping celery config override; 'celery.override' field is empty.")
else:
    override_dict = nested_namespace_to_dicts(CONFIG.celery.override)
    override_str = ""
    i = 0
    for k, v in override_dict.items():
        if k not in str(app.conf.__dict__):
            raise ValueError(f"'{k}' is not a celery configuration.")
        override_str += f"\t{k}:\t{v}"
        if i != len(override_dict) - 1:
            override_str += "\n"
        i += 1
    LOG.info(
        f"Overriding default celery config with 'celery.override' in 'app.yaml':\n{override_str}"
    )
    app.conf.update(**override_dict)

# auto-discover tasks
app.autodiscover_tasks(["merlin.common"])


@worker_process_init.connect()
def setup(**kwargs):
    """
    Set affinity for the worker on startup (works on toss3 nodes)

    :param `**kwargs`: keyword arguments
    """
    if "CELERY_AFFINITY" in os.environ and int(os.environ["CELERY_AFFINITY"]) > 1:
        # Number of cpus between workers.
        cpu_skip = int(os.environ["CELERY_AFFINITY"])
        npu = psutil.cpu_count()
        p = psutil.Process()
        current = billiard.current_process()
        prefork_id = current._identity[0] - 1  # range 0:nworkers-1
        cpu_slot = (prefork_id * cpu_skip) % npu
        p.cpu_affinity(list(range(cpu_slot, cpu_slot + cpu_skip)))
