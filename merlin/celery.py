##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""Updated celery configuration."""
from __future__ import absolute_import, print_function

import logging
import os
from typing import Any, Dict, List, Optional, Union

import billiard
import celery
import psutil
from celery import Celery, states
from celery.backends.redis import RedisBackend  # noqa: F401 ; Needed for celery patch
from celery.signals import celeryd_init, worker_process_init, worker_shutdown

import merlin.common.security.encrypt_backend_traffic
from merlin.common.enums import WorkerStatus
from merlin.config import broker, celeryconfig, results_backend
from merlin.config.configfile import CONFIG
from merlin.config.utils import Priority, get_priority
from merlin.db_scripts.merlin_db import MerlinDatabase
from merlin.utils import nested_namespace_to_dicts


LOG: logging.Logger = logging.getLogger(__name__)


def patch_celery():
    """
    Patch redis backend so that errors in chords don't break workflows.
    Celery has error callbacks but they do not work properly on chords that
    are nested within chains.

    Credit to this function goes to
    [the following post](https://danidee10.github.io/2019/07/09/celery-chords.html).
    """

    def _unpack_chord_result(
        self,
        tup,
        decode,
        EXCEPTION_STATES=states.EXCEPTION_STATES,
        PROPAGATE_STATES=states.PROPAGATE_STATES,
    ):
        _, tid, state, retval = decode(tup)

        if state in EXCEPTION_STATES:
            retval = self.exception_to_python(retval)
        if state in PROPAGATE_STATES:
            # retval is an Exception
            retval = f"{retval.__class__.__name__}: {str(retval)}"

        return retval

    celery.backends.redis.RedisBackend._unpack_chord_result = _unpack_chord_result

    return celery


# This function has to have specific args/return values for celery so ignore pylint
def route_for_task(
    name: str,
    args: List[Any],
    kwargs: Dict[Any, Any],
    options: Dict[Any, Any],
    task: celery.Task = None,
    **kw: Dict[Any, Any],
) -> Dict[Any, Any]:  # pylint: disable=W0613,R1710
    """
    Custom task router for Celery queues.

    This function routes tasks to specific queues based on the task name.
    If the task name contains a colon, it splits the name to determine the queue.

    Args:
        name: The name of the task being routed.
        args: The positional arguments passed to the task.
        kwargs: The keyword arguments passed to the task.
        options: Additional options for the task.
        task: The task instance (default is None).
        **kw: Additional keyword arguments for THIS function (not the task).

    Returns:
        A dictionary specifying the queue to route the task to.
            If the task name contains a colon, it returns a dictionary with
            the key "queue" set to the queue name. Otherwise, it returns
            an empty dictionary.

    Example:
        Using a colon in the name will return the string before the colon as the queue:

        ```python
        >>> route_for_task("my_queue:my_task")
        {"queue": "my_queue"}
        ```
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
    sanitized_broker_uri = broker.get_connection_string(include_password=False)
    LOG.debug(f"Broker connection string: {sanitized_broker_uri}.")
    BROKER_SSL = broker.get_ssl_config()
    LOG.debug(f"Broker SSL {'enabled' if BROKER_SSL else 'disabled'}.")
    RESULTS_BACKEND_URI = results_backend.get_connection_string()
    sanitized_results_backend_uri = results_backend.get_connection_string(include_password=False)
    LOG.debug(f"Results backend connection string: {sanitized_results_backend_uri}.")
    RESULTS_SSL = results_backend.get_ssl_config(celery_check=True)
    LOG.debug(f"Results backend SSL {'enabled' if RESULTS_SSL else 'disabled'}.")
except ValueError:
    # These variables won't be set if running with '--local'.
    BROKER_URI = None
    RESULTS_BACKEND_URI = None

app_name = "merlin_test_app" if os.getenv("CELERY_ENV") == "test" else "merlin"

# initialize app with essential properties
app: Celery = patch_celery().Celery(
    app_name,
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


@celeryd_init.connect
def handle_worker_startup(sender: str = None, **kwargs):
    """
    Store information about each physical worker instance in the database.

    When workers first start up, the `celeryd_init` signal is the first signal
    that they receive. This specific function will create a
    [`PhysicalWorkerModel`][db_scripts.data_models.PhysicalWorkerModel] and
    store it in the database. It does this through the use of the
    [`MerlinDatabase`][db_scripts.merlin_db.MerlinDatabase] class.

    Args:
        sender (str): The hostname of the worker that was just started
    """
    if sender is not None:
        LOG.debug(f"Worker {sender} has started.")
        options = kwargs.get("options", None)
        if options is not None:
            try:
                # Sender name is of the form celery@worker_name.%hostname
                worker_name, host = sender.split("@")[1].split(".%")
                merlin_db = MerlinDatabase()
                logical_worker = merlin_db.get("logical_worker", worker_name=worker_name, queues=options.get("queues"))
                physical_worker = merlin_db.create(
                    "physical_worker",
                    name=str(sender),
                    host=host,
                    status=WorkerStatus.RUNNING,
                    logical_worker_id=logical_worker.get_id(),
                    pid=os.getpid(),
                )
                logical_worker.add_physical_worker(physical_worker.get_id())
            # Without this exception catcher, celery does not output any errors that happen here
            except Exception as exc:
                LOG.error(f"An error occurred when processing handle_worker_startup: {exc}")
        else:
            LOG.warning("On worker connect could not retrieve worker options from Celery.")
    else:
        LOG.warning("On worker connect no sender was provided from Celery.")


@worker_shutdown.connect
def handle_worker_shutdown(sender: str = None, **kwargs):
    """
    Update the database for a worker entry when a worker shuts down.

    Args:
        sender (str): The hostname of the worker that was just started
    """
    if sender is not None:
        LOG.debug(f"Worker {sender} is shutting down.")
        merlin_db = MerlinDatabase()
        physical_worker = merlin_db.get("physical_worker", str(sender))
        if physical_worker:
            physical_worker.set_status(WorkerStatus.STOPPED)
            physical_worker.set_pid(None)  # Clear the pid
        else:
            LOG.warning(f"Worker {sender} not found in the database.")
    else:
        LOG.warning("On worker shutdown no sender was provided from Celery.")


# Pylint believes the args are unused, I believe they're used after decoration
@worker_process_init.connect()
def setup(**kwargs: Dict[Any, Any]):  # pylint: disable=W0613
    """
    Set affinity for the worker on startup (works on toss3 nodes).

    Args:
        **kwargs: Keyword arguments.
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
