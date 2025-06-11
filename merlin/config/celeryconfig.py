##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""This module houses the default Celery configuration settings for Merlin."""

from merlin.log_formatter import FORMATS


DICT = {
    "task_serializer": "pickle",
    "accept_content": ["pickle"],
    "result_serializer": "pickle",
    "task_acks_late": True,
    "task_reject_on_worker_lost": True,
    "task_publish_retry_policy": {
        "interval_start": 10,
        "interval_step": 10,
        "interval_max": 60,
    },
    "redis_max_connections": 100000,
    "broker_transport_options": {
        "visibility_timeout": 60 * 60 * 24,
        "max_connections": 100,
    },
    "broker_pool_limit": 0,
    "task_default_queue": "merlin",
    "worker_log_color": True,
    "worker_log_format": FORMATS["DEFAULT"],
    "worker_task_log_format": FORMATS["WORKER"],
    "worker_cancel_long_running_tasks_on_connection_loss": True,
}
