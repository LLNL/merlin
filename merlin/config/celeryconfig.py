##############################################################################
# Copyright (c) Lawrence Livermore National Security, LLC and other Merlin
# Project developers. See top-level LICENSE and COPYRIGHT files for dates and
# other details. No copyright assignment is required to contribute to Merlin.
##############################################################################

"""This module houses the default Celery configuration settings for Merlin."""

from merlin.log_formatter import FORMATS


DICT = {
    # -------- SERIALIZER SETTINGS --------
    "task_serializer": "pickle",
    "accept_content": ["pickle"],
    "result_serializer": "pickle",
    # ----------- TASK SETTINGS -----------
    "task_acks_late": True,
    "task_reject_on_worker_lost": True,
    "task_publish_retry_policy": {
        "interval_start": 10,
        "interval_step": 10,
        "interval_max": 300,
    },
    "task_default_queue": "merlin",
    # ---------- BROKER SETTINGS ----------
    "broker_transport_options": {
        "visibility_timeout": 60 * 60 * 24,
        "max_connections": 100,
        "socket_timeout": 300,
        "retry_policy": {
            "timeout": 600,
        },
    },
    "broker_connection_timeout": 60,
    "broker_pool_limit": 0,
    # --------- BACKEND SETTINGS ----------
    "result_backend_always_retry": True,
    "result_backend_max_retries": 20,
    # ---------- REDIS SETTINGS -----------
    "redis_max_connections": 100000,
    "redis_retry_on_timeout": True,
    "redis_socket_connect_timeout": 300,
    "redis_socket_timeout": 300,
    "redis_socket_keepalive": True,
    # ---------- WORKER SETTINGS ----------
    "worker_log_color": True,
    "worker_log_format": FORMATS["DEFAULT"],
    "worker_task_log_format": FORMATS["WORKER"],
    "worker_cancel_long_running_tasks_on_connection_loss": True,
}
